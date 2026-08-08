use crate::delta::PageInfo;
use crate::digest::StateDigest;
use crate::node::Node;
use crate::types::{Entry, NodeId};
use anyhow::{bail, Context, Result};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// v1 wire format — frozen for the duration of the mixed-version window.
//
// These structs are shared with wavekv 1.x. Adding, removing or reordering a field
// breaks every v1 peer, so they must not change until the RFC 0001 Phase 2 gate ("all
// nodes on v2 for N days") is met.
//
// The library does not encode them: the transport is the embedder's, and so is the
// choice of codec. Field *order* only matters under a positional encoder (`rmp_serde`'s
// default `to_vec`); an embedder on a named encoder (`to_vec_named`, JSON) is bound by
// field *names* instead. Either way the freeze applies — just to a different property
// than the type declaration suggests.
// ---------------------------------------------------------------------------

/// Bidirectional sync: sender includes their local_ack AND their new entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncMessage {
    /// The unique numeric identifier of the sender node
    pub sender_id: NodeId,
    /// Optional sender's UUID. This may be used to detect node id duplication
    pub sender_uuid: Vec<u8>,
    /// How far the sender has synced each node's logs (local_ack)
    pub sender_ack: HashMap<NodeId, u64>,
    /// Sender's new log entries (incremental or full dump)
    pub entries: Vec<Entry>,
}

/// Unified log exchange response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    pub peer_id: NodeId,
    pub entries: Vec<Entry>,
    pub progress: HashMap<NodeId, u64>, // Responder's local_ack for each node
    pub is_snapshot: bool,              // Indicates if this is a full KV->log conversion
}

// ---------------------------------------------------------------------------
// v2 wire format
// ---------------------------------------------------------------------------

/// Envelope schema version. Bumped only for changes that a v2 peer cannot decode;
/// additive fields ride the named-map encoding instead (see [`SyncEnvelope::encode`]).
pub const ENVELOPE_VERSION: u16 = 1;

/// The single, symmetric v2 message. Request and response are structurally identical:
/// each side ships the delta the other is missing plus the coverage it claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEnvelope {
    pub version: u16,
    pub sender_id: NodeId,
    pub sender_uuid: Vec<u8>,
    /// The sender's `acks`, read from the same state snapshot as `entries` (rule R4).
    #[serde(default)]
    pub acks: HashMap<NodeId, u64>,
    /// Live entries filtered against the receiver's coverage.
    #[serde(default)]
    pub entries: Vec<Entry>,
    /// Digest of the sender's full data map, for divergence detection.
    #[serde(default)]
    pub digest: Option<StateDigest>,
    /// Present when the delta was truncated. `None` means a single complete delta.
    #[serde(default)]
    pub page: Option<PageInfo>,
    /// Where the responder should resume its delta scan from.
    #[serde(default)]
    pub resume_from: Option<(NodeId, u64)>,
    /// Ask the peer to forget its cached view of our coverage and resend in full.
    /// Set when repeated digest mismatches indicate silent divergence.
    #[serde(default)]
    pub reset_acks: bool,
    /// Rule R3: this envelope was delivered opportunistically and carries no
    /// complete-delta guarantee, so the receiver merges data but must not move acks.
    #[serde(default)]
    pub push_only: bool,
}

impl SyncEnvelope {
    pub fn new(sender_id: NodeId, sender_uuid: Vec<u8>) -> Self {
        Self {
            version: ENVELOPE_VERSION,
            sender_id,
            sender_uuid,
            acks: HashMap::new(),
            entries: Vec::new(),
            digest: None,
            page: None,
            resume_from: None,
            reset_acks: false,
            push_only: false,
        }
    }

    /// Whether the receiver may adopt `acks` after merging (rules R2 and R3).
    pub fn permits_ack_adoption(&self) -> bool {
        !self.push_only && self.page.as_ref().is_none_or(|page| page.last)
    }

    /// Encode as a MessagePack **map keyed by field name**.
    ///
    /// Named encoding (rather than the positional form the v1 structs above use) is
    /// what makes the envelope additively evolvable: a peer built against an older
    /// schema skips fields it does not know and defaults those it does not receive,
    /// so a new optional field does not require a version bump or a flag day.
    pub fn encode(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec_named(self).context("failed to encode sync envelope")
    }

    /// Decode, rejecting an unknown major schema and any trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        let mut de = rmp_serde::Deserializer::new(&mut cursor);
        let env = Self::deserialize(&mut de).context("failed to decode sync envelope")?;
        drop(de);
        if cursor.position() != bytes.len() as u64 {
            bail!(
                "trailing bytes after sync envelope: {}",
                bytes.len() as u64 - cursor.position()
            );
        }
        if env.version > ENVELOPE_VERSION {
            bail!(
                "unsupported sync envelope version: understand up to {}, got {}",
                ENVELOPE_VERSION,
                env.version
            );
        }
        Ok(env)
    }
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

/// Which protocol a peer was last observed to speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerProtocol {
    V1,
    V2,
}

pub trait ExchangeInterface: Send + Sync + 'static {
    fn uuid(&self) -> Vec<u8> {
        Vec::new()
    }
    fn query_uuid(&self, _node_id: NodeId) -> Option<Vec<u8>> {
        None
    }

    /// v1 wire exchange. Required: it is the fallback for any peer that has not been
    /// upgraded yet.
    fn sync_to(
        &self,
        node: &Node,
        peer: NodeId,
        msg: SyncMessage,
    ) -> impl Future<Output = Result<SyncResponse>> + Send;

    /// v2 wire exchange.
    ///
    /// Return `Ok(None)` when the peer does not expose the v2 route (HTTP 404/405 or
    /// equivalent) so the manager records the peer as v1 and falls back. Return `Err`
    /// for transport failures, which are retried without changing the cached protocol.
    fn sync_v2_to(
        &self,
        _node: &Node,
        _peer: NodeId,
        _env: SyncEnvelope,
    ) -> impl Future<Output = Result<Option<SyncEnvelope>>> + Send {
        async { Ok(None) }
    }

    /// Opportunistic push (RFC 3.9). Best-effort: failures are logged and dropped,
    /// because the periodic round remains the anti-entropy backstop.
    fn push_to(
        &self,
        _node: &Node,
        _peer: NodeId,
        _env: SyncEnvelope,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

/// Configuration for sync manager
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Interval between sync attempts
    pub interval: Duration,
    /// Timeout for each sync request
    pub timeout: Duration,
    /// How long to keep treating a peer as v1-only before re-probing the v2 route.
    /// Without this an upgraded peer would stay on the v1 path forever.
    pub protocol_reprobe: Duration,
    /// Coalescing window for opportunistic push. `None` disables push.
    pub coalesce_window: Option<Duration>,
    /// Consecutive rounds with empty deltas but mismatched digests before declaring
    /// divergence and forcing a full re-exchange.
    pub digest_check_rounds: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(10),
            protocol_reprobe: Duration::from_secs(300),
            coalesce_window: Some(Duration::from_millis(200)),
            digest_check_rounds: 3,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PeerLink {
    protocol: PeerProtocol,
    probed_at: Option<Instant>,
    digest_mismatches: u32,
    /// Rounds that have failed back to back. A transport failure deliberately does not
    /// change `protocol` (only a definitive 404/405 does), so without this counter a
    /// peer that fails every single round is indistinguishable from a healthy one.
    consecutive_failures: u32,
}

impl Default for PeerLink {
    fn default() -> Self {
        Self {
            // Optimistic: probe v2 first, demote on a definitive "no such route".
            protocol: PeerProtocol::V2,
            probed_at: None,
            digest_mismatches: 0,
            consecutive_failures: 0,
        }
    }
}

/// Per-peer sync telemetry surfaced to embedders (RFC 3.11).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerLinkStatus {
    pub id: NodeId,
    pub protocol: &'static str,
    pub digest_mismatches: u32,
    /// Rounds failed back to back; `0` once a round succeeds. A peer stuck on a 5xx
    /// keeps `protocol` unchanged by design, so this is the only field that moves.
    pub consecutive_failures: u32,
}

/// Dual-stack sync manager: speaks v2 natively and v1 for peers that have not been
/// upgraded, with per-peer capability caching and periodic re-probing.
pub struct SyncManager<Net> {
    store: Node,
    app: Net,
    config: SyncConfig,
    links: Mutex<HashMap<NodeId, PeerLink>>,
    push_signal: Notify,
}

impl<Net: ExchangeInterface + Clone> SyncManager<Net> {
    pub fn new(store: Node, network: Net) -> Self {
        Self::with_config(store, network, SyncConfig::default())
    }

    pub fn with_config(store: Node, network: Net, config: SyncConfig) -> Self {
        Self {
            store,
            app: network,
            config,
            links: Mutex::new(HashMap::new()),
            push_signal: Notify::new(),
        }
    }

    fn link(&self, peer: NodeId) -> PeerLink {
        #[allow(clippy::expect_used)]
        let links = self.links.lock().expect("lock should never fail");
        links.get(&peer).copied().unwrap_or_default()
    }

    fn update_link(&self, peer: NodeId, f: impl FnOnce(&mut PeerLink)) {
        #[allow(clippy::expect_used)]
        let mut links = self.links.lock().expect("lock should never fail");
        f(links.entry(peer).or_default());
    }

    /// Per-peer protocol and digest telemetry.
    /// Telemetry for **every** known peer, not just the ones the cache happens to hold.
    ///
    /// The cache is written on demotion and on a completed round, so reporting it
    /// directly would omit exactly the peers an operator most needs to see: one that has
    /// never been reached at all, and one whose every round fails without a demotion.
    /// Peers absent from the cache report the optimistic default they are actually
    /// being treated as.
    pub fn link_status(&self) -> Vec<PeerLinkStatus> {
        let peers = self.store.read().get_peers();
        #[allow(clippy::expect_used)]
        let links = self.links.lock().expect("lock should never fail");
        let mut out: Vec<_> = peers
            .into_iter()
            .map(|id| {
                let link = links.get(&id).copied().unwrap_or_default();
                PeerLinkStatus {
                    id,
                    protocol: match link.protocol {
                        PeerProtocol::V1 => "v1",
                        PeerProtocol::V2 => "v2",
                    },
                    digest_mismatches: link.digest_mismatches,
                    consecutive_failures: link.consecutive_failures,
                }
            })
            .collect();
        out.sort_by_key(|s| s.id);
        out
    }

    /// Whether the v2 route should be attempted for this peer right now.
    fn should_try_v2(&self, peer: NodeId) -> bool {
        let link = self.link(peer);
        match link.protocol {
            PeerProtocol::V2 => true,
            PeerProtocol::V1 => link
                .probed_at
                .is_none_or(|at| at.elapsed() >= self.config.protocol_reprobe),
        }
    }

    /// Bootstrap: sync from all peers and recover `next_seq` before serving writes.
    /// Critical after data loss, to avoid reusing sequence numbers.
    pub async fn bootstrap(&self) -> Result<()> {
        let peers = self.store.read().get_peers();
        let results = self.sync_to_all_peers().await;
        let mut success_count = 0;
        for (peer, result) in results {
            match result {
                Ok(_) => {
                    success_count += 1;
                    info!("successfully bootstrapped from peer {peer}");
                }
                Err(err) => {
                    warn!("failed to bootstrap from peer {peer}: {err:?}");
                }
            }
        }

        // One range scan over origin_index replaces v1's walk of every log bucket.
        self.store.write().recover_next_seq();

        if success_count == 0 && !peers.is_empty() {
            warn!("bootstrap: failed to sync from any peer, proceeding anyway");
        } else {
            info!(
                "bootstrap: successfully synced from {}/{} peers",
                success_count,
                peers.len()
            );
        }

        Ok(())
    }

    /// Start the periodic exchange task, plus the push task when enabled.
    pub async fn start_sync_tasks(self: Arc<Self>) {
        let periodic = self.clone();
        tokio::spawn(async move {
            periodic.periodic_exchange().await;
        });

        if self.config.coalesce_window.is_some() {
            let pusher = self.clone();
            tokio::spawn(async move {
                pusher.push_loop().await;
            });
        }
    }

    /// Signal that local writes occurred and should be pushed after the coalescing
    /// window. Cheap and lossy by design: a missed signal costs at most one sync
    /// interval of extra latency.
    pub fn notify_local_write(&self) {
        if self.config.coalesce_window.is_some() {
            self.push_signal.notify_one();
        }
    }

    async fn push_loop(&self) {
        let Some(window) = self.config.coalesce_window else {
            return;
        };
        let mut ticker = interval(window);
        loop {
            // Either a local write signalled us or the window elapsed. Ticking as well
            // as waiting on the signal means a missed notification costs latency, not
            // correctness — and the loop still drains if the embedder never signals.
            tokio::select! {
                _ = ticker.tick() => {}
                _ = self.push_signal.notified() => {}
            }

            let Some(mut env) = self.store.write().take_push_envelope() else {
                continue;
            };
            // `NodeState` has no access to the network, so the identity is stamped here,
            // exactly as `sync_v1` and `sync_v2` do. Omitting it makes every push fail
            // `check_uuid` on any embedder that implements `query_uuid`.
            env.sender_uuid = self.app.uuid();

            let peers = self.store.read().get_peers();
            let futures: Vec<_> = peers
                .iter()
                .map(|&peer| {
                    let env = env.clone();
                    async move {
                        if !self.should_try_v2(peer) {
                            // v1 peers have no opportunistic channel; they will pull.
                            return;
                        }
                        if let Err(err) = self.app.push_to(&self.store, peer, env).await {
                            debug!("opportunistic push to {peer} failed (harmless): {err}");
                        }
                    }
                })
                .collect();
            join_all(futures).await;
        }
    }

    async fn periodic_exchange(&self) {
        let mut ticker = interval(self.config.interval);

        loop {
            ticker.tick().await;

            let results = self.sync_to_all_peers().await;
            for (peer, result) in results {
                match result {
                    Ok(_) => debug!("successfully synced with peer {peer}"),
                    Err(e) => warn!("failed to sync with peer {peer}: {e:?}"),
                }
            }
        }
    }

    /// Handle an inbound v1 message. Delegates to the compatibility shim.
    #[tracing::instrument(skip(self, msg), fields(from = msg.sender_id))]
    pub fn handle_sync(&self, msg: SyncMessage) -> Result<SyncResponse> {
        self.check_uuid(msg.sender_id, &msg.sender_uuid)?;
        self.store.write().handle_sync_v1(msg)
    }

    /// Handle an inbound v2 envelope and produce the response envelope.
    #[tracing::instrument(skip(self, env), fields(from = env.sender_id))]
    pub fn handle_envelope(&self, env: SyncEnvelope) -> Result<SyncEnvelope> {
        self.check_uuid(env.sender_id, &env.sender_uuid)?;
        let uuid = self.app.uuid();
        self.store.write().handle_envelope(env, uuid)
    }

    /// Handle an inbound opportunistic push. Merges data only (rule R3).
    #[tracing::instrument(skip(self, env), fields(from = env.sender_id))]
    pub fn handle_push(&self, env: SyncEnvelope) -> Result<()> {
        self.check_uuid(env.sender_id, &env.sender_uuid)?;
        self.store.write().merge_push(env)
    }

    /// Reject a peer presenting an identity that disagrees with the one we have on
    /// record for its node id.
    ///
    /// Deliberately applied to inbound *requests* only, never to responses. A node id
    /// is configuration while the uuid is derived from local state, so a node whose
    /// data directory is recreated comes back with the same id and a new uuid — which,
    /// from here, is indistinguishable from two machines sharing an id. Recovery
    /// depends on the peer's fresh identity record reaching us, and it can only arrive
    /// in a response, because our request already fails the peer's own inbound check.
    /// Checking responses as well closes the last path and the pair never converges
    /// again. Divergence that slips through is caught by the digest instead.
    fn check_uuid(&self, peer_id: NodeId, presented: &[u8]) -> Result<()> {
        if let Some(expected_uuid) = self.app.query_uuid(peer_id) {
            if expected_uuid != presented {
                warn!(
                    "UUID mismatch for peer {peer_id}: expected {}, got {}",
                    hex::encode(&expected_uuid),
                    hex::encode(presented)
                );
                bail!("UUID mismatch for peer {peer_id}. Don't reuse node IDs for peers.");
            }
        }
        Ok(())
    }

    async fn sync_to_all_peers(&self) -> Vec<(NodeId, Result<()>)> {
        let peers = self.store.read().get_peers();

        if peers.is_empty() {
            debug!("no peers configured, nothing to sync");
            return vec![];
        }

        let sync_futures: Vec<_> = peers
            .iter()
            .map(|&peer| async move {
                let result = self.sync_to(peer).await;
                self.update_link(peer, |link| {
                    link.consecutive_failures = if result.is_ok() {
                        0
                    } else {
                        link.consecutive_failures.saturating_add(1)
                    };
                });
                (peer, result)
            })
            .collect();

        join_all(sync_futures).await
    }

    /// One exchange with `peer`, preferring v2 and falling back to v1.
    #[tracing::instrument(skip(self))]
    async fn sync_to(&self, peer: NodeId) -> Result<()> {
        if self.should_try_v2(peer) {
            match self.sync_v2(peer).await {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    // Definitive "no v2 route": record and fall through to v1.
                    info!("peer {peer} does not speak v2 yet, falling back to v1");
                    self.update_link(peer, |link| {
                        link.protocol = PeerProtocol::V1;
                        link.probed_at = Some(Instant::now());
                    });
                }
                Err(err) => return Err(err),
            }
        }
        self.sync_v1(peer).await
    }

    /// Returns `Ok(false)` when the peer has no v2 route.
    async fn sync_v2(&self, peer: NodeId) -> Result<bool> {
        let timeout = self.config.timeout;
        let uuid = self.app.uuid();

        // R4: entries, acks and digest all come from one guard.
        let request = {
            let state = self.store.read();
            state.prepare_sync(peer, uuid)
        };
        let request_acks = request.acks.clone();

        let result = tokio::time::timeout(timeout, self.app.sync_v2_to(&self.store, peer, request))
            .await
            .map_err(|_| anyhow::anyhow!("sync request timed out after {timeout:?}"))?;

        let Some(response) = result? else {
            return Ok(false);
        };
        self.update_link(peer, |link| {
            link.protocol = PeerProtocol::V2;
            link.probed_at = Some(Instant::now());
        });

        let outcome = self.store.write().apply_envelope(response)?;

        // Divergence detection: only meaningful once both sides have nothing to send.
        let quiescent = outcome.merged == 0 && outcome.peer_delta_empty;
        if quiescent {
            match outcome.digest_match {
                Some(true) => self.update_link(peer, |link| link.digest_mismatches = 0),
                Some(false) => {
                    let mismatches = {
                        self.update_link(peer, |link| link.digest_mismatches += 1);
                        self.link(peer).digest_mismatches
                    };
                    if mismatches >= self.config.digest_check_rounds {
                        error!(
                            peer,
                            mismatches,
                            "state digests differ after {mismatches} quiescent rounds; \
                             forcing a full re-exchange"
                        );
                        // Safe by construction: the data map is never truncated, so
                        // lowering acks can only cause retransmission.
                        self.store.write().reset_peer_coverage(peer);
                        self.update_link(peer, |link| link.digest_mismatches = 0);
                    }
                }
                None => {}
            }
        }

        // Drain remaining pages before declaring the round done.
        let mut resume = outcome.resume_from;
        while let Some(cursor) = resume {
            let mut request = {
                let state = self.store.read();
                state.prepare_sync(peer, self.app.uuid())
            };
            request.resume_from = Some(cursor);
            request.acks = request_acks.clone();
            let Some(response) =
                tokio::time::timeout(timeout, self.app.sync_v2_to(&self.store, peer, request))
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!("paged sync request timed out after {timeout:?}")
                    })??
            else {
                break;
            };
            resume = self.store.write().apply_envelope(response)?.resume_from;
        }

        Ok(true)
    }

    async fn sync_v1(&self, peer: NodeId) -> Result<()> {
        let timeout = self.config.timeout;

        // RFC 8.2.1: push nothing. A v2 node's own live seq space legitimately contains
        // holes (own writes superseded by others), and v1's gap check drops the whole
        // batch when a hole lands at the front. The v1 peer pulls our data through its
        // own round against our shim instead, costing at most one extra interval.
        let msg = {
            let state = self.store.read();
            SyncMessage {
                sender_id: state.id,
                sender_uuid: self.app.uuid(),
                sender_ack: state.acks_snapshot(),
                entries: Vec::new(),
            }
        };

        let result = tokio::time::timeout(timeout, self.app.sync_to(&self.store, peer, msg))
            .await
            .map_err(|_| anyhow::anyhow!("sync request timed out after {timeout:?}"))?;

        match result {
            Ok(response) => self.store.write().apply_v1_response(response),
            Err(e) => {
                warn!("v1 exchange with peer {peer} failed: {e}");
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Metadata;

    fn sample() -> SyncEnvelope {
        let mut env = SyncEnvelope::new(7, b"uuid".to_vec());
        env.acks.insert(1, 42);
        env.entries.push(Entry::new(
            "k".into(),
            Some(b"v".to_vec()),
            Metadata::new(1, 42, 1234),
        ));
        env.digest = Some(StateDigest { hash: [9u8; 32] });
        env
    }

    #[test]
    fn envelopes_round_trip() {
        let env = sample();
        let decoded = SyncEnvelope::decode(&env.encode().unwrap()).unwrap();
        assert_eq!(decoded.sender_id, 7);
        assert_eq!(decoded.acks.get(&1), Some(&42));
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.digest.unwrap().hash, [9u8; 32]);
    }

    #[test]
    fn envelopes_are_named_maps_so_fields_can_be_added() {
        let encoded = sample().encode().unwrap();
        assert!(
            matches!(encoded.first().copied(), Some(0x80..=0x8f | 0xde | 0xdf)),
            "named-map encoding is what makes the envelope additively evolvable"
        );
    }

    #[test]
    fn a_future_schema_version_is_refused() {
        let mut env = sample();
        env.version = ENVELOPE_VERSION + 1;
        let err = SyncEnvelope::decode(&env.encode().unwrap()).unwrap_err();
        assert!(err
            .to_string()
            .contains("unsupported sync envelope version"));
    }

    #[test]
    fn trailing_bytes_are_refused() {
        let mut encoded = sample().encode().unwrap();
        encoded.push(0xff);
        assert!(SyncEnvelope::decode(&encoded).is_err());
    }

    #[test]
    fn ack_adoption_follows_r2_and_r3() {
        let mut env = sample();
        assert!(
            env.permits_ack_adoption(),
            "an unpaged sync envelope may be adopted"
        );

        env.page = Some(PageInfo {
            cursor: (1, 5),
            last: false,
        });
        assert!(!env.permits_ack_adoption(), "R2: not mid-pagination");

        env.page = Some(PageInfo {
            cursor: (1, 5),
            last: true,
        });
        assert!(
            env.permits_ack_adoption(),
            "R2: the final page may be adopted"
        );

        env.push_only = true;
        assert!(!env.permits_ack_adoption(), "R3: pushes never move acks");
    }
}
