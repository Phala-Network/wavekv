use crate::admission::{Admission, NodeConfig};
use crate::delta::compute_delta;
use crate::digest::StateDigest;
use crate::ops::{CoreState, StateOp};
use crate::sync::{SyncEnvelope, SyncMessage, SyncResponse};
use crate::types::{compare_entries, Entry, Metadata, NodeId};
use crate::wal::WriteAheadLog;
use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use fs_err::{self as fs, File, OpenOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::watch;
use tracing::{debug, error, info, trace, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: NodeId,
    pub n_kvs: usize,
    pub next_seq: u64,
    pub dirty: bool,
    pub wal: bool,
    pub digest: String,
    pub entries_merged: u64,
    pub entries_rejected: u64,
    pub peers: Vec<PeerStatus>,
}

/// Per-peer view. Replaces v1's `{ack, pack, logs}`, which could not distinguish a
/// healthy replica from a permanently diverged one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerStatus {
    pub id: NodeId,
    /// How far we cover this origin's writes.
    pub ack: u64,
    /// How far this peer told us it covers our writes.
    pub peer_ack: u64,
    /// Whether this peer has ever reported an ack map.
    pub heard_from: bool,
}

/// What a merge round did, and whether the round may move acks.
#[derive(Debug, Clone, Default)]
pub struct MergeOutcome {
    /// Entries that changed local state.
    pub merged: usize,
    /// Entries refused by limits or the admission policy.
    pub rejected: usize,
    /// True when the peer's delta was empty — one half of the quiescence test.
    pub peer_delta_empty: bool,
    /// `Some(true)`/`Some(false)` when both sides supplied a digest.
    pub digest_match: Option<bool>,
    /// Set when the peer paginated and more pages remain.
    pub resume_from: Option<(NodeId, u64)>,
    /// Whether acks were adopted this round (false if R1/R2/R3 blocked it).
    pub acks_adopted: bool,
    /// Whether *this envelope's* batch merged without a refusal. A paginated round must
    /// carry this across pages: R1 is a property of the whole delta, not of one page.
    pub complete: bool,
}

/// Core mutable state - all protected by a single RwLock for consistency
pub struct NodeState {
    pub id: NodeId,

    /// Core state: data + origin index + acks + next_seq
    core: CoreState,

    /// WAL for durability - included in state to ensure atomic updates
    wal: Option<WriteAheadLog>,

    /// Optional snapshot path for full state persistence
    snapshot_path: Option<PathBuf>,

    /// Unified watchers for both exact keys and prefixes
    watchers: Vec<Watcher>,

    /// Tracks whether state has unpersisted changes
    dirty: bool,

    config: NodeConfig,

    /// Keys written locally since the last opportunistic push (RFC 3.9).
    pending_push: Vec<String>,

    entries_merged: u64,
    entries_rejected: u64,
    /// Set while this node is bootstrapping, which suspends adoption of a *requester's*
    /// ack map. See [`NodeState::begin_bootstrap`].
    bootstrap_pending: bool,
}

enum WatchPattern {
    Exact(String),
    Prefix(String),
}

struct Watcher {
    pattern: WatchPattern,
    sender: watch::Sender<()>,
}

/// On-disk container. Deliberately unchanged from v1 — same magic, same version, same
/// body shape — so that a node can be rolled back to the v1 binary at any point during
/// the migration (RFC 8.3). `CoreState`'s own `Serialize` impl does the projection.
#[derive(Debug, Serialize, Deserialize)]
struct SnapshotFile {
    magic: [u8; 4],
    version: u32,
    node_id: NodeId,
    core: CoreState,
}

impl SnapshotFile {
    const VERSION: u32 = 1;
    const MAGIC: [u8; 4] = *b"WVKV";

    fn from_state(state: &NodeState) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            node_id: state.id,
            core: state.core.clone(),
        }
    }

    fn validate(&self, expected_node: NodeId) -> Result<()> {
        if self.magic != Self::MAGIC {
            bail!("invalid snapshot magic header");
        }
        if self.version != Self::VERSION {
            bail!(
                "unsupported snapshot version: expected {}, found {}",
                Self::VERSION,
                self.version
            );
        }
        if self.node_id != expected_node {
            bail!(
                "snapshot node_id mismatch: expected {}, found {}",
                expected_node,
                self.node_id
            );
        }
        Ok(())
    }
}

/// Thread-safe handle to a WaveKV node.
#[derive(Clone)]
pub struct Node {
    state: Arc<RwLock<NodeState>>,
}

impl NodeState {
    fn snapshot_path(&self) -> Result<&Path> {
        self.snapshot_path
            .as_deref()
            .ok_or_else(|| anyhow!("snapshot path not configured"))
    }

    fn load_snapshot_if_exists(&mut self) -> Result<bool> {
        let Some(path) = self.snapshot_path.clone() else {
            return Ok(false);
        };
        let backup = path.with_extension("snapshot.bak");

        for candidate in [&path, &backup] {
            if !candidate.exists() {
                continue;
            }
            match Self::read_snapshot(candidate, self.id) {
                Ok(core) => {
                    self.core = core;
                    self.core.set_id(self.id);
                    self.dirty = false;
                    if candidate == &backup {
                        warn!("primary snapshot unusable; recovered from {backup:?}");
                    }
                    return Ok(true);
                }
                Err(err) => {
                    // WaveKV state is fully replicated, so local damage is a
                    // quarantine-and-resync event, never a refusal to start.
                    warn!("failed to load snapshot {candidate:?}: {err:#}");
                }
            }
        }
        if path.exists() || backup.exists() {
            // A snapshot was on disk and neither generation could be read. Recovery is
            // automatic only while a peer is reachable; a node that starts empty and
            // alone serves empty state, so this is louder than the per-file warning.
            error!("no usable snapshot found; starting empty and re-syncing from peers");
        }
        Ok(false)
    }

    fn read_snapshot(path: &Path, expected_node: NodeId) -> Result<CoreState> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        let snapshot: SnapshotFile =
            rmp_serde::from_slice(&buf).context("failed to deserialize snapshot")?;
        snapshot.validate(expected_node)?;
        Ok(snapshot.core)
    }

    pub fn persist_to_disk(&mut self) -> Result<()> {
        let snapshot_path = self.snapshot_path()?.to_path_buf();
        if let Some(parent) = snapshot_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp_path = snapshot_path.with_extension("snapshot.tmp");
        let snapshot = SnapshotFile::from_state(self);
        let encoded = rmp_serde::to_vec(&snapshot)?;

        {
            let mut writer = BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp_path)?,
            );
            writer.write_all(&encoded)?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        // Keep one previous generation as a fallback (RFC 3.10).
        if snapshot_path.exists() {
            let backup = snapshot_path.with_extension("snapshot.bak");
            let _ = fs::rename(&snapshot_path, &backup);
        }
        fs::rename(&tmp_path, &snapshot_path)?;

        if let Some(parent) = snapshot_path.parent() {
            if let Ok(dir_file) = File::open(parent) {
                let _ = dir_file.sync_all();
            }
        }

        if let Some(wal) = self.wal.as_mut() {
            wal.reset()?;
        }

        debug!("persisted snapshot to {:?}", snapshot_path);
        self.dirty = false;
        Ok(())
    }

    fn notify_watchers(&mut self, key: &str) {
        self.watchers.retain(|watcher| {
            let matches = match &watcher.pattern {
                WatchPattern::Exact(watch_key) => watch_key == key,
                WatchPattern::Prefix(prefix) => key.starts_with(prefix),
            };
            if matches {
                watcher.sender.send(()).is_ok()
            } else {
                true
            }
        });
    }

    pub fn watch_key(&mut self, key: &str) -> watch::Receiver<()> {
        let (sender, receiver) = watch::channel(());
        self.watchers.push(Watcher {
            pattern: WatchPattern::Exact(key.to_string()),
            sender,
        });
        receiver
    }

    pub fn watch_prefix(&mut self, prefix: &str) -> watch::Receiver<()> {
        let (sender, receiver) = watch::channel(());
        self.watchers.push(Watcher {
            pattern: WatchPattern::Prefix(prefix.to_string()),
            sender,
        });
        receiver
    }

    /// Execute state operations, persisting the durable ones.
    ///
    /// v2's WAL carries only `Set`/`Clear`; ack bookkeeping is volatile by design, so
    /// an idle sync round no longer costs an fsync.
    fn execute_ops(&mut self, ops: Vec<StateOp>) -> Result<()> {
        self.execute_ops_impl(ops, true)
    }

    fn execute_ops_impl(&mut self, ops: Vec<StateOp>, write_to_wal: bool) -> Result<()> {
        let ops: Vec<_> = ops
            .into_iter()
            .filter(|op| {
                if self.core.is_noop(op) {
                    trace!("skipping noop op: {op:?}");
                    false
                } else {
                    true
                }
            })
            .collect();

        if ops.is_empty() {
            return Ok(());
        }

        if write_to_wal {
            if let Some(wal) = self.wal.as_mut() {
                let durable: Vec<StateOp> =
                    ops.iter().filter(|op| op.is_durable()).cloned().collect();
                if !durable.is_empty() {
                    wal.write_ops(&durable)?;
                }
            }
        }

        for op in ops {
            self.execute_op(op);
        }
        self.mark_dirty();
        Ok(())
    }

    fn execute_op(&mut self, op: StateOp) {
        let changed_key = match &op {
            StateOp::Set(entry) => Some(entry.key.clone()),
            _ => None,
        };
        self.core.execute(op);
        if let Some(key) = changed_key {
            self.notify_watchers(&key);
        }
    }

    fn replay_ops(&mut self, ops: Vec<StateOp>) -> Result<()> {
        self.execute_ops_impl(ops, false)
    }

    // -----------------------------------------------------------------------
    // Merge
    // -----------------------------------------------------------------------

    /// Admission checks applied to every entry arriving from a peer (RFC 3.8).
    fn admit(&self, entry: &Entry, now_ms: i64) -> Admission {
        if let Admission::Reject { reason } = self.config.limits.check_entry(entry) {
            return Admission::Reject { reason };
        }
        if let Admission::Reject { reason } = self.config.limits.check_clock(entry, now_ms) {
            return Admission::Reject { reason };
        }
        // Capacity is only consulted for keys that would be newly created.
        if !self.core.data().contains_key(&entry.key) {
            let bytes: usize = self
                .core
                .data()
                .values()
                .map(|e| e.key.len() + e.value.as_ref().map_or(0, |v| v.len()))
                .sum();
            if let Admission::Reject { reason } = self
                .config
                .limits
                .check_capacity(self.core.data().len(), bytes)
            {
                return Admission::Reject { reason };
            }
        }
        if let Some(policy) = &self.config.admission {
            if let Admission::Reject { reason } = policy.admit(entry) {
                return Admission::Reject { reason };
            }
        }
        Admission::Accept
    }

    /// Merge one entry under LWW. Returns whether local state changed.
    pub fn sync(&mut self, entry: Entry) -> Result<bool> {
        let now = Utc::now().timestamp_millis();
        match self.admit(&entry, now) {
            Admission::Accept => {}
            Admission::Reject { reason } => {
                self.entries_rejected += 1;
                bail!("entry {} rejected: {reason}", entry.key);
            }
        }

        let should_update = match self.core.data().get(&entry.key) {
            Some(existing) => compare_entries(existing, &entry) == std::cmp::Ordering::Less,
            None => true,
        };

        if should_update {
            self.execute_ops(vec![StateOp::Set(entry)])
                .context("failed to apply merged entry")?;
            self.entries_merged += 1;
        }
        Ok(should_update)
    }

    /// Merge a batch. Rule R1: a single failure blocks ack adoption for the whole
    /// round. Merging is idempotent, so the retry next round costs nothing.
    fn merge_batch(&mut self, entries: Vec<Entry>) -> (usize, usize, bool) {
        let mut merged = 0usize;
        let mut rejected = 0usize;
        let mut complete = true;
        for entry in entries {
            let key = entry.key.clone();
            match self.sync(entry) {
                Ok(true) => merged += 1,
                Ok(false) => {}
                Err(err) => {
                    rejected += 1;
                    complete = false;
                    warn!("refusing entry {key}: {err:#}");
                }
            }
        }
        (merged, rejected, complete)
    }

    // -----------------------------------------------------------------------
    // v2 protocol
    // -----------------------------------------------------------------------

    /// Build the request envelope for `peer` (rule R4: one consistent snapshot).
    pub fn prepare_sync(&self, peer: NodeId, uuid: Vec<u8>) -> SyncEnvelope {
        let peer_view = self.core.peer_acks_for(peer).cloned().unwrap_or_default();
        let delta = compute_delta(
            self.core.data(),
            self.core.origin_index(),
            &peer_view,
            None,
            self.config.max_delta_entries,
            self.config.max_delta_bytes,
        );

        let mut env = SyncEnvelope::new(self.id, uuid);
        env.acks = self.core.acks().clone();
        env.entries = delta.entries;
        env.page = delta.page;
        // Deliberately no digest. The responder never reads one off a request, so
        // sending it buys nothing and hands any responder the answer: echo it back and
        // `digest_match` is `Some(true)` every round, pinning the divergence counter at
        // zero for as long as it likes. Comparison happens on the initiator, against the
        // digest the *responder* volunteers.
        env
    }

    /// Drain the pending local writes into an opportunistic push envelope
    /// (rule R3: entries only, no ack authority). Returns `None` when nothing is
    /// pending, so the push loop can idle without allocating.
    ///
    /// The envelope is returned with an empty `sender_uuid`: this type has no access to
    /// the [`ExchangeInterface`](crate::sync::ExchangeInterface). The caller must stamp
    /// it before sending, or the receiver's `check_uuid` will reject the push.
    pub fn take_push_envelope(&mut self) -> Option<SyncEnvelope> {
        let keys = std::mem::take(&mut self.pending_push);
        if keys.is_empty() {
            return None;
        }
        let mut env = SyncEnvelope::new(self.id, Vec::new());
        env.push_only = true;
        env.entries = keys
            .iter()
            .filter_map(|key| self.core.data().get(key).cloned())
            .collect();
        (!env.entries.is_empty()).then_some(env)
    }

    /// Merge an opportunistically pushed envelope. Data only — never acks (R3).
    pub fn merge_push(&mut self, env: SyncEnvelope) -> Result<()> {
        if !env.push_only {
            debug!(
                "envelope from {} delivered on the push channel without push_only; \
                 merging data only",
                env.sender_id
            );
        }
        self.merge_batch(env.entries);
        Ok(())
    }

    /// Handle an inbound request envelope and produce the response (responder side).
    pub fn handle_envelope(&mut self, env: SyncEnvelope, uuid: Vec<u8>) -> Result<SyncEnvelope> {
        let peer = env.sender_id;
        let requested_acks = env.acks.clone();
        let resume_from = env.resume_from;
        let reset = env.reset_acks;
        let adoption_allowed = env.permits_ack_adoption();

        let (_, _, complete) = self.merge_batch(env.entries);

        // R1 + R2: adopt only after a complete merge of a complete delta.
        //
        // R5, while bootstrapping: a requester's delta is computed against its cached
        // view of *our* coverage, and a node rebuilt from an empty data directory is
        // precisely the case where that cache over-states us. The delta then arrives
        // nearly empty, merges "completely", and adopting its acks would have us claim
        // coverage of state we do not hold — which propagates to every peer and feeds
        // the tombstone GC watermark. Our own rounds still adopt (see `apply_envelope`):
        // there we sent the acks, so the delta is complete relative to the truth.
        if complete && adoption_allowed && !self.bootstrap_pending {
            self.core.adopt_acks(&requested_acks);
        }
        self.core.record_peer_acks(peer, requested_acks.clone());

        // R4: the response delta and the acks we claim come from the state as it is now,
        // after the merge above, under this same guard.
        let filter = if reset {
            HashMap::new()
        } else {
            requested_acks
        };
        let delta = compute_delta(
            self.core.data(),
            self.core.origin_index(),
            &filter,
            resume_from,
            self.config.max_delta_entries,
            self.config.max_delta_bytes,
        );

        let mut response = SyncEnvelope::new(self.id, uuid);
        response.acks = self.core.acks().clone();
        response.entries = delta.entries;
        response.page = delta.page;
        response.digest = Some(self.state_digest());
        Ok(response)
    }

    /// Consume a response envelope (initiator side) that is not part of a paginated
    /// round, or is its first page.
    pub fn apply_envelope(&mut self, env: SyncEnvelope) -> Result<MergeOutcome> {
        self.apply_envelope_in_round(env, true)
    }

    /// Consume a later page of a paginated round.
    ///
    /// `earlier_pages_complete` is false once any earlier page of the same round had an
    /// entry refused. Without it, R1 is evaluated per envelope: a refusal on page one
    /// followed by a clean final page adopts the peer's whole ack map, claiming coverage
    /// of the entry that was refused. Unpaged, that refusal correctly parks the acks and
    /// the entry comes back next round.
    pub fn apply_envelope_in_round(
        &mut self,
        env: SyncEnvelope,
        earlier_pages_complete: bool,
    ) -> Result<MergeOutcome> {
        let peer = env.sender_id;
        let peer_acks = env.acks.clone();
        let adoption_allowed = env.permits_ack_adoption();
        let peer_digest = env.digest;
        let resume_from = env
            .page
            .as_ref()
            .and_then(|p| (!p.last).then_some(p.cursor));
        let peer_delta_empty = env.entries.is_empty();

        let (merged, rejected, complete) = self.merge_batch(env.entries);

        let acks_adopted = complete && earlier_pages_complete && adoption_allowed;
        if acks_adopted {
            self.core.adopt_acks(&peer_acks);
        }
        self.core.record_peer_acks(peer, peer_acks);

        let digest_match = peer_digest.map(|theirs| theirs == self.state_digest());

        Ok(MergeOutcome {
            merged,
            rejected,
            peer_delta_empty,
            digest_match,
            resume_from,
            acks_adopted,
            complete,
        })
    }

    /// Drop our cached view of a peer's coverage and our own coverage claims, forcing a
    /// full re-exchange. The repair half of the divergence loop (RFC 3.6).
    ///
    /// Always safe: the data map is never truncated, so a lowered ack can only cause
    /// retransmission — never loss. This is precisely what v1 could not do.
    pub fn reset_peer_coverage(&mut self, peer: NodeId) {
        self.core.forget_peer_acks(peer);
        self.core.reset_ack(Some(peer));
    }

    pub fn state_digest(&self) -> StateDigest {
        StateDigest::compute(self.core.data())
    }

    pub fn acks_snapshot(&self) -> HashMap<NodeId, u64> {
        self.core.acks().clone()
    }

    // -----------------------------------------------------------------------
    // v1 compatibility shim (RFC 8.2.1)
    // -----------------------------------------------------------------------

    /// Serve a v1 peer.
    ///
    /// The pivot is `is_snapshot = true`: by v1 property P3 the client adopts
    /// `progress` monotonically and then merges `entries`, which is exactly delta-state
    /// adoption semantics. v1 clients thereby follow the v2 invariant without a code
    /// change. What v1 *does* with the flag is what v2 needs; v1 never verifies the
    /// flag's nominal meaning ("this is a full dump").
    pub fn handle_sync_v1(&mut self, msg: SyncMessage) -> Result<SyncResponse> {
        let peer = msg.sender_id;
        let requested = msg.sender_ack.clone();

        // Merge everything the v1 peer pushed. v1's gap concept does not apply here: a
        // hole is a superseded write, and INV — not contiguity — governs ack movement.
        self.merge_batch(msg.entries);

        // Deliberately NOT adopting `sender_ack`: a v1 push carries only the sender's
        // own log suffix, which is not a complete delta, so R1 forbids adoption.
        self.core.record_peer_acks(peer, requested.clone());

        // The response must be the *complete* delta, because the v1 client will adopt
        // our progress map wholesale. Pagination is therefore disabled on this path —
        // a partial delta paired with a full progress claim is exactly the hole INV
        // forbids, and v1 has no way to signal "more pages follow".
        let delta = compute_delta(
            self.core.data(),
            self.core.origin_index(),
            &requested,
            None,
            usize::MAX,
            usize::MAX,
        );
        debug_assert!(delta.page.is_none(), "the v1 shim must never paginate");

        Ok(SyncResponse {
            peer_id: self.id,
            entries: delta.entries,
            progress: self.core.acks().clone(),
            is_snapshot: true,
        })
    }

    /// Consume a v1 peer's response to our (empty) v1 push.
    pub fn apply_v1_response(&mut self, resp: SyncResponse) -> Result<()> {
        let peer = resp.peer_id;
        let progress = resp.progress.clone();
        let is_snapshot = resp.is_snapshot;

        let (_, _, complete) = self.merge_batch(resp.entries);

        // Only a v1 full dump is a complete delta. v1's incremental path silently skips
        // origins whose logs were truncated below our ack (`get_peer_missing_logs`
        // `continue`s), so adopting after an incremental response could claim coverage
        // of writes we never received. Declining costs one retransmission per round and
        // resolves itself: our ack stays put, so the peer's log eventually cannot cover
        // it and v1 escalates to a full dump, which we do adopt.
        if complete && is_snapshot {
            self.core.adopt_acks(&progress);
        } else if complete {
            trace!(
                peer,
                "declining ack adoption from an incremental v1 response"
            );
        }
        self.core.record_peer_acks(peer, progress);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Writes
    // -----------------------------------------------------------------------

    fn alloc_entry_meta(&mut self) -> Metadata {
        let seq = self.core.next_seq();
        self.core.execute(StateOp::IncrementSeq);
        let timestamp = Utc::now().timestamp_millis();
        Metadata::new(self.id, seq, timestamp)
    }

    fn record_own_write(&mut self, entry: &Entry) {
        // Our own writes are allocated in order, so we cover them contiguously.
        self.core.bump_ack(self.id, entry.meta.seq);
        if self.config.coalesce_window.is_some() {
            // Bounded: if the push loop is not draining (or is not running at all),
            // drop the backlog rather than growing without limit. The periodic round
            // is the backstop, so the only cost is latency.
            if self.pending_push.len() >= self.config.max_delta_entries {
                self.pending_push.clear();
            }
            self.pending_push.push(entry.key.clone());
        }
    }

    pub fn put(&mut self, key: String, value: impl Into<Vec<u8>>) -> Result<Entry> {
        let meta = self.alloc_entry_meta();
        let entry = Entry::new(key, Some(value.into()), meta);
        self.execute_ops(vec![StateOp::Set(entry.clone())])?;
        self.record_own_write(&entry);
        Ok(entry)
    }

    /// Write with a timestamp guaranteed to beat the current winner.
    ///
    /// The operator escape hatch for a key poisoned by a peer with a runaway clock:
    /// plain `put` would lose LWW against a far-future stamp until real time catches up.
    pub fn force_put(&mut self, key: String, value: impl Into<Vec<u8>>) -> Result<Entry> {
        let seq = self.core.next_seq();
        self.core.execute(StateOp::IncrementSeq);
        let now = Utc::now().timestamp_millis();
        let timestamp = match self.core.data().get(&key) {
            Some(existing) => now.max(existing.meta.timestamp.saturating_add(1)),
            None => now,
        };
        let entry = Entry::new(
            key,
            Some(value.into()),
            Metadata::new(self.id, seq, timestamp),
        );
        self.execute_ops(vec![StateOp::Set(entry.clone())])?;
        self.record_own_write(&entry);
        Ok(entry)
    }

    pub fn delete(&mut self, key: String) -> Result<Option<Entry>> {
        let meta = self.alloc_entry_meta();
        let tombstone = Entry::new(key.clone(), None, meta);
        let previous = self.core.data().get(&key).cloned();
        self.execute_ops(vec![StateOp::Set(tombstone.clone())])?;
        self.record_own_write(&tombstone);
        Ok(previous)
    }

    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    pub fn get(&self, key: &str) -> Option<Entry> {
        self.core
            .data()
            .get(key)
            .cloned()
            .filter(|entry| entry.value.is_some())
    }

    pub fn get_including_tombstones(&self, key: &str) -> Option<Entry> {
        self.core.data().get(key).cloned()
    }

    pub fn get_by_prefix(&self, prefix: &str) -> HashMap<String, Entry> {
        self.iter_by_prefix(prefix)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn iter_by_prefix<'a, 'b>(
        &'a self,
        prefix: &'b str,
    ) -> impl Iterator<Item = (&'a String, &'a Entry)> + use<'a, 'b> {
        self.core
            .data()
            .range(prefix.to_string()..)
            .take_while(move |(k, _)| k.starts_with(prefix))
            .filter(|(_, v)| v.value.is_some())
    }

    pub fn get_all_including_tombstones(&self) -> HashMap<String, Entry> {
        self.iter_all_including_tombstones()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn iter_all_including_tombstones(&self) -> impl Iterator<Item = (&String, &Entry)> {
        self.core.data().iter()
    }

    // -----------------------------------------------------------------------
    // Membership and maintenance
    // -----------------------------------------------------------------------

    pub fn get_peers(&self) -> Vec<NodeId> {
        self.core
            .members()
            .iter()
            .filter(|&&id| id != self.id)
            .copied()
            .collect()
    }

    pub fn get_all_nodes(&self) -> Vec<NodeId> {
        self.core.members().to_vec()
    }

    pub fn add_peer(&mut self, peer_id: NodeId) -> Result<bool> {
        if self.core.members().contains(&peer_id) {
            return Ok(false);
        }
        self.execute_ops(vec![StateOp::AddPeer { peer_id }])?;
        info!("added peer node: {peer_id}");
        Ok(true)
    }

    pub fn remove_peer(&mut self, peer_id: NodeId) -> Result<bool> {
        if peer_id == self.id || !self.core.members().contains(&peer_id) {
            return Ok(false);
        }
        self.execute_ops(vec![StateOp::RemovePeer { peer_id }])?;
        info!("removed peer node: {peer_id}");
        Ok(true)
    }

    /// Collect tombstones every known peer has already covered (RFC section 6).
    ///
    /// A tombstone authored by origin `n` at seq `s` may be cleared only once every
    /// known peer reports `peer_acks[p][n] >= s`. v1's local-clock TTL is gone: under
    /// any state-shipping scheme an uncoordinated GC lets a lagging replica resurrect a
    /// deleted key — for dstack-gateway, a deregistered CVM reappearing in every node's
    /// WireGuard config.
    ///
    /// Caveat (accepted, documented): the watermark spans *known* peers, so a
    /// permanently retired peer pins GC until `remove_peer` is called for it. Note that
    /// `remove_peer` deliberately keeps `acks[removed]` — that is our coverage of the
    /// entries the departed node authored, which outlive it, and it is what lets the
    /// rest of the cluster still compute a watermark for that origin.
    ///
    /// Second caveat, and it is sharper than it first looks: GC cadence is
    /// uncoordinated while the digest covers tombstones (section 3.6), so two honest,
    /// fully converged peers that collect at different times report unequal digests.
    /// The repair then makes the laggard re-send the tombstone, the collector merges it
    /// back, the digests agree — and the collector's next GC cycle drops it again. That
    /// is an oscillation at GC cadence, not a one-off, and it persists for as long as
    /// one side collects and the other does not. It is safe throughout (the repair only
    /// lowers acks, which costs a retransmission and cannot lose data) but a cluster on
    /// a wildly uneven cadence pays a full re-exchange each cycle.
    ///
    /// Neither obvious fix is taken, deliberately. Dropping tombstones from the digest
    /// would blind it to a replica that *lost* one, which is the resurrection this
    /// watermark exists to prevent. Excluding only collectable tombstones would make the
    /// digest depend on `peer_acks`, which is per-node bookkeeping that legitimately
    /// differs between converged replicas — reintroducing false divergence by the same
    /// route. A correct fix needs the collector to remember what it collected so a
    /// post-repair full dump does not reinstate it; that is a real design change to the
    /// one mechanism that catches silent divergence, and is not worth making blind.
    ///
    /// No embedder in this repository calls this method, so the cost above is latent.
    pub fn collect_tombstone_garbage(&mut self) -> Result<usize> {
        let peers = self.get_peers();
        if peers.is_empty() {
            // A single-node cluster has nobody to resurrect from.
            let keys: Vec<String> = self
                .core
                .data()
                .iter()
                .filter(|(_, e)| e.is_deleted())
                .map(|(k, _)| k.clone())
                .collect();
            let removed = keys.len();
            self.execute_ops(keys.into_iter().map(StateOp::Clear).collect())?;
            return Ok(removed);
        }

        let watermark = |origin: NodeId| -> Option<u64> {
            let mut low = u64::MAX;
            for peer in &peers {
                let reported = self
                    .core
                    .peer_acks_for(*peer)
                    .and_then(|m| m.get(&origin).copied())?;
                low = low.min(reported);
            }
            Some(low)
        };

        let collectable: Vec<String> = self
            .core
            .data()
            .iter()
            .filter(|(_, entry)| entry.is_deleted())
            .filter(|(_, entry)| watermark(entry.meta.node).is_some_and(|w| w >= entry.meta.seq))
            .map(|(key, _)| key.clone())
            .collect();

        let removed = collectable.len();
        self.execute_ops(collectable.into_iter().map(StateOp::Clear).collect())?;
        if removed > 0 {
            debug!("collected {removed} fully-replicated tombstones");
        }
        Ok(removed)
    }

    /// Recover `next_seq` from the live index after a restart or bootstrap.
    /// One range scan, replacing v1's walk over every log bucket plus the data map.
    pub fn recover_next_seq(&mut self) {
        // `max_own_seq` scans the live index, which is not a record of what we authored:
        // an entry we wrote is evicted when a peer wins the key under LWW, or when its
        // tombstone is collected. Our own ack survives both — it advances on every local
        // write and never regresses — so it is the lower bound the index cannot supply.
        // Underestimating here reissues a seq that peers already cover, and they filter
        // the new write out as "already seen" with no error anywhere.
        let max_own = self.core.max_own_seq().max(self.core.ack_for(self.id));
        if max_own > 0 {
            self.core.execute(StateOp::SetNextSeq(max_own + 1));
        }
    }

    /// Suspend adoption of a requester's ack map until [`NodeState::finish_bootstrap`].
    ///
    /// Called by `SyncManager::bootstrap`. A node that never bootstraps is unaffected:
    /// the flag starts clear, so this is opt-in and cannot silently freeze coverage.
    pub fn begin_bootstrap(&mut self) {
        self.bootstrap_pending = true;
    }

    pub fn finish_bootstrap(&mut self) {
        self.bootstrap_pending = false;
    }

    pub fn ensure_next_seq(&mut self, min_next_seq: u64) {
        self.core.execute(StateOp::SetNextSeq(min_next_seq));
    }

    pub fn get_next_seq(&self) -> u64 {
        self.core.next_seq()
    }

    fn persist_if_dirty(&mut self) -> Result<bool> {
        if !self.dirty {
            return Ok(false);
        }
        self.persist_to_disk()?;
        Ok(true)
    }

    #[inline]
    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn status(&self) -> NodeStatus {
        let mut peers: Vec<PeerStatus> = self
            .core
            .members()
            .iter()
            .map(|&id| PeerStatus {
                id,
                ack: self.core.ack_for(id),
                peer_ack: self
                    .core
                    .peer_acks_for(id)
                    .and_then(|m| m.get(&self.id).copied())
                    .unwrap_or(0),
                heard_from: self.core.peer_acks_for(id).is_some(),
            })
            .collect();
        peers.sort_by_key(|p| p.id);

        NodeStatus {
            id: self.id,
            n_kvs: self.core.data().len(),
            next_seq: self.core.next_seq(),
            dirty: self.dirty,
            wal: self.wal.is_some(),
            digest: self.state_digest().to_hex(),
            entries_merged: self.entries_merged,
            entries_rejected: self.entries_rejected,
            peers,
        }
    }
}

impl Node {
    pub fn new(id: NodeId, peer_ids: Vec<NodeId>) -> Self {
        Self::with_config(id, peer_ids, NodeConfig::default())
    }

    pub fn with_config(id: NodeId, peer_ids: Vec<NodeId>, config: NodeConfig) -> Self {
        let state = NodeState {
            id,
            core: CoreState::new(id, peer_ids),
            wal: None,
            snapshot_path: None,
            watchers: Vec::new(),
            dirty: false,
            config,
            pending_push: Vec::new(),
            entries_merged: 0,
            entries_rejected: 0,
            bootstrap_pending: false,
        };
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub fn new_with_persistence<P: Into<PathBuf>>(
        id: NodeId,
        peers: Vec<NodeId>,
        data_dir: P,
    ) -> Result<Self> {
        Self::with_persistence_and_config(id, peers, data_dir, NodeConfig::default())
    }

    pub fn with_persistence_and_config<P: Into<PathBuf>>(
        id: NodeId,
        peers: Vec<NodeId>,
        data_dir: P,
        config: NodeConfig,
    ) -> Result<Self> {
        let data_dir = data_dir.into();
        let wal_path = data_dir.join(format!("node_{id}.wal"));
        let snapshot_path = data_dir.join(format!("node_{id}.snapshot"));

        if let Some(parent) = wal_path.parent() {
            fs_err::create_dir_all(parent)?;
        }

        let wal = WriteAheadLog::new(&wal_path, id)?;
        let existing_ops = wal.read_all_ops()?;

        let mut state = NodeState {
            id,
            core: CoreState::new(id, peers),
            wal: Some(wal),
            snapshot_path: Some(snapshot_path.clone()),
            watchers: Vec::new(),
            dirty: false,
            config,
            pending_push: Vec::new(),
            entries_merged: 0,
            entries_rejected: 0,
            bootstrap_pending: false,
        };

        if state.load_snapshot_if_exists()? {
            info!("loaded snapshot from {}", snapshot_path.display());
        }

        if !existing_ops.is_empty() {
            info!(
                "recovering {} state operations from WAL",
                existing_ops.len()
            );
            state.replay_ops(existing_ops)?;
        }
        // A v1 WAL/snapshot pair may leave next_seq behind the entries it replayed.
        state.recover_next_seq();

        Ok(Self {
            state: Arc::new(RwLock::new(state)),
        })
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, NodeState> {
        #[allow(clippy::expect_used)]
        self.state.write().expect("lock should never fail")
    }

    pub fn read(&self) -> RwLockReadGuard<'_, NodeState> {
        #[allow(clippy::expect_used)]
        self.state.read().expect("lock should never fail")
    }

    pub fn persist(&self) -> Result<()> {
        self.write().persist_to_disk()
    }

    pub fn persist_if_dirty(&self) -> Result<bool> {
        self.write().persist_if_dirty()
    }

    pub fn state_digest(&self) -> StateDigest {
        self.read().state_digest()
    }

    pub fn watch(&self, key: &str) -> watch::Receiver<()> {
        self.write().watch_key(key)
    }

    pub fn watch_prefix(&self, prefix: &str) -> watch::Receiver<()> {
        self.write().watch_prefix(prefix)
    }
}
