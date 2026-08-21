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
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
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

    /// Cached encoded payload size used by admission control. Derived from `core.data`
    /// and maintained with every Set/Clear so ingest does not scan the full map for
    /// each entry in a batch.
    data_bytes: usize,

    config: NodeConfig,

    /// Keys written locally since the last opportunistic push (RFC 3.9).
    pending_push: Vec<String>,

    entries_merged: u64,
    entries_rejected: u64,
    /// Set while this node is bootstrapping, which suspends adoption of a *requester's*
    /// ack map. See [`NodeState::begin_bootstrap`].
    bootstrap_pending: bool,

    /// Durable ops written after an in-flight snapshot was encoded.
    wal_tail: Option<Vec<StateOp>>,
}

/// A snapshot encoded under the state lock and written after releasing it.
struct PendingSnapshot {
    path: PathBuf,
    encoded: Vec<u8>,
}

impl PendingSnapshot {
    fn write(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = self.path.with_extension("snapshot.tmp");
        {
            let mut writer = BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp_path)?,
            );
            writer.write_all(&self.encoded)?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        if self.path.exists() {
            let backup = self.path.with_extension("snapshot.bak");
            let _ = fs::rename(&self.path, &backup);
        }
        fs::rename(&tmp_path, &self.path)?;
        if let Some(parent) = self.path.parent() {
            File::open(parent)?.sync_all()?;
        }
        debug!("persisted snapshot to {:?}", self.path);
        Ok(())
    }
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

/// Whether `candidate` should replace what is currently held for its key.
///
/// The one place the LWW rule is spelled out, so a batch and a single merge
/// cannot drift apart on it.
fn beats(current: Option<&Entry>, candidate: &Entry) -> bool {
    match current {
        Some(current) => compare_entries(current, candidate) == std::cmp::Ordering::Less,
        None => true,
    }
}

/// The winners a merge round has decided so far, plus what they will cost the
/// store, so that later entries in the same round are judged against them.
#[derive(Default)]
struct StagedBatch {
    winners: Vec<Entry>,
    /// Position in `winners` for each staged key.
    index: HashMap<String, usize>,
    /// Entries that beat what was held, counted per decision rather than per
    /// key: two winners for one key are two merges, as they were when each was
    /// applied on its own.
    decided: usize,
    /// Keys the store does not hold yet.
    new_keys: usize,
    /// Net change to `data_bytes`, signed because a winner can be smaller than
    /// what it replaces.
    bytes_delta: isize,
}

impl StagedBatch {
    fn winner(&self, key: &str) -> Option<&Entry> {
        self.index.get(key).map(|at| &self.winners[*at])
    }

    /// Record a winner. `replaced` is the size of whatever it displaces, absent
    /// when the key is new to both the store and this batch.
    fn stage(&mut self, entry: Entry, replaced: Option<usize>) {
        self.decided += 1;
        self.bytes_delta += entry_storage_bytes(&entry) as isize;
        match replaced {
            Some(bytes) => self.bytes_delta -= bytes as isize,
            None => self.new_keys += 1,
        }
        match self.index.get(&entry.key) {
            Some(at) => self.winners[*at] = entry,
            None => {
                self.index.insert(entry.key.clone(), self.winners.len());
                self.winners.push(entry);
            }
        }
    }

    fn into_ops(self) -> Vec<StateOp> {
        self.winners.into_iter().map(StateOp::Set).collect()
    }
}

fn entry_storage_bytes(entry: &Entry) -> usize {
    entry.key.len() + entry.value.as_ref().map_or(0, Vec::len)
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
    /// Snapshot I/O runs outside `state`, so serialize snapshot writers separately.
    persist_lock: Arc<Mutex<()>>,
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
        let Some(pending) = self.begin_persist(true)? else {
            return Ok(());
        };
        let written = pending.write();
        self.finish_persist(written.is_ok())?;
        written
    }

    fn begin_persist(&mut self, force: bool) -> Result<Option<PendingSnapshot>> {
        if !force && !self.dirty {
            return Ok(None);
        }
        if self.wal_tail.is_some() {
            bail!("a snapshot is already in flight");
        }

        let path = self.snapshot_path()?.to_path_buf();
        let encoded = rmp_serde::to_vec(&SnapshotFile::from_state(self))?;
        self.dirty = false;
        if self.wal.is_some() {
            self.wal_tail = Some(Vec::new());
        }
        Ok(Some(PendingSnapshot { path, encoded }))
    }

    fn finish_persist(&mut self, written: bool) -> Result<()> {
        let tail = self.wal_tail.take().unwrap_or_default();
        if !written {
            self.dirty = true;
            return Ok(());
        }
        let Some(wal) = self.wal.as_mut() else {
            return Ok(());
        };
        if let Err(err) = wal.replace_with_ops(&tail) {
            self.dirty = true;
            return Err(err);
        }
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
            let durable: Vec<StateOp> = ops.iter().filter(|op| op.is_durable()).cloned().collect();
            self.append_durable(&durable)?;
        }

        for op in ops {
            self.execute_op(op);
        }
        self.mark_dirty();
        Ok(())
    }

    fn execute_op(&mut self, op: StateOp) {
        let size_change = match &op {
            StateOp::Set(entry) => {
                let old = self
                    .core
                    .data()
                    .get(&entry.key)
                    .map_or(0, entry_storage_bytes);
                Some((old, entry_storage_bytes(entry)))
            }
            StateOp::Clear(key) => self
                .core
                .data()
                .get(key)
                .map(|entry| (entry_storage_bytes(entry), 0)),
            _ => None,
        };
        let changed_key = match &op {
            StateOp::Set(entry) => Some(entry.key.clone()),
            _ => None,
        };
        self.core.execute(op);
        if let Some((old, new)) = size_change {
            self.data_bytes = self.data_bytes.saturating_sub(old).saturating_add(new);
        }
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
            if let Admission::Reject { reason } = self
                .config
                .limits
                .check_capacity(self.core.data().len(), self.data_bytes)
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
        if let Admission::Reject { reason } = self.admit(&entry, now) {
            self.entries_rejected += 1;
            bail!("entry {} rejected: {reason}", entry.key);
        }

        if !beats(self.core.data().get(&entry.key), &entry) {
            return Ok(false);
        }
        self.execute_ops(vec![StateOp::Set(entry)])
            .context("failed to apply merged entry")?;
        self.entries_merged += 1;
        Ok(true)
    }

    /// Merge a batch: decide every entry, append the winners once, then apply.
    ///
    /// Rule R1: a single failure blocks ack adoption for the whole round.
    /// Merging is idempotent, so the retry next round costs nothing.
    ///
    /// The three passes are not cosmetic.
    ///
    /// Deciding first, against a view of the live map overlaid with what this
    /// batch has already decided, is what keeps LWW intact: `StateOp::Set`
    /// applies unconditionally — the comparison lives here — so two versions of
    /// one key in one envelope must be resolved before either is written, or
    /// whichever arrived last would win regardless of which one LWW picks.
    ///
    /// Appending before applying is what keeps the log ahead of memory. If the
    /// append fails, nothing has been applied, so the entries are still absent
    /// and the peer's retransmission decides and appends them again. Applying
    /// first and appending after cannot recover: the retransmission would find
    /// the entries already present, decide nothing, append nothing, and report
    /// the round complete — adopting acks for entries that never reached the
    /// log. Idempotence, which makes the retry safe, is exactly what makes it
    /// useless as a repair.
    fn merge_batch(&mut self, entries: Vec<Entry>) -> (usize, usize, bool) {
        let now = Utc::now().timestamp_millis();
        let mut rejected = 0usize;
        let mut complete = true;
        let mut staged = StagedBatch::default();

        for entry in entries {
            if let Admission::Reject { reason } = self.admit_staged(&entry, now, &staged) {
                self.entries_rejected += 1;
                rejected += 1;
                complete = false;
                warn!("refusing entry {}: {reason}", entry.key);
                continue;
            }
            let previous = staged
                .winner(&entry.key)
                .or_else(|| self.core.data().get(&entry.key));
            if !beats(previous, &entry) {
                continue;
            }
            let replaced = previous.map(entry_storage_bytes);
            staged.stage(entry, replaced);
        }

        let merged = staged.decided;
        let ops = staged.into_ops();
        if let Err(err) = self.append_durable(&ops) {
            // Nothing has been applied, so this round changed nothing at all.
            // Reporting it incomplete parks ack adoption (rule R1); the peer
            // offers the same entries next round, and because they are still
            // absent from memory they are decided and appended again.
            error!("failed to append a merged batch to the WAL: {err:#}");
            return (0, rejected, false);
        }
        if let Err(err) = self.replay_ops(ops) {
            error!("failed to apply a merged batch: {err:#}");
            return (0, rejected, false);
        }
        self.entries_merged += merged as u64;

        (merged, rejected, complete)
    }

    /// Admission for an entry in a batch that has already decided others.
    ///
    /// Capacity is charged against what the batch will add, not only against
    /// what the store already holds. Without that, deferring application to the
    /// end of the round would let one envelope overshoot `max_keys` and
    /// `max_total_bytes` by a whole delta.
    fn admit_staged(&self, entry: &Entry, now_ms: i64, staged: &StagedBatch) -> Admission {
        let known =
            self.core.data().contains_key(&entry.key) || staged.winner(&entry.key).is_some();
        if !known {
            if let Admission::Reject { reason } = self.config.limits.check_capacity(
                self.core.data().len() + staged.new_keys,
                self.data_bytes.saturating_add_signed(staged.bytes_delta),
            ) {
                return Admission::Reject { reason };
            }
        }
        // `admit` re-runs the capacity check for a key the store does not hold,
        // against unadjusted totals; it can only be more permissive than the
        // check above, which has already passed.
        self.admit(entry, now_ms)
    }

    /// Append durable ops to the log, and to the tail an in-flight snapshot
    /// will be rebased onto.
    ///
    /// Whether the append is forced to disk before returning is the policy in
    /// [`NodeConfig::wal_sync_interval`]. Either way the bytes reach the kernel
    /// here, and either way the ops reach `wal_tail`: a rotation must carry
    /// what the log holds, not what the disk has confirmed.
    fn append_durable(&mut self, ops: &[StateOp]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let Some(wal) = self.wal.as_mut() else {
            return Ok(());
        };
        wal.write_ops(ops)?;
        if let Some(tail) = self.wal_tail.as_mut() {
            tail.extend_from_slice(ops);
        }
        Ok(())
    }

    /// How many times this node's WAL has been forced to disk since it was
    /// opened, or zero when the node keeps no log.
    ///
    /// The cost of a write path is in its fsync count, not in what it returns:
    /// an embedder can graph this, and a test can pin "one batch, one fsync"
    /// without timing anything.
    pub fn wal_sync_count(&self) -> u64 {
        self.wal.as_ref().map_or(0, |wal| wal.sync_count())
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
    /// permanently retired peer pins GC until `remove_peer` is called for it — and then
    /// the opposite bites, because `remove_peer` also drops `acks[removed]`. Once every
    /// node has retired a peer, no ack map mentions that origin, the watermark for it is
    /// unknowable, and the tombstones it authored are uncollectable for good. That is
    /// forced by the on-disk format rather than chosen; see the `RemovePeer` arm of
    /// [`CoreState::execute`](crate::ops::CoreState::execute) for why keeping the ack is
    /// worse.
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
            data_bytes: 0,
            config,
            pending_push: Vec::new(),
            entries_merged: 0,
            entries_rejected: 0,
            bootstrap_pending: false,
            wal_tail: None,
        };
        Self {
            state: Arc::new(RwLock::new(state)),
            persist_lock: Arc::new(Mutex::new(())),
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
            data_bytes: 0,
            config,
            pending_push: Vec::new(),
            entries_merged: 0,
            entries_rejected: 0,
            bootstrap_pending: false,
            wal_tail: None,
        };

        if state.load_snapshot_if_exists()? {
            info!("loaded snapshot from {}", snapshot_path.display());
        }
        state.data_bytes = state.core.data().values().map(entry_storage_bytes).sum();

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
            persist_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, NodeState> {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn read(&self) -> RwLockReadGuard<'_, NodeState> {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn persist(&self) -> Result<()> {
        self.persist_inner(true).map(|_| ())
    }

    pub fn persist_if_dirty(&self) -> Result<bool> {
        self.persist_inner(false)
    }

    fn persist_inner(&self, force: bool) -> Result<bool> {
        let _persist = self
            .persist_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(pending) = self.write().begin_persist(force)? else {
            return Ok(false);
        };

        let written = pending.write();
        self.write().finish_persist(written.is_ok())?;
        written.map(|()| true)
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

#[cfg(test)]
mod persistence_tests {
    use super::*;

    fn value_of(node: &Node, key: &str) -> Option<Vec<u8>> {
        node.read().get(key).and_then(|entry| entry.value)
    }

    #[test]
    fn ops_applied_while_a_snapshot_is_in_flight_survive_the_wal_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let node = Node::new_with_persistence(1, vec![], dir.path()).unwrap();
        node.write().put("before".into(), b"1".to_vec()).unwrap();

        let pending = node.write().begin_persist(false).unwrap().unwrap();
        node.write().put("during".into(), b"2".to_vec()).unwrap();
        pending.write().unwrap();
        node.write().finish_persist(true).unwrap();

        drop(node);
        let reopened = Node::new_with_persistence(1, vec![], dir.path()).unwrap();
        assert_eq!(
            value_of(&reopened, "before").as_deref(),
            Some(b"1".as_ref())
        );
        assert_eq!(
            value_of(&reopened, "during").as_deref(),
            Some(b"2".as_ref())
        );
    }

    fn envelope_from(peer: NodeId, count: u64) -> Vec<Entry> {
        (0..count)
            .map(|i| {
                Entry::new_put(
                    Metadata::new(peer, i + 1, 1_000),
                    format!("key-{i}"),
                    b"value".to_vec(),
                )
            })
            .collect()
    }

    /// A round the log refused must leave no trace in memory.
    ///
    /// Applying first and appending after looks recoverable — the round reports
    /// itself incomplete, acks stay parked, the peer retransmits — but it is
    /// not. The retransmitted entries would compare equal to what is already in
    /// memory, so the round would decide nothing, append nothing, and report
    /// itself complete; the acks would then be adopted for entries that never
    /// reached the log, and the peer would stop offering them. Idempotence is
    /// what makes the retry safe and what makes it useless as a repair.
    #[test]
    fn a_batch_the_log_refused_is_repaired_by_the_retransmission() {
        let dir = tempfile::tempdir().unwrap();
        let node = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();

        // Break the log: appends fail from here.
        node.write().wal.as_mut().unwrap().close().unwrap();

        let (merged, rejected, complete) = node.write().merge_batch(envelope_from(2, 10));
        assert_eq!(
            (merged, rejected, complete),
            (0, 0, false),
            "a round that could not be logged must not report itself complete"
        );
        assert!(
            value_of(&node, "key-0").is_none(),
            "memory must not run ahead of a log that refused the write"
        );

        // The peer offers the same entries again, and this time the log takes them.
        node.write()
            .wal
            .as_mut()
            .unwrap()
            .replace_with_ops(&[])
            .unwrap();
        let (merged, _, complete) = node.write().merge_batch(envelope_from(2, 10));
        assert_eq!(
            (merged, complete),
            (10, true),
            "the retransmission must be a real merge, not an idempotent no-op"
        );

        drop(node);
        let reopened = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
        for i in 0..10 {
            assert_eq!(
                value_of(&reopened, &format!("key-{i}")).as_deref(),
                Some(b"value".as_ref()),
                "entry {i} was acknowledged but never made it to the log"
            );
        }
    }

    /// A panic raised by embedder code mid-round must not leave the node in a
    /// state where later writes are silently not logged.
    ///
    /// `admit` calls into an embedder-supplied policy, and `Node::write` hands
    /// out the lock again after a poisoning panic, so the node keeps serving. A
    /// round therefore may not park anything in the node that only its own
    /// normal exit puts back.
    #[test]
    fn a_panic_inside_a_round_does_not_wedge_later_writes() {
        struct Exploding;
        impl crate::admission::AdmissionPolicy for Exploding {
            fn admit(&self, _entry: &Entry) -> Admission {
                panic!("embedder policy panicked");
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let node = Node::with_persistence_and_config(
            1,
            vec![2],
            dir.path(),
            NodeConfig {
                admission: Some(Arc::new(Exploding)),
                ..Default::default()
            },
        )
        .unwrap();

        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            node.write().merge_batch(envelope_from(2, 4));
        }));
        assert!(
            unwound.is_err(),
            "the fixture depends on the policy panicking"
        );

        let before = node.read().wal_sync_count();
        node.write().put("after".into(), b"1".to_vec()).unwrap();
        assert_eq!(
            node.read().wal_sync_count(),
            before + 1,
            "a write after the panic was not forced to the log"
        );

        drop(node);
        let reopened = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
        assert_eq!(
            value_of(&reopened, "after").as_deref(),
            Some(b"1".as_ref()),
            "the write that reported success was never durable"
        );
    }

    /// The same rotation, but the writes arrive as a merged batch.
    ///
    /// A batch defers its append to the end of the round, so it has to feed
    /// `wal_tail` at that point rather than per entry. If it does not, the
    /// snapshot rotation replaces the log with a tail that never heard of the
    /// round, and everything the peer sent while the snapshot was in flight is
    /// gone at the next restart.
    #[test]
    fn a_batch_merged_while_a_snapshot_is_in_flight_survives_the_wal_rotation() {
        use crate::sync::SyncEnvelope;

        let dir = tempfile::tempdir().unwrap();
        let node = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
        node.write().put("before".into(), b"1".to_vec()).unwrap();

        let pending = node.write().begin_persist(false).unwrap().unwrap();

        let mut env = SyncEnvelope::new(2, b"uuid-2".to_vec());
        env.push_only = true;
        env.entries = (0..10)
            .map(|i| {
                Entry::new_put(
                    Metadata::new(2, i + 1, 1_000),
                    format!("during-{i}"),
                    b"2".to_vec(),
                )
            })
            .collect();
        node.write().merge_push(env).unwrap();

        pending.write().unwrap();
        node.write().finish_persist(true).unwrap();

        drop(node);
        let reopened = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
        assert_eq!(
            value_of(&reopened, "before").as_deref(),
            Some(b"1".as_ref())
        );
        for i in 0..10 {
            assert_eq!(
                value_of(&reopened, &format!("during-{i}")).as_deref(),
                Some(b"2".as_ref()),
                "entry {i} was merged during the snapshot and lost in the rotation"
            );
        }
    }

    #[test]
    fn a_failed_snapshot_write_leaves_the_wal_authoritative() {
        let dir = tempfile::tempdir().unwrap();
        let node = Node::new_with_persistence(1, vec![], dir.path()).unwrap();
        node.write().put("key".into(), b"value".to_vec()).unwrap();

        fs::create_dir(dir.path().join("node_1.snapshot.tmp")).unwrap();
        assert!(node.persist_if_dirty().is_err());
        assert!(node.read().status().dirty);

        drop(node);
        let reopened = Node::new_with_persistence(1, vec![], dir.path()).unwrap();
        assert_eq!(
            value_of(&reopened, "key").as_deref(),
            Some(b"value".as_ref())
        );
    }

    #[test]
    fn a_second_snapshot_cannot_start_while_one_is_in_flight() {
        let dir = tempfile::tempdir().unwrap();
        let node = Node::new_with_persistence(1, vec![], dir.path()).unwrap();
        node.write().put("key".into(), b"value".to_vec()).unwrap();

        let pending = node.write().begin_persist(false).unwrap().unwrap();
        assert!(node.write().persist_to_disk().is_err());
        pending.write().unwrap();
        node.write().finish_persist(true).unwrap();
    }
}
