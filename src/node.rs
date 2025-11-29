use crate::ops::{CoreState, StateOp};
use crate::sync::{SyncMessage, SyncResponse};
use crate::types::{compare_entries, Entry, Metadata, NodeId, PeerState};
use crate::wal::WriteAheadLog;
use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use fs_err::{self as fs, File, OpenOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

const DEFAULT_MAX_LOG_ENTRIES: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: NodeId,
    pub n_kvs: usize,
    pub next_seq: u64,
    pub dirty: bool,
    pub wal: bool,
    pub peers: Vec<PeerStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerStatus {
    pub id: NodeId,
    pub ack: u64,
    pub pack: u64,
    pub logs: usize,
}

/// Core mutable state - all protected by a single RwLock for consistency
pub struct NodeState {
    pub id: NodeId,

    /// Core state: data + peers + next_seq
    core: CoreState,

    // WAL for durability - included in state to ensure atomic updates
    wal: Option<WriteAheadLog>,

    /// Maximum number of log entries to keep per node
    max_log_entries: usize,

    /// Optional snapshot path for full state persistence
    snapshot_path: Option<PathBuf>,

    /// Unified watchers for both exact keys and prefixes
    watchers: Vec<Watcher>,

    /// Tracks whether state has unpersisted changes
    dirty: bool,
}

enum WatchPattern {
    Exact(String),
    Prefix(String),
}

struct Watcher {
    pattern: WatchPattern,
    sender: watch::Sender<()>,
}

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
            bail!("Invalid snapshot magic header");
        }
        if self.version != Self::VERSION {
            bail!(
                "Unsupported snapshot version: expected {}, found {}",
                Self::VERSION,
                self.version
            );
        }
        if self.node_id != expected_node {
            bail!(
                "Snapshot node_id mismatch: expected {}, found {}",
                expected_node,
                self.node_id
            );
        }
        Ok(())
    }
}

/// Simplified store following design:
/// - Pure in-memory KV + per-node bucketed logs
/// - LWW conflict resolution
/// - Single RwLock protecting all state for consistency
/// - BTreeMap for prefix scanning capability
pub struct Node {
    // All core state protected by single RwLock
    // Includes WAL to ensure atomic updates
    state: Arc<RwLock<NodeState>>,
}

impl NodeState {
    fn snapshot_path(&self) -> Result<&Path> {
        self.snapshot_path
            .as_deref()
            .ok_or_else(|| anyhow!("Snapshot path not configured"))
    }

    fn load_snapshot_if_exists(&mut self) -> Result<bool> {
        let Some(path) = self.snapshot_path.clone() else {
            return Ok(false);
        };

        if !path.exists() {
            return Ok(false);
        }

        let mut reader = BufReader::new(File::open(&path)?);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        let snapshot: SnapshotFile =
            bincode::deserialize(&buf).context("Failed to deserialize snapshot")?;
        snapshot.validate(self.id)?;

        self.core = snapshot.core;
        self.dirty = false;
        Ok(true)
    }

    pub fn persist_to_disk(&mut self) -> Result<()> {
        let snapshot_path = self.snapshot_path()?.to_path_buf();
        if let Some(parent) = snapshot_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp_path = snapshot_path.with_extension("snapshot.tmp");
        let snapshot = SnapshotFile::from_state(self);
        let encoded = bincode::serialize(&snapshot)?;

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

        fs::rename(&tmp_path, &snapshot_path)?;

        if let Some(parent) = snapshot_path.parent() {
            if let Ok(dir_file) = File::open(parent) {
                let _ = dir_file.sync_all();
            }
        }

        if let Some(wal) = self.wal.as_mut() {
            wal.reset()?;
        }

        info!("Persisted snapshot to {:?}", snapshot_path);
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

    /// Execute state operations with WAL logging
    /// This is the ONLY method that mutates state - all mutations go through here
    fn execute_ops(&mut self, ops: Vec<StateOp>) -> Result<()> {
        self.execute_ops_impl(ops, true)
    }

    fn execute_ops_impl(&mut self, ops: Vec<StateOp>, write_to_wal: bool) -> Result<()> {
        // Write to WAL first for durability
        for op in ops {
            if self.core.is_noop(&op) {
                trace!("Skipping noop op: {op:?}");
                continue;
            }
            if write_to_wal {
                if let Some(wal) = self.wal.as_mut() {
                    wal.write_op(&op)?;
                }
            }
            self.execute_op(op);
            self.mark_dirty();
        }
        Ok(())
    }

    /// Execute ops during recovery (no WAL writing)
    fn execute_op(&mut self, op: StateOp) {
        let changed_key = if let StateOp::Set(ref entry) = op {
            Some(entry.key.clone())
        } else {
            None
        };
        self.core.execute(op);
        if let Some(key) = changed_key {
            self.notify_watchers(&key);
        }
    }

    /// Execute ops during recovery (no WAL writing)
    fn replay_ops(&mut self, ops: Vec<StateOp>) -> Result<()> {
        self.execute_ops_impl(ops, false)
    }

    /// Sync an entry from another node (LWW resolution)
    pub fn sync(&mut self, entry: Entry) -> Result<bool> {
        debug!(entry.key, "Syncing entry, meta: {:?}", entry.meta);

        // Check LWW before applying
        let should_update = if let Some(existing) = self.core.data().get(&entry.key) {
            compare_entries(existing, &entry) == std::cmp::Ordering::Less
        } else {
            true
        };

        let mut ops = vec![];
        // Always update peer log if this is new
        ops.push(StateOp::PushPeerLog {
            peer_id: entry.meta.node,
            entry: entry.clone(),
            max_entries: self.max_log_entries,
        });

        if should_update {
            ops.push(StateOp::Set(entry));
        }
        self.execute_ops(ops)
            .context("Failed to execute ops in sync")?;

        Ok(should_update)
    }

    /// Update peer_ack: the peer tells us how far they've synced our logs
    pub fn update_peer_ack(&mut self, peer_id: NodeId, ack_seq: u64) -> Result<()> {
        self.execute_ops(vec![StateOp::UpdatePeerAck {
            peer_id,
            ack_seq,
            monotonic: false,
        }])
    }

    /// Update local_ack from remote full dump progress
    /// This ensures our local_ack is consistent with what the remote node has synced
    fn update_local_ack(&mut self, progress: &HashMap<NodeId, u64>) -> Result<()> {
        let ops: Vec<StateOp> = progress
            .iter()
            .map(|(&peer_id, &ack_seq)| StateOp::UpdateLocalAck {
                peer_id,
                ack_seq,
                monotonic: true,
            })
            .collect();
        self.execute_ops(ops)
    }

    pub fn apply_pulled_entries(&mut self, sync_message: SyncResponse) -> Result<()> {
        if sync_message.is_snapshot {
            debug!("Applying pulled snapshot");
            self.update_local_ack(&sync_message.progress)?;
        }
        for entry in sync_message.entries {
            self.sync(entry).context("Failed to sync entry")?;
        }
        let peer_ack = sync_message.progress.get(&self.id).copied().unwrap_or(0);
        self.update_peer_ack(sync_message.peer_id, peer_ack)?;
        Ok(())
    }

    pub fn apply_pushed_entries(&mut self, sync_message: SyncMessage) -> Result<()> {
        let Some(first) = sync_message.entries.first() else {
            return Ok(());
        };

        let local_ack = self
            .core
            .peers()
            .get(&sync_message.sender_id)
            .map(|p| p.local_ack)
            .unwrap_or(0);
        let sender_id = sync_message.sender_id;
        let expected_since = local_ack + 1;
        let actual_since = first.meta.seq;
        if actual_since > expected_since {
            warn!(
                sender_id,
                expected_since, actual_since, "Received entries with gap"
            );
            return Ok(());
        }

        for entry in sync_message.entries {
            self.sync(entry).context("Failed to sync entry")?;
        }
        Ok(())
    }

    fn alloc_entry_meta(&mut self) -> Metadata {
        let seq = self.core.next_seq();
        self.core.execute(StateOp::IncrementSeq);
        let timestamp = Utc::now().timestamp_millis();
        Metadata::new(self.id, seq, timestamp)
    }

    pub fn put(&mut self, key: String, value: impl Into<Vec<u8>>) -> Result<Entry> {
        let value = value.into();
        let meta = self.alloc_entry_meta();
        let entry = Entry::new(key.clone(), Some(value), meta);

        // Compile to StateOp sequence
        let ops = vec![
            StateOp::PushPeerLog {
                peer_id: self.id,
                entry: entry.clone(),
                max_entries: self.max_log_entries,
            },
            StateOp::Set(entry.clone()),
        ];

        self.execute_ops(ops)?;
        Ok(entry)
    }

    pub fn get(&self, key: &str) -> Option<Entry> {
        self.core
            .data()
            .get(key)
            .cloned()
            .filter(|entry| entry.value.is_some())
    }

    /// Get items including tombstones by key (for debugging)
    pub fn get_including_tombstones(&self, key: &str) -> Option<Entry> {
        self.core.data().get(key).cloned()
    }

    /// Get items by prefix (for range scans)
    pub fn get_by_prefix(&self, prefix: &str) -> HashMap<String, Entry> {
        let mut result = HashMap::new();

        // Use BTreeMap's range to efficiently scan by prefix
        for (k, v) in self.core.data().range(prefix.to_string()..) {
            if !k.starts_with(prefix) {
                break;
            }
            if v.value.is_some() {
                result.insert(k.clone(), v.clone());
            }
        }
        result
    }

    pub fn get_all_including_tombstones(&self) -> HashMap<String, Entry> {
        self.core
            .data()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn delete(&mut self, key: String) -> Result<Option<Entry>> {
        let meta = self.alloc_entry_meta();
        let tombstone = Entry::new(key.clone(), None, meta);

        let previous = self.core.data().get(&key).cloned();

        // Compile to StateOp sequence
        let ops = vec![
            StateOp::PushPeerLog {
                peer_id: self.id,
                entry: tombstone.clone(),
                max_entries: self.max_log_entries,
            },
            StateOp::Set(tombstone),
        ];

        self.execute_ops(ops)?;
        Ok(previous)
    }

    /// Get missing log entries for a peer based on their progress
    /// If peer has progress for all nodes, returns only missing entries
    /// Otherwise returns all entries across all nodes that peer hasn't seen yet
    pub fn get_peer_missing_logs(
        &self,
        peer_progress: &HashMap<NodeId, u64>,
    ) -> Option<Vec<Entry>> {
        // If peer has no progress at all, return None to indicate full dump needed
        if peer_progress.is_empty() {
            debug!("Peer has no progress, returning full dump");
            return None;
        }

        let mut missing_entries = Vec::new();

        // Send logs from all nodes that we have
        for (node_id, peer_state) in self.core.peers().iter() {
            let node_log = &peer_state.log;
            let peer_ack = peer_progress.get(node_id).cloned().unwrap_or(0);

            // Check if requested start_seq is still available (not truncated)
            if let Some(oldest_entry) = node_log.front() {
                if peer_ack < oldest_entry.meta.seq {
                    if node_id == &self.id {
                        // Requested seq has been truncated, need full dump
                        debug!(
                            node_id,
                            peer_ack,
                            oldest_log = oldest_entry.meta.seq,
                            "Requested my seq has been truncated, need full dump"
                        );
                        return None;
                    } else {
                        debug!(
                            node_id,
                            peer_ack,
                            oldest_log = oldest_entry.meta.seq,
                            "Requested peer seq has been truncated, skipping"
                        );
                        continue;
                    }
                }
            }

            // Collect entries after the start sequence
            for entry in node_log {
                if entry.meta.seq > peer_ack {
                    missing_entries.push(entry.clone());
                }
            }
        }

        Some(missing_entries)
    }

    /// Convert current KV state to log entries (for full dump)
    pub fn kv_to_log_entries(&self) -> Vec<Entry> {
        self.core.data().values().cloned().collect()
    }

    /// Get local_ack for all nodes (for sync message)
    /// Returns: HashMap<NodeId, u64> where value is local_ack
    pub fn get_local_ack(&self) -> HashMap<NodeId, u64> {
        // Return our local_ack for all nodes: how far we've synced each node's logs
        // For self, local_ack represents how far we've "synced" our own logs (i.e., generated)
        self.core
            .peers()
            .iter()
            .map(|(id, peer_state)| (*id, peer_state.local_ack))
            .collect()
    }

    /// Get peer node IDs (excluding self)
    pub fn get_peers(&self) -> Vec<NodeId> {
        self.core
            .peers()
            .keys()
            .filter(|&&id| id != self.id)
            .copied()
            .collect()
    }

    /// Add a new peer node (dynamic membership)
    pub fn add_peer(&mut self, peer_id: NodeId) -> Result<bool> {
        if self.core.peers().contains_key(&peer_id) {
            return Ok(false);
        }
        let ops = vec![StateOp::AddPeer { peer_id }];
        self.execute_ops(ops)?;
        info!("Added peer node: {}", peer_id);
        Ok(true)
    }

    /// Remove a peer node (dynamic membership)
    pub fn remove_peer(&mut self, peer_id: NodeId) -> Result<bool> {
        if peer_id == self.id || !self.core.peers().contains_key(&peer_id) {
            return Ok(false);
        }
        let ops = vec![StateOp::RemovePeer { peer_id }];
        self.execute_ops(ops)?;
        info!("Removed peer node: {}", peer_id);
        Ok(true)
    }

    /// Get all known nodes (all keys in peers map)
    pub fn get_all_nodes(&self) -> Vec<NodeId> {
        self.core.peers().keys().copied().collect()
    }

    /// Get peer state for a specific node
    pub fn get_peer_state(&self, node_id: NodeId) -> Option<PeerState> {
        self.core.peers().get(&node_id).cloned()
    }

    /// Get peer state for a specific node
    pub fn get_peer_logs_since(&self, node_id: NodeId, since: u64) -> Option<Vec<Entry>> {
        let peer_state = self.core.peers().get(&node_id)?;
        let log = &peer_state.log;
        // Find first entry with seq > since
        let since_index = log.iter().position(|entry| entry.meta.seq > since)?;
        Some(log.iter().skip(since_index).cloned().collect())
    }

    /// Cleanup expired tombstones
    pub fn cleanup_expired_tombstones(&mut self, ttl: Duration) -> Result<usize> {
        let now = Utc::now().timestamp_millis();
        let ttl_ms = ttl.as_millis() as i64;

        let expired_keys = self
            .core
            .data()
            .iter()
            .flat_map(|(k, item)| {
                if item.is_expired_tombstone(ttl_ms, now) {
                    Some(StateOp::Clear(k.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let removed = expired_keys.len();
        self.execute_ops(expired_keys)?;
        Ok(removed)
    }

    /// Get a snapshot of all logs (for demo/debugging)
    pub fn get_log_snapshot(&self) -> Vec<Entry> {
        let mut all_entries = Vec::new();

        for (_node_id, peer_state) in self.core.peers().iter() {
            let entries = &peer_state.log;
            all_entries.extend(entries.iter().cloned());
        }

        // Sort by timestamp and node_id for consistent ordering
        all_entries.sort_by_key(|e| (e.meta.timestamp, e.meta.node, e.meta.seq));

        all_entries
    }

    /// Update next_seq to ensure it's at least the given value
    /// Used during bootstrap to avoid sequence number reuse
    pub fn ensure_next_seq(&mut self, min_next_seq: u64) {
        let ops = vec![StateOp::SetNextSeq(min_next_seq)];
        let _ = self.execute_ops(ops);
    }

    /// Get current next_seq value (for debugging/monitoring)
    pub fn get_next_seq(&self) -> u64 {
        self.core.next_seq()
    }

    /// Get all peer states (for bootstrap scanning)
    pub fn get_all_peer_states(&self) -> &HashMap<NodeId, PeerState> {
        &self.core.peers()
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

    /// Get node status (for debugging/monitoring)
    pub fn status(&self) -> NodeStatus {
        let peers: Vec<PeerStatus> = self
            .core
            .peers()
            .iter()
            .map(|(peer_id, peer_state)| PeerStatus {
                id: *peer_id,
                ack: peer_state.local_ack,
                pack: peer_state.peer_ack,
                logs: peer_state.log.len(),
            })
            .collect();

        NodeStatus {
            id: self.id,
            n_kvs: self.core.data().len(),
            next_seq: self.core.next_seq(),
            dirty: self.dirty,
            wal: self.wal.is_some(),
            peers,
        }
    }
}

impl Node {
    pub fn new(id: NodeId, peer_ids: Vec<NodeId>) -> Self {
        let core = CoreState::new(id, peer_ids);

        let state = NodeState {
            id,
            core,
            wal: None, // No WAL by default
            max_log_entries: DEFAULT_MAX_LOG_ENTRIES,
            snapshot_path: None,
            watchers: Vec::new(),
            dirty: false,
        };

        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Create a new store with persistence enabled
    pub fn new_with_persistence<P: Into<PathBuf>>(
        id: NodeId,
        peers: Vec<NodeId>,
        data_dir: P,
    ) -> Result<Self> {
        let data_dir = data_dir.into();
        let wal_path = data_dir.join(format!("node_{id}.wal"));
        let snapshot_path = data_dir.join(format!("node_{id}.snapshot"));

        // Ensure directory exists
        if let Some(parent) = wal_path.parent() {
            fs_err::create_dir_all(parent)?;
        }

        let wal = WriteAheadLog::new(&wal_path, id)?;

        // Read existing log operations for recovery
        let existing_ops = wal.read_all_ops()?;

        // Initialize core state
        let core = CoreState::new(id, peers);

        let mut state = NodeState {
            id,
            core,
            wal: Some(wal),
            max_log_entries: DEFAULT_MAX_LOG_ENTRIES,
            snapshot_path: Some(snapshot_path.clone()),
            watchers: Vec::new(),
            dirty: false,
        };

        let loaded = state.load_snapshot_if_exists()?;
        if loaded {
            info!("Loaded snapshot from {}", snapshot_path.display());
            let status = state.status();
            info!("Node status: {:#?}", status);
        }

        // Recover from WAL if needed
        if !existing_ops.is_empty() {
            info!(
                "Recovering {} state operations from WAL",
                existing_ops.len()
            );
            state.replay_ops(existing_ops)?;
            let status = state.status();
            info!("Node status after recovery: {:#?}", status);
        }

        let store = Self {
            state: Arc::new(RwLock::new(state)),
        };

        info!(
            "Persistence enabled: WAL={:?}, snapshot={:?}",
            wal_path, snapshot_path
        );
        Ok(store)
    }

    pub fn write(&self) -> RwLockWriteGuard<NodeState> {
        self.state.write().expect("Failed to lock store state")
    }

    pub fn read(&self) -> RwLockReadGuard<NodeState> {
        self.state.read().expect("Failed to lock store state")
    }

    /// Persist current state to disk snapshot (resets WAL)
    pub fn persist(&self) -> Result<()> {
        self.write().persist_to_disk()
    }

    pub fn persist_if_dirty(&self) -> Result<bool> {
        self.write().persist_if_dirty()
    }

    /// Watch a specific key for changes. Receiver notifies on any change to the key.
    pub fn watch(&self, key: &str) -> watch::Receiver<()> {
        self.write().watch_key(key)
    }

    /// Watch keys sharing a prefix. Receiver notifies on any change to matching keys.
    pub fn watch_prefix(&self, prefix: &str) -> watch::Receiver<()> {
        self.write().watch_prefix(prefix)
    }
}
