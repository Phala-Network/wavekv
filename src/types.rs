use std::{cmp::Ordering, collections::VecDeque};

use serde::{Deserialize, Serialize};
// LWW mode: no per-key vector clocks

pub type NodeId = u32;

/// Peer state tracking - includes all state for a node
/// Note: Node active/inactive status should be stored in KV store
/// using special keys (e.g., "_cluster:node:{id}:status") to ensure
/// consistent synchronization across all nodes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeerState {
    /// Local acknowledgment: we've synced this peer's logs up to this sequence
    /// (i.e., we've consumed their generated logs up to this point)
    pub local_ack: u64,

    /// Peer acknowledgment: this peer has synced our logs up to this sequence
    /// (i.e., they've consumed our generated logs up to this point)
    pub peer_ack: u64,

    /// Log entries from this peer (bucketed by node)
    pub log: VecDeque<Entry>,
}

impl PeerState {
    pub fn new() -> Self {
        Self {
            local_ack: 0,
            peer_ack: 0,
            log: VecDeque::new(),
        }
    }

    pub fn with_seq(seq: u64) -> Self {
        Self {
            local_ack: seq,
            peer_ack: seq,
            log: VecDeque::new(),
        }
    }

    pub fn push(&mut self, entry: Entry, max_entries: usize) {
        self.log.push_back(entry);
        while self.log.len() > max_entries {
            self.log.pop_front();
        }
    }
}

impl Default for PeerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Unified entry for both KV state and log entries
#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct Entry {
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub meta: Metadata,
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("key", &self.key)
            .field("value", &self.value.is_some())
            .field("meta", &self.meta)
            .finish()
    }
}

impl Entry {
    /// Create a new entry
    pub fn new(key: String, value: Option<Vec<u8>>, meta: Metadata) -> Self {
        Self { key, value, meta }
    }

    /// Create a new put entry
    pub fn new_put(meta: Metadata, key: String, value: Vec<u8>) -> Self {
        Self {
            key,
            value: Some(value),
            meta,
        }
    }

    /// Create a new delete entry (tombstone)
    pub fn new_delete(meta: Metadata, key: String) -> Self {
        Self {
            key,
            value: None,
            meta,
        }
    }

    /// Check if this entry represents a deletion
    pub fn is_deleted(&self) -> bool {
        self.value.is_none()
    }

    /// Check if this is an expired tombstone
    pub fn is_expired_tombstone(&self, ttl_ms: i64, current_time_ms: i64) -> bool {
        self.value.is_none() && (current_time_ms >= self.meta.timestamp.saturating_add(ttl_ms))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Metadata {
    pub node: NodeId,
    pub seq: u64,
    pub timestamp: i64,
}

impl Metadata {
    pub fn new(node: NodeId, seq: u64, timestamp: i64) -> Self {
        Self {
            node,
            seq,
            timestamp,
        }
    }
}

impl PartialOrd for Metadata {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Metadata {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.timestamp.cmp(&other.timestamp) {
            Ordering::Equal => match self.node.cmp(&other.node) {
                Ordering::Equal => self.seq.cmp(&other.seq),
                node_ord => node_ord,
            },
            timestamp_ord => timestamp_ord,
        }
    }
}

/// Compare two entries using LWW (Last Write Wins) rules
pub fn compare_entries(entry1: &Entry, entry2: &Entry) -> Ordering {
    entry1.meta.cmp(&entry2.meta)
}
