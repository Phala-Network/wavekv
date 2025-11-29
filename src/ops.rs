use crate::types::{Entry, NodeId, PeerState};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Atomic state operations - lowest level instructions that mutate CoreState
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateOp {
    /// Set a KV entry (used for Put/Delete/Sync - all just set an entry)
    Set(Entry),

    /// Clear a key from storage (used for tombstone GC)
    Clear(String),

    /// Update peer_ack: peer tells us how far they've synced our logs
    UpdatePeerAck {
        peer_id: NodeId,
        ack_seq: u64,
        monotonic: bool,
    },

    /// Update local_ack: we've synced this peer's logs up to this sequence
    UpdateLocalAck {
        peer_id: NodeId,
        ack_seq: u64,
        monotonic: bool,
    },

    /// Add entry to peer's log
    PushPeerLog {
        peer_id: NodeId,
        entry: Entry,
        max_entries: usize,
    },

    /// Increment next_seq
    IncrementSeq,

    /// Set next_seq to a specific value (for bootstrap recovery)
    SetNextSeq(u64),

    /// Add a new peer to the cluster
    AddPeer { peer_id: NodeId },

    /// Remove a peer from the cluster
    RemovePeer { peer_id: NodeId },
}

/// Core state: the minimal state that defines the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreState {
    /// KV storage using BTreeMap for prefix scanning support
    data: BTreeMap<String, Entry>,

    /// All nodes state (including self and peers)
    /// Each PeerState contains: local_ack, peer_ack, and log entries
    peers: HashMap<NodeId, PeerState>,

    /// Sequence generator for this node's log IDs
    next_seq: u64,
}

impl CoreState {
    pub fn new(node_id: NodeId, peer_ids: Vec<NodeId>) -> Self {
        let mut peers = HashMap::new();
        peers.insert(node_id, PeerState::new());
        for peer_id in peer_ids {
            if peer_id != node_id {
                peers.insert(peer_id, PeerState::new());
            }
        }

        Self {
            data: BTreeMap::new(),
            peers,
            next_seq: 1,
        }
    }

    fn peer_mut(&mut self, peer_id: NodeId) -> &mut PeerState {
        self.peers.entry(peer_id).or_insert_with(PeerState::new)
    }

    pub fn is_noop(&self, op: &StateOp) -> bool {
        match op {
            StateOp::Set(_) => false,
            StateOp::Clear(key) => self.data.get(key).is_none(),
            StateOp::UpdatePeerAck {
                peer_id,
                ack_seq,
                monotonic,
            } => {
                let Some(peer_state) = self.peers.get(peer_id) else {
                    return false;
                };
                let final_ack_seq = if *monotonic {
                    peer_state.peer_ack.max(*ack_seq)
                } else {
                    *ack_seq
                };
                peer_state.peer_ack == final_ack_seq
            }
            StateOp::UpdateLocalAck {
                peer_id,
                ack_seq,
                monotonic,
            } => {
                let Some(peer_state) = self.peers.get(peer_id) else {
                    return false;
                };
                let final_ack_seq = if *monotonic {
                    peer_state.local_ack.max(*ack_seq)
                } else {
                    *ack_seq
                };
                peer_state.local_ack == final_ack_seq
            }
            StateOp::PushPeerLog {
                peer_id,
                entry,
                max_entries: _,
            } => {
                let Some(peer_state) = self.peers.get(peer_id) else {
                    return false;
                };
                // Use local_ack for consistency with execute()
                entry.meta.seq <= peer_state.local_ack
            }
            StateOp::IncrementSeq => false,
            StateOp::SetNextSeq(seq) => self.next_seq == *seq,
            StateOp::AddPeer { peer_id } => self.peers.contains_key(&peer_id),
            StateOp::RemovePeer { peer_id } => !self.peers.contains_key(&peer_id),
        }
    }

    /// Execute a state operation - the only way to mutate CoreState
    pub fn execute(&mut self, op: StateOp) {
        match op {
            StateOp::Set(entry) => {
                self.data.insert(entry.key.clone(), entry);
            }
            StateOp::Clear(key) => {
                self.data.remove(&key);
            }
            StateOp::UpdatePeerAck {
                peer_id,
                ack_seq,
                monotonic,
            } => {
                let peer_state = self.peer_mut(peer_id);
                if monotonic {
                    peer_state.peer_ack = peer_state.peer_ack.max(ack_seq);
                } else {
                    peer_state.peer_ack = ack_seq;
                }
            }
            StateOp::UpdateLocalAck {
                peer_id,
                ack_seq,
                monotonic,
            } => {
                let peer_state = self.peer_mut(peer_id);
                if monotonic {
                    peer_state.local_ack = peer_state.local_ack.max(ack_seq);
                } else {
                    peer_state.local_ack = ack_seq;
                }
            }
            StateOp::PushPeerLog {
                peer_id,
                entry,
                max_entries,
            } => {
                let peer_state = self.peer_mut(peer_id);
                if entry.meta.seq <= peer_state.local_ack {
                    return;
                }
                peer_state.push(entry.clone(), max_entries);
                peer_state.local_ack = peer_state.local_ack.max(entry.meta.seq);
            }
            StateOp::AddPeer { peer_id } => {
                _ = self.peer_mut(peer_id);
            }
            StateOp::RemovePeer { peer_id } => {
                self.peers.remove(&peer_id);
            }
            StateOp::IncrementSeq => {
                self.next_seq += 1;
            }
            StateOp::SetNextSeq(seq) => {
                self.next_seq = self.next_seq.max(seq);
            }
        }
    }

    pub fn peers(&self) -> &HashMap<NodeId, PeerState> {
        &self.peers
    }

    pub fn data(&self) -> &BTreeMap<String, Entry> {
        &self.data
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}
