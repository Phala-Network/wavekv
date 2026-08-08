use crate::types::{Entry, NodeId, PeerState};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Atomic state operations - lowest level instructions that mutate CoreState
///
/// # Wire compatibility
///
/// This enum is persisted in the WAL via bincode, which encodes enum variants by
/// **index**. The variant order below is therefore frozen: reordering or inserting a
/// variant silently reinterprets every existing WAL record. New variants may only be
/// appended.
///
/// v2 writes only [`StateOp::Set`] and [`StateOp::Clear`] — a strict subset of the v1
/// op set, so a v1 binary replays a v2 WAL natively. The remaining variants are
/// retained solely so that v2 can replay a WAL written by v1; see
/// [`CoreState::execute`] for how each is folded into v2 state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateOp {
    /// Set a KV entry (used for Put/Delete/Sync - all just set an entry)
    Set(Entry),

    /// Clear a key from storage (used for tombstone GC)
    Clear(String),

    /// v1 only: peer tells us how far they've synced our logs.
    /// Replayed by v2 as a `peer_acks[peer_id][self]` update.
    UpdatePeerAck {
        peer_id: NodeId,
        ack_seq: u64,
        monotonic: bool,
    },

    /// v1 only: we've synced this peer's logs up to this sequence.
    /// Replayed by v2 as an `acks[peer_id]` update.
    UpdateLocalAck {
        peer_id: NodeId,
        ack_seq: u64,
        monotonic: bool,
    },

    /// v1 only: append to a peer's log. v2 has no logs; replay folds the entry's seq
    /// into `acks[peer_id]`. The entry's data arrives via the paired `Set`.
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

impl StateOp {
    /// Whether v2 persists this op. v2's WAL is `Set`/`Clear` only: ack bookkeeping is
    /// volatile (a lost ack costs one larger delta, nothing more) and `next_seq` is
    /// recovered from the replayed entries themselves.
    pub fn is_durable(&self) -> bool {
        matches!(self, StateOp::Set(_) | StateOp::Clear(_))
    }
}

/// The v1 on-disk shape of [`CoreState`], used verbatim as the snapshot container so
/// that a v1 binary can load a snapshot written by v2 (RFC 8.3).
///
/// `peers` is reconstructed from v2's ack maps with **empty log deques**: a v1 node
/// reading this simply answers early pulls with full dumps until its logs repopulate,
/// which is correct, just less incremental.
#[derive(Serialize, Deserialize)]
struct CoreStateV1Repr {
    data: BTreeMap<String, Entry>,
    peers: HashMap<NodeId, PeerState>,
    next_seq: u64,
}

/// Core state: the minimal state that defines the database.
///
/// v2 replaces v1's per-origin log buckets with `origin_index`, a secondary index over
/// the live data map. The index answers the only question the logs ever answered —
/// "which writes by origin `n` above seq `s` does a peer still need?" — without storing
/// a second copy of every entry, and without ever truncating.
#[derive(Debug, Clone)]
pub struct CoreState {
    /// KV storage using BTreeMap for prefix scanning support.
    data: BTreeMap<String, Entry>,

    /// Secondary index `(origin, seq) -> key` over the live entries in `data`.
    ///
    /// Derived state: maintained on every `Set`/`Clear` and rebuilt from `data` on
    /// load. Never persisted.
    origin_index: BTreeMap<(NodeId, u64), String>,

    /// `acks[n]`: the largest seq of origin `n`'s writes this node provably covers.
    /// See the coverage invariant (INV) in RFC 0001 section 3.4.
    acks: HashMap<NodeId, u64>,

    /// `peer_acks[p]`: the ack map peer `p` reported in its last envelope. A cache used
    /// to size outgoing deltas; volatile, and safe to lose or under-report.
    peer_acks: HashMap<NodeId, HashMap<NodeId, u64>>,

    /// Cluster membership. Tracked separately from `acks` so that a peer that has never
    /// been heard from is still a known peer (it pins tombstone GC).
    members: Vec<NodeId>,

    /// Sequence generator for this node's log IDs.
    next_seq: u64,

    /// This node's id. Needed to project `peer_acks` back into v1's `PeerState.peer_ack`
    /// on serialize. Not part of the persisted shape (the snapshot header carries it).
    id: NodeId,
}

impl CoreState {
    pub fn new(node_id: NodeId, peer_ids: Vec<NodeId>) -> Self {
        let mut members = vec![node_id];
        for peer_id in peer_ids {
            if peer_id != node_id && !members.contains(&peer_id) {
                members.push(peer_id);
            }
        }

        Self {
            data: BTreeMap::new(),
            origin_index: BTreeMap::new(),
            acks: HashMap::new(),
            peer_acks: HashMap::new(),
            members,
            next_seq: 1,
            id: node_id,
        }
    }

    /// Re-attach the owning node id after deserializing a snapshot (the id lives in the
    /// snapshot header, not in the state body).
    pub fn set_id(&mut self, id: NodeId) {
        self.id = id;
        if !self.members.contains(&id) {
            self.members.push(id);
        }
    }

    /// Rebuild `origin_index` from `data`. Called after any bulk load.
    pub fn rebuild_origin_index(&mut self) {
        self.origin_index = self
            .data
            .iter()
            .map(|(key, entry)| ((entry.meta.node, entry.meta.seq), key.clone()))
            .collect();
    }

    fn index_remove_for_key(&mut self, key: &str) {
        if let Some(existing) = self.data.get(key) {
            self.origin_index
                .remove(&(existing.meta.node, existing.meta.seq));
        }
    }

    pub fn is_noop(&self, op: &StateOp) -> bool {
        match op {
            StateOp::Set(_) => false,
            StateOp::Clear(key) => !self.data.contains_key(key),
            StateOp::UpdatePeerAck { .. }
            | StateOp::UpdateLocalAck { .. }
            | StateOp::PushPeerLog { .. } => false,
            StateOp::IncrementSeq => false,
            StateOp::SetNextSeq(seq) => self.next_seq >= *seq,
            StateOp::AddPeer { peer_id } => self.members.contains(peer_id),
            StateOp::RemovePeer { peer_id } => !self.members.contains(peer_id),
        }
    }

    /// Execute a state operation - the only way to mutate CoreState.
    pub fn execute(&mut self, op: StateOp) {
        match op {
            StateOp::Set(entry) => {
                self.index_remove_for_key(&entry.key);
                self.origin_index
                    .insert((entry.meta.node, entry.meta.seq), entry.key.clone());
                self.data.insert(entry.key.clone(), entry);
                // Deliberately does NOT touch `acks`. Holding entry (n, s) does not
                // mean covering (n, s): an unseen (n, s') with s' < s may still be
                // needed. Acks move only via own writes or R1 batch-then-ack adoption.
            }
            StateOp::Clear(key) => {
                self.index_remove_for_key(&key);
                self.data.remove(&key);
            }
            // ---- v1 WAL replay only, below ----
            StateOp::UpdatePeerAck {
                peer_id, ack_seq, ..
            } => {
                let my_id = self.id;
                let slot = self.peer_acks.entry(peer_id).or_default();
                let current = slot.entry(my_id).or_insert(0);
                *current = (*current).max(ack_seq);
            }
            StateOp::UpdateLocalAck {
                peer_id, ack_seq, ..
            } => {
                self.bump_ack(peer_id, ack_seq);
            }
            StateOp::PushPeerLog { peer_id, entry, .. } => {
                self.bump_ack(peer_id, entry.meta.seq);
            }
            StateOp::AddPeer { peer_id } => {
                if !self.members.contains(&peer_id) {
                    self.members.push(peer_id);
                }
            }
            StateOp::RemovePeer { peer_id } => {
                self.members.retain(|&id| id != peer_id);
                self.acks.remove(&peer_id);
                self.peer_acks.remove(&peer_id);
            }
            StateOp::IncrementSeq => {
                self.next_seq += 1;
            }
            StateOp::SetNextSeq(seq) => {
                self.next_seq = self.next_seq.max(seq);
            }
        }
    }

    /// Raise `acks[origin]` monotonically. Lowering an ack is never done implicitly;
    /// see [`CoreState::reset_ack`] for the deliberate repair path.
    pub fn bump_ack(&mut self, origin: NodeId, seq: u64) {
        let slot = self.acks.entry(origin).or_insert(0);
        *slot = (*slot).max(seq);
    }

    /// Deliberately lower (or clear) an ack to force retransmission.
    ///
    /// Always safe: the data map is never truncated, so the peer can always re-derive
    /// whatever the lowered ack asks for. This is the repair half of the divergence
    /// loop in RFC 0001 section 3.6.
    pub fn reset_ack(&mut self, origin: Option<NodeId>) {
        match origin {
            Some(origin) => {
                self.acks.remove(&origin);
            }
            None => self.acks.clear(),
        }
    }

    pub fn acks(&self) -> &HashMap<NodeId, u64> {
        &self.acks
    }

    pub fn ack_for(&self, origin: NodeId) -> u64 {
        self.acks.get(&origin).copied().unwrap_or(0)
    }

    /// Adopt a peer's reported ack map monotonically (R1/R2 are enforced by the caller).
    pub fn adopt_acks(&mut self, reported: &HashMap<NodeId, u64>) {
        for (&origin, &seq) in reported {
            self.bump_ack(origin, seq);
        }
    }

    pub fn record_peer_acks(&mut self, peer: NodeId, acks: HashMap<NodeId, u64>) {
        self.peer_acks.insert(peer, acks);
    }

    pub fn peer_acks_for(&self, peer: NodeId) -> Option<&HashMap<NodeId, u64>> {
        self.peer_acks.get(&peer)
    }

    pub fn forget_peer_acks(&mut self, peer: NodeId) {
        self.peer_acks.remove(&peer);
    }

    pub fn peer_acks(&self) -> &HashMap<NodeId, HashMap<NodeId, u64>> {
        &self.peer_acks
    }

    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    pub fn data(&self) -> &BTreeMap<String, Entry> {
        &self.data
    }

    pub fn origin_index(&self) -> &BTreeMap<(NodeId, u64), String> {
        &self.origin_index
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Highest seq this node has ever authored, recovered from the live index.
    /// Replaces v1's scan over every log bucket plus the data map (RFC 3.7).
    pub fn max_own_seq(&self) -> u64 {
        self.origin_index
            .range((self.id, 0)..=(self.id, u64::MAX))
            .next_back()
            .map(|((_, seq), _)| *seq)
            .unwrap_or(0)
    }
}

impl Serialize for CoreState {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut peers: HashMap<NodeId, PeerState> = HashMap::new();
        for &member in &self.members {
            peers.insert(
                member,
                PeerState {
                    local_ack: self.ack_for(member),
                    peer_ack: self
                        .peer_acks
                        .get(&member)
                        .and_then(|m| m.get(&self.id))
                        .copied()
                        .unwrap_or(0),
                    log: Default::default(),
                },
            );
        }
        // Origins we have data from but that are not (or no longer) members still need
        // their ack recorded, otherwise a rollback to v1 would re-request their history.
        for (&origin, &ack) in &self.acks {
            peers.entry(origin).or_insert_with(|| PeerState {
                local_ack: ack,
                peer_ack: 0,
                log: Default::default(),
            });
        }

        CoreStateV1Repr {
            data: self.data.clone(),
            peers,
            next_seq: self.next_seq,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CoreState {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = CoreStateV1Repr::deserialize(deserializer)?;

        let mut acks: HashMap<NodeId, u64> = HashMap::new();
        let mut members: Vec<NodeId> = Vec::new();
        for (&node, peer_state) in &repr.peers {
            members.push(node);
            let mut ack = peer_state.local_ack;
            // A v1 snapshot may carry log entries beyond local_ack; they are covered
            // data, so fold them in rather than re-requesting them.
            for entry in &peer_state.log {
                if entry.meta.node == node {
                    ack = ack.max(entry.meta.seq);
                }
            }
            acks.insert(node, ack);
        }
        members.sort_unstable();

        let mut state = Self {
            data: repr.data,
            origin_index: BTreeMap::new(),
            acks,
            peer_acks: HashMap::new(),
            members,
            next_seq: repr.next_seq,
            // Overwritten by set_id() once the caller knows the owning node.
            id: 0,
        };
        state.rebuild_origin_index();
        // `acks` is taken from v1's `local_ack` (plus any log entries) and nothing
        // else. Deriving acks from the data map would over-claim coverage: holding
        // (n, s) says nothing about an unseen, unsuperseded (n, s') with s' < s.
        Ok(state)
    }
}
