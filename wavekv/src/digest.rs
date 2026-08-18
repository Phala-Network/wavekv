//! Canonical state digest (RFC 0001 section 3.6).
//!
//! v1 tracked only log positions, so a silently diverged replica and a healthy one were
//! indistinguishable and the divergence was permanent. The digest supplies the missing
//! detection half: two replicas that have converged produce equal digests by
//! construction, because LWW merge resolves every key to the same winning entry.

use crate::types::Entry;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// SHA-256 over the canonical encoding of the replicated state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateDigest {
    pub hash: [u8; 32],
}

impl StateDigest {
    /// Compute the digest over a data map in `BTreeMap` (lexicographic key) order.
    ///
    /// Tombstones are included — they are replicated state, and a replica that has
    /// *lost* a tombstone will resurrect the key. Per-node bookkeeping (`acks`,
    /// `peer_acks`, `next_seq`) is excluded: it legitimately differs between converged
    /// replicas.
    ///
    /// This does put the digest in tension with tombstone GC, which is uncoordinated: a
    /// replica that has *collected* a tombstone is tidy, not diverged, but is
    /// indistinguishable here from one that lost it. See
    /// [`NodeState::collect_tombstone_garbage`](crate::node::NodeState::collect_tombstone_garbage)
    /// for what that costs and why neither obvious fix is taken.
    ///
    /// The encoding is length-prefixed at every variable-width field so that no two
    /// distinct states can produce the same byte stream (e.g. a key `"ab"` with an
    /// empty value cannot collide with a key `"a"` whose value is `"b"`).
    pub fn compute(data: &BTreeMap<String, Entry>) -> Self {
        let mut hasher = Sha256::new();
        for (key, entry) in data {
            hasher.update((key.len() as u32).to_le_bytes());
            hasher.update(key.as_bytes());
            hasher.update(entry.meta.node.to_le_bytes());
            hasher.update(entry.meta.seq.to_le_bytes());
            hasher.update(entry.meta.timestamp.to_le_bytes());
            match &entry.value {
                None => hasher.update([0x00]),
                Some(value) => {
                    hasher.update([0x01]);
                    hasher.update((value.len() as u32).to_le_bytes());
                    hasher.update(value);
                }
            }
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&hasher.finalize());
        Self { hash }
    }

    pub fn to_hex(self) -> String {
        hex::encode(self.hash)
    }
}

impl std::fmt::Display for StateDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.hash[..8]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Metadata;

    fn entry(key: &str, value: Option<&[u8]>, node: u32, seq: u64, ts: i64) -> Entry {
        Entry::new(
            key.to_string(),
            value.map(|v| v.to_vec()),
            Metadata::new(node, seq, ts),
        )
    }

    fn map(entries: Vec<Entry>) -> BTreeMap<String, Entry> {
        entries.into_iter().map(|e| (e.key.clone(), e)).collect()
    }

    #[test]
    fn converged_replicas_agree_regardless_of_insertion_order() {
        let a = map(vec![
            entry("a", Some(b"1"), 1, 1, 100),
            entry("b", Some(b"2"), 2, 5, 200),
        ]);
        let b = map(vec![
            entry("b", Some(b"2"), 2, 5, 200),
            entry("a", Some(b"1"), 1, 1, 100),
        ]);
        assert_eq!(StateDigest::compute(&a), StateDigest::compute(&b));
    }

    #[test]
    fn tombstones_are_part_of_the_state() {
        let with = map(vec![entry("a", None, 1, 1, 100)]);
        let without = map(vec![]);
        assert_ne!(
            StateDigest::compute(&with),
            StateDigest::compute(&without),
            "a replica that dropped a tombstone has diverged and must be detectable"
        );
    }

    #[test]
    fn metadata_differences_are_visible() {
        let a = map(vec![entry("k", Some(b"v"), 1, 1, 100)]);
        let b = map(vec![entry("k", Some(b"v"), 1, 1, 101)]);
        assert_ne!(StateDigest::compute(&a), StateDigest::compute(&b));
    }

    #[test]
    fn length_prefixes_prevent_field_boundary_collisions() {
        // Without length prefixing, ("ab", "") and ("a", "b") would hash identically.
        let a = map(vec![entry("ab", Some(b""), 1, 1, 100)]);
        let b = map(vec![entry("a", Some(b"b"), 1, 1, 100)]);
        assert_ne!(StateDigest::compute(&a), StateDigest::compute(&b));
    }

    #[test]
    fn an_empty_value_differs_from_a_tombstone() {
        let empty = map(vec![entry("k", Some(b""), 1, 1, 100)]);
        let tomb = map(vec![entry("k", None, 1, 1, 100)]);
        assert_ne!(StateDigest::compute(&empty), StateDigest::compute(&tomb));
    }
}
