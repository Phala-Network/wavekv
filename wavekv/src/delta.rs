//! Delta computation over the live data map (RFC 0001 sections 3.1, 3.7).
//!
//! A delta is `{ e in data : e.meta.seq > acks[e.meta.node] }`. Because each entry is
//! itself a one-element LWW map, a delta is a valid mini-state: applying it is a merge,
//! hence idempotent, commutative, and tolerant of loss, duplication and reordering.
//!
//! The full dump degenerates into "delta against an all-zero ack map", so bootstrap and
//! steady state share one code path — deleting v1's least-tested branch.

use crate::types::{Entry, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Resume information for a delta that did not fit in one message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageInfo {
    /// Inclusive cursor: the last `(origin, seq)` carried by this page.
    pub cursor: (NodeId, u64),
    /// True on the final page. Per rule R2, a receiver may adopt the envelope's acks
    /// only from an unpaged envelope or from the final page.
    pub last: bool,
}

/// Approximate on-wire cost of one entry, used only for page sizing.
fn entry_weight(entry: &Entry) -> usize {
    // key + value + (node, seq, timestamp) + msgpack framing slack
    entry.key.len() + entry.value.as_ref().map_or(0, |v| v.len()) + 32
}

/// Result of a delta query.
#[derive(Debug, Clone)]
pub struct Delta {
    pub entries: Vec<Entry>,
    /// `None` when the delta is complete; `Some(page)` when it was truncated and the
    /// requester must ask again from `page.cursor`.
    pub page: Option<PageInfo>,
}

impl Delta {
    /// Whether the receiver is permitted to adopt the sender's ack map after merging
    /// this delta (rule R2).
    pub fn permits_ack_adoption(&self) -> bool {
        match &self.page {
            None => true,
            Some(page) => page.last,
        }
    }
}

/// Compute the delta owed to a peer whose coverage is described by `acks`.
///
/// Walks `origin_index` in `(origin, seq)` order, seeking past whole origins the peer
/// has already covered instead of filtering them one entry at a time — so the cost is
/// `O(origins * log n + |delta|)`, not `O(n)`.
///
/// `start_after` resumes a paginated scan; pass `None` for a fresh query.
pub fn compute_delta(
    data: &BTreeMap<String, Entry>,
    origin_index: &BTreeMap<(NodeId, u64), String>,
    acks: &HashMap<NodeId, u64>,
    start_after: Option<(NodeId, u64)>,
    max_entries: usize,
    max_bytes: usize,
) -> Delta {
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    let mut last_included: Option<(NodeId, u64)> = None;

    // Exclusive resume: begin just past the cursor.
    let mut cursor = match start_after {
        None => (NodeId::MIN, 0u64),
        Some((origin, seq)) => match seq.checked_add(1) {
            Some(next) => (origin, next),
            None => match origin.checked_add(1) {
                Some(next_origin) => (next_origin, 0),
                None => {
                    return Delta {
                        entries,
                        page: None,
                    }
                }
            },
        },
    };

    loop {
        let Some((&(origin, seq), key)) = origin_index.range(cursor..).next() else {
            // Scanned to the end: the delta is complete.
            return Delta {
                entries,
                page: None,
            };
        };

        let ack = acks.get(&origin).copied().unwrap_or(0);
        if seq <= ack {
            // This whole origin is covered up to `ack`; jump rather than scan.
            cursor = match ack.checked_add(1) {
                Some(next) => (origin, next),
                None => match origin.checked_add(1) {
                    Some(next_origin) => (next_origin, 0),
                    None => {
                        return Delta {
                            entries,
                            page: None,
                        }
                    }
                },
            };
            continue;
        }

        let Some(entry) = data.get(key) else {
            // origin_index is derived from data; a miss means the two drifted.
            debug_assert!(
                false,
                "origin_index references a key absent from data: {key}"
            );
            cursor = advance(origin, seq);
            continue;
        };

        let weight = entry_weight(entry);
        // Always emit at least one entry, otherwise an oversized entry stalls the scan
        // forever at the same cursor.
        if !entries.is_empty() && (entries.len() >= max_entries || bytes + weight > max_bytes) {
            return Delta {
                entries,
                page: Some(PageInfo {
                    cursor: last_included.unwrap_or((origin, seq)),
                    last: false,
                }),
            };
        }

        entries.push(entry.clone());
        bytes += weight;
        last_included = Some((origin, seq));
        cursor = advance(origin, seq);
    }
}

fn advance(origin: NodeId, seq: u64) -> (NodeId, u64) {
    match seq.checked_add(1) {
        Some(next) => (origin, next),
        None => (origin.saturating_add(1), 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Metadata;

    fn entry(key: &str, node: NodeId, seq: u64) -> Entry {
        Entry::new(
            key.to_string(),
            Some(vec![0u8; 4]),
            Metadata::new(node, seq, seq as i64),
        )
    }

    fn build(entries: Vec<Entry>) -> (BTreeMap<String, Entry>, BTreeMap<(NodeId, u64), String>) {
        let mut data = BTreeMap::new();
        let mut index = BTreeMap::new();
        for e in entries {
            index.insert((e.meta.node, e.meta.seq), e.key.clone());
            data.insert(e.key.clone(), e);
        }
        (data, index)
    }

    fn acks(pairs: &[(NodeId, u64)]) -> HashMap<NodeId, u64> {
        pairs.iter().copied().collect()
    }

    #[test]
    fn an_empty_ack_map_yields_the_whole_live_state() {
        let (data, index) = build(vec![entry("a", 1, 1), entry("b", 2, 7)]);
        let delta = compute_delta(&data, &index, &HashMap::new(), None, 100, 1 << 20);
        assert_eq!(
            delta.entries.len(),
            2,
            "bootstrap is just a delta against zero"
        );
        assert!(delta.permits_ack_adoption());
    }

    #[test]
    fn covered_origins_are_skipped() {
        let (data, index) = build(vec![entry("a", 1, 1), entry("b", 1, 2), entry("c", 2, 5)]);
        let delta = compute_delta(&data, &index, &acks(&[(1, 2)]), None, 100, 1 << 20);
        let keys: Vec<_> = delta.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, vec!["c"]);
    }

    #[test]
    fn seq_holes_are_not_treated_as_gaps() {
        // Origin 1 wrote seq 1..3 but only seq 3 survived; the rest were superseded.
        let (data, index) = build(vec![entry("k", 1, 3)]);
        let delta = compute_delta(&data, &index, &acks(&[(1, 0)]), None, 100, 1 << 20);
        assert_eq!(
            delta.entries.len(),
            1,
            "a hole is a superseded write, not missing data"
        );
    }

    #[test]
    fn pagination_resumes_without_loss_or_repetition() {
        let all: Vec<Entry> = (1..=10).map(|i| entry(&format!("k{i}"), 1, i)).collect();
        let (data, index) = build(all);

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let delta = compute_delta(&data, &index, &HashMap::new(), cursor, 3, 1 << 20);
            seen.extend(delta.entries.iter().map(|e| e.meta.seq));
            match delta.page {
                Some(page) => {
                    assert!(!page.last);
                    assert!(
                        !delta.permits_ack_adoption(),
                        "R2: no adoption mid-pagination"
                    );
                    cursor = Some(page.cursor);
                }
                None => break,
            }
        }
        assert_eq!(seen, (1..=10).collect::<Vec<_>>());
    }

    #[test]
    fn an_oversized_entry_still_makes_progress() {
        let big = Entry::new(
            "big".to_string(),
            Some(vec![0u8; 1000]),
            Metadata::new(1, 1, 1),
        );
        let (data, index) = build(vec![big, entry("small", 1, 2)]);
        let delta = compute_delta(&data, &index, &HashMap::new(), None, 100, 10);
        assert_eq!(
            delta.entries.len(),
            1,
            "a single entry over the byte budget must still be emitted, not stall"
        );
        assert!(delta.page.is_some());
    }

    #[test]
    fn scanning_is_stable_across_interleaved_origins() {
        let (data, index) = build(vec![
            entry("a", 1, 1),
            entry("b", 2, 1),
            entry("c", 1, 2),
            entry("d", 3, 9),
        ]);
        let delta = compute_delta(&data, &index, &acks(&[(1, 1), (3, 9)]), None, 100, 1 << 20);
        let mut keys: Vec<_> = delta.entries.iter().map(|e| e.key.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["b", "c"]);
    }
}
