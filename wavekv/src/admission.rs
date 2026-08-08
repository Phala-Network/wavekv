//! Ingest hardening (RFC 0001 section 3.8).
//!
//! v1 applied any entry a peer sent. v2 enforces quotas and an optional embedder policy
//! inside `merge`, which is the only place that covers both sync directions — a
//! transport-level check on the request path cannot see entries arriving in a response.

use crate::types::Entry;
use std::sync::Arc;
use std::time::Duration;

/// Outcome of an admission check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    Accept,
    Reject { reason: &'static str },
}

/// Embedder-supplied policy, e.g. enforcing a key-prefix schema so that a compromised
/// or buggy peer cannot flood the replicated namespace.
pub trait AdmissionPolicy: Send + Sync {
    fn admit(&self, entry: &Entry) -> Admission;
}

impl<F> AdmissionPolicy for F
where
    F: Fn(&Entry) -> Admission + Send + Sync,
{
    fn admit(&self, entry: &Entry) -> Admission {
        self(entry)
    }
}

/// Hard quotas applied to every merged entry.
#[derive(Debug, Clone)]
pub struct Limits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_keys: usize,
    pub max_total_bytes: usize,
    /// How far into the future a peer's `timestamp` may be before the entry is
    /// rejected. Under raw LWW a node with a runaway clock can poison keys unfixably
    /// until real time catches up; clamping bounds the blast radius.
    pub max_clock_drift: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_key_bytes: 1024,
            max_value_bytes: 1024 * 1024,
            max_keys: 1_000_000,
            max_total_bytes: 1024 * 1024 * 1024,
            max_clock_drift: Duration::from_secs(300),
        }
    }
}

impl Limits {
    /// Quota checks that depend only on the entry itself.
    pub fn check_entry(&self, entry: &Entry) -> Admission {
        if entry.key.len() > self.max_key_bytes {
            return Admission::Reject {
                reason: "key exceeds max_key_bytes",
            };
        }
        if let Some(value) = &entry.value {
            if value.len() > self.max_value_bytes {
                return Admission::Reject {
                    reason: "value exceeds max_value_bytes",
                };
            }
        }
        Admission::Accept
    }

    /// Reject entries stamped too far in the future relative to local wall time.
    ///
    /// Only the future side is bounded: an entry from the past is harmless under LWW
    /// (it simply loses), while a far-future stamp wins against every honest write.
    pub fn check_clock(&self, entry: &Entry, now_ms: i64) -> Admission {
        let drift_ms = self.max_clock_drift.as_millis().min(i64::MAX as u128) as i64;
        if entry.meta.timestamp.saturating_sub(now_ms) > drift_ms {
            return Admission::Reject {
                reason: "timestamp exceeds max_clock_drift",
            };
        }
        Admission::Accept
    }

    /// Capacity checks that depend on the store as a whole.
    pub fn check_capacity(&self, current_keys: usize, current_bytes: usize) -> Admission {
        if current_keys >= self.max_keys {
            return Admission::Reject {
                reason: "store exceeds max_keys",
            };
        }
        if current_bytes >= self.max_total_bytes {
            return Admission::Reject {
                reason: "store exceeds max_total_bytes",
            };
        }
        Admission::Accept
    }
}

/// Tuning knobs for a v2 node.
#[derive(Clone)]
pub struct NodeConfig {
    pub limits: Limits,
    pub admission: Option<Arc<dyn AdmissionPolicy>>,
    /// Consecutive rounds with empty deltas but unequal digests before the node
    /// declares divergence and forces a full re-exchange.
    pub digest_check_rounds: u32,
    /// Coalescing window for opportunistic push. `None` disables push entirely.
    pub coalesce_window: Option<Duration>,
    pub max_delta_entries: usize,
    pub max_delta_bytes: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            admission: None,
            digest_check_rounds: 3,
            coalesce_window: Some(Duration::from_millis(200)),
            max_delta_entries: 4096,
            max_delta_bytes: 4 * 1024 * 1024,
        }
    }
}

impl std::fmt::Debug for NodeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeConfig")
            .field("limits", &self.limits)
            .field("admission", &self.admission.is_some())
            .field("digest_check_rounds", &self.digest_check_rounds)
            .field("coalesce_window", &self.coalesce_window)
            .field("max_delta_entries", &self.max_delta_entries)
            .field("max_delta_bytes", &self.max_delta_bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Entry, Metadata};

    fn entry(key: &str, value: Vec<u8>, ts: i64) -> Entry {
        Entry::new(key.to_string(), Some(value), Metadata::new(1, 1, ts))
    }

    #[test]
    fn oversized_keys_and_values_are_rejected() {
        let limits = Limits {
            max_key_bytes: 4,
            max_value_bytes: 4,
            ..Default::default()
        };
        assert_eq!(
            limits.check_entry(&entry("toolong", vec![], 0)),
            Admission::Reject {
                reason: "key exceeds max_key_bytes"
            }
        );
        assert_eq!(
            limits.check_entry(&entry("k", vec![0; 5], 0)),
            Admission::Reject {
                reason: "value exceeds max_value_bytes"
            }
        );
        assert_eq!(
            limits.check_entry(&entry("k", vec![0; 4], 0)),
            Admission::Accept
        );
    }

    #[test]
    fn future_stamps_are_bounded_but_past_stamps_are_not() {
        let limits = Limits {
            max_clock_drift: Duration::from_secs(60),
            ..Default::default()
        };
        let now = 1_000_000i64;
        assert_eq!(
            limits.check_clock(&entry("k", vec![], now + 61_000), now),
            Admission::Reject {
                reason: "timestamp exceeds max_clock_drift"
            }
        );
        assert_eq!(
            limits.check_clock(&entry("k", vec![], now + 59_000), now),
            Admission::Accept
        );
        assert_eq!(
            limits.check_clock(&entry("k", vec![], 0), now),
            Admission::Accept,
            "an old entry simply loses LWW; it needs no clock guard"
        );
    }

    #[test]
    fn a_closure_can_serve_as_a_policy() {
        let policy = |entry: &Entry| {
            if entry.key.starts_with("allowed/") {
                Admission::Accept
            } else {
                Admission::Reject { reason: "prefix" }
            }
        };
        assert_eq!(
            policy.admit(&entry("allowed/x", vec![], 0)),
            Admission::Accept
        );
        assert_eq!(
            policy.admit(&entry("other", vec![], 0)),
            Admission::Reject { reason: "prefix" }
        );
    }
}
