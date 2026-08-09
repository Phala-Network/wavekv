//! WaveKV - An embeddable, eventually consistent, distributed key-value store
//!
//! WaveKV is a Rust library that provides an eventually consistent, in-memory
//! distributed key-value store core. It focuses on simplicity over completeness.
//!
//! # Key Features
//!
//! - **Peer-to-peer architecture**: All nodes have equal roles with no leader or coordinator
//! - **No minimum cluster size**: Works with any number of nodes (even just one)
//! - **Last-write-wins conflict resolution**: Simple and deterministic conflict resolution
//! - **Eventually consistent**: Delta-state replication ensures convergence
//! - **Embeddable**: Designed to be embedded into your Rust programs
//! - **Transport-agnostic**: Core only - you provide the network layer
//! - **In-memory**: Best suited for small to medium datasets that fit in RAM
//!
//! # Quick Start
//!
//! ```rust
//! use wavekv::Node;
//!
//! // Create a node with ID 1, knowing about peer 2
//! let node = Node::new(1, vec![2]);
//!
//! // Put and get values
//! let mut state = node.write();
//! state.put("key".to_string(), b"value".to_vec()).unwrap();
//! let entry = state.get("key").unwrap();
//! assert_eq!(entry.value.as_ref().unwrap(), b"value");
//! ```
//!
//! # Architecture
//!
//! WaveKV replicates *state*, not a log. Each node keeps the winning entry per key
//! plus an index over `(origin, seq)`, and a sync round ships the delta implied by the
//! peer's declared coverage (its `acks`). There are no per-peer logs to keep, bound or
//! replay, so a node that has fallen arbitrarily far behind costs one larger delta
//! rather than a special path. A write-ahead log still backs local durability, and a
//! state digest exchanged each round detects the silent divergence that log-driven
//! replication could not see. See `rfcs/0001-delta-state-sync.md`.
//!
//! The core types you'll work with:
//! - [`Node`] - Thread-safe wrapper around node state
//! - [`types::Entry`] - A key-value entry with metadata
//! - [`SyncEnvelope`] - One side of a sync round: entries, coverage and digest
//! - [`sync::SyncManager`] - Handles synchronization between nodes
//!
//! # Non-goals
//!
//! WaveKV intentionally does NOT provide:
//! - Strong consistency or linearizability
//! - ACID transactions
//! - Authentication or access control
//! - Production-grade durability guarantees
//! - Network transport layer
//! - Support for large datasets (limited by available RAM)
//!
//! For more details, see the [README](https://github.com/Phala-Network/wavekv).

pub mod admission;
pub mod delta;
pub mod digest;
pub mod node;
pub mod ops;
pub mod sync;
pub mod types;
pub mod wal;

pub use admission::{Admission, AdmissionPolicy, Limits, NodeConfig};
pub use delta::PageInfo;
pub use digest::StateDigest;
pub use node::Node;
pub use sync::SyncEnvelope;

#[cfg(test)]
mod tests;
