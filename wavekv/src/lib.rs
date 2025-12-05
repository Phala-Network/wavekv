//! WaveKV - An embeddable, eventually consistent, distributed key-value store
//!
//! WaveKV is a Rust library that provides a log-driven, eventually consistent,
//! in-memory distributed key-value store core. It focuses on clarity over completeness.
//!
//! # Key Features
//!
//! - **Peer-to-peer architecture**: All nodes have equal roles with no leader or coordinator
//! - **No minimum cluster size**: Works with any number of nodes (even just one)
//! - **Last-write-wins conflict resolution**: Simple and deterministic conflict resolution
//! - **Eventually consistent**: Log-driven replication ensures convergence
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
//! WaveKV uses a log-driven architecture where every mutation becomes a `StateOp`
//! appended to a write-ahead log (WAL). Nodes exchange incremental logs and snapshots
//! to achieve eventual consistency.
//!
//! The core types you'll work with:
//! - [`Node`] - Thread-safe wrapper around node state
//! - [`types::Entry`] - A key-value entry with metadata
//! - [`types::PeerState`] - Tracks synchronization state for each peer
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

pub mod node;
pub mod ops;
pub mod sync;
pub mod types;
pub mod wal;

pub use node::Node;

#[cfg(test)]
mod tests;
