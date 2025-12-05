# WaveKV

WaveKV is an embeddable Rust library for building distributed key-value stores. It focuses on simplicity over completeness.

**Key characteristics:**
- **Peer-to-peer architecture**: All nodes have equal roles. No leader, no coordinator, no special node types.
- **No minimum cluster size**: Works with a single node or any number of nodes. Scale freely without reconfiguration.
- **Last-write-wins conflict resolution**: Conflicts are resolved automatically based on `(timestamp, origin node id)`. Simple and deterministic.
- **Eventually consistent**: Log-driven replication ensures all nodes converge to the same state. No transactions or quorum reads/writes.
- **Embeddable library**: Not a standalone application. Designed to be embedded into your Rust programs.
- **Core only**: Provides the replication algorithm and state management. Network transport is left to the application layer.
- **In-memory storage**: Keeps all data in memory. Best suited for small to medium datasets that fit in RAM.

## What it actually does

- **Single Source of Truth**: Every mutation becomes a `StateOp` and is appended to a write-ahead log (WAL). Recovery replays the exact same ops.
- **Simple Core State**: `CoreState` only tracks three things—KV data, peer metadata (logs + ack progress), and `next_seq`.
- **Eventual Sync**: Nodes exchange incremental logs plus snapshots when peers fall behind. A background loop keeps trying until it works.
- **Auto Discovery**: Incoming sync traffic automatically registers the sender as a peer, so a new node can simply point at any existing node and learn about the rest.


## Limitations and Non-goals

WaveKV intentionally does NOT provide:

- **Strong consistency or linearizability**: Only eventual consistency with last-writer-wins based on `(timestamp, origin node id)`.
- **ACID transactions or CAS**: No transactional isolation, no compare-and-swap operations.
- **Authentication or access control**: No built-in security mechanisms.
- **Production-grade durability**: WAL preallocates and fsyncs, but there are no comprehensive checkpoints beyond the snapshot files used by the demo scripts.
- **Network protocol implementation**: Transport layer is the application's responsibility.
- **Large dataset support**: In-memory design limits data size to available RAM.
- **Query language or secondary indexes**: Simple key-value interface only.
- **Encryption**: No encryption at rest or in transit.
- **Multi-datacenter replication**: Not optimized for wide-area network scenarios.
- **Performance optimization**: Simplicity is prioritized over raw performance.

## Debug helpers (optional)

There is a `filesync` binary under `src/bin/` that mirrors a local directory into WaveKV. It exists to exercise the replication path on a laptop. Typical workflow:

```
cargo build --release --bin filesync
./scripts/test-cluster.sh   # launches three demo nodes inside tmux
./scripts/test-ops.sh       # generates local file churn
```

The scripts configure a minimal chain of `--peers` flags so you can watch nodes auto-discover each other via the shared KV store. Feel free to ignore this section if you only care about the embedded library.

## License

Apache License Version 2.0 – see `LICENSE` for full text.
