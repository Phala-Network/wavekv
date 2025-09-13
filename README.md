# WaveKV

WaveKV is a small Rust playground for building a log-driven, eventually consistent key-value store. It focuses on clarity over completeness: there are no transactions, no quorum reads/writes, and conflict resolution is the simplest possible last-writer-wins rule.

## What it actually does

- **Single Source of Truth**: Every mutation becomes a `StateOp` and is appended to a write-ahead log (WAL). Recovery replays the exact same ops.
- **Simple Core State**: `CoreState` only tracks three things—KV data, peer metadata (logs + ack progress), and `next_seq`.
- **Eventual Sync**: Nodes exchange incremental logs plus snapshots when peers fall behind. A background loop keeps trying until it works.
- **Auto Discovery**: Incoming sync traffic automatically registers the sender as a peer, so a new node can simply point at any existing node and learn about the rest.

None of this is production-grade. There is no authentication, no durability guarantees beyond “fsync the WAL”, and no attempt at transactional isolation.

## Getting started

```
git clone https://github.com/kvinwang/wavekv.git
cd wavekv

cargo fmt
cargo clippy --all-targets
cargo test --lib
```

Those commands build the crate, lint it, and run the current unit tests. Everything else in this repo (WAL tooling, snapshot loader, sync loops) lives under `src/` and can be embedded into your own experiments.

## Debug helpers (optional)

There is a `filesync` binary under `src/bin/` that mirrors a local directory into WaveKV. It exists to exercise the replication path on a laptop. Typical workflow:

```
cargo build --release --bin filesync
./scripts/test-cluster.sh   # launches three demo nodes inside tmux
./scripts/test-ops.sh       # generates local file churn
cargo run --release --bin filesync -- check --dirs DIR1,DIR2,DIR3
```

The scripts configure a minimal chain of `--peers` flags so you can watch nodes auto-discover each other via the shared KV store. Feel free to ignore this section if you only care about the embedded library.

## Limitations (by design)

- **No transactions or CAS**: last writer wins, based on `(timestamp, origin node id)`.
- **Best-effort durability**: WAL preallocates and fsyncs, but there are no checkpoints beyond the snapshot files used by the demo scripts.
- **Backpressure**: Logs truncate only when all peers acknowledge entries. If a peer disappears forever, logs will grow until you remove it manually.

## License

MIT License – see `LICENSE` for full text.
