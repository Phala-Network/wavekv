# Formal proofs of the wavekv replication core

Lean 4 models of the pure core of `wavekv/src`, with machine-checked proofs.
Core Lean only (no Mathlib). No `sorry`, `axiom`, `native_decide`, or unchecked code.
CI rejects all of these.

```sh
cd formal && lake build
```

## What is modeled

| Module | Rust | Main results |
| --- | --- | --- |
| `Metadata` | `types.rs`: `impl Ord for Metadata` | the `(timestamp, node, seq)` comparison is a total order |
| `Lww` | `node.rs`: `beats`, `sync`; `ops.rs`: `execute(Set)` | per-key and whole-state merge is a join-semilattice; `convergence` (strong eventual consistency) |
| `MergeBatch` | `node.rs`: `merge_batch`, `StagedBatch`, `put`/`delete`; `admission.rs`: `check_clock` | the staged batch equals one-at-a-time merge; convergence under entry-only admission; counterexamples for clock admission and for local writes |
| `Delta` | `delta.rs`: `compute_delta` | resuming from each page cursor yields exactly the owed entries, once each |
| `TombstoneGc` | `node.rs`: `collect_tombstone_garbage`; `ops.rs`: `execute(Clear)` | GC clears only fully acknowledged tombstones; no older value is ever held again |
| `Digest` | `digest.rs`: `StateDigest::compute` | the length-prefixed encoding is injective, so digests are equal iff states are |

Each module's header names the Rust functions it mirrors, its assumptions, and the
gap between model and code. Several Rust unit tests are restated as `example`s.

## Trusted assumptions

These are explicit hypotheses of the theorems that use them, never axioms:

- **Coherence:** a `(node, seq)` pair is never reused with a different entry
  (`Coherent`, `BatchCoherent`).
- **Ack soundness:** a peer's reported ack means it holds nothing older than the
  acknowledged tombstone (`AckSound`, RFC 0001 INV). The proofs also assume closed
  membership and that no stale message still carries the old value.
- **Hash injectivity:** SHA-256 is treated as injective on encoded states, an
  idealization of collision resistance. Field lengths must fit their `u32` prefixes.

## Model and code gap

- `BTreeMap`/`HashMap` become total functions or sorted lists. `u32`/`u64`/`i64`
  become `Nat`/`Int`, so the overflow branches are not modeled.
- Wall-clock time is a parameter. Capacity admission, WAL durability, persistence,
  and the transport are not modeled.
- The cluster model in `TombstoneGc` is an abstract step relation: send any held
  entries, deliver (with loss, duplication, and reordering), collect, and write.

## Findings

- `put`/`delete` apply `StateOp::Set` without `beats`. If a peer's clock runs ahead
  (within `max_clock_drift`), a local write stamped below the held entry shows up
  locally but loses on every peer. Since the writer has already acked the winner, the
  winner is not re-sent, and the replicas diverge until digest repair. Repair then
  discards the local write; for a delete, the key comes back
  (`MergeBatch.local_write_diverges`).
- Clock admission depends on local time, so equal received sets do not imply equal
  states until clocks catch up (`clock_admission_diverges`,
  `convergence_clock_caught_up`).
- Ack soundness alone does not rule out resurrection by a message built before the
  peer merged the tombstone and delivered after GC (`stale_message_resurrects`).
