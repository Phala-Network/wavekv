# Formal proofs of the wavekv replication core

Lean 4 models of the pure core of `wavekv/src` (as of wavekv 2.2.0), with
machine-checked proofs.
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
| `MergeBatch` | `node.rs`: `merge_batch`, `StagedBatch`, `alloc_entry_meta`, `put`/`delete`; `admission.rs`: `check_clock` | the staged batch equals one-at-a-time merge; convergence under entry-only admission; a counterexample for clock admission; local writes always beat the held entry, so convergence includes them |
| `Delta` | `delta.rs`: `compute_delta` | resuming from each page cursor yields exactly the owed entries, once each |
| `TombstoneGc` | `node.rs`: `collect_tombstone_garbage` (two-phase `gc_marked`); `ops.rs`: `execute(Clear)` | GC clears only tombstones that every peer acked and the previous call marked; no older value is ever held again under the delivery bound |
| `Digest` | `digest.rs`: `StateDigest::compute` | the length-prefixed encoding is injective, so digests are equal iff states are |

Each module's header names the Rust functions it mirrors, its assumptions, and the
gap between model and code. Several Rust unit tests are restated as `example`s.

## Trusted assumptions

These are explicit hypotheses of the theorems that use them, never axioms:

- **Coherence:** a `(node, seq)` pair is never reused with a different entry
  (`Coherent`, `BatchCoherent`).
- **Ack soundness:** a peer's reported ack means it holds the acknowledged tombstone
  or something newer (`AckSound`, RFC 0001 INV). Closed membership is also assumed.
- **Delivery bound:** a message is delivered or dropped before its receiver's second
  GC call after it was sent. This is what two-phase GC relies on; a message delayed
  past two calls still resurrects the key (`delayed_message_resurrects`).
- **Hash injectivity:** SHA-256 is treated as injective on encoded states, an
  idealization of collision resistance. Field lengths must fit their `u32` prefixes.

## Model and code gap

- `BTreeMap`/`HashMap` become total functions or sorted lists. `u32`/`u64`/`i64`
  become `Nat`/`Int`, so the overflow branches are not modeled.
- Wall-clock time is a parameter. Capacity admission, WAL durability, persistence,
  and the transport are not modeled.
- The cluster model in `TombstoneGc` is an abstract step relation: send any held
  entries, deliver (with loss, duplication, and reordering), collect, and write.
  `no_resurrection` starts from a state where the delete has reached every member
  and no member has marked its copy yet (`inv_establish`). Reaching that state from
  the sync protocol (rules R1-R4) is not derived.

## Findings

Two issues these proofs found were fixed in wavekv 2.2.0 and are now proven fixed:
local writes stamped below the held entry (`clock_skew_scenario_converges`), and stale
messages delivered after GC (`stale_message_within_period_rejected`,
`no_resurrection`). One caveat remains: clock admission depends on local time, so
equal received sets do not imply equal states until clocks catch up
(`clock_admission_diverges`, `convergence_clock_caught_up`).
