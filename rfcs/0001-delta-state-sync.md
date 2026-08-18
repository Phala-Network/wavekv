# RFC 0001: Delta-State Synchronization (WaveKV v2)

- **Status**: Implemented (wavekv 2.0)
- **Author(s)**: Kevin Wang
- **Created**: 2026-08-07
- **Target version**: wavekv 2.0
- **Related**: dstack-gateway (primary embedder)

## Summary

Replace WaveKV's op-log replication protocol with **delta-state synchronization**.
The per-origin operation logs, log truncation, the full-dump fallback path, gap
detection, and the optimistic `peer_ack` advancement are all removed. The data
map itself — where every entry permanently carries its origin `(node, seq)`
stamp — becomes the only replicated state, and incremental sync is answered by
filtering the live data map against a per-origin ack map. A state digest is
added to detect silent replica divergence, turning any ack-related bug from
"permanent data loss" into "detectable and self-healing".

The redesign is wire-compatible enough that v1 and v2 nodes can coexist in one
cluster during a rolling upgrade, with a small v1 compatibility shim and a
staged migration plan (Section 8).

## 1. Motivation

WaveKV v1 is an LWW (last-write-wins) replicated KV store. LWW state with
`(timestamp, node, seq)` metadata is a state-based CRDT: merging is commutative,
associative, and idempotent, so replicas converge regardless of message loss,
duplication, or reordering. That algebra is where all of WaveKV's robustness
comes from.

However, v1 does not exploit it. It replicates via **operation logs**: every
node buffers a bounded `VecDeque<Entry>` per origin node (`PeerState.log`,
capped at `DEFAULT_MAX_LOG_ENTRIES = 1000`) and ships log suffixes. Op-based
replication requires exactly-once, ordered-ish delivery, which forced a series
of compensating mechanisms — each of which is a source of complexity and, in
several cases, of real fragility:

1. **Dual state representation.** Every entry is stored twice: once in the data
   map and once in the origin's log bucket. `PushPeerLog` + `Set` are written as
   two WAL records per synced entry. The two representations can drift (e.g.
   after a partial WAL replay), and nothing detects the drift.

2. **Log truncation forces a rarely-exercised fallback.** When a peer's ack
   falls behind a truncated log, the protocol switches to a full KV dump
   (`kv_to_log_entries`). This low-frequency path carries its own ack
   semantics (`is_snapshot`, `update_local_ack`) and is the least-tested code
   in the system.

3. **Gap handling drops data on the floor.** `apply_pushed_entries` rejects a
   whole batch when the first entry's seq is ahead of `local_ack + 1` (warn and
   return `Ok`). Recovery depends on the pull path noticing later. Internal
   gaps within a batch are not checked at all.

4. **Optimistic ack advancement.** `handle_sync` advances `peer_ack` "assuming
   the peer will accept our logs" and relies on subsequent rounds to
   self-correct. Combined with monotonic and non-monotonic ack update variants,
   the ack state machine is hard to reason about.

5. **No convergence verification.** `local_ack`/`peer_ack` only track log
   positions, not actual state. If replicas diverge silently — a dropped batch
   that pull never repairs, a WAL replay that lost ops relative to the
   snapshot's ack values, disk corruption that still decodes — nothing in the
   system can ever detect it, and the divergence is permanent.

6. **WAL churn from bookkeeping.** `UpdatePeerAck`/`UpdateLocalAck` ops are
   written (and fsynced) to the WAL on every sync round even when no data
   changed. Ack durability has no value: a lost ack merely causes one larger
   delta on the next round.

The key observation: **for LWW semantics, the op log preserves information that
is guaranteed to be useless.** Intermediate versions of a key are, by
definition, superseded; the entire ack/gap/truncation machinery exists to
reliably deliver entries that nobody will ever read. Delta-state synchronization
delivers only the surviving state, inherits the CRDT's tolerance of loss,
duplication, and reordering, and lets us delete the machinery.

## 2. Background: v1 protocol summary

For reference, the v1 sync round (`SyncManager::sync_to` / `handle_sync`):

```
A -> B  SyncMessage  { sender_id, sender_uuid,
                       sender_ack: Map<NodeId, u64>,   # A's local_ack for every node
                       entries }                        # A's OWN log suffix (seq > peer_ack)
B: apply_pushed_entries(msg)          # gap check on first entry; LWW merge
B -> A  SyncResponse { peer_id, entries,               # logs from ALL nodes per sender_ack,
                       progress: Map<NodeId, u64>,     #   or full dump if truncated
                       is_snapshot }
A: apply_pulled_entries(resp)         # LWW merge; adopt progress if is_snapshot;
                                      # update peer_ack from progress[A]
```

Three properties of the v1 implementation matter for migration (Section 8):

- **(P1)** The pull path (`apply_pulled_entries`) performs **no gap check**; it
  merges whatever entries arrive.
- **(P2)** `local_ack` advances by **max**, not by contiguity:
  `PushPeerLog` sets `local_ack = max(local_ack, entry.meta.seq)`.
- **(P3)** When `is_snapshot = true`, the v1 client **adopts the responder's
  progress map** (monotonically) and then merges the entries.

## 3. Design overview

### 3.1 Core idea

Every `Entry` already carries `meta = (node, seq, timestamp)`: which node wrote
it and that node's write counter at the time. The data map holds exactly one
entry per key — the current LWW winner. Therefore the data map itself can
answer the only question the log ever answered:

> "Which writes authored by node *n* with `seq > s` does the requester still
> need?"

Answer: the **live** entries (present in the data map, including tombstones)
with `meta.node == n && meta.seq > s`. Entries that no longer appear were
superseded; the requester does not need them — it needs the superseding entries,
which are also in the data map and are covered by the same filter under their
own origin's ack.

A **delta** is therefore a filtered subset of the data map. Since each entry is
itself a one-element LWW map, a delta is a valid mini-state: applying it is a
merge, hence idempotent, commutative, and tolerant of duplication and loss. The
full dump degenerates into "delta against an all-zero ack map" — one code path
serves both bootstrap and steady-state.

### 3.2 State changes

```rust
struct CoreState {
    /// Unchanged: key -> current winning entry (tombstones included).
    data: BTreeMap<String, Entry>,

    /// NEW: secondary index for delta queries.
    /// Assumes (meta.node, meta.seq) is unique per origin across all writes; if conflicting pairs are observed on ingest, treat it as a protocol violation and reject.
    /// Maintained on every Set/Clear; rebuilt from `data` on startup (not persisted).
    origin_index: BTreeMap<(NodeId, u64 /* seq */), String /* key */>,

    /// Per-peer ack bookkeeping. Replaces PeerState { local_ack, peer_ack, log }.
    /// acks[n]      : max seq of node n's writes this node has fully covered (see INV).
    /// peer_acks[p] : the ack map peer p reported in its last message (cache; volatile).
    acks: HashMap<NodeId, u64>,
    peer_acks: HashMap<NodeId, HashMap<NodeId, u64>>,

    /// Unchanged.
    next_seq: u64,
}
```

Removed entirely: `PeerState.log`, `max_log_entries`, log truncation,
`kv_to_log_entries` as a distinct path.

Index maintenance:

- `Set(entry)`: remove the old index item for the key (if any), insert
  `(entry.meta.node, entry.meta.seq) -> key`.
- `Clear(key)` (tombstone GC): remove the index item.
- Startup: one pass over `data` rebuilds the index; it is derived state and is
  never persisted.

The delta query for origin `n` above ack `s` is a range scan
`origin_index.range((n, s+1)..=(n, u64::MAX))`.

### 3.3 Wire protocol v2

One symmetric message type; request and response are structurally identical:

```rust
struct SyncEnvelope {
    sender_id: NodeId,
    sender_uuid: Vec<u8>,
    /// Sender's ack map (its `acks`), read from the same state snapshot as `entries`.
    acks: HashMap<NodeId, u64>,
    /// Delta: live entries filtered against the receiver's acks
    /// (request: against the last acks the receiver reported; response: against
    /// the acks in the request — exact).
    entries: Vec<Entry>,
    /// State digest for divergence detection (Section 3.6). Optional so that
    /// digest computation can be sampled rather than per-round if needed.
    digest: Option<StateDigest>,
    /// Pagination (Section 3.7). None = single complete delta.
    page: Option<PageInfo>,
}

struct StateDigest {
    /// SHA-256 over the canonical encoding of the full data map (Section 3.6).
    hash: [u8; 32],
}

struct PageInfo {
    /// Exclusive resume cursor: the last (origin, seq) included in this page.
    cursor: (NodeId, u64),
    /// True on the final page. `acks` in the envelope MUST only be adopted
    /// from an unpaged envelope or from the final page (rule R2).
    last: bool,
}
```

A sync round:

```
A -> B  SyncEnvelope { acks: acks_A, entries: delta(data_A, peer_acks_A[B]), digest_A }
B: merge entries; if complete (R1/R2 hold): acks_B[n] = max(acks_B[n], acks_A[n]) for all n
   peer_acks_B[A] = acks_A
B -> A  SyncEnvelope { acks: acks_B', entries: delta(data_B, request.acks), digest_B' }
A: merge entries; if complete: adopt acks_B' monotonically; peer_acks_A[B] = acks_B'
```

Where `delta(data, acks) = { e in data : e.meta.seq > acks[e.meta.node] }`
(missing map keys are treated as 0), computed via `origin_index`, tombstones
included.

Notes:

- The request's `entries` are computed against a possibly stale `peer_acks_A[B]`
  cache. While a peer's coverage only ever grows, the cache can only lag, which
  makes the delta larger and never incomplete relative to the claimed `acks_A`
  (see R4).
- That premise fails for a peer whose coverage *regresses* — a node rebuilt from
  an empty data directory, which the inbound-only uuid check in Section 8.2.1
  exists to support. Then the cache is an over-estimate: A sends a nearly empty delta, and B, merging it cleanly,
  adopts `acks_A` wholesale and claims coverage of state it does not hold. B's
  claim then propagates into every node's `peer_acks` and is a valid input to the
  tombstone GC watermark, so under the wrong ordering a deleted key can come
  back. A node therefore suspends adoption of a *requester's* ack map for the
  duration of its bootstrap (`begin_bootstrap`/`finish_bootstrap`), which is
  exactly the window in which its own coverage is unknown to it. Adoption from
  its *own* rounds continues throughout: there it sent the acks the delta was
  computed against, so the delta is complete relative to the truth. Outside that
  window the premise holds again, because coverage only regresses across a
  restart. An embedder that rebuilds a node should still prefer a fresh node id,
  which makes the regression impossible rather than merely bounded.
- UUID-based node-id-reuse detection is carried over unchanged from v1.
- WaveKV remains transport-agnostic: the application delivers envelopes however
  it likes. Transport-level concerns (authentication, compression, request size
  and decompressed-size limits) remain the embedder's responsibility, but the
  library now exposes entry-count and byte-size caps that `merge` enforces
  (Section 3.8).

### 3.4 Correctness

**Definition (coverage).** Node B *covers* `(n, s)` if for every entry `e`
authored by `n` with `e.meta.seq <= s`, either B has merged `e`, or `e` has been
superseded by some entry `e'` (any origin) that B has merged or that is itself
covered under `e'.meta.node`'s ack in B.

**Invariant (INV).** For every node B and every origin `n`:
`acks_B[n]` never exceeds the largest `s` such that B covers `(n, s)`.

INV is what makes skipping seq holes safe: a hole is precisely a superseded
write, and the superseding entry travels in the same (or an already-merged)
delta under its own origin's ack.

**Why adoption preserves INV.** Suppose sender P claims `acks_P[n] = s` and, in
the same envelope, includes `delta(data_P, acks_B)` computed from one state
snapshot. By induction P covers `(n, s)`: every write of `n` with seq `<= s` is
either live in `data_P` — and, if `> acks_B[n]`, included in the delta — or
superseded within `data_P` by a live entry likewise included or already covered
by B. After B merges the **complete** delta, B covers `(n, s)`, so adopting
`acks_B[n] = max(acks_B[n], s)` preserves INV. The base case is a node that has
only its own writes, for which INV holds trivially.

INV is maintained if and only if implementations obey four rules:

- **R1 — Atomic batch-then-ack.** Acks are adopted only after every entry in
  the envelope has been merged successfully. Any per-entry failure (storage
  error, rejected by an admission hook) blocks ack adoption for that round.
  Merging is idempotent, so the retry on the next round is free.
- **R2 — Pagination boundaries.** When a delta is paged, acks are adopted only
  with the final page; alternatively, pages are ordered by `(origin, seq)` and
  each page may advance acks up to the per-origin upper bound that the
  contiguously-received pages fully cover. This RFC specifies the simpler
  final-page rule.
- **R3 — Opportunistic entries never move acks.** Any channel that delivers
  entries without the complete-delta guarantee (e.g. event-driven push,
  Section 3.9) merges data only. Ack adoption is exclusive to full sync rounds.
- **R4 — Single-snapshot sender.** `entries`, `acks`, and `digest` in one
  envelope must be read from one consistent view of the state (in practice:
  under one lock guard). Pairing a newer ack map with an older delta
  manufactures exactly the hole INV forbids.

**Self-healing property.** Because the data map is never truncated, lowering an
ack is always safe and merely causes retransmission. Consequently, even if a
bug violates INV, the damage is not permanent the way a truncated op log is:
resetting `acks_B[n]` (to 0 in the limit) provably restores convergence. The
digest (Section 3.6) provides the detection half of this repair loop.

### 3.5 LWW order and timestamps

The LWW comparison `(timestamp, node, seq)` is unchanged in v2 (wire
compatibility, Section 8). Two behavioral hardenings are added, both
encoding-compatible:

- **Future-drift rejection.** `merge` rejects entries
  whose `meta.timestamp` exceeds local wall time by more than
  `max_clock_drift` (default: 5 minutes). This bounds the blast radius of a
  node with a runaway clock, which under raw LWW can poison keys unfixably
  until real time catches up.
- **Administrative override.** A `force_put` API writes with
  `timestamp = max(existing.timestamp + 1, now)`, providing an escape hatch
  for operator repair of a poisoned key.

Replacing `(timestamp, seq)` with a single hybrid logical clock is explicitly
**deferred** to a post-migration RFC: it changes the `Metadata` encoding and
would break v1 coexistence (Section 8.1).

### 3.6 State digest and divergence repair

v2 adds a canonical digest of the replicated state:

```
digest = SHA-256 over, for each (key, entry) in data-map (BTreeMap) order:
    u32_le(len(key)) || key ||
    u32_le(meta.node) || u64_le(meta.seq) || i64_le(meta.timestamp) ||
    (0x00 for tombstone | 0x01 || u32_le(len(value)) || value)
```

Tombstones are included (they are replicated state); `acks`, `peer_acks`, and
`next_seq` are excluded (they are per-node bookkeeping). Two replicas that have
converged have equal digests by construction, because merge resolves every key
to the same winning entry.

Usage:

- Each *response* carries the responder's digest, and the initiator compares it
  against its own. Equal digests after a round with an empty bidirectional delta
  confirm convergence at O(1) network cost.
- Requests deliberately do not carry a digest. A responder has no use for one,
  and sending it would hand every responder the exact value it needs to look
  healthy: echo it back and `digest_match` is `Some(true)` every round, holding
  the divergence counter at zero indefinitely. A peer that stays silent about
  its digest still cannot be checked — detection needs the peer to volunteer
  something — but it can no longer forge agreement, and a peer that volunteers a
  wrong one only triggers a repair, which is safe by construction.
- Persistently unequal digests across `digest_check_rounds` consecutive rounds
  with empty deltas indicate silent divergence (the class of bug v1 cannot
  see). The response is automatic: log at error level, bump a counter the
  embedder can alarm on, and reset `peer_acks[peer]` and request the peer reset
  theirs (a `reset_acks` hint flag in the envelope), forcing a full-delta
  exchange that converges the pair.
- Both halves are load-bearing, and for different origins. Clearing
  `peer_acks[peer]` makes *our* next delta a full dump, repairing anything the
  peer is missing. But the peer's reply is filtered by the acks we send, and we
  can only lower `acks[peer]` — our claims about a third node C's entries are
  not in doubt from our side and stay untouched. Without the `reset_acks` hint
  the peer therefore keeps filtering out precisely the C-authored entries we are
  missing, and the pair diverges permanently while re-dumping in one direction
  every `digest_check_rounds`.
- The digest is exposed via `Node::state_digest()` so embedders can compare it
  across a cluster out-of-band (metrics, admin endpoints) — this also gives v1
  clusters a divergence check before the v2 migration (Section 8.4, Phase 0).

Cost: a full recomputation is O(total state size). At the target scale
(10^4–10^5 keys) this is single-digit milliseconds per sync round. An
incrementally-maintained digest (or a bucketed/Merkle variant enabling
sub-range repair) is future work (Section 11) and can be introduced without a
wire change thanks to the `Option<StateDigest>` field.

### 3.7 Bootstrap and pagination

A new node (empty ack map) receives `delta(data, {})` — the entire live state,
tombstones included. To bound message sizes, a responder paginates deltas
larger than `max_delta_entries` / `max_delta_bytes` by `(origin, seq)` order
with a resume cursor (`PageInfo`). Per R2, the requester adopts acks only from
the final page. An interrupted pagination is harmless but not free: nothing was
adopted, and the merged prefix is idempotent, so correctness holds — but because
R2 blocks adoption, the requester's filter is unchanged and the next round
resends the same pages. The work is repeated, not resumed. Making a partial
prefix advance the filter would require admitting coverage of a delta that was
never completed, which is exactly what R1 forbids.

Pagination applies to the *responder's* delta only. A request whose own delta
exceeds the budget is truncated, and because R2 forbids the responder from
adopting acks off a non-final page, the requester's next round rebuilds and
resends the same first page. The resume cursor resumes the responder's scan, not
the requester's. In a cluster where every pair exchanges rounds in both
directions this is invisible: the peer's own round carries the same data the
other way. It only bites under one-way reachability, which is the same topology
constraint Section 8.2.1 records for a v1 hop — a mixed or partitioned cluster
must stay meshed for the pairs that need to exchange data.

`ensure_next_seq` bootstrap recovery simplifies: scan `origin_index` for own
entries (one range scan) instead of scanning all logs plus the data map.

### 3.8 Admission control (ingest hardening)

v1 applies any entry a peer sends. v2 adds an optional admission hook and hard
quotas, enforced inside `merge` (thus covering both request and response
directions, which transport-level checks cannot):

```rust
pub enum Admission { Accept, Reject { reason: &'static str } }

pub trait AdmissionPolicy: Send + Sync {
    fn admit(&self, entry: &Entry) -> Admission;
}

pub struct Limits {
    pub max_key_bytes: usize,      // default 1 KiB
    pub max_value_bytes: usize,    // default 1 MiB
    pub max_keys: usize,           // default 1M
    pub max_total_bytes: usize,    // default 1 GiB
    pub max_clock_drift: Duration, // default 5 min (Section 3.5)
}
```

Rejected entries are counted and reported per prefix/origin (Section 3.11);
under R1 a rejection blocks ack adoption for the round, so a peer persistently
sending inadmissible data keeps its acks parked rather than silently losing
entries. Embedders (e.g. dstack-gateway) can use the hook to enforce key-prefix
schemas so that a buggy peer cannot flood the replicated namespace.

### 3.9 Event-driven push (latency, optional)

v1 propagation latency equals the sync interval, which is user-visible in
dstack-gateway (an instance registered on node A is unroutable on node B until
the next round). v2 adds an opportunistic channel: after a local write, a node
may push the recently-written entries to peers after a small coalescing window
(default 200 ms). Per R3 these envelopes carry `entries` only — no acks, no
digest — and the receiver merges without ack movement. Loss, duplication, and
reordering are all harmless; the periodic round remains the anti-entropy
backstop and the only ack authority.

### 3.10 Durability changes

- **WAL records only `Set` and `Clear`.** Acks are not WAL-durable; losing them is safe (they can be recovered by re-syncing; worst case is one larger delta). This removes the v1 pattern of
  two WAL records per synced entry plus fsync traffic on idle sync rounds, and
  shrinks the `StateOp` enum to the two ops that define the database.
- **Snapshots** persist `data` + `acks` + `next_seq` (`origin_index` is
  rebuilt). For migration, the on-disk container remains v1's
  `SnapshotFile`/`CoreState` shape (Section 8.3).
- **Recovery hardening** (independent of the protocol change, included in the
  v2 release): WAL replay treats a torn or checksum-failed tail as truncation
  (warn + stop) instead of a fatal error, matching `find_last_sequence`'s
  existing behavior; a corrupt record length is bounds-checked against the
  remaining file size before allocation; snapshot files keep one previous
  generation (`.snapshot.bak`) as a fallback. Rationale: WaveKV state is fully
  replicated, so the correct response to local persistence damage is quarantine
  and re-sync, never a permanent refusal to start.

### 3.11 Observability

`Node::status()` exposes the state digest, cluster-wide entries
merged/rejected counters, and per peer `acks`/`peer_acks` plus whether the peer
has ever been heard from. `SyncManager::link_status()` adds, per link, the
negotiated protocol, consecutive digest mismatches, and consecutive round
failures. Together these replace the v1 `PeerStatus { ack, pack, logs }` and
carry the signals needed to alarm on divergence and on a peer that fails every
round. (In v1, a permanently diverged replica and a healthy one are
indistinguishable from status output.)

Not implemented, and deliberately listed rather than assumed: last-successful-
round timestamps, pagination state, and per-origin rejection counts. The first
two are bookkeeping the sync loop does not currently keep; the third needs
`merge` to attribute rejections by origin, which it does not.

## 4. What gets deleted

| v1 mechanism | v2 replacement |
| --- | --- |
| Per-origin `VecDeque` log buckets | `origin_index` over the data map (never truncated) |
| `max_log_entries`, log truncation | — (concept gone) |
| Full-dump fallback (`kv_to_log_entries`, `is_snapshot`) | Same filter with empty acks; pagination |
| Gap check + whole-batch drop | — (holes are superseded writes; INV) |
| Optimistic `peer_ack` advancement | `peer_acks` cache updated from received envelopes only |
| `UpdatePeerAck`/`UpdateLocalAck`/`PushPeerLog`/`AddPeer`/`RemovePeer` in WAL | WAL = `Set`/`Clear` only |
| `is_noop` mirror logic for the above ops | trivial (`Set` never no-op at that layer, `Clear` presence check) |
| `SyncMessage` vs `SyncResponse` asymmetry | one `SyncEnvelope` |

Net effect: the steady-state path and the bootstrap path become the same code;
the least-tested branch of the system ceases to exist.

## 5. Non-goals

Unchanged from v1's README: no strong consistency, no transactions or CAS, no
built-in transport/auth/encryption, in-memory dataset scale. This RFC does not
change the consistency model — only how the same LWW state is replicated.

## 6. Tombstone GC

Unchanged requirement, now made explicit and mechanized. A tombstone authored
by origin `n` at seq `s` may be cleared only when every known peer `p` has
`peer_acks[p][n] >= s`. v1's `cleanup_expired_tombstones` (local-clock TTL) is
removed: under any state-shipping scheme, uncoordinated GC lets a lagging
replica resurrect deleted keys — in dstack-gateway terms, a deregistered CVM
reappearing in every node's WireGuard config.

Caveat (documented, accepted): the watermark is computed over *known* peers. A
peer that is permanently retired must be removed via `remove_peer`, or it pins
GC forever; a node offline longer than its own membership record re-bootstraps
from scratch. Digest comparison (3.6) detects any resurrection that slips
through in mixed failure scenarios.

## 7. API sketch

```rust
impl NodeState {
    pub fn put(&mut self, key: String, value: impl Into<Vec<u8>>) -> Result<Entry>;   // unchanged
    pub fn delete(&mut self, key: String) -> Result<Option<Entry>>;                    // unchanged
    pub fn force_put(&mut self, key: String, value: impl Into<Vec<u8>>) -> Result<Entry>; // 3.5
    pub fn get / get_by_prefix / iter_by_prefix / watch_*                              // unchanged

    /// Build an outgoing envelope for `peer` (R4: one snapshot).
    pub fn prepare_sync(&self, peer: NodeId) -> SyncEnvelope;
    /// Merge an incoming envelope; returns the response envelope when handling
    /// a request. Applies R1–R3 internally.
    pub fn handle_envelope(&mut self, env: SyncEnvelope) -> Result<SyncEnvelope>;

    pub fn state_digest(&self) -> StateDigest;                                         // 3.6
}

pub struct NodeConfig {
    pub limits: Limits,                       // 3.8
    pub admission: Option<Arc<dyn AdmissionPolicy>>,
    pub digest_check_rounds: u32,             // 3.6
    pub coalesce_window: Option<Duration>,    // 3.9, None = push disabled
    pub max_delta_entries: usize,             // 3.7
    pub max_delta_bytes: usize,               // 3.7
}
```

`SyncManager` keeps the v1 shape (periodic driver + `ExchangeInterface`), with
the v1/v2 negotiation described next.

## 8. Compatibility and migration

The hard requirement: **v1 and v2 nodes must coexist in one cluster during a
rolling upgrade, and any single node must be able to roll back to the v1 binary
at any point.** dstack-gateway clusters upgrade one CVM at a time (each upgrade
is an on-chain-governed compose-hash change), so the mixed window is measured
in hours to days.

### 8.1 Wire-format freeze

Coexistence constrains what v2 may change, because in dstack-gateway the sync
payloads are encoded with `rmp_serde` using **positional** (array) struct encoding
— under that encoding, adding a field to a struct breaks v1 decoders. Frozen for the entire transition:

- `Entry` and `Metadata` layouts (hence: HLC unification is deferred, 3.5).
- v1 `SyncMessage`/`SyncResponse` layouts (the shim speaks them verbatim).
- The v1 snapshot container (8.3).

Version negotiation therefore cannot ride inside v1 messages; it happens at the
transport layer (8.2). New v2 structures (`SyncEnvelope` etc.) are free to
evolve until 2.0 ships; from 2.0 on, additive evolution must go through
envelope-level version/optional fields.

### 8.2 Dual-stack node and negotiation

A v2 node implements both protocols:

- `handle_sync_v1(SyncMessage) -> SyncResponse` — compatibility shim (8.2.1)
- `handle_envelope(SyncEnvelope) -> SyncEnvelope` — native (Section 3)

WaveKV is transport-agnostic, so negotiation is a documented embedder pattern
rather than library code. Recommended (and what dstack-gateway will do): expose
the native handler at a new route (e.g. `POST /wavekv/sync2/{store}`) alongside
the v1 route; as an initiator, probe the v2 route and fall back to v1 on
404/405; cache the per-peer capability and re-probe periodically so upgraded
peers are picked up. The library supports this by letting `ExchangeInterface`
implementations report which protocol a peer accepted.

#### 8.2.1 The v1 shim

Serving v1 peers requires no log structures, thanks to properties P1–P3
(Section 2):

**v1 peer initiates (v1 -> v2).** The shim accepts the pushed entries and
merges all of them — the v1 gap concept does not apply to a delta-state
receiver (INV governs ack movement instead, and the v1 push carries no ack
claim to adopt beyond `sender_ack`, which is handled as below). The response is
built as:

```
SyncResponse {
    peer_id:     my_id,
    entries:     delta(data, msg.sender_ack),   # exact filter, tombstones included
    progress:    my acks,
    is_snapshot: true,                          # deliberate, see below
}
```

`is_snapshot = true` is the pivot of the whole scheme: by P3, the v1 client
responds by adopting `progress` monotonically and then merging `entries` — which
is **exactly the delta-state adoption semantics**, and it is sound for exactly
the reason INV's adoption step is sound (the response is a complete filtered
delta against the acks the client itself declared, computed from one snapshot).
v1 clients thereby follow the v2 invariant without a code change. The v1
meaning of the flag ("entries are a full dump") is never verified by v1 code;
what v1 *does* with the flag is precisely what v2 needs.

Residual (accepted): v1's `apply_pulled_entries` adopts `progress` *before*
merging entries, so a mid-batch storage failure on the v1 side can strand acks
ahead of data. This failure mode exists identically in v1<->v1 full dumps
today, is limited to WAL I/O errors, and is caught by the Phase-0 digest check.

**v2 initiates toward a v1 peer (v2 -> v1).** The v2 node sends a v1
`SyncMessage` with **empty `entries`** and its ack map as `sender_ack`, then
consumes the response with native merge + adoption rules (R1 applies; a v1
full-dump response is just a large delta). Rationale for the empty push: a v2
node's own live seq space legitimately contains holes (own writes superseded by
other nodes), and if a hole falls at the batch front, v1's gap check
(`apply_pushed_entries`) discards the whole batch. Rather than special-casing
hole-avoidance, the v2 node relies on the v1 peer's own periodic rounds to pull
its data (the shim path above). Both directions run every interval from both
sides, so the propagation cost is at most one extra sync interval, only for
v2 -> v1 pairs, only during the mixed window.

**No relay through v1 nodes.** An earlier draft of this section claimed that v1
nodes buffer v2-origin entries in their per-origin logs and relay them onward.
They do not, and the test
`a_v1_hop_does_not_relay_so_the_cluster_must_stay_meshed` pins the actual
behaviour. `apply_pulled_entries` writes what it pulled into the data map only,
never into the per-origin logs that v1's incremental server path reads, so a v1
node answers an already-covered peer with an empty response indefinitely. This
is origin-agnostic and predates v2: v1 has never relayed data it learned by
pulling.

This is a topology constraint, not a defect introduced by the migration. Two
nodes that must exchange data need a direct edge, which is what auto-discovery
and the bootnode establish — a node joining with an empty ack map receives a
full dump, the one path on which a v1 node does hand on everything it holds. A
partially connected mixed cluster would not converge, in exactly the way a
partially connected v1 cluster does not converge today.

Holes in v1's logs remain benign by P1/P2 (pull applies without gap checks;
`local_ack` advances by max), and a front-hole rejection on a v1<->v1 push
self-heals through the v1 pull path as it does today. UUID-based node-id-reuse
detection is format-identical in both versions and keeps working across them;
it is applied to inbound requests only, since a node whose data directory was
recreated returns with the same id and a new uuid and can only republish its
identity in a response.

Behavior matrix:

| Initiator -> Responder | Route | Behavior |
| --- | --- | --- |
| v2 -> v2 | sync2 | Native delta-state, symmetric envelopes |
| v1 -> v2 | sync (shim) | v1 pushes logs; v2 merges all; response = exact delta + `is_snapshot=true`; v1 adopts correctly via P3 |
| v2 -> v1 | sync (v1 wire) | Empty push + native consumption of the response; v2's data reaches the peer via the peer's own initiated rounds |
| v1 -> v1 | sync | Untouched |

### 8.3 Persistence compatibility and rollback

Rollback of any single node to the v1 binary must work at any time during the
transition:

- **Snapshot**: v2 writes the v1 `SnapshotFile`/`CoreState` container — `data`
  unchanged, `peers` populated with the v2 acks and **empty log VecDeques**,
  same `SnapshotFile::VERSION`. Note that `peers` carries both membership and
  ack coverage with no way to tell them apart, which is why `RemovePeer` drops
  the retired peer's ack: an ack outliving its membership reappears as a member
  on the next load. A v1 binary loads it; empty logs simply mean
  the node answers early pulls with full dumps until its logs repopulate. No
  data loss. (`origin_index` is derived and never persisted.)
- **WAL**: v2 writes `Set`/`Clear` and the membership ops `AddPeer`/`RemovePeer`
  — a strict subset of the v1 op set, at identical `StateOp` discriminants, so
  v1 replay handles them natively. (Membership is durable because the tombstone
  GC watermark is a minimum over known peers: a peer forgotten in a crash would
  let the node collect tombstones that peer never covered.) Ack bookkeeping is
  deliberately *not* durable — a lost ack costs one larger delta — and `next_seq`
  is recovered from the replayed entries themselves. v1 WALs replay on v2 with
  the log-manipulating ops (`PushPeerLog` etc.) mapped to index/ack updates or
  ignored.
- **Last resort**: wiping the local data dir and re-bootstrapping from peers
  works across versions in both directions, because bootstrap is just "sync
  with empty acks" in v2 and "full dump" in v1, and the shim bridges the two.

### 8.4 Staged rollout

```
Phase 0 — v1.x preparatory release
  * Ship Node::state_digest() and per-peer sync metrics on the v1 codebase
    (no wire change; digest is compared out-of-band via the embedder's
    admin/metrics plane).
  * Confirm tombstone GC remains disabled (status quo for dstack-gateway).
  * Gate for Phase 1: digests equal across the production cluster — this
    baselines "no pre-existing divergence" before changing the protocol.

Phase 1 — v2 dual-stack release, rolling upgrade
  * Upgrade one node at a time. After each node:
      - cluster-wide digest equality (the primary promotion gate),
      - sync success rate per peer,
      - shim-path counters (v1 gap-drop warns should be ~zero given the
        empty-push strategy).
  * Any anomaly: roll back that node alone (8.3); the rest of the cluster is
    unaffected.

Phase 2 — cleanup release
  * Gate: all nodes on v2 and digests continuously equal for >= N days
    (suggested N = 14).
  * Disable, then remove, the v1 shim and v1 wire structures.
  * Only after Phase 2 may wire-breaking follow-ups land (HLC, Metadata
    compaction, envelope schema changes).
```

### 8.5 Migration test matrix

To be covered by unit + mixed-version e2e tests (dstack-gateway's three-node
e2e harness is the reference environment):

1. Mixed clusters `[v1, v1, v2]` and `[v1, v2, v2]`: concurrent writes on all
   nodes; assert digest convergence.
2. Shim adoption: v1 client consumes a v2 `is_snapshot=true` response; assert
   ack adoption and data equality (validates the P3 pivot explicitly).
3. Restart/bootstrap in the mixed window: v1 node cold-starts against v2 peers
   and vice versa.
4. Rollback: node runs v2, writes snapshot + WAL, rolls back to v1 binary,
   rejoins, converges.
5. Tombstone propagation across versions in both directions (delete on v2 node
   visible on v1 node and vice versa) — the security-sensitive path.
6. Fault injection on v2<->v2: drop/duplicate/reorder envelopes, kill mid-
   pagination, per-entry admission failures; assert INV via digest equality
   after quiescence (property-based where practical).
7. Clock skew: a node with +1h wall clock; assert rejection bounds the damage
   and `force_put` recovers a poisoned key.

## 9. Performance notes

- **Memory**: removes up to `1000 x N_nodes` buffered entry clones (each a full
  key + value copy). The `origin_index` costs one `(u64-pair, String)` map item
  per live key — strictly smaller than what it replaces for any realistic
  workload.
- **Write path**: one extra `BTreeMap` insert/remove per `Set` (O(log n));
  WAL records per synced entry drop from two to one, and idle-round WAL fsyncs
  disappear (3.10).
- **Sync CPU**: delta query is a range scan per origin, O(log n + |delta|);
  digest is O(n) per round (3.6), acceptable at target scale, optimizable later
  without wire changes.
- **Network**: steady-state deltas shrink (superseded intermediate versions are
  never shipped — v1 ships up to 1000 of them per origin). Bootstrap payload is
  unchanged (full live state) but now paginated.
- **Latency**: optional coalesced push (3.9) cuts propagation latency from
  one sync interval to ~coalescing window.

## 10. Alternatives considered

- **Keep op-based replication and patch the issues.** Each known issue (gap
  drops, truncation fallback, ack hacks, silent divergence) is patchable in
  isolation, but the fixes add machinery to the exact paths that are already
  the least tested, and the dual state representation — the root cause —
  remains. Rejected in favor of removing the machinery's reason to exist.
- **Vector-clock / per-key causal metadata.** Provides causal consistency,
  which nothing in the target workload needs, at O(N_nodes) metadata per key
  and substantially more complex conflict handling. LWW is retained per the
  project's stated scope.
- **Merkle-tree anti-entropy as the primary protocol** (Dynamo/Riak style,
  or MST). Solves divergence detection and repair localization, but does not by
  itself provide low-latency incremental sync, and at the target scale the
  flat digest already buys the detection half at a fraction of the complexity.
  Kept as future work for repair localization once state size warrants it.
- **Adopt an existing embedded replicated store.** Conflicts with WaveKV's
  goals (embeddable, transport-agnostic, minimal, TEE-friendly); evaluated
  options bring consensus machinery or storage engines out of scope here.

## 11. Future work (post-Phase 2)

- **HLC**: unify `(timestamp, seq)` into a hybrid logical clock; ack maps range
  over HLC values; removes the separate seq-recovery logic and strengthens the
  clock-skew story beyond drift rejection. Wire-breaking; requires the Phase 2
  gate.
- **Bucketed digests / Merkle Search Tree**: localize divergence repair to key
  ranges instead of resetting a peer's acks wholesale; introducible via the
  optional digest field without a new wire version.
- **Conditional writes**: `put_if(key, expected_meta, value)` as a node-local
  CAS primitive with LWW propagation, upgrading embedder lock patterns
  (e.g. dstack-gateway's certificate renewal locks) from pure best-effort to
  single-node-strict with fencing metadata.
- **Snapshot signing hooks**: allow embedders in TEE deployments to bind the
  state digest into attestation evidence (rollback detection is already
  partially covered by cross-peer digest comparison).

## 12. Open questions

1. Should the digest be mandatory per round (simplest operationally) or sampled
   (cheaper at large state sizes)? Proposal: mandatory until profiling shows
   otherwise; the field is already optional in the envelope.
2. `reset_acks` handshake (3.6): one-shot flag vs. a small state machine with
   backoff to avoid reset storms between two nodes that disagree persistently
   for a non-ack reason (e.g. divergent admission policies — which the design
   makes possible and which operators must treat as a misconfiguration).
3. Exact `Limits` defaults, and whether admission rejection should optionally
   quarantine (retain the raw entry for operator inspection) rather than drop.
4. Whether `remove_peer` should require an explicit tombstone-GC watermark
   handoff to avoid the pinned-GC failure mode in 6.
