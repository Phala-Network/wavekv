# Writing tests that can fail

A green test is worth exactly as much as the set of wrong implementations it rules
out. Three tests in this suite were green for years while asserting nothing:

- A push test whose data arrived over the periodic round instead, because
  `tokio::time::interval` fires its first tick immediately.
- A push test whose fake transport called `NodeState::merge_push` directly,
  below the `SyncManager` layer where the identity check lives — and which left
  `query_uuid` at its default `None`, so the check was inert anyway. It passed
  while every opportunistic push in production was being rejected.
- Three tests named `..._equal_timestamp_tombstone_wins`, asserting a rule that
  does not exist. The LWW order is `(timestamp, node, seq)`; tombstones get no
  precedence. Each fixture happened to put the tombstone on the higher node id,
  so "the tombstone wins" and "the higher node id wins" predicted the same
  outcome.

All three are one mistake: **the observation was consistent with the claimed
mechanism, but not only with it.** An uncontrolled alternative explanation makes
a passing test a measurement of nothing.

## Before you believe a test

**Break the thing it tests and watch it go red.** This is not optional and not a
formality — it is the only direct evidence the test constrains anything. If you
cannot find code whose removal turns the test red, the property you think you
are testing may not exist. That is how the tombstone rule was found: the
mutation had nowhere to be applied.

**Put doubles at the process boundary.** A fake replaces the network, the clock
or the disk — never a layer of our own logic. Its body must re-enter where the
real peer would: `SyncManager::handle_*`, not `NodeState::*`. Anything the layer
you skipped does is invisible to your test, including whatever gets added to it
next year. `PeerEndpoint` in `tests.rs` exists so the correct shape is also the
convenient one.

**Opt in to opt-in mechanisms.** `query_uuid` returns `None` by default and the
identity check silently does nothing. A harness assembled from defaults is not a
model of production. Mirror the real wiring.

**Kill the paths you are not testing.** If two mechanisms could produce your
observation, disable one structurally rather than hoping it stays quiet. The
push test now makes both sync legs return `Err`, so anything reaching the peer
can only have come through the push channel.

**Make the fixture discriminate.** For a claimed rule, construct the input where
that rule and the most plausible competing rule disagree. If no such input
exists, the rule is not observable — say so, and do not name a test after it.

**Test the boundary, not a comfortable distance from it.** "Well over the limit
is rejected, well under is accepted" leaves the limit free to move by one in
either direction. A case sitting exactly on it is what separates `>` from `>=`.

**Name the assertion, not the intention.** An overstated name is worse than a
missing test: it advertises coverage that is not there, and it stops the next
person from adding the real one.

## The mechanical check

Judgement has been wrong here repeatedly, so do not rely on it alone:

```sh
cargo install cargo-mutants --locked
cargo mutants -p wavekv
```

It rewrites the source one edit at a time — `>` to `>=`, `+` to `*`, a function
body to a constant — and reports every edit the suite failed to notice. The
first run over this crate surfaced an untested byte-budget in `compute_delta`,
two off-by-one boundaries in `admission`, an unpinned `next_seq` recovery, and
one real defect: the responder stamped its identity into the v2 envelope and the
initiator never read it, so node-id-reuse detection ran in one direction only.

Read the survivors, do not chase the score. Some are equivalent mutants —
`max_own >= 0` on a `u64` is always true, and setting `next_seq` to `1` on an
empty node changes nothing. Those are noise. Cosmetic survivors in `Debug` and
`Display` impls are noise too. What matters is a survivor on a comparison, an
arithmetic operator, or a condition that decides whether state changes.
