import WavekvFormal.MergeBatch

/-!
# Two-phase tombstone GC never resurrects a deleted key

Models wavekv 2.2.0, `wavekv/src/node.rs: NodeState::collect_tombstone_garbage` (the
`watermark` closure, the two-phase `gc_marked` rule, and the `StateOp::Clear` it issues;
`ops.rs: CoreState::execute` arm `Clear`), together with an asynchronous cluster whose
nodes send deltas, deliver them (`merge_batch`), collect, and write locally
(`put`/`delete` with `alloc_entry_meta` stamping).

Proven:
* `gcCall_only_clears_marked`: a call clears a key only if it held a tombstone `(n, s)`
  that every known peer reported covering (`peer_acks[p][n] >= s`) and that the previous
  call had already marked with the same metadata (or there are no peers).
  `gcCall_keeps_live`, `gcCall_needs_mark`: live entries and unmarked tombstones stay.
* `no_resurrection`: from any configuration satisfying `Inv`, no member ever again holds
  the older write `v`. This holds in any execution of sends, deliveries (with loss,
  duplication, and reordering), local writes, and collections, *provided every message
  is delivered or dropped within one GC period of its receiver*.
  `inv_establish` and `reached_everyone_of_ackSound`: `Inv` holds once the delete (or a
  newer entry) has reached every member and no member has marked its copy yet; ack
  soundness is what gives the first half.
* `premature_gc_resurrects`: ack soundness is needed.
* `stale_message_within_period_rejected`: a stale delta delivered between the marking
  call and the clearing call loses to the tombstone. `delayed_message_resurrects`: a
  message delayed past two calls still resurrects the key, so the delivery bound is the
  remaining assumption.

Assumptions:
* Delivery bound (the remaining assumption, stated by the Rust doc comment): a message
  is delivered or dropped before its receiver's second GC call after it was sent.
  Modeled by a per-message `age` that counts the receiver's calls; `Step.collect` may
  run only when no message to that node has `age ≥ 1`.
* Ack soundness (`AckSound`, RFC 0001 INV): a peer that reported covering `t` holds
  `t` or something newer at `t.key`.
* Closed membership (`get_peers()` covers the cluster), and in the no-peers path no
  message is in flight to the collector (nobody can send to it).
* Seq freshness: a new local write is never `v` itself.

The 2.2.0 local-write stamping (`stamped_gt`) is also used: a local write on a key
strictly raises the held entry, so it cannot let `v` win later. Under 2.1.0 stamping a
local write could lower the held entry below `v`, after which a delivered `v` would win.

Modeling gap: the watermark is evaluated from arbitrary per-call reports `pa`, which
covers `record_peer_acks` replacing a peer's map with a stale one (that only delays
collection). Acks are `Nat`; `u64::MAX` is the initial watermark. `gc_marked` is a
function `Key → Option Metadata`. Digest repair is a `send`.
-/

namespace Wavekv

/-- `node.rs: collect_tombstone_garbage`, closure `watermark(origin)`: the minimum of
`peer_acks[p][origin]` over the peers, `None` if any peer has not reported. -/
def watermarkFrom (pa : Nat → Nat → Option Nat) (origin : Nat) : Nat → List Nat → Option Nat
  | low, [] => some low
  | low, p :: ps =>
    match pa p origin with
    | none => none
    | some r => watermarkFrom pa origin (min low r) ps

def u64Max : Nat := 2 ^ 64 - 1

def watermark (pa : Nat → Nat → Option Nat) (peers : List Nat) (origin : Nat) : Option Nat :=
  watermarkFrom pa origin u64Max peers

/-- The `collectable` filter: a tombstone covered by every peer's report. -/
def collectable (pa : Nat → Nat → Option Nat) (peers : List Nat) (e : Entry) : Bool :=
  e.isDeleted && (watermark pa peers e.metadata.node).any (fun w => decide (e.metadata.seq ≤ w))

/-- `node.rs: NodeState::gc_marked: HashMap<String, Metadata>`. -/
abbrev Marks := Key → Option Metadata

/-- The `collectable` map built by one call: key to the metadata of its collectable
tombstone. -/
def collectableMeta (peers : List Nat) (pa : Nat → Nat → Option Nat) (s : State) : Marks :=
  fun k =>
    match s k with
    | some e => if collectable pa peers e then some e.metadata else none
    | none => none

/-- `due`: collectable now, and marked with the same metadata by the previous call. -/
def due (coll marked : Marks) (k : Key) : Bool := (coll k).isSome && decide (coll k = marked k)

/-- `node.rs: NodeState::collect_tombstone_garbage` (2.2.0). Without peers, clear every
tombstone and leave the marks alone. Otherwise compute `collectable`, replace the marks
with it, clear the keys that were already marked with the same metadata, and unmark
them. Returns the new data map and the new marks. -/
def gcCall (peers : List Nat) (pa : Nat → Nat → Option Nat) (s : State) (marked : Marks) :
    State × Marks :=
  if peers.isEmpty then
    (fun k => match s k with
      | some e => if e.isDeleted then none else some e
      | none => none, marked)
  else
    let coll := collectableMeta peers pa s
    (fun k => if due coll marked k then none else s k,
     fun k => if due coll marked k then none else coll k)

theorem watermarkFrom_ge (pa : Nat → Nat → Option Nat) (origin seq : Nat) :
    ∀ (peers : List Nat) (low w : Nat), watermarkFrom pa origin low peers = some w →
      seq ≤ w → seq ≤ low ∧ ∀ p ∈ peers, ∃ r, pa p origin = some r ∧ seq ≤ r
  | [], low, w, h, hs => by simp [watermarkFrom] at h; subst h; exact ⟨hs, by simp⟩
  | p :: ps, low, w, h, hs => by
    simp only [watermarkFrom] at h
    split at h
    · simp at h
    · rename_i r hr
      obtain ⟨h1, h2⟩ := watermarkFrom_ge pa origin seq ps _ w h hs
      refine ⟨by omega, ?_⟩
      intro q hq
      simp at hq
      rcases hq with rfl | hq
      · exact ⟨r, hr, by omega⟩
      · exact h2 q hq

theorem collectable_acked {pa : Nat → Nat → Option Nat} {peers : List Nat} {t : Entry}
    (h : collectable pa peers t = true) :
    t.isDeleted = true ∧ ∀ p ∈ peers, ∃ r, pa p t.metadata.node = some r ∧ t.metadata.seq ≤ r := by
  simp only [collectable, Bool.and_eq_true] at h
  obtain ⟨hd, hw⟩ := h
  refine ⟨hd, ?_⟩
  cases hwm : watermark pa peers t.metadata.node with
  | none => rw [hwm] at hw; simp at hw
  | some w =>
    rw [hwm] at hw
    simp at hw
    exact (watermarkFrom_ge pa _ _ peers u64Max w hwm hw).2

theorem collectableMeta_some {peers : List Nat} {pa : Nat → Nat → Option Nat} {s : State}
    {k : Key} {m : Metadata} (h : collectableMeta peers pa s k = some m) :
    ∃ t, s k = some t ∧ t.metadata = m ∧ collectable pa peers t = true := by
  unfold collectableMeta at h
  split at h
  · rename_i t ht
    split at h
    · rename_i hc; simp at h; exact ⟨t, ht, h, hc⟩
    · simp at h
  · simp at h

theorem due_true {coll marked : Marks} {k : Key} (h : due coll marked k = true) :
    ∃ m, coll k = some m ∧ marked k = some m := by
  simp only [due, Bool.and_eq_true, decide_eq_true_eq] at h
  obtain ⟨h1, h2⟩ := h
  cases hc : coll k with
  | none => rw [hc] at h1; simp at h1
  | some m => exact ⟨m, rfl, by rw [← h2, hc]⟩

theorem gcCall_fst_of_empty {peers : List Nat} {pa : Nat → Nat → Option Nat} {s : State}
    {marked : Marks} (hp : peers.isEmpty = true) (k : Key) :
    (gcCall peers pa s marked).1 k =
      match s k with
      | some e => if e.isDeleted then none else some e
      | none => none := by
  simp [gcCall, hp]

theorem gcCall_snd_of_empty {peers : List Nat} {pa : Nat → Nat → Option Nat} {s : State}
    {marked : Marks} (hp : peers.isEmpty = true) : (gcCall peers pa s marked).2 = marked := by
  simp [gcCall, hp]

theorem gcCall_fst_of_ne {peers : List Nat} {pa : Nat → Nat → Option Nat} {s : State}
    {marked : Marks} (hp : peers.isEmpty = false) (k : Key) :
    (gcCall peers pa s marked).1 k =
      if due (collectableMeta peers pa s) marked k then none else s k := by
  simp [gcCall, hp]

theorem gcCall_snd_of_ne {peers : List Nat} {pa : Nat → Nat → Option Nat} {s : State}
    {marked : Marks} (hp : peers.isEmpty = false) (k : Key) :
    (gcCall peers pa s marked).2 k =
      if due (collectableMeta peers pa s) marked k then none
      else collectableMeta peers pa s k := by
  simp [gcCall, hp]

/-- **GC rule.** A cleared key held a tombstone that every peer reported covering and
that the previous call had marked with the same metadata. -/
theorem gcCall_only_clears_marked (peers : List Nat) (pa : Nat → Nat → Option Nat)
    (s : State) (marked : Marks) (k : Key) (t : Entry) (ht : s k = some t)
    (hgc : (gcCall peers pa s marked).1 k = none) :
    t.isDeleted = true ∧
      (peers = [] ∨ (marked k = some t.metadata ∧
        ∀ p ∈ peers, ∃ r, pa p t.metadata.node = some r ∧ t.metadata.seq ≤ r)) := by
  cases hp : peers.isEmpty with
  | true =>
    rw [gcCall_fst_of_empty hp, ht] at hgc
    by_cases hd : t.isDeleted = true
    · exact ⟨hd, Or.inl (List.isEmpty_iff.1 hp)⟩
    · simp [hd] at hgc
  | false =>
    rw [gcCall_fst_of_ne hp] at hgc
    by_cases hdue : due (collectableMeta peers pa s) marked k = true
    · obtain ⟨m, hc, hm⟩ := due_true hdue
      obtain ⟨t', ht', hmt, hcol⟩ := collectableMeta_some hc
      rw [ht] at ht'; cases ht'
      obtain ⟨hd, hacks⟩ := collectable_acked hcol
      exact ⟨hd, Or.inr ⟨by rw [hm, hmt], hacks⟩⟩
    · simp [hdue, ht] at hgc

theorem gcCall_key (peers : List Nat) (pa : Nat → Nat → Option Nat) (s : State)
    (marked : Marks) (k : Key) :
    (gcCall peers pa s marked).1 k = none ∨ (gcCall peers pa s marked).1 k = s k := by
  cases hp : peers.isEmpty with
  | true =>
    rw [gcCall_fst_of_empty hp]
    cases he : s k with
    | none => exact Or.inl rfl
    | some e => by_cases hd : e.isDeleted = true <;> simp [hd]
  | false =>
    rw [gcCall_fst_of_ne hp]
    by_cases hdue : due (collectableMeta peers pa s) marked k = true <;> simp [hdue]

theorem gcCall_keeps_live (peers : List Nat) (pa : Nat → Nat → Option Nat) (s : State)
    (marked : Marks) (k : Key) (e : Entry) (he : s k = some e) (hlive : e.isDeleted = false) :
    (gcCall peers pa s marked).1 k = some e := by
  rcases gcCall_key peers pa s marked k with h | h
  · have := (gcCall_only_clears_marked peers pa s marked k e he h).1
    rw [hlive] at this; simp at this
  · rw [h, he]

/-- With peers, the first call that finds a tombstone collectable only marks it. -/
theorem gcCall_needs_mark (peers : List Nat) (pa : Nat → Nat → Option Nat) (s : State)
    (marked : Marks) (k : Key) (e : Entry) (he : s k = some e) (hne : peers ≠ [])
    (hmark : marked k ≠ some e.metadata) : (gcCall peers pa s marked).1 k = some e := by
  rcases gcCall_key peers pa s marked k with h | h
  · rcases (gcCall_only_clears_marked peers pa s marked k e he h).2 with h' | h'
    · exact absurd h' hne
    · exact absurd h'.1 hmark
  · rw [h, he]

/-! ## Mirrored tests -/

def gcTomb : Entry := ⟨"key1", none, ⟨1, 2, 1000⟩⟩
def gcStore : State := fun k => if k = "key1" then some gcTomb else none
def noMarks : Marks := fun _ => none

def paNone : Nat → Nat → Option Nat := fun _ _ => none
def paBehind : Nat → Nat → Option Nat := fun p o => if p = 2 ∧ o = 1 then some 1 else none
def paCaughtUp : Nat → Nat → Option Nat := fun p o => if p = 2 ∧ o = 1 then some 2 else none

/-- Run successive GC calls with the given reports; return the final store and marks. -/
def gcCalls (peers : List Nat) (s : State) (m : Marks) :
    List (Nat → Nat → Option Nat) → State × Marks
  | [] => (s, m)
  | pa :: rest => let r := gcCall peers pa s m; gcCalls peers r.1 r.2 rest

-- `tests.rs: tombstone_gc_waits_for_every_peer_to_cover_the_delete`: no report, a
-- report just behind, then caught up (marks only), a stale envelope regressing the
-- report (unmarks), caught up again (marks), and the next call clears.
example : (gcCalls [2] gcStore noMarks [paNone]).1 "key1" = some gcTomb := by decide
example : (gcCalls [2] gcStore noMarks [paNone, paBehind]).1 "key1" = some gcTomb := by decide
example : (gcCalls [2] gcStore noMarks [paNone, paBehind, paCaughtUp]).1 "key1"
    = some gcTomb := by decide
example : (gcCalls [2] gcStore noMarks [paNone, paBehind, paCaughtUp, paBehind,
    paCaughtUp]).1 "key1" = some gcTomb := by decide
example : (gcCalls [2] gcStore noMarks [paNone, paBehind, paCaughtUp, paBehind,
    paCaughtUp, paCaughtUp]).1 "key1" = none := by decide
-- `tests.rs: a_single_node_cluster_collects_tombstones_immediately`.
example : (gcCall [] paNone gcStore noMarks).1 "key1" = none := by decide

def oldValue : Entry := ⟨"key1", some [1], ⟨2, 1, 500⟩⟩

/-- A stale delta carrying the deleted write, delivered after the marking call and
before the clearing call, loses to the tombstone (the `stale` step of the Rust test). -/
theorem stale_message_within_period_rejected :
    let r := gcCall [2] paCaughtUp gcStore noMarks
    r.1 "key1" = some gcTomb ∧
    mergeBatch (fun _ => true) r.1 [oldValue] "key1" = some gcTomb := by
  decide

/-- **Counterexample (delivery bound violated).** The same delta delayed past the
second call meets an empty slot and the deleted key comes back. -/
theorem delayed_message_resurrects :
    let r := gcCalls [2] gcStore noMarks [paCaughtUp, paCaughtUp]
    r.1 "key1" = none ∧
    mergeBatch (fun _ => true) r.1 [oldValue] "key1" = some oldValue := by
  decide

/-- **Counterexample (unsound ack).** Peer 2 claims to cover the delete but still holds
the older value. Before GC the tombstone rejects its delta; once two calls have cleared
the tombstone, the same delta lands. -/
theorem premature_gc_resurrects :
    let r := gcCalls [2] gcStore noMarks [paCaughtUp, paCaughtUp]
    mergeBatch (fun _ => true) gcStore [oldValue] "key1" = some gcTomb ∧
    r.1 "key1" = none ∧
    mergeBatch (fun _ => true) r.1 [oldValue] "key1" = some oldValue := by
  decide

/-! ## Cluster model -/

/-- A delta in flight to `dst`. `age` counts the GC calls `dst` has made since it was
sent. -/
structure Msg where
  dst : Nat
  entries : List Entry
  age : Nat

/-- Data maps and GC marks of all nodes, plus messages in flight. -/
structure Cluster where
  data : Nat → State
  marks : Nat → Marks
  inflight : List Msg

def update {α : Type} (f : Nat → α) (n : Nat) (a : α) : Nat → α :=
  fun m => if m = n then a else f m

def ageAt (n : Nat) (msg : Msg) : Msg :=
  if msg.dst = n then { msg with age := msg.age + 1 } else msg

theorem ageAt_entries (n : Nat) (msg : Msg) : (ageAt n msg).entries = msg.entries := by
  unfold ageAt; split <;> rfl

/-- Steps of an asynchronous cluster. `v` is an older write that local writes are
fresh against. -/
inductive Step (members : List Nat) (admit : Entry → Bool) (v : Entry) :
    Cluster → Cluster → Prop
  /-- `compute_delta` / `take_push_envelope`: any entries a member holds. -/
  | send (c : Cluster) (src dst : Nat) (m : List Entry) (_ : src ∈ members)
      (_ : ∀ e ∈ m, c.data src e.key = some e) :
      Step members admit v c { c with inflight := ⟨dst, m, 0⟩ :: c.inflight }
  /-- `merge_batch` of an in-flight message; the message stays (duplication). -/
  | deliver (c : Cluster) (msg : Msg) (_ : msg ∈ c.inflight) :
      Step members admit v c
        { c with
          data := (update c.data msg.dst (mergeBatch admit (c.data msg.dst) msg.entries)) }
  /-- Message loss. -/
  | drop (c : Cluster) (pre post : List Msg) (msg : Msg) (_ : c.inflight = pre ++ msg :: post) :
      Step members admit v c { c with inflight := pre ++ post }
  /-- `collect_tombstone_garbage` with any peer list and any reports. The delivery bound:
  no message to `n` survives from before its previous call. -/
  | collect (c : Cluster) (n : Nat) (peers : List Nat) (pa : Nat → Nat → Option Nat)
      (_ : n ∈ members) (_ : ∀ msg ∈ c.inflight, msg.dst = n → msg.age = 0)
      (_ : peers = [] → ∀ msg ∈ c.inflight, msg.dst ≠ n) :
      Step members admit v c
        { data := update c.data n (gcCall peers pa (c.data n) (c.marks n)).1,
          marks := update c.marks n (gcCall peers pa (c.data n) (c.marks n)).2,
          inflight := c.inflight.map (ageAt n) }
  /-- `put` / `delete` with `alloc_entry_meta` stamping. -/
  | write (c : Cluster) (n node seq : Nat) (now : Int) (k : Key) (val : Option Value)
      (_ : n ∈ members) (_ : stamped (c.data n) node seq now k val ≠ v) :
      Step members admit v c
        { c with
          data := (update c.data n (localWrite (c.data n) (stamped (c.data n) node seq now k val))) }

inductive Reachable (members : List Nat) (admit : Entry → Bool) (v : Entry) :
    Cluster → Cluster → Prop
  | refl (c : Cluster) : Reachable members admit v c c
  | tail (a b c : Cluster) : Reachable members admit v a b → Step members admit v b c →
      Reachable members admit v a c

/-- A message carrying `v` is harmless while its receiver holds something newer that
the receiver will not clear before the message is gone: whatever is marked is no newer
than the held entry, and if the held entry itself is marked the message has already
lived through one call (so the delivery bound forbids the clearing call). -/
def Guard (c : Cluster) (v : Entry) (msg : Msg) : Prop :=
  ∃ x, c.data msg.dst v.key = some x ∧ v.metadata < x.metadata ∧
    (∀ m, c.marks msg.dst v.key = some m → m ≤ x.metadata) ∧
    (c.marks msg.dst v.key = some x.metadata → 1 ≤ msg.age)

/-- The safety invariant: no member holds `v`, and every message carrying `v` is
guarded. -/
def Inv (members : List Nat) (v : Entry) (c : Cluster) : Prop :=
  (∀ n ∈ members, c.data n v.key ≠ some v) ∧ ∀ msg ∈ c.inflight, v ∈ msg.entries → Guard c v msg

/-- The held entry either stays or is replaced by a strictly newer one. -/
def Grows (a b : Option Entry) : Prop :=
  b = a ∨ ∃ y, b = some y ∧ ∀ x, a = some x → x.metadata < y.metadata

theorem grows_trans {a b c : Option Entry} (h₁ : Grows a b) (h₂ : Grows b c) : Grows a c := by
  rcases h₁ with rfl | ⟨y, rfl, hy⟩
  · exact h₂
  · rcases h₂ with rfl | ⟨z, rfl, hz⟩
    · exact Or.inr ⟨y, rfl, hy⟩
    · exact Or.inr ⟨z, rfl, fun x hx => Metadata.lt_trans (hy x hx) (hz y rfl)⟩

theorem apply_grows (s : State) (e : Entry) (k : Key) : Grows (s k) (apply s e k) := by
  unfold apply
  by_cases hb : beats (s e.key) e = true
  · simp only [hb, ite_true, setEntry]
    by_cases hk : k = e.key
    · subst hk
      refine Or.inr ⟨e, by simp, ?_⟩
      intro x hx; rw [hx] at hb; simpa [beats] using hb
    · simp [hk, Grows]
  · simp [hb, Grows]

theorem applyList_grows (s : State) (l : List Entry) (k : Key) :
    Grows (s k) (applyList s l k) := by
  induction l generalizing s with
  | nil => exact Or.inl rfl
  | cons e l ih => exact grows_trans (apply_grows s e k) (ih (apply s e))

theorem mergeBatch_grows (admit : Entry → Bool) (s : State) (l : List Entry) (k : Key) :
    Grows (s k) (mergeBatch admit s l k) := by
  rw [mergeBatch_eq, mergeAdmitted_eq_filter]; exact applyList_grows _ _ _

theorem applyList_from (s : State) (l : List Entry) (k : Key) (x : Entry)
    (h : applyList s l k = some x) : s k = some x ∨ x ∈ l := by
  rw [applyList_apply] at h
  obtain ⟨hx, -⟩ := (best_isMax (s k) (l.filter fun e => e.key = k)).2 x h
  simp only [candidates, List.mem_append, List.mem_filter] at hx
  rcases hx with hx | hx
  · left; cases hs : s k <;> simp_all
  · exact Or.inr hx.1

theorem mergeBatch_from (admit : Entry → Bool) (s : State) (l : List Entry) (k : Key)
    (x : Entry) (h : mergeBatch admit s l k = some x) : s k = some x ∨ x ∈ l := by
  rw [mergeBatch_eq, mergeAdmitted_eq_filter] at h
  rcases applyList_from _ _ _ _ h with h | h
  · exact Or.inl h
  · exact Or.inr (List.mem_filter.1 h).1

theorem localWrite_grows (s : State) (node seq : Nat) (now : Int) (k k' : Key)
    (val : Option Value) :
    Grows (s k') (localWrite s (stamped s node seq now k val) k') := by
  simp only [localWrite, setEntry]
  by_cases hk : k' = k
  · subst hk
    refine Or.inr ⟨stamped s node seq now k' val, by simp [stamped], ?_⟩
    intro x hx; exact stamped_gt s node seq now k' val x hx
  · have : ¬ k' = (stamped s node seq now k val).key := by simpa [stamped] using hk
    simp [this, Grows]

/-- A guard survives growth of the receiver's entry when its marks do not change. -/
theorem guard_of_grows {c c' : Cluster} {v : Entry} {msg msg' : Msg}
    (hg : Guard c v msg) (hdst : msg'.dst = msg.dst) (hage : msg.age ≤ msg'.age)
    (hgrow : Grows (c.data msg.dst v.key) (c'.data msg.dst v.key))
    (hmarks : c'.marks msg.dst v.key = c.marks msg.dst v.key) : Guard c' v msg' := by
  obtain ⟨x, hx, hvx, hm, hage1⟩ := hg
  unfold Guard; rw [hdst]
  rcases hgrow with h | ⟨y, hy, hxy⟩
  · refine ⟨x, by rw [h, hx], hvx, ?_, ?_⟩
    · intro m hm'; rw [hmarks] at hm'; exact hm m hm'
    · intro hm'; rw [hmarks] at hm'; have := hage1 hm'; omega
  · have hxy := hxy x hx
    refine ⟨y, hy, Metadata.lt_trans hvx hxy, ?_, ?_⟩
    · intro m hm'; rw [hmarks] at hm'
      exact Metadata.le_of_lt (Metadata.lt_of_le_of_lt (hm m hm') hxy)
    · intro hm'; rw [hmarks] at hm'
      have := Metadata.lt_of_le_of_lt (hm _ hm') hxy
      exact absurd this (Metadata.lt_irrefl _)

/-- After the receiver's entry grows, it cannot be `v` if it was not `v` before, unless
`v` arrived in a message whose guard says the receiver already held something newer. -/
theorem not_v_after_deliver {s : State} {admit : Entry → Bool} {l : List Entry} {v : Entry}
    (hold : s v.key ≠ some v)
    (hguard : v ∈ l → ∃ x, s v.key = some x ∧ v.metadata < x.metadata) :
    mergeBatch admit s l v.key ≠ some v := by
  intro h
  rcases mergeBatch_from _ _ _ _ _ h with h' | h'
  · exact hold h'
  · obtain ⟨x, hx, hvx⟩ := hguard h'
    rcases mergeBatch_grows admit s l v.key with hg | ⟨y, hy, hxy⟩
    · rw [hg, hx] at h; cases h; exact Metadata.lt_irrefl _ hvx
    · rw [hy] at h; cases h
      exact Metadata.lt_irrefl _ (Metadata.lt_trans hvx (hxy x hx))

theorem guard_other {c c' : Cluster} {v : Entry} {msg : Msg} (hg : Guard c v msg)
    (hd : c'.data msg.dst = c.data msg.dst) (hm : c'.marks msg.dst = c.marks msg.dst) :
    Guard c' v msg :=
  guard_of_grows hg rfl (Nat.le_refl _) (by rw [hd]; exact Or.inl rfl) (by rw [hm])

theorem step_inv {members : List Nat} {admit : Entry → Bool} {v : Entry} {a b : Cluster}
    (hs : Step members admit v a b) (ha : Inv members v a) : Inv members v b := by
  obtain ⟨hd, hm⟩ := ha
  cases hs with
  | send src dst m hsrc hsend =>
    refine ⟨hd, ?_⟩
    intro msg hmsg hv
    simp only [List.mem_cons] at hmsg
    rcases hmsg with rfl | hmsg
    · exact absurd (hsend v hv) (hd src hsrc)
    · exact guard_other (hm msg hmsg hv) rfl rfl
  | deliver msg0 hin =>
    refine ⟨?_, ?_⟩
    · intro n hn
      simp only [update]
      split
      · rename_i h; subst h
        apply not_v_after_deliver (hd _ hn)
        intro hv
        obtain ⟨x, hx, hvx, -, -⟩ := hm msg0 hin hv
        exact ⟨x, hx, hvx⟩
      · exact hd n hn
    · intro msg hmsg hv
      by_cases hdst : msg.dst = msg0.dst
      · apply guard_of_grows (hm msg hmsg hv) rfl (Nat.le_refl _)
        · simp only [update, hdst, ite_true]; exact mergeBatch_grows _ _ _ _
        · rfl
      · exact guard_other (hm msg hmsg hv) (by simp [update, hdst]) rfl
  | drop pre post msg0 heq =>
    refine ⟨hd, ?_⟩
    intro msg hmsg hv
    have : msg ∈ a.inflight := by
      rw [heq]; simp only [List.mem_append, List.mem_cons] at hmsg ⊢
      rcases hmsg with h | h
      · exact Or.inl h
      · exact Or.inr (Or.inr h)
    exact guard_other (hm msg this hv) rfl rfl
  | collect n peers pa hn hbound hsingle =>
    refine ⟨?_, ?_⟩
    · intro n' hn'
      simp only [update]
      split
      · rename_i h; subst h
        rcases gcCall_key peers pa (a.data n') (a.marks n') v.key with h | h <;> rw [h]
        · simp
        · exact hd n' hn'
      · exact hd n' hn'
    · intro msg' hmsg' hv
      obtain ⟨msg, hmsg, rfl⟩ := List.mem_map.1 hmsg'
      rw [ageAt_entries] at hv
      by_cases hdst : msg.dst = n
      · -- The receiver collected. Its guard must still hold.
        have hage0 := hbound msg hmsg hdst
        obtain ⟨x, hx, hvx, hmk, hmx⟩ := hm msg hmsg hv
        rw [hdst] at hx hmk hmx
        have hageAt : ageAt n msg = { msg with age := msg.age + 1 } := by simp [ageAt, hdst]
        rw [hageAt]
        by_cases hp : peers.isEmpty = true
        · exact absurd hdst (hsingle (List.isEmpty_iff.1 hp) msg hmsg)
        · -- Two-phase path.
          have hgc : gcCall peers pa (a.data n) (a.marks n) =
              (fun k => if due (collectableMeta peers pa (a.data n)) (a.marks n) k then none
                else a.data n k,
               fun k => if due (collectableMeta peers pa (a.data n)) (a.marks n) k then none
                else collectableMeta peers pa (a.data n) k) := by
            simp [gcCall, hp]
          by_cases hdue : due (collectableMeta peers pa (a.data n)) (a.marks n) v.key = true
          · obtain ⟨m, hc, hmm⟩ := due_true hdue
            obtain ⟨t, ht, htm, -⟩ := collectableMeta_some hc
            rw [hx] at ht; cases ht
            rw [htm] at hmx
            have := hmx hmm; omega
          · refine ⟨x, ?_, hvx, ?_, ?_⟩
            · simp [update, hdst, hgc, hdue, hx]
            · intro m hm'
              simp only [update, hdst, ite_true, hgc, hdue] at hm'
              simp only [Bool.false_eq_true, ite_false] at hm'
              obtain ⟨t, ht, htm, -⟩ := collectableMeta_some hm'
              rw [hx] at ht; cases ht; rw [htm]; exact Metadata.le_refl _
            · intro _; simp
      · have hageAt : ageAt n msg = msg := by simp [ageAt, hdst]
        rw [hageAt]
        exact guard_other (hm msg hmsg hv) (by simp [update, hdst]) (by simp [update, hdst])
  | write n node seq now k val hn hfresh =>
    refine ⟨?_, ?_⟩
    · intro n' hn'
      simp only [update]
      split
      · rename_i h; subst h
        simp only [localWrite, setEntry]
        split
        · intro h; exact hfresh (Option.some.inj h)
        · exact hd n' hn'
      · exact hd n' hn'
    · intro msg hmsg hv
      by_cases hdst : msg.dst = n
      · apply guard_of_grows (hm msg hmsg hv) rfl (Nat.le_refl _)
        · simp only [update, hdst, ite_true]; exact localWrite_grows _ _ _ _ _ _ _
        · rfl
      · exact guard_other (hm msg hmsg hv) (by simp [update, hdst]) rfl

theorem reachable_inv {members : List Nat} {admit : Entry → Bool} {v : Entry}
    {a b : Cluster} (h : Reachable members admit v a b) (ha : Inv members v a) :
    Inv members v b := by
  induction h with
  | refl => exact ha
  | tail _ _ _ hs ih => exact step_inv hs ih

/-- **No resurrection.** From a configuration satisfying `Inv`, no member ever again
holds the older write `v`, whatever the cluster does under the delivery bound. -/
theorem no_resurrection (members : List Nat) (admit : Entry → Bool) (v : Entry)
    (a b : Cluster) (ha : Inv members v a) (hreach : Reachable members admit v a b) :
    ∀ n ∈ members, b.data n v.key ≠ some v :=
  (reachable_inv hreach ha).1

/-- `Inv` holds once every member holds something newer than `v` at its key, no member
has marked the entry it holds there, and every message is addressed to a member. -/
theorem inv_establish (members : List Nat) (v : Entry) (c : Cluster)
    (hheld : ∀ n ∈ members, ∃ x, c.data n v.key = some x ∧ v.metadata < x.metadata ∧
      ∀ m, c.marks n v.key = some m → m < x.metadata)
    (hdst : ∀ msg ∈ c.inflight, msg.dst ∈ members) : Inv members v c := by
  refine ⟨?_, ?_⟩
  · intro n hn h
    obtain ⟨x, hx, hvx, -⟩ := hheld n hn
    rw [h] at hx; cases hx; exact Metadata.lt_irrefl _ hvx
  · intro msg hmsg _
    obtain ⟨x, hx, hvx, hm⟩ := hheld msg.dst (hdst msg hmsg)
    refine ⟨x, hx, hvx, fun m h => Metadata.le_of_lt (hm m h), ?_⟩
    intro h; exact absurd (hm _ h) (Metadata.lt_irrefl _)

/-- **Ack soundness** (RFC 0001 INV): a peer whose reported ack for `t`'s origin reaches
`t.seq` holds `t` or something newer at `t.key`. -/
def AckSound (pa : Nat → Nat → Option Nat) (peers : List Nat) (data : Nat → State)
    (t : Entry) : Prop :=
  ∀ p ∈ peers, ∀ r, pa p t.metadata.node = some r → t.metadata.seq ≤ r →
    ∃ x, data p t.key = some x ∧ t.metadata ≤ x.metadata

/-- When the collector `c` finds its tombstone `t` collectable under sound reports, the
delete has reached every member: each holds something newer than any older write `v`. -/
theorem reached_everyone_of_ackSound (members peers : List Nat) (pa : Nat → Nat → Option Nat)
    (data : Nat → State) (c : Nat) (t v : Entry) (hpeers : ∀ n ∈ members, n ≠ c → n ∈ peers)
    (ht : data c t.key = some t) (hcol : collectable pa peers t = true)
    (hsound : AckSound pa peers data t) (hv : v.key = t.key) (hvt : v.metadata < t.metadata) :
    ∀ n ∈ members, ∃ x, data n v.key = some x ∧ v.metadata < x.metadata := by
  intro n hn
  rw [hv]
  by_cases hnc : n = c
  · subst hnc; exact ⟨t, ht, hvt⟩
  · obtain ⟨-, hacks⟩ := collectable_acked hcol
    obtain ⟨r, hr, hle⟩ := hacks n (hpeers n hn hnc)
    obtain ⟨x, hx, htx⟩ := hsound n (hpeers n hn hnc) r hr hle
    exact ⟨x, hx, Metadata.lt_of_lt_of_le hvt htx⟩

end Wavekv
