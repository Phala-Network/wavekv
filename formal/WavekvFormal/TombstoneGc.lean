import WavekvFormal.MergeBatch

/-!
# Tombstone GC never resurrects a deleted key

Models `wavekv/src/node.rs: NodeState::collect_tombstone_garbage` (the `watermark`
closure and the `StateOp::Clear` it issues; `ops.rs: CoreState::execute` arm `Clear`),
and an asynchronous cluster whose nodes send deltas, deliver them (`merge_batch`), run
GC, and write locally.

Proven:
* `gc_only_collects_acked_tombstones`: a key is cleared only if it held a tombstone
  `(n, s)` and every known peer `p` reported `peer_acks[p][n] >= s` (or there are no
  peers). `gc_keeps_live` / `gc_keeps_unacked`: live entries are never cleared.
* `no_resurrection`: after a node collects tombstone `t` for key `k`, no older write
  `v` of `k` (`v.meta < t.meta`) is ever held again by any member, in any execution of
  sends, deliveries (with loss, duplication, reordering), further GCs, and local writes.
* `premature_gc_resurrects`, `stale_message_resurrects`: both hypotheses are needed.

Assumptions (explicit hypotheses of `no_resurrection`):
* Ack soundness (`AckSound`, RFC 0001 INV): if peer `p` reported `ack >= t.seq` for
  `t`'s origin, then `p` currently holds nothing older than `t` at `t.key`.
* Closed membership: every node that can send to the collector is a known peer
  (`get_peers()` covers the cluster).
* No in-flight message still carries `v` at GC time. Acks alone do not ensure this: a
  delta or push built before `p` merged `t` and delivered after GC re-introduces `v`
  (`stale_message_resurrects`).
* Seq freshness: a new local write is never `v` itself (`alloc_entry_meta` never
  reissues a `(node, seq)`).

Modeling gap: `peer_acks` and the GC decision are taken at one instant; acks are `Nat`;
`u64::MAX` is the initial watermark. Digest repair is not modeled (it only re-sends
entries a node holds, i.e. it is a `send`).
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

/-- `node.rs: NodeState::collect_tombstone_garbage`: `Clear` every collectable key;
with no peers, every tombstone. -/
def gc (peers : List Nat) (pa : Nat → Nat → Option Nat) (s : State) : State := fun k =>
  match s k with
  | none => none
  | some e =>
    if (peers.isEmpty && e.isDeleted) || collectable pa peers e then none else some e

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

/-- **GC rule.** A cleared key held a tombstone that every peer reported covering. -/
theorem gc_only_collects_acked_tombstones (peers : List Nat) (pa : Nat → Nat → Option Nat)
    (s : State) (k : Key) (t : Entry) (ht : s k = some t) (hgc : gc peers pa s k = none) :
    t.isDeleted = true ∧
      (peers = [] ∨ ∀ p ∈ peers, ∃ r, pa p t.metadata.node = some r ∧ t.metadata.seq ≤ r) := by
  simp only [gc, ht] at hgc
  split at hgc
  · rename_i hc
    simp only [Bool.or_eq_true, Bool.and_eq_true, List.isEmpty_iff] at hc
    rcases hc with ⟨hp, hd⟩ | hc
    · exact ⟨hd, Or.inl hp⟩
    · simp only [collectable, Bool.and_eq_true] at hc
      obtain ⟨hd, hw⟩ := hc
      refine ⟨hd, Or.inr ?_⟩
      cases hwm : watermark pa peers t.metadata.node with
      | none => rw [hwm] at hw; simp at hw
      | some w =>
        rw [hwm] at hw
        simp at hw
        exact (watermarkFrom_ge pa _ _ peers u64Max w hwm hw).2
  · simp at hgc

theorem gc_keeps_live (peers : List Nat) (pa : Nat → Nat → Option Nat) (s : State) (k : Key)
    (e : Entry) (he : s k = some e) (hlive : e.isDeleted = false) : gc peers pa s k = some e := by
  simp [gc, he, hlive, collectable]

theorem gc_keeps_unacked (peers : List Nat) (pa : Nat → Nat → Option Nat) (s : State) (k : Key)
    (e : Entry) (he : s k = some e) (hne : peers ≠ [])
    (p : Nat) (hp : p ∈ peers) (hlag : ∀ r, pa p e.metadata.node = some r → r < e.metadata.seq) :
    gc peers pa s k = some e := by
  cases h : gc peers pa s k with
  | some e' => simp only [gc, he] at h; split at h <;> simp_all
  | none =>
    obtain ⟨-, hc⟩ := gc_only_collects_acked_tombstones peers pa s k e he h
    rcases hc with hc | hc
    · exact absurd hc hne
    · obtain ⟨r, hr, hle⟩ := hc p hp
      have := hlag r hr; omega

theorem gc_key (peers : List Nat) (pa : Nat → Nat → Option Nat) (s : State) (k : Key) :
    gc peers pa s k = none ∨ gc peers pa s k = s k := by
  simp only [gc]; split
  · exact Or.inl rfl
  · rename_i e he; rw [he]; split <;> simp

/-! ## Mirrored tests -/

def gcTomb : Entry := ⟨"key1", none, ⟨1, 2, 1000⟩⟩
def gcStore : State := fun k => if k = "key1" then some gcTomb else none

-- `tests.rs: tombstone_gc_waits_for_every_peer_to_cover_the_delete`.
example : gc [2] (fun _ _ => none) gcStore "key1" = some gcTomb := by decide
example : gc [2] (fun p o => if p = 2 ∧ o = 1 then some 1 else none) gcStore "key1"
    = some gcTomb := by decide
example : gc [2] (fun p o => if p = 2 ∧ o = 1 then some 2 else none) gcStore "key1"
    = none := by decide
-- `tests.rs: a_single_node_cluster_collects_tombstones_immediately`.
example : gc [] (fun _ _ => none) gcStore "key1" = none := by decide

/-! ## Cluster model -/

/-- Data maps of all nodes, plus messages in flight (a message is a delta: a list of
entries, possibly delivered many times or never). -/
structure Cluster where
  data : Nat → State
  inflight : List (List Entry)

def update (f : Nat → State) (n : Nat) (s : State) : Nat → State :=
  fun m => if m = n then s else f m

/-- Steps of an asynchronous cluster. `v` is an older write that local writes are
fresh against. -/
inductive Step (members : List Nat) (admit : Entry → Bool) (v : Entry) :
    Cluster → Cluster → Prop
  /-- `compute_delta` / `take_push_envelope`: any entries the sender holds. -/
  | send (c : Cluster) (n : Nat) (m : List Entry) (_ : n ∈ members)
      (_ : ∀ e ∈ m, c.data n e.key = some e) :
      Step members admit v c { c with inflight := m :: c.inflight }
  /-- `merge_batch` of an in-flight message; the message stays (duplication). -/
  | deliver (c : Cluster) (n : Nat) (m : List Entry) (_ : n ∈ members) (_ : m ∈ c.inflight) :
      Step members admit v c { c with data := update c.data n (mergeBatch admit (c.data n) m) }
  /-- Message loss. -/
  | drop (c : Cluster) (m : List Entry) (rest : List (List Entry))
      (_ : c.inflight = m :: rest) :
      Step members admit v c { c with inflight := rest }
  /-- `collect_tombstone_garbage` with any peer list and any (even unsound) reports. -/
  | collect (c : Cluster) (n : Nat) (peers : List Nat) (pa : Nat → Nat → Option Nat)
      (_ : n ∈ members) :
      Step members admit v c { c with data := update c.data n (gc peers pa (c.data n)) }
  /-- `put` / `delete`: a fresh entry, `Set` unconditionally. -/
  | write (c : Cluster) (n : Nat) (w : Entry) (_ : n ∈ members) (_ : w ≠ v) :
      Step members admit v c { c with data := update c.data n (localWrite (c.data n) w) }

inductive Reachable (members : List Nat) (admit : Entry → Bool) (v : Entry) :
    Cluster → Cluster → Prop
  | refl (c : Cluster) : Reachable members admit v c c
  | tail (a b c : Cluster) : Reachable members admit v a b → Step members admit v b c →
      Reachable members admit v a c

/-- Nobody holds `v`, and no message carries it. -/
def Clean (members : List Nat) (v : Entry) (c : Cluster) : Prop :=
  (∀ n ∈ members, c.data n v.key ≠ some v) ∧ ∀ m ∈ c.inflight, v ∉ m

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

theorem step_clean {members : List Nat} {admit : Entry → Bool} {v : Entry} {a b : Cluster}
    (hs : Step members admit v a b) (ha : Clean members v a) : Clean members v b := by
  obtain ⟨hd, hm⟩ := ha
  cases hs with
  | send n m hn hsend =>
    refine ⟨hd, ?_⟩
    intro m' hm'
    simp at hm'
    rcases hm' with rfl | hm'
    · intro hv; exact hd n hn (hsend v hv)
    · exact hm m' hm'
  | deliver n m hn hin =>
    refine ⟨?_, hm⟩
    intro n' hn'
    simp only [update]
    split
    · rename_i h; subst h
      intro hx
      rcases mergeBatch_from _ _ _ _ _ hx with hx | hx
      · exact hd n' hn' hx
      · exact hm m hin hx
    · exact hd n' hn'
  | drop m rest heq =>
    refine ⟨hd, ?_⟩
    intro m' hm'; exact hm m' (by rw [heq]; simp [hm'])
  | collect n peers pa hn =>
    refine ⟨?_, hm⟩
    intro n' hn'
    simp only [update]
    split
    · rename_i h; subst h
      rcases gc_key peers pa (a.data n') v.key with h | h <;> rw [h]
      · simp
      · exact hd n' hn'
    · exact hd n' hn'
  | write n w hn hw =>
    refine ⟨?_, hm⟩
    intro n' hn'
    simp only [update]
    split
    · rename_i h; subst h
      simp only [localWrite, setEntry]
      split
      · intro h; exact hw (Option.some.inj h)
      · exact hd n' hn'
    · exact hd n' hn'

theorem reachable_clean {members : List Nat} {admit : Entry → Bool} {v : Entry}
    {a b : Cluster} (h : Reachable members admit v a b) (ha : Clean members v a) :
    Clean members v b := by
  induction h with
  | refl => exact ha
  | tail _ _ _ hs ih => exact step_clean hs ih

/-- `p` holds nothing older than `t` at `t.key`. -/
def Covers (s : State) (t : Entry) : Prop :=
  ∀ x, s t.key = some x → t.metadata ≤ x.metadata

/-- **Ack soundness** (RFC 0001 INV, at the GC instant): a peer whose reported ack for
`t`'s origin reaches `t.seq` holds nothing older than `t` at `t.key`. -/
def AckSound (pa : Nat → Nat → Option Nat) (peers : List Nat) (data : Nat → State)
    (t : Entry) : Prop :=
  ∀ p ∈ peers, ∀ r, pa p t.metadata.node = some r → t.metadata.seq ≤ r → Covers (data p) t

/-- **No resurrection.** Node `c` collects its tombstone `t`. Then no member ever again
holds an older write `v` of the same key, whatever the cluster does next. -/
theorem no_resurrection (members peers : List Nat) (admit : Entry → Bool)
    (pa : Nat → Nat → Option Nat) (cl : Cluster) (c : Nat) (t v : Entry)
    (hpeers : ∀ n ∈ members, n ≠ c → n ∈ peers)
    (ht : cl.data c t.key = some t) (hcollected : gc peers pa (cl.data c) t.key = none)
    (hsound : AckSound pa peers cl.data t)
    (hv : v.key = t.key) (hvt : v.metadata < t.metadata)
    (hmsgs : ∀ m ∈ cl.inflight, v ∉ m)
    (cl' : Cluster)
    (hreach : Reachable members admit v
      { cl with data := update cl.data c (gc peers pa (cl.data c)) } cl') :
    ∀ n ∈ members, cl'.data n v.key ≠ some v := by
  refine (reachable_clean hreach ⟨?_, hmsgs⟩).1
  intro n hn
  simp only [update]
  split
  · rw [hv, hcollected]; simp
  · rename_i hnc
    intro hx
    obtain ⟨-, hc⟩ := gc_only_collects_acked_tombstones peers pa _ _ t ht hcollected
    rcases hc with hc | hc
    · have := hpeers n hn hnc; rw [hc] at this; simp at this
    · obtain ⟨r, hr, hle⟩ := hc n (hpeers n hn hnc)
      have hcov := hsound n (hpeers n hn hnc) r hr hle v (by rw [← hv]; exact hx)
      exact Metadata.lt_irrefl _ (Metadata.lt_of_lt_of_le hvt hcov)

/-! ## The hypotheses are needed -/

def oldValue : Entry := ⟨"key1", some [1], ⟨2, 1, 500⟩⟩
def peerStore : State := fun k => if k = "key1" then some oldValue else none

/-- **Counterexample (unsound ack).** Peer 2 claims to cover the delete but still holds
the older value. Before GC the tombstone rejects it; after GC the same delta lands. -/
theorem premature_gc_resurrects :
    let pa := fun p o => if p = 2 ∧ o = 1 then some 2 else none
    mergeBatch (fun _ => true) gcStore [oldValue] "key1" = some gcTomb ∧
    gc [2] pa gcStore "key1" = none ∧
    mergeBatch (fun _ => true) (gc [2] pa gcStore) [peerStore "key1"].reduceOption "key1"
      = some oldValue := by
  decide

/-- **Counterexample (stale message).** Peer 2 does hold the tombstone, so its ack is
sound, but a delta it built earlier (carrying `oldValue`) is still in flight. -/
theorem stale_message_resurrects :
    let pa := fun p o => if p = 2 ∧ o = 1 then some 2 else none
    let peerNow : State := gcStore
    Covers peerNow gcTomb ∧
    mergeBatch (fun _ => true) (gc [2] pa gcStore) [oldValue] "key1" = some oldValue := by
  refine ⟨?_, by decide⟩
  intro x hx
  simp [gcStore, gcTomb] at hx
  subst hx; exact Metadata.le_refl _

end Wavekv
