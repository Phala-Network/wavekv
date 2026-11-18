import WavekvFormal.Metadata

/-!
# LWW merge: semilattice and strong eventual consistency

Models:
* `wavekv/src/types.rs`: `struct Entry` (a tombstone is `value = None`).
* `wavekv/src/node.rs`: `fn beats`, and the merge half of `NodeState::sync`
  (`beats` then `StateOp::Set`).
* `wavekv/src/ops.rs`: `CoreState::execute`, arm `StateOp::Set` (an unconditional
  insert into `data`).

Proven:
* `join_comm`, `join_assoc`, `join_idem`: the per-key merge is a join-semilattice
  (commutativity needs the two entries to be *coherent*: equal metadata implies equal
  entry). Lifted pointwise to whole states: `State.merge_comm/assoc/idem`.
* `applyList_apply`: replaying a list of entries leaves at each key the coherent maximum
  of the start value and the entries for that key.
* `convergence`: two replicas that start from the same state and receive the same *set*
  of entries (any order, any duplicates) end in the same state.
* Tombstones are ordinary entries: `beats` never looks at `value`
  (`beats_ignores_value`), so a delete wins or loses exactly like a put.
* `coherence_needed`: without coherence the result depends on delivery order.

Assumption: coherence (`Coherent`) of all entries that share a key, i.e. a
`(node, seq)` pair is never reused with a different key/value. The Rust code defends
this with seq recovery (`NodeState::recover_next_seq`) and node-id reuse rejection; it
is a hypothesis here.

Modeling gap: `BTreeMap<String, Entry>` is a total function `String → Option Entry`
(finite support and iteration order are not modeled). `Clear` (tombstone GC) is not a
join and is treated separately in `WavekvFormal.TombstoneGc`. Values are `List UInt8`.
-/

namespace Wavekv

abbrev Key := String
abbrev Value := List UInt8

/-- `types.rs: struct Entry { key, value, meta }`; `value = none` is a tombstone. -/
structure Entry where
  key : Key
  value : Option Value
  /-- Rust field `meta` (`meta` is a Lean keyword). -/
  metadata : Metadata
  deriving DecidableEq, Repr

/-- `types.rs: Entry::is_deleted`. -/
def Entry.isDeleted (e : Entry) : Bool := e.value.isNone

/-- `types.rs: compare_entries`: `entry1.meta.cmp(&entry2.meta)`. -/
def compareEntries (a b : Entry) : Ordering := a.metadata.cmp b.metadata

/-- `node.rs: fn beats`: `candidate` replaces `current` iff `current < candidate`
under `compare_entries`, or nothing is held. -/
def beats : Option Entry → Entry → Bool
  | some current, candidate => decide (current.metadata < candidate.metadata)
  | none, _ => true

/-- `ops.rs: CoreState::execute(StateOp::Set(entry))`: unconditional insert. -/
def setEntry (s : Key → Option Entry) (e : Entry) : Key → Option Entry :=
  fun k => if k = e.key then some e else s k

/-- Replicated data map: `ops.rs: CoreState::data: BTreeMap<String, Entry>`. -/
abbrev State := Key → Option Entry

def State.empty : State := fun _ => none

/-- `node.rs: NodeState::sync` without admission: `beats`, then `Set`. -/
def apply (s : State) (e : Entry) : State :=
  if beats (s e.key) e then setEntry s e else s

/-- Merge a received sequence of entries one by one. -/
def applyList (s : State) (l : List Entry) : State := l.foldl apply s

/-- Per-key join: the larger entry under the LWW order (left wins a tie). -/
def join : Option Entry → Option Entry → Option Entry
  | none, b => b
  | some a, none => some a
  | some a, some b => if a.metadata < b.metadata then some b else some a

/-- Entries are coherent when equal metadata forces equality. -/
def Coherent (a b : Entry) : Prop := a.metadata = b.metadata → a = b

def OCoherent : Option Entry → Option Entry → Prop
  | some a, some b => Coherent a b
  | _, _ => True

theorem beats_ignores_value (cur : Option Entry) (e : Entry) (v : Option Value) :
    beats cur { e with value := v } = beats cur e := by
  cases cur <;> rfl

theorem beats_iff_some (c e : Entry) : beats (some c) e = true ↔ c.metadata < e.metadata := by
  simp [beats]

/-- `beats` is `compare_entries(current, candidate) == Ordering::Less`. -/
theorem beats_iff_compareEntries (c e : Entry) :
    beats (some c) e = true ↔ compareEntries c e = .lt := by
  simp [beats, compareEntries]; rfl

theorem apply_key (s : State) (e : Entry) (k : Key) :
    apply s e k = if k = e.key then join (s k) (some e) else s k := by
  unfold apply setEntry
  by_cases hk : k = e.key
  · subst hk
    cases h : s e.key with
    | none => simp [beats, join]
    | some c => by_cases hlt : c.metadata < e.metadata <;> simp [beats, join, h, hlt]
  · by_cases hb : beats (s e.key) e = true <;> simp [hb, hk]

/-! ## Semilattice laws -/

theorem join_idem (a : Option Entry) : join a a = a := by
  cases a with
  | none => rfl
  | some a => simp [join, Metadata.lt_irrefl]

theorem join_comm (a b : Option Entry) (h : OCoherent a b) : join a b = join b a := by
  cases a with
  | none => cases b <;> rfl
  | some a =>
    cases b with
    | none => rfl
    | some b =>
      simp only [join]
      rcases Metadata.lt_total a.metadata b.metadata with hab | hab | hab
      · simp [hab, Metadata.lt_asymm hab]
      · have := h hab; subst this; simp
      · simp [hab, Metadata.lt_asymm hab]

theorem join_assoc (a b c : Option Entry) : join (join a b) c = join a (join b c) := by
  cases a with
  | none => cases b <;> cases c <;> rfl
  | some a =>
    cases b with
    | none => cases c <;> rfl
    | some b =>
      cases c with
      | none => by_cases hab : a.metadata < b.metadata <;> simp [join, hab]
      | some c =>
        by_cases hab : a.metadata < b.metadata <;> by_cases hbc : b.metadata < c.metadata <;>
          by_cases hac : a.metadata < c.metadata <;> simp [join, hab, hbc, hac]
        · exact absurd (Metadata.lt_trans hab hbc) hac
        · have hca : c.metadata ≤ a.metadata := Metadata.le_trans
            (Metadata.not_lt_iff_le.1 hbc) (Metadata.not_lt_iff_le.1 hab)
          exact absurd (Metadata.lt_of_lt_of_le hac hca) (Metadata.lt_irrefl _)

/-- Whole-state merge (a delta is itself a state). -/
def State.merge (s t : State) : State := fun k => join (s k) (t k)

def State.Coherent (s t : State) : Prop := ∀ k, OCoherent (s k) (t k)

theorem State.merge_comm (s t : State) (h : State.Coherent s t) :
    s.merge t = t.merge s := funext fun k => join_comm _ _ (h k)

theorem State.merge_assoc (s t u : State) : (s.merge t).merge u = s.merge (t.merge u) :=
  funext fun _ => join_assoc _ _ _

theorem State.merge_idem (s : State) : s.merge s = s := funext fun _ => join_idem _

/-! ## Characterising a replay -/

/-- The candidates a key's final value is chosen from. -/
def candidates (init : Option Entry) (l : List Entry) : List Entry := init.toList ++ l

/-- `r` is a maximum of the candidate list `c` under the LWW order. -/
def IsMax (r : Option Entry) (c : List Entry) : Prop :=
  (r = none ↔ c = []) ∧ ∀ x, r = some x → x ∈ c ∧ ∀ y ∈ c, y.metadata ≤ x.metadata

/-- Fold of `join` over the entries for one key. -/
def best (init : Option Entry) (l : List Entry) : Option Entry :=
  l.foldl (fun acc e => join acc (some e)) init

theorem join_some_isMax (acc : Option Entry) (e : Entry) (c : List Entry)
    (h : IsMax acc c) : IsMax (join acc (some e)) (c ++ [e]) := by
  obtain ⟨h1, h2⟩ := h
  cases acc with
  | none =>
    have hc : c = [] := h1.1 rfl
    subst hc
    refine ⟨by simp [join], ?_⟩
    intro x hx
    simp [join] at hx; subst hx
    simp [Metadata.le_refl]
  | some a =>
    obtain ⟨ha, hmax⟩ := h2 a rfl
    refine ⟨by simp [join]; split <;> simp, ?_⟩
    intro x hx
    simp only [join] at hx
    by_cases hlt : a.metadata < e.metadata
    · simp [hlt] at hx; subst hx
      refine ⟨by simp, ?_⟩
      intro y hy
      simp at hy
      rcases hy with hy | hy
      · exact Metadata.le_of_lt (Metadata.lt_of_le_of_lt (hmax y hy) hlt)
      · subst hy; exact Metadata.le_refl _
    · simp [hlt] at hx; subst hx
      refine ⟨by simp [ha], ?_⟩
      intro y hy
      simp at hy
      rcases hy with hy | hy
      · exact hmax y hy
      · subst hy; exact Metadata.not_lt_iff_le.1 hlt

theorem foldl_join_isMax (l : List Entry) (acc : Option Entry) (c : List Entry)
    (h : IsMax acc c) : IsMax (l.foldl (fun acc e => join acc (some e)) acc) (c ++ l) := by
  induction l generalizing acc c with
  | nil => simpa using h
  | cons e l ih =>
    simp only [List.foldl_cons]
    have := ih _ _ (join_some_isMax acc e c h)
    simpa using this

theorem best_isMax (init : Option Entry) (l : List Entry) :
    IsMax (best init l) (candidates init l) := by
  unfold best candidates
  apply foldl_join_isMax
  cases init with
  | none => exact ⟨by simp, by simp⟩
  | some a => exact ⟨by simp, by simp [Metadata.le_refl]⟩

/-- Two maxima over candidate lists with the same members are equal, given coherence. -/
theorem isMax_unique {r₁ r₂ : Option Entry} {c₁ c₂ : List Entry}
    (h₁ : IsMax r₁ c₁) (h₂ : IsMax r₂ c₂) (hmem : ∀ x, x ∈ c₁ ↔ x ∈ c₂)
    (hcoh : ∀ x ∈ c₁, ∀ y ∈ c₁, Coherent x y) : r₁ = r₂ := by
  cases r₁ with
  | none =>
    have hc1 : c₁ = [] := h₁.1.1 rfl
    have hc2 : c₂ = [] := by
      cases c₂ with
      | nil => rfl
      | cons y _ => exact absurd ((hmem y).2 (by simp)) (by simp [hc1])
    exact (h₂.1.2 hc2).symm
  | some x =>
    obtain ⟨hx, hxmax⟩ := h₁.2 x rfl
    cases r₂ with
    | none =>
      have hc2 : c₂ = [] := h₂.1.1 rfl
      exact absurd ((hmem x).1 hx) (by simp [hc2])
    | some y =>
      obtain ⟨hy, hymax⟩ := h₂.2 y rfl
      have hy1 : y ∈ c₁ := (hmem y).2 hy
      have hle1 : y.metadata ≤ x.metadata := hxmax y hy1
      have hle2 : x.metadata ≤ y.metadata := hymax x ((hmem x).1 hx)
      rw [hcoh x hx y hy1 (Metadata.le_antisymm hle2 hle1)]

theorem applyList_apply (s : State) (l : List Entry) (k : Key) :
    applyList s l k = best (s k) (l.filter (fun e => e.key = k)) := by
  induction l generalizing s with
  | nil => rfl
  | cons e l ih =>
    simp only [applyList, List.foldl_cons] at ih ⊢
    rw [ih, apply_key]
    by_cases hk : k = e.key
    · subst hk; simp [best]
    · have : ¬ e.key = k := fun h => hk h.symm
      simp [hk, this]

/-- Coherence of a start state and a batch of entries, per key. -/
def BatchCoherent (s : State) (l : List Entry) : Prop :=
  ∀ k, ∀ x ∈ candidates (s k) (l.filter (fun e => e.key = k)),
    ∀ y ∈ candidates (s k) (l.filter (fun e => e.key = k)), Coherent x y

/-- **Strong eventual consistency.** Replicas that start from the same state and
receive the same set of entries, in any order and with any duplicates, are equal. -/
theorem convergence (s : State) (l₁ l₂ : List Entry) (hset : ∀ e, e ∈ l₁ ↔ e ∈ l₂)
    (hcoh : BatchCoherent s l₁) : applyList s l₁ = applyList s l₂ := by
  funext k
  rw [applyList_apply, applyList_apply]
  refine isMax_unique (best_isMax _ _) (best_isMax _ _) ?_ (hcoh k)
  intro x
  simp [candidates, hset]

/-- The same, stated from the empty store: delivery order and duplication are invisible. -/
theorem convergence_empty (l₁ l₂ : List Entry) (hset : ∀ e, e ∈ l₁ ↔ e ∈ l₂)
    (hcoh : ∀ x ∈ l₁, ∀ y ∈ l₁, x.key = y.key → Coherent x y) :
    applyList State.empty l₁ = applyList State.empty l₂ := by
  refine convergence _ _ _ hset ?_
  intro k x hx y hy
  simp [candidates, State.empty] at hx hy
  exact hcoh x hx.1 y hy.1 (by rw [hx.2, hy.2])

/-! ## Necessity of coherence -/

/-- Two writes for one key that reuse a metadata triple with different values. -/
def clashA : Entry := ⟨"k", some [1], ⟨1, 1, 100⟩⟩
def clashB : Entry := ⟨"k", some [2], ⟨1, 1, 100⟩⟩

/-- Without coherence, delivery order decides the outcome (the first one stays). -/
theorem coherence_needed :
    applyList State.empty [clashA, clashB] "k" = some clashA ∧
    applyList State.empty [clashB, clashA] "k" = some clashB := by
  decide

/-! ## Mirrored tests -/

-- `tests.rs: sync_breaks_an_equal_timestamp_tie_by_node_id`: a tombstone on the higher
-- node id displaces the put, and a put on the higher node id displaces a tombstone.
example :
    let put : Entry := ⟨"k", some [118], ⟨1, 1, 1000⟩⟩
    let del : Entry := ⟨"k", none, ⟨2, 2, 1000⟩⟩
    applyList State.empty [put, del] "k" = some del := by decide

example :
    let delLow : Entry := ⟨"j", none, ⟨1, 1, 1000⟩⟩
    let putHigh : Entry := ⟨"j", some [118], ⟨2, 2, 1000⟩⟩
    applyList State.empty [delLow, putHigh] "j" = some putHigh := by decide

-- `tests.rs: test_concurrent_writes_resolution`: the later timestamp wins.
example :
    let i1 : Entry := ⟨"key1", some [1], ⟨1, 1, 10⟩⟩
    let i2 : Entry := ⟨"key1", some [2], ⟨2, 1, 20⟩⟩
    applyList State.empty [i1, i2] "key1" = some i2 ∧
    applyList State.empty [i2, i1] "key1" = some i2 := by decide

end Wavekv
