/-!
# Delta pagination

Models `wavekv/src/delta.rs`: `compute_delta`, `PageInfo`, `Delta`, and the resume
loop that `sync.rs` drives with `start_after = Some(page.cursor)`.

The scan runs over `origin_index: BTreeMap<(NodeId, u64), String>`, here a strictly
sorted list of `(origin, seq)` keys. An index key stands for the entry `data[key]`.

Proven:
* `jump_sound`: the Rust shortcut `cursor = (origin, ack + 1)` passes over only index
  keys of the same origin with `seq <= ack`, i.e. keys a one-at-a-time filter would
  skip too. The model therefore skips covered keys one at a time.
* `computeDelta_spec`: one call returns a prefix of the owed keys after the start
  cursor; a truncated page is non-empty, is not final, and its cursor is its last key.
* `paginate_complete`: concatenating the pages, each resumed from the previous cursor,
  yields exactly `owed idx acks` = the index keys with `seq > acks[origin]`, in index
  order; `paginate_nodup`: with no repetition.

Assumptions: `idx` is strictly sorted (it is a `BTreeMap` key sequence) and does not
change between pages; `origin_index` agrees with `data` (the `debug_assert!` branch is
unreachable).

Modeling gap: `u32`/`u64` are `Nat`, so the `checked_add` overflow branches (cursor at
`u64::MAX`, origin `u32::MAX`) are not modeled. `entry_weight(data[key])` is an
arbitrary function `weight` of the index key. Real deltas carry entries; the model
returns their index keys.
-/

namespace Wavekv.Delta

/-- An `origin_index` key `(origin, seq)`. -/
abbrev IdxKey := Nat × Nat

/-- Tuple order of `BTreeMap<(NodeId, u64), _>`. -/
def klt (a b : IdxKey) : Bool := a.1 < b.1 || (a.1 == b.1 && a.2 < b.2)

theorem klt_irrefl (a : IdxKey) : klt a a = false := by simp [klt]

theorem klt_trans {a b c : IdxKey} (h₁ : klt a b = true) (h₂ : klt b c = true) :
    klt a c = true := by
  simp [klt] at *; omega

theorem klt_asymm {a b : IdxKey} (h : klt a b = true) : klt b a = false := by
  simp [klt] at *; omega

theorem klt_total (a b : IdxKey) : klt a b = true ∨ a = b ∨ klt b a = true := by
  obtain ⟨a1, a2⟩ := a; obtain ⟨b1, b2⟩ := b
  simp [klt]; omega

/-- `delta.rs: struct PageInfo`. -/
structure PageInfo where
  cursor : IdxKey
  last : Bool
  deriving DecidableEq, Repr

/-- `delta.rs: struct Delta` (index keys in place of entries). -/
structure Delta where
  entries : List IdxKey
  page : Option PageInfo
  deriving DecidableEq, Repr

/-- `max_entries`, `max_bytes`, and `entry_weight` of the entry behind each index key. -/
structure Budget where
  maxEntries : Nat
  maxBytes : Nat
  weight : IdxKey → Nat

/-- `acks.get(&origin).copied().unwrap_or(0)`; a `HashMap<NodeId, u64>` with default 0. -/
abbrev Acks := Nat → Nat

/-- The `loop` body of `compute_delta`, one index key at a time. -/
def scan (acks : Acks) (b : Budget) :
    List IdxKey → List IdxKey → Nat → Option IdxKey → Delta
  | [], entries, _, _ => ⟨entries, none⟩
  | k :: rest, entries, bytes, lastIncluded =>
    if k.2 ≤ acks k.1 then
      -- `seq <= ack`: covered, skip.
      scan acks b rest entries bytes lastIncluded
    else if !entries.isEmpty &&
        (decide (entries.length ≥ b.maxEntries) || decide (bytes + b.weight k > b.maxBytes)) then
      ⟨entries, some ⟨lastIncluded.getD k, false⟩⟩
    else
      scan acks b rest (entries ++ [k]) (bytes + b.weight k) (some k)

/-- Exclusive resume: keys strictly after `start_after`. -/
def after : Option IdxKey → IdxKey → Bool
  | none, _ => true
  | some c, k => klt c k

/-- `delta.rs: compute_delta(data, origin_index, acks, start_after, max_entries, max_bytes)`. -/
def computeDelta (idx : List IdxKey) (acks : Acks) (start : Option IdxKey) (b : Budget) :
    Delta :=
  scan acks b (idx.filter (after start)) [] 0 none

/-- What the peer is owed: `{ e in data : e.meta.seq > acks[e.meta.node] }`. -/
def owed (acks : Acks) (l : List IdxKey) : List IdxKey := l.filter (fun k => acks k.1 < k.2)

/-- The requester's resume loop: ask again from `page.cursor` until the page is final. -/
def paginate (idx : List IdxKey) (acks : Acks) (b : Budget) :
    Nat → Option IdxKey → List IdxKey
  | 0, _ => []
  | n + 1, start =>
    let d := computeDelta idx acks start b
    match d.page with
    | none => d.entries
    | some p => d.entries ++ paginate idx acks b n (some p.cursor)

/-! ## The jump shortcut -/

/-- In `compute_delta`, from `(origin, seq)` with `seq <= ack` the cursor jumps to
`(origin, ack + 1)`. Every key in between has that origin and `seq <= ack`. -/
theorem jump_sound (origin seq ack : Nat) (k : IdxKey) (_ : seq ≤ ack)
    (hlo : klt (origin, seq) k = true ∨ k = (origin, seq))
    (hhi : klt k (origin, ack + 1) = true) : k.1 = origin ∧ k.2 ≤ ack := by
  obtain ⟨k1, k2⟩ := k
  simp [klt] at hlo hhi ⊢; omega

/-! ## One call -/

theorem scan_spec (acks : Acks) (b : Budget) (l : List IdxKey) (entries : List IdxKey)
    (bytes : Nat) (lastIncluded : Option IdxKey) (hlast : lastIncluded = entries.getLast?) :
    ∃ X rest, owed acks l = X ++ rest ∧
      (scan acks b l entries bytes lastIncluded).entries = entries ++ X ∧
      match (scan acks b l entries bytes lastIncluded).page with
      | none => rest = []
      | some p => rest ≠ [] ∧ p.last = false ∧ (entries ++ X).getLast? = some p.cursor := by
  induction l generalizing entries bytes lastIncluded with
  | nil => exact ⟨[], [], by simp [owed], by simp [scan], by simp [scan]⟩
  | cons k rest ih =>
    by_cases hcov : k.2 ≤ acks k.1
    · have ho : owed acks (k :: rest) = owed acks rest := by
        simp [owed, List.filter_cons]; omega
      simp only [scan, hcov, ite_true]
      rw [ho]; exact ih entries bytes lastIncluded hlast
    · have ho : owed acks (k :: rest) = k :: owed acks rest := by
        simp [owed, List.filter_cons]; omega
      by_cases hstop : (!entries.isEmpty && (decide (entries.length ≥ b.maxEntries) ||
          decide (bytes + b.weight k > b.maxBytes))) = true
      · simp only [scan, hcov, hstop, ite_true, ite_false]
        refine ⟨[], k :: owed acks rest, by simp [ho], by simp, ?_⟩
        refine ⟨by simp, by simp, ?_⟩
        have hne : entries ≠ [] := by
          intro h; subst h; simp at hstop
        simp only [List.append_nil, hlast]
        cases h : entries.getLast? with
        | none => exact absurd (List.getLast?_eq_none_iff.1 h) hne
        | some c => rfl
      · simp only [scan, hcov, ite_false]
        simp only [Bool.not_eq_true] at hstop
        simp only [hstop, Bool.false_eq_true, ite_false]
        obtain ⟨X, rest', h1, h2, h3⟩ :=
          ih (entries ++ [k]) (bytes + b.weight k) (some k) (by simp)
        refine ⟨k :: X, rest', by rw [ho, h1]; rfl, by rw [h2]; simp, ?_⟩
        simpa using h3

theorem computeDelta_spec (idx : List IdxKey) (acks : Acks) (start : Option IdxKey)
    (b : Budget) :
    ∃ rest, owed acks (idx.filter (after start)) = (computeDelta idx acks start b).entries ++ rest ∧
      match (computeDelta idx acks start b).page with
      | none => rest = []
      | some p => rest ≠ [] ∧ p.last = false ∧
          (computeDelta idx acks start b).entries.getLast? = some p.cursor := by
  obtain ⟨X, rest, h1, h2, h3⟩ := scan_spec acks b (idx.filter (after start)) [] 0 none rfl
  simp only [List.nil_append] at h2 h3
  unfold computeDelta
  refine ⟨rest, by rw [h1, h2], ?_⟩
  rw [h2]; exact h3

/-! ## Sorted lists -/

abbrev Sorted (l : List IdxKey) : Prop := l.Pairwise (fun a b => klt a b = true)

theorem sorted_eq_of_mem_iff : ∀ {l₁ l₂ : List IdxKey}, Sorted l₁ → Sorted l₂ →
    (∀ x, x ∈ l₁ ↔ x ∈ l₂) → l₁ = l₂
  | [], [], _, _, _ => rfl
  | [], b :: _, _, _, h => absurd ((h b).2 (by simp)) (by simp)
  | a :: _, [], _, _, h => absurd ((h a).1 (by simp)) (by simp)
  | a :: t₁, b :: t₂, h₁, h₂, h => by
    unfold Sorted at h₁ h₂
    rw [List.pairwise_cons] at h₁ h₂
    have hab : a = b := by
      have ha := (h a).1 (by simp)
      have hb := (h b).2 (by simp)
      simp at ha hb
      rcases ha with ha | ha
      · exact ha
      · rcases hb with hb | hb
        · exact hb.symm
        · have := klt_asymm (h₂.1 a ha); rw [h₁.1 b hb] at this; simp at this
    subst hab
    have ht : ∀ x, x ∈ t₁ ↔ x ∈ t₂ := by
      intro x
      constructor
      · intro hx
        have := (h x).1 (by simp [hx])
        simp at this
        rcases this with rfl | h'
        · have := h₁.1 x hx; rw [klt_irrefl] at this; simp at this
        · exact h'
      · intro hx
        have := (h x).2 (by simp [hx])
        simp at this
        rcases this with rfl | h'
        · have := h₂.1 x hx; rw [klt_irrefl] at this; simp at this
        · exact h'
    rw [sorted_eq_of_mem_iff h₁.2 h₂.2 ht]

theorem mem_le_getLast : ∀ {X : List IdxKey} {c x : IdxKey}, Sorted X → X.getLast? = some c →
    x ∈ X → x = c ∨ klt x c = true
  | [], _, _, _, h, _ => by simp at h
  | [a], c, x, _, h, hx => by simp at h hx; exact Or.inl (hx.trans h)
  | a :: b :: t, c, x, hs, h, hx => by
    have hs' : Sorted (b :: t) := (List.pairwise_cons.1 hs).2
    have h' : (b :: t).getLast? = some c := by simpa using h
    simp only [List.mem_cons] at hx
    rcases hx with rfl | hx
    · right
      rcases mem_le_getLast hs' h' (List.mem_cons_self) with hb | hb
      · rw [← hb]; exact (List.pairwise_cons.1 hs).1 b (by simp)
      · exact klt_trans ((List.pairwise_cons.1 hs).1 b (by simp)) hb
    · exact mem_le_getLast hs' h' (by simpa using hx)

theorem after_trans {s : Option IdxKey} {c x : IdxKey} (hc : after s c = true)
    (hx : klt c x = true) : after s x = true := by
  cases s with
  | none => rfl
  | some s => exact klt_trans hc hx

/-- The keys after `c` in a sorted list, given that `X ++ rest` is the part after
`start` and `c` closes `X`. -/
theorem filter_after_cursor (L X rest : List IdxKey) (start : Option IdxKey) (c : IdxKey)
    (hL : Sorted L) (hsplit : L.filter (after start) = X ++ rest)
    (hc : X.getLast? = some c) : L.filter (klt c) = rest := by
  have hD : Sorted (X ++ rest) := by rw [← hsplit]; exact hL.filter _
  have hX : Sorted X := (List.pairwise_append.1 hD).1
  have hR : Sorted rest := (List.pairwise_append.1 hD).2.1
  have hcX : c ∈ X := List.mem_of_getLast? hc
  have hcD : c ∈ L.filter (after start) := by rw [hsplit]; simp [hcX]
  have hcs : after start c = true := (List.mem_filter.1 hcD).2
  apply sorted_eq_of_mem_iff (hL.filter _) hR
  intro x
  constructor
  · intro hx
    obtain ⟨hxL, hcx⟩ := List.mem_filter.1 hx
    have : x ∈ X ++ rest := by
      rw [← hsplit]; exact List.mem_filter.2 ⟨hxL, after_trans hcs hcx⟩
    rcases List.mem_append.1 this with hxX | hxr
    · rcases mem_le_getLast hX hc hxX with rfl | hxc
      · rw [klt_irrefl] at hcx; simp at hcx
      · rw [klt_asymm hxc] at hcx; simp at hcx
    · exact hxr
  · intro hx
    have hxD : x ∈ L.filter (after start) := by rw [hsplit]; simp [hx]
    exact List.mem_filter.2 ⟨(List.mem_filter.1 hxD).1,
      (List.pairwise_append.1 hD).2.2 c hcX x hx⟩

theorem owed_filter_comm (acks : Acks) (l : List IdxKey) (p : IdxKey → Bool) :
    owed acks (l.filter p) = (owed acks l).filter p := by
  simp only [owed, List.filter_filter, Bool.and_comm]

/-! ## The resume loop -/

theorem paginate_from (idx : List IdxKey) (acks : Acks) (b : Budget) (hidx : Sorted idx) :
    ∀ (fuel : Nat) (start : Option IdxKey),
      (owed acks (idx.filter (after start))).length < fuel →
      paginate idx acks b fuel start = owed acks (idx.filter (after start)) := by
  intro fuel
  induction fuel with
  | zero => intro _ h; simp at h
  | succ n ih =>
    intro start hlen
    obtain ⟨rest, hsplit, hpage⟩ := computeDelta_spec idx acks start b
    simp only [paginate]
    split
    · rename_i hp
      rw [hp] at hpage; simp only at hpage
      rw [hsplit, hpage]; simp
    · rename_i p hp
      rw [hp] at hpage; simp only at hpage
      obtain ⟨hne, -, hcur⟩ := hpage
      have hL : Sorted (owed acks idx) := hidx.filter _
      have hrest : owed acks (idx.filter (after (some p.cursor))) = rest := by
        rw [owed_filter_comm]
        apply filter_after_cursor (owed acks idx) _ rest start p.cursor hL _ hcur
        rw [← owed_filter_comm]; exact hsplit
      have hentries : (computeDelta idx acks start b).entries ≠ [] := by
        intro h; rw [h] at hcur; simp at hcur
      have : rest.length < n := by
        rw [hsplit, List.length_append] at hlen
        have := List.length_pos_iff.2 hentries
        omega
      rw [ih (some p.cursor) (by rw [hrest]; exact this), hrest, hsplit]

/-- **Pagination is lossless and repetition-free.** Starting from `None` and resuming
from each returned cursor, the pages concatenate to exactly the owed keys, in order. -/
theorem paginate_complete (idx : List IdxKey) (acks : Acks) (b : Budget) (hidx : Sorted idx) :
    paginate idx acks b ((owed acks idx).length + 1) none = owed acks idx := by
  have h := paginate_from idx acks b hidx ((owed acks idx).length + 1) none
  have hf : idx.filter (after none) = idx := List.filter_eq_self.2 (by simp [after])
  rw [hf] at h
  exact h (by omega)

theorem paginate_nodup (idx : List IdxKey) (acks : Acks) (b : Budget) (hidx : Sorted idx) :
    (paginate idx acks b ((owed acks idx).length + 1) none).Nodup := by
  rw [paginate_complete idx acks b hidx]
  exact (hidx.filter _).imp fun h e => by subst e; rw [klt_irrefl] at h; simp at h

theorem mem_owed (acks : Acks) (idx : List IdxKey) (k : IdxKey) :
    k ∈ owed acks idx ↔ k ∈ idx ∧ acks k.1 < k.2 := by
  simp [owed]

/-! ## Mirrored tests (`delta.rs` inline tests) -/

def unbounded : Budget := ⟨100, 1 <<< 20, fun _ => 36⟩

-- `pagination_resumes_without_loss_or_repetition`: ten keys, three per page.
example :
    paginate ((List.range 10).map fun i => (1, i + 1)) (fun _ => 0) ⟨3, 1 <<< 20, fun _ => 36⟩
      20 none = (List.range 10).map fun i => (1, i + 1) := by decide

-- `covered_origins_are_skipped`.
example : (computeDelta [(1, 1), (1, 2), (2, 5)] (fun o => if o = 1 then 2 else 0) none
    unbounded).entries = [(2, 5)] := by decide

-- `seq_holes_are_not_treated_as_gaps`.
example : (computeDelta [(1, 3)] (fun _ => 0) none unbounded).entries = [(1, 3)] := by decide

-- `the_byte_budget_breaks_the_page_at_an_exact_boundary`: weight 100, budget 200.
example :
    let idx := [(1, 1), (1, 2), (1, 3), (1, 4)]
    let b : Budget := ⟨1000, 200, fun _ => 100⟩
    computeDelta idx (fun _ => 0) none b = ⟨[(1, 1), (1, 2)], some ⟨(1, 2), false⟩⟩ ∧
    (computeDelta idx (fun _ => 0) (some (1, 2)) b).entries = [(1, 3), (1, 4)] := by decide

-- `an_oversized_entry_still_makes_progress`.
example : (computeDelta [(1, 1), (1, 2)] (fun _ => 0) none
    ⟨100, 10, fun k => if k = (1, 1) then 1035 else 37⟩).entries = [(1, 1)] := by decide

-- `scanning_is_stable_across_interleaved_origins`.
example : (computeDelta [(1, 1), (1, 2), (2, 1), (3, 9)]
    (fun o => if o = 1 then 1 else if o = 3 then 9 else 0) none unbounded).entries =
    [(1, 2), (2, 1)] := by decide

end Wavekv.Delta
