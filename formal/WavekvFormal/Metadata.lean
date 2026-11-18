/-!
# LWW order on `Metadata`

Models `wavekv/src/types.rs`: `struct Metadata`, `impl Ord for Metadata` (`cmp`)
and `compare_entries`.

Proven:
* `Metadata.cmp` is a lawful total order: `cmp_eq_iff` (equal exactly on equal
  metadata), `cmp_swap` (antisymmetry), `lt_trans`, `lt_irrefl`, `lt_total`.
* `cmp_eq_lt_iff` characterises `cmp a b = .lt` as the lexicographic order on
  `(timestamp, node, seq)`.

Modeling gap: `NodeId = u32` and `seq: u64` are modeled as `Nat`, `timestamp: i64`
as `Int`. The Rust comparison never does arithmetic on these fields, so the
unbounded types change nothing about the order.
-/

namespace Wavekv

/-- `std::cmp::Ord::cmp` on `u32`/`u64`, modeled on `Nat`. -/
def cmpNat (a b : Nat) : Ordering :=
  if a < b then .lt else if a = b then .eq else .gt

/-- `std::cmp::Ord::cmp` on `i64`, modeled on `Int`. -/
def cmpInt (a b : Int) : Ordering :=
  if a < b then .lt else if a = b then .eq else .gt

/-- `types.rs: struct Metadata { node, seq, timestamp }`. -/
structure Metadata where
  node : Nat
  seq : Nat
  timestamp : Int
  deriving DecidableEq, Repr

namespace Metadata

/-- `types.rs: impl Ord for Metadata { fn cmp }`: timestamp, then node, then seq. -/
def cmp (a b : Metadata) : Ordering :=
  match cmpInt a.timestamp b.timestamp with
  | .eq =>
    match cmpNat a.node b.node with
    | .eq => cmpNat a.seq b.seq
    | nodeOrd => nodeOrd
  | timestampOrd => timestampOrd

/-- The lexicographic strict order on `(timestamp, node, seq)`. -/
def Lex (a b : Metadata) : Prop :=
  a.timestamp < b.timestamp ∨
    (a.timestamp = b.timestamp ∧ (a.node < b.node ∨ (a.node = b.node ∧ a.seq < b.seq)))

/-- `a < b` in the Rust `Ord`. -/
def lt (a b : Metadata) : Prop := a.cmp b = .lt

/-- `a <= b` in the Rust `Ord`. -/
def le (a b : Metadata) : Prop := a.cmp b ≠ .gt

instance : LT Metadata := ⟨lt⟩
instance : LE Metadata := ⟨le⟩

instance (a b : Metadata) : Decidable (a < b) :=
  inferInstanceAs (Decidable (a.cmp b = .lt))

instance (a b : Metadata) : Decidable (a ≤ b) :=
  inferInstanceAs (Decidable (a.cmp b ≠ .gt))

theorem ext' {a b : Metadata} (h1 : a.node = b.node) (h2 : a.seq = b.seq)
    (h3 : a.timestamp = b.timestamp) : a = b := by
  cases a; cases b; simp_all

theorem cmp_eq_lt_iff (a b : Metadata) : a.cmp b = .lt ↔ Lex a b := by
  unfold cmp Lex cmpInt cmpNat
  by_cases h1 : a.timestamp < b.timestamp <;> by_cases h2 : a.timestamp = b.timestamp <;>
    by_cases h3 : a.node < b.node <;> by_cases h4 : a.node = b.node <;>
    by_cases h5 : a.seq < b.seq <;> by_cases h6 : a.seq = b.seq <;>
    simp [h1, h2, h3, h4, h5, h6] <;> omega

theorem cmp_eq_eq_iff (a b : Metadata) : a.cmp b = .eq ↔ a = b := by
  constructor
  · intro h
    unfold cmp cmpInt cmpNat at h
    by_cases h1 : a.timestamp < b.timestamp <;> by_cases h2 : a.timestamp = b.timestamp <;>
      by_cases h3 : a.node < b.node <;> by_cases h4 : a.node = b.node <;>
      by_cases h5 : a.seq < b.seq <;> by_cases h6 : a.seq = b.seq <;>
      simp [h1, h2, h3, h4, h5, h6] at h <;> exact ext' h4 h6 h2
  · intro h; subst h; simp [cmp, cmpInt, cmpNat]

theorem cmp_eq_gt_iff (a b : Metadata) : a.cmp b = .gt ↔ Lex b a := by
  unfold cmp Lex cmpInt cmpNat
  by_cases h1 : a.timestamp < b.timestamp <;> by_cases h2 : a.timestamp = b.timestamp <;>
    by_cases h3 : a.node < b.node <;> by_cases h4 : a.node = b.node <;>
    by_cases h5 : a.seq < b.seq <;> by_cases h6 : a.seq = b.seq <;>
    simp [h1, h2, h3, h4, h5, h6] <;> omega

theorem lt_iff (a b : Metadata) : a < b ↔ Lex a b := cmp_eq_lt_iff a b

theorem le_iff (a b : Metadata) : a ≤ b ↔ Lex a b ∨ a = b := by
  show a.cmp b ≠ .gt ↔ _
  rw [← cmp_eq_lt_iff, ← cmp_eq_eq_iff]
  cases a.cmp b <;> simp

/-- Antisymmetry of the comparison: `b.cmp a` is `a.cmp b` reversed. -/
theorem cmp_swap (a b : Metadata) : b.cmp a = (a.cmp b).swap := by
  cases h : a.cmp b
  · rw [cmp_eq_lt_iff] at h; simp only [Ordering.swap]; rw [cmp_eq_gt_iff]; exact h
  · rw [cmp_eq_eq_iff] at h; subst h; simp only [Ordering.swap]; rw [cmp_eq_eq_iff]
  · rw [cmp_eq_gt_iff] at h; simp only [Ordering.swap]; rw [cmp_eq_lt_iff]; exact h

theorem lt_irrefl (a : Metadata) : ¬ a < a := by
  rw [lt_iff]; unfold Lex; omega

theorem lt_trans {a b c : Metadata} : a < b → b < c → a < c := by
  simp only [lt_iff, Lex]; omega

theorem lt_asymm {a b : Metadata} : a < b → ¬ b < a := by
  simp only [lt_iff, Lex]; omega

/-- Totality (trichotomy). -/
theorem lt_total (a b : Metadata) : a < b ∨ a = b ∨ b < a := by
  rw [lt_iff, lt_iff]
  by_cases h : a = b
  · exact Or.inr (Or.inl h)
  · have : ¬ (a.node = b.node ∧ a.seq = b.seq ∧ a.timestamp = b.timestamp) :=
      fun ⟨h1, h2, h3⟩ => h (ext' h1 h2 h3)
    unfold Lex; omega

theorem le_refl (a : Metadata) : a ≤ a := (le_iff a a).2 (Or.inr rfl)

theorem le_antisymm {a b : Metadata} : a ≤ b → b ≤ a → a = b := by
  rw [le_iff, le_iff]
  intro h1 h2
  rcases h1 with h1 | h1
  · rcases h2 with h2 | h2
    · exact absurd ((lt_iff _ _).2 h2) (lt_asymm ((lt_iff _ _).2 h1))
    · exact h2.symm
  · exact h1

theorem le_trans {a b c : Metadata} : a ≤ b → b ≤ c → a ≤ c := by
  rw [le_iff, le_iff, le_iff]
  rintro (h1 | rfl) (h2 | rfl)
  · exact Or.inl ((lt_iff _ _).1 (lt_trans ((lt_iff _ _).2 h1) ((lt_iff _ _).2 h2)))
  · exact Or.inl h1
  · exact Or.inl h2
  · exact Or.inr rfl

theorem le_total (a b : Metadata) : a ≤ b ∨ b ≤ a := by
  rw [le_iff, le_iff, ← lt_iff, ← lt_iff]
  rcases lt_total a b with h | h | h
  · exact Or.inl (Or.inl h)
  · exact Or.inl (Or.inr h)
  · exact Or.inr (Or.inl h)

theorem not_lt_iff_le {a b : Metadata} : ¬ a < b ↔ b ≤ a := by
  show ¬ a.cmp b = .lt ↔ b.cmp a ≠ .gt
  rw [cmp_swap a b]; cases a.cmp b <;> simp [Ordering.swap]

theorem lt_of_lt_of_le {a b c : Metadata} : a < b → b ≤ c → a < c := by
  intro h1 h2
  rcases (le_iff _ _).1 h2 with h2 | rfl
  · exact lt_trans h1 ((lt_iff _ _).2 h2)
  · exact h1

theorem lt_of_le_of_lt {a b c : Metadata} : a ≤ b → b < c → a < c := by
  intro h1 h2
  rcases (le_iff _ _).1 h1 with h1 | rfl
  · exact lt_trans ((lt_iff _ _).2 h1) h2
  · exact h2

theorem le_of_lt {a b : Metadata} : a < b → a ≤ b := fun h => (le_iff _ _).2 (Or.inl ((lt_iff _ _).1 h))

end Metadata

-- `tests.rs: test_item_comparison`.
example : (Metadata.mk 1 1 1000).cmp (Metadata.mk 2 2 2000) = .lt := by decide
example : (Metadata.mk 2 2 2000).cmp (Metadata.mk 1 1 1000) = .gt := by decide
-- `tests.rs: test_lww_equal_timestamp_node_id_tie_non_tombstone`: node id breaks ties.
example : (Metadata.mk 1 1 2000).cmp (Metadata.mk 2 2 2000) = .lt := by decide
-- Seq breaks a tie on equal timestamp and node.
example : (Metadata.mk 1 1 5).cmp (Metadata.mk 1 2 5) = .lt := by decide

end Wavekv
