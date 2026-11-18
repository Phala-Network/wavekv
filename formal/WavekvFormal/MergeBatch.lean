import WavekvFormal.Lww

/-!
# Batched merge, admission, and local writes

Models:
* `wavekv/src/node.rs`: `NodeState::merge_batch` (decide every entry against the live
  map overlaid with `StagedBatch`, then apply the winners as `StateOp::Set`),
  `StagedBatch::{winner, stage, into_ops}`, `NodeState::admit`.
* `wavekv/src/admission.rs`: `Limits::check_clock`.
* `wavekv/src/node.rs`: `NodeState::put` / `NodeState::delete` (an unconditional
  `StateOp::Set` of a freshly stamped entry).

Proven:
* `mergeBatch_eq`: the three-pass staged batch equals merging its admitted entries one
  at a time, so the batch inherits `convergence`.
* `convergence_admit`: convergence holds for any admission predicate that depends only
  on the entry.
* `clock_admission_diverges`: with `check_clock`, which depends on local wall time, the
  same entry is rejected by one replica and accepted by another, so equal received sets
  do not give equal states. `checkClock_mono` and `convergence_clock_caught_up` give the
  recovery: once every replica's clock has passed `timestamp - drift` for every entry,
  admission is the identity and convergence holds again (R1 retransmission makes the
  rejected entry arrive again).
* `local_write_diverges`: `put`/`delete` do not go through `beats`. A local write whose
  wall-clock stamp is below the held entry's (a peer clock that runs ahead, within
  `max_clock_drift`) is visible locally but loses on every peer, so the replicas differ
  although both hold the same two entries. `local_write_eq_apply` gives the condition
  under which a local write is a merge.

Modeling gap: `admit` also consults `check_capacity`, which depends on the store size
and on the staged batch; it is not entry-only, is not modeled, and is a further source
of order dependence (a full replica refuses a new key that another accepts). The Rust
`saturating_sub` in `check_clock` is plain `Int` subtraction here. Durability (WAL
append) is omitted.
-/

namespace Wavekv

/-! ## The staged batch -/

/-- `node.rs: StagedBatch::winner`: the staged entry for a key, if any. -/
def stagedWinner (w : List Entry) (k : Key) : Option Entry := w.find? (fun x => x.key == k)

/-- `node.rs: StagedBatch::stage`: overwrite the key's slot in place, else append. -/
def stage (w : List Entry) (e : Entry) : List Entry :=
  if w.any (fun x => x.key == e.key) then w.map (fun x => if x.key == e.key then e else x)
  else w ++ [e]

/-- One iteration of the decide loop in `node.rs: NodeState::merge_batch`.
`previous = staged.winner(key).or_else(|| data.get(key))`. -/
def decideStep (admit : Entry → Bool) (data : State) (w : List Entry) (e : Entry) :
    List Entry :=
  if admit e then
    if beats ((stagedWinner w e.key).or (data e.key)) e then stage w e else w
  else w

/-- `node.rs: NodeState::merge_batch`: decide, then apply the winners as `Set`
(`StagedBatch::into_ops` followed by `replay_ops`). -/
def mergeBatch (admit : Entry → Bool) (data : State) (es : List Entry) : State :=
  (es.foldl (decideStep admit data) []).foldl setEntry data

/-- Reference semantics: `NodeState::sync` applied to each admitted entry in turn. -/
def mergeAdmitted (admit : Entry → Bool) (s : State) (es : List Entry) : State :=
  es.foldl (fun s e => if admit e then apply s e else s) s

theorem mergeAdmitted_eq_filter (admit : Entry → Bool) (s : State) (es : List Entry) :
    mergeAdmitted admit s es = applyList s (es.filter admit) := by
  induction es generalizing s with
  | nil => rfl
  | cons e es ih =>
    simp only [mergeAdmitted, List.foldl_cons] at ih ⊢
    by_cases h : admit e = true
    · simp only [h, ite_true, List.filter_cons]; exact ih _
    · simp only [h, List.filter_cons]; exact ih _

/-- Keys of the winners list are distinct (`StagedBatch::index` is a map). -/
def KeysNodup (w : List Entry) : Prop := (w.map (·.key)).Pairwise (· ≠ ·)

theorem foldl_setEntry (w : List Entry) (d : State) (h : KeysNodup w) (k : Key) :
    w.foldl setEntry d k = (stagedWinner w k).or (d k) := by
  induction w generalizing d with
  | nil => rfl
  | cons x w ih =>
    simp only [KeysNodup, List.map_cons, List.pairwise_cons] at h
    simp only [List.foldl_cons, stagedWinner, List.find?_cons]
    rw [ih _ h.2]
    by_cases hk : x.key = k
    · subst hk
      have : w.find? (fun y => y.key == x.key) = none := by
        rw [List.find?_eq_none]
        intro y hy hyk
        have hyx : y.key = x.key := by simpa using hyk
        exact h.1 y.key (List.mem_map_of_mem hy) hyx.symm
      simp [stagedWinner, this, setEntry]
    · have hk' : ¬ k = x.key := fun e => hk e.symm
      have hb : (x.key == k) = false := by simp [hk]
      simp [stagedWinner, setEntry, hk', hb]

theorem stage_keys (w : List Entry) (e : Entry) (h : KeysNodup w) : KeysNodup (stage w e) := by
  unfold stage
  split
  · have : (w.map fun x => if x.key == e.key then e else x).map (·.key) = w.map (·.key) := by
      rw [List.map_map]; apply List.map_congr_left
      intro x _; by_cases hx : x.key = e.key <;> simp [hx]
    unfold KeysNodup; rw [this]; exact h
  · rename_i hany
    unfold KeysNodup
    rw [List.map_append, List.pairwise_append]
    refine ⟨h, by simp, ?_⟩
    intro a ha b hb
    simp at hb; subst hb
    simp only [List.mem_map] at ha
    obtain ⟨x, hx, rfl⟩ := ha
    intro hxe
    exact hany (List.any_eq_true.2 ⟨x, hx, by simp [hxe]⟩)

theorem stagedWinner_stage (w : List Entry) (e : Entry) (k : Key) :
    stagedWinner (stage w e) k = if k = e.key then some e else stagedWinner w k := by
  unfold stage stagedWinner
  split
  · rename_i hany
    rw [List.find?_map]
    have hp : ((fun x => x.key == k) ∘ fun x => if x.key == e.key then e else x) =
        (fun x => x.key == k) := by
      funext x; by_cases hx : x.key = e.key <;> simp [hx]
    rw [hp]
    by_cases hk : k = e.key
    · subst hk
      obtain ⟨x, hx, hxk⟩ := List.any_eq_true.1 hany
      cases hf : w.find? (fun x => x.key == e.key) with
      | none => exact absurd hf (by rw [List.find?_eq_none]; intro h; simp_all)
      | some y =>
        have := List.find?_some hf
        simp at this
        simp [this]
    · cases hf : w.find? (fun x => x.key == k) with
      | none => simp [hk]
      | some y =>
        have := List.find?_some hf
        simp at this
        have : ¬ y.key = e.key := by rw [this]; exact hk
        simp [hk, this]
  · rename_i hany
    rw [List.find?_append]
    by_cases hk : k = e.key
    · subst hk
      have : w.find? (fun x => x.key == e.key) = none := by
        rw [List.find?_eq_none]; intro x hx hxk
        exact hany (List.any_eq_true.2 ⟨x, hx, hxk⟩)
      simp [this]
    · have : ¬ e.key = k := fun h => hk h.symm
      simp [hk, this]

theorem foldl_decide (admit : Entry → Bool) (data : State) (es : List Entry)
    (w : List Entry) (s : State) (hw : KeysNodup w) (hs : w.foldl setEntry data = s) :
    (es.foldl (decideStep admit data) w).foldl setEntry data =
      mergeAdmitted admit s es := by
  induction es generalizing w s with
  | nil => simpa [mergeAdmitted] using hs
  | cons e es ih =>
    simp only [List.foldl_cons, mergeAdmitted] at ih ⊢
    have hview : (stagedWinner w e.key).or (data e.key) = s e.key := by
      rw [← hs, foldl_setEntry _ _ hw]
    by_cases ha : admit e = true
    · simp only [ha, ite_true]
      by_cases hb : beats (s e.key) e = true
      · have : decideStep admit data w e = stage w e := by simp [decideStep, ha, hview, hb]
        rw [this]
        apply ih _ _ (stage_keys w e hw)
        funext k
        rw [foldl_setEntry _ _ (stage_keys w e hw), stagedWinner_stage]
        simp only [apply, hb, ite_true, setEntry]
        by_cases hk : k = e.key
        · simp [hk]
        · simp only [hk, ite_false]; rw [← hs, foldl_setEntry _ _ hw]
      · have : decideStep admit data w e = w := by simp [decideStep, ha, hview, hb]
        rw [this]
        apply ih _ _ hw
        simp [apply, hb, hs]
    · have : decideStep admit data w e = w := by simp [decideStep, ha]
      rw [this]
      simp only [ha]
      exact ih _ _ hw hs

/-- The staged three-pass `merge_batch` equals one-at-a-time `sync` of admitted entries. -/
theorem mergeBatch_eq (admit : Entry → Bool) (data : State) (es : List Entry) :
    mergeBatch admit data es = mergeAdmitted admit data es :=
  foldl_decide admit data es [] data (List.Pairwise.nil) rfl

/-- Convergence under any entry-only admission predicate (`check_entry`, an embedder
`AdmissionPolicy`), for the real batched merge. -/
theorem convergence_admit (admit : Entry → Bool) (s : State) (l₁ l₂ : List Entry)
    (hset : ∀ e, e ∈ l₁ ↔ e ∈ l₂) (hcoh : BatchCoherent s (l₁.filter admit)) :
    mergeBatch admit s l₁ = mergeBatch admit s l₂ := by
  rw [mergeBatch_eq, mergeBatch_eq, mergeAdmitted_eq_filter, mergeAdmitted_eq_filter]
  apply convergence _ _ _ _ hcoh
  intro e; simp [List.mem_filter, hset]

-- `tests.rs: a_batch_holding_two_versions_of_a_key_keeps_the_later_one`.
example :
    let older : Entry := ⟨"k", some [1], ⟨2, 1, 100⟩⟩
    let newer : Entry := ⟨"k", some [2], ⟨2, 2, 200⟩⟩
    mergeBatch (fun _ => true) State.empty [newer, older] "k" = some newer ∧
    mergeBatch (fun _ => true) State.empty [older, newer] "k" = some newer := by decide

/-! ## Clock admission -/

/-- `admission.rs: Limits::check_clock`: accept iff `timestamp - now <= drift`. -/
def checkClock (drift now : Int) (e : Entry) : Bool := decide (e.metadata.timestamp - now ≤ drift)

-- `admission.rs: future_stamps_are_bounded_but_past_stamps_are_not` (drift 60 s).
example : checkClock 60000 1000000 ⟨"k", some [], ⟨1, 1, 1000000 + 61000⟩⟩ = false := by decide
example : checkClock 60000 1000000 ⟨"k", some [], ⟨1, 1, 1000000 + 60000⟩⟩ = true := by decide
example : checkClock 60000 1000000 ⟨"k", some [], ⟨1, 1, 1000000 + 60001⟩⟩ = false := by decide
example : checkClock 60000 1000000 ⟨"k", some [], ⟨1, 1, 0⟩⟩ = true := by decide

/-- A clock-admitted entry stays admitted as local time advances. -/
theorem checkClock_mono {drift now now' : Int} {e : Entry} (h : now ≤ now')
    (ha : checkClock drift now e = true) : checkClock drift now' e = true := by
  simp [checkClock] at *; omega

def futureEntry : Entry := ⟨"k", some [1], ⟨2, 1, 500⟩⟩

/-- **Counterexample.** Two replicas receive the same single entry; the one whose
clock reads 0 rejects it (500 ms ahead, drift 60 ms), the one at 1000 accepts it. -/
theorem clock_admission_diverges :
    mergeBatch (checkClock 60 0) State.empty [futureEntry] "k" = none ∧
    mergeBatch (checkClock 60 1000) State.empty [futureEntry] "k" = some futureEntry := by
  decide

/-- Once both clocks have passed `timestamp - drift` for every entry, the clock check
admits everything and replicas with equal received sets agree. -/
theorem convergence_clock_caught_up (drift now₁ now₂ : Int) (s : State)
    (l₁ l₂ : List Entry) (hset : ∀ e, e ∈ l₁ ↔ e ∈ l₂) (hcoh : BatchCoherent s l₁)
    (h₁ : ∀ e ∈ l₁, e.metadata.timestamp - drift ≤ now₁)
    (h₂ : ∀ e ∈ l₂, e.metadata.timestamp - drift ≤ now₂) :
    mergeBatch (checkClock drift now₁) s l₁ = mergeBatch (checkClock drift now₂) s l₂ := by
  rw [mergeBatch_eq, mergeBatch_eq, mergeAdmitted_eq_filter, mergeAdmitted_eq_filter]
  have f₁ : l₁.filter (checkClock drift now₁) = l₁ := by
    rw [List.filter_eq_self]; intro e he; have := h₁ e he; simp [checkClock]; omega
  have f₂ : l₂.filter (checkClock drift now₂) = l₂ := by
    rw [List.filter_eq_self]; intro e he; have := h₂ e he; simp [checkClock]; omega
  rw [f₁, f₂]
  exact convergence _ _ _ hset hcoh

/-! ## Local writes bypass `beats` -/

/-- `node.rs: NodeState::put` / `delete`: `StateOp::Set` of the freshly stamped entry,
with no `beats` check against what is held. -/
def localWrite (s : State) (e : Entry) : State := setEntry s e

/-- A local write is a merge exactly when its stamp beats the held entry. -/
theorem local_write_eq_apply (s : State) (e : Entry) (h : beats (s e.key) e = true) :
    localWrite s e = apply s e := by simp [localWrite, apply, h]

/-- Entry authored by node 2, whose clock runs 10 ms ahead of node 1's. -/
def aheadWrite : Entry := ⟨"k", some [1], ⟨2, 1, 1000⟩⟩
/-- Node 1 then deletes `k`; its wall clock reads 990. -/
def behindDelete : Entry := ⟨"k", none, ⟨1, 1, 990⟩⟩

/-- **Counterexample.** Node 1 merges `aheadWrite` and then deletes the key locally;
node 2 wrote `aheadWrite` and merges node 1's tombstone. Both hold the same two entries,
node 1 reports the key deleted, node 2 reports it live, and LWW over the two entries
(`applyList`, either order) says live. A later re-merge of `aheadWrite` on node 1 (the
digest repair path) brings the deleted key back. -/
theorem local_write_diverges :
    let node1 := localWrite (applyList State.empty [aheadWrite]) behindDelete
    let node2 := applyList (localWrite State.empty aheadWrite) [behindDelete]
    node1 "k" = some behindDelete ∧ node2 "k" = some aheadWrite ∧
    applyList State.empty [aheadWrite, behindDelete] "k" = some aheadWrite ∧
    applyList State.empty [behindDelete, aheadWrite] "k" = some aheadWrite ∧
    apply node1 aheadWrite "k" = some aheadWrite := by
  decide

end Wavekv
