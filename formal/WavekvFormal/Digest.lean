import WavekvFormal.Lww

/-!
# State digest: equal digests iff equal data maps

Models `wavekv/src/digest.rs: StateDigest::compute`: SHA-256 over, for each
`(key, entry)` in `BTreeMap` order,
`u32_le(len key) ++ key ++ u32_le node ++ u64_le seq ++ i64_le timestamp ++
(0x00 | 0x01 ++ u32_le(len value) ++ value)`.

Proven:
* `encode_inj`: the byte stream fed to the hasher is injective on data maps whose
  fields fit their wire widths (`Bounded`). This is the content of the length
  prefixes: no two distinct maps produce the same stream.
* `digest_eq_iff`: if the hash is injective on these streams, equal digests hold iff
  the data maps (as `BTreeMap` iteration lists) are equal. With `convergence`, two
  converged replicas report equal digests, and unequal digests prove divergence.

Assumptions: injectivity of SHA-256 on the encoded streams is an explicit hypothesis
(`hinj`), an idealisation of collision resistance. Field bounds are hypotheses: the
Rust casts `len() as u32` and would wrap a key or value of 4 GiB or more (keys are
capped at `max_key_bytes`, values at `max_value_bytes`, by admission).

Modeling gap: a `BTreeMap<String, Entry>` is its iteration list of `(key, entry)`
pairs; two `BTreeMap`s are equal iff these lists are. `Entry.key` is not hashed (the
map key is), as in Rust. The digest/GC tension documented on
`collect_tombstone_garbage` is outside this module.
-/

namespace Wavekv.Digest

/-- `to_le_bytes` of a `w`-byte unsigned integer. -/
def leBytes : Nat → Nat → List UInt8
  | 0, _ => []
  | w + 1, x => UInt8.ofNat (x % 256) :: leBytes w (x / 256)

/-- Two's-complement bits of an `i64`, for `i64::to_le_bytes`. -/
def i64Bits (t : Int) : Nat := (t % 2 ^ 64).toNat

/-- `key.as_bytes()`. -/
def keyBytes (k : Key) : List UInt8 := k.toByteArray.data.toList

/-- The bytes `StateDigest::compute` feeds the hasher for one map item. -/
def encItem (item : Key × Entry) : List UInt8 :=
  let kb := keyBytes item.1
  let e := item.2
  leBytes 4 kb.length ++ kb ++ leBytes 4 e.metadata.node ++ leBytes 8 e.metadata.seq ++
    leBytes 8 (i64Bits e.metadata.timestamp) ++
    match e.value with
    | none => [0]
    | some v => [1] ++ leBytes 4 v.length ++ v

/-- The full stream for a data map in `BTreeMap` order. -/
def encode (l : List (Key × Entry)) : List UInt8 := l.flatMap encItem

/-- `StateDigest::compute` with an abstract hash `H`. -/
def digest {D : Type} (H : List UInt8 → D) (l : List (Key × Entry)) : D := H (encode l)

/-- The map key is the entry's key (maintained by `CoreState::execute`), and every
field fits its wire width. -/
def Bounded (item : Key × Entry) : Prop :=
  item.2.key = item.1 ∧ (keyBytes item.1).length < 2 ^ 32 ∧ item.2.metadata.node < 2 ^ 32 ∧
    item.2.metadata.seq < 2 ^ 64 ∧
    -2 ^ 63 ≤ item.2.metadata.timestamp ∧ item.2.metadata.timestamp < 2 ^ 63 ∧
    ∀ v, item.2.value = some v → v.length < 2 ^ 32

theorem leBytes_length (w x : Nat) : (leBytes w x).length = w := by
  induction w generalizing x with
  | zero => rfl
  | succ w ih => simp [leBytes, ih]

theorem leBytes_inj (w : Nat) : ∀ {x y : Nat}, x < 256 ^ w → y < 256 ^ w →
    leBytes w x = leBytes w y → x = y := by
  induction w with
  | zero => intro x y hx hy _; simp at hx hy; omega
  | succ w ih =>
    intro x y hx hy h
    simp only [leBytes, List.cons.injEq] at h
    obtain ⟨h1, h2⟩ := h
    have h1' := congrArg UInt8.toNat h1
    simp only [UInt8.toNat_ofNat'] at h1'
    have hp : 256 ^ (w + 1) = 256 ^ w * 256 := by rw [Nat.pow_succ]
    have := ih (x := x / 256) (y := y / 256) (Nat.div_lt_of_lt_mul (by rw [Nat.mul_comm]; omega))
      (Nat.div_lt_of_lt_mul (by rw [Nat.mul_comm]; omega)) h2
    omega

theorem keyBytes_inj {a b : Key} (h : keyBytes a = keyBytes b) : a = b := by
  unfold keyBytes at h
  exact String.toByteArray_inj.1 (ByteArray.ext (Array.toList_inj.1 h))

theorem i64Bits_inj {a b : Int} (ha : -2 ^ 63 ≤ a ∧ a < 2 ^ 63) (hb : -2 ^ 63 ≤ b ∧ b < 2 ^ 63)
    (h : i64Bits a = i64Bits b) : a = b := by
  unfold i64Bits at h; omega

theorem i64Bits_lt (t : Int) : i64Bits t < 256 ^ 8 := by
  unfold i64Bits; have : (256 : Nat) ^ 8 = 2 ^ 64 := by decide
  rw [this]; omega

/-- Peel a fixed-width field off the front of two equal streams. -/
theorem split_fixed {w x y : Nat} {r₁ r₂ : List UInt8} (hx : x < 256 ^ w) (hy : y < 256 ^ w)
    (h : leBytes w x ++ r₁ = leBytes w y ++ r₂) : x = y ∧ r₁ = r₂ := by
  obtain ⟨h1, h2⟩ := List.append_inj h (by simp [leBytes_length])
  exact ⟨leBytes_inj w hx hy h1, h2⟩

theorem p32 : (2 : Nat) ^ 32 = 256 ^ 4 := by decide
theorem p64 : (2 : Nat) ^ 64 = 256 ^ 8 := by decide

/-- One item is self-delimiting: equal streams start with the same item. -/
theorem encItem_prefix {a b : Key × Entry} {r₁ r₂ : List UInt8} (ha : Bounded a)
    (hb : Bounded b) (h : encItem a ++ r₁ = encItem b ++ r₂) : a = b ∧ r₁ = r₂ := by
  obtain ⟨ka, ⟨eka, va, ⟨na, sa, ta⟩⟩⟩ := a
  obtain ⟨kb, ⟨ekb, vb, ⟨nb, sb, tb⟩⟩⟩ := b
  obtain ⟨hea, hka, hna, hsa, hta1, hta2, hva⟩ := ha
  obtain ⟨heb, hkb, hnb, hsb, htb1, htb2, hvb⟩ := hb
  simp only at hea heb hka hkb hna hnb hsa hsb hta1 hta2 htb1 htb2 hva hvb
  subst hea heb
  simp only [encItem, List.append_assoc] at h
  rw [p32] at hka hkb hna hnb; rw [p64] at hsa hsb
  obtain ⟨hlen, h1⟩ := split_fixed hka hkb h
  obtain ⟨hk, h2⟩ := List.append_inj h1 hlen
  obtain ⟨hn, h3⟩ := split_fixed hna hnb h2
  obtain ⟨hs, h4⟩ := split_fixed hsa hsb h3
  obtain ⟨ht, h5⟩ := split_fixed (i64Bits_lt ta) (i64Bits_lt tb) h4
  have ht' := i64Bits_inj ⟨hta1, hta2⟩ ⟨htb1, htb2⟩ ht
  have hk' := keyBytes_inj hk
  clear h h1 h2 h3 h4
  subst hk' hn hs ht'
  cases va with
  | none =>
    cases vb with
    | none => simp at h5; exact ⟨rfl, h5⟩
    | some v => simp at h5
  | some v =>
    cases vb with
    | none => simp at h5
    | some w =>
      simp only [List.cons_append, List.nil_append, List.cons.injEq, true_and,
        List.append_assoc] at h5
      have hv := hva v rfl; have hw := hvb w rfl
      rw [p32] at hv hw
      obtain ⟨hl, h6⟩ := split_fixed hv hw h5
      obtain ⟨hvw, h7⟩ := List.append_inj h6 hl
      subst hvw
      exact ⟨rfl, h7⟩

theorem encItem_ne_nil (a : Key × Entry) : encItem a ≠ [] := by
  simp [encItem, leBytes]

/-- **The encoding is injective** on maps whose fields fit their widths. -/
theorem encode_inj : ∀ {l₁ l₂ : List (Key × Entry)}, (∀ a ∈ l₁, Bounded a) →
    (∀ a ∈ l₂, Bounded a) → encode l₁ = encode l₂ → l₁ = l₂
  | [], [], _, _, _ => rfl
  | [], b :: _, _, _, h => by
    simp only [encode, List.flatMap_nil, List.flatMap_cons] at h
    exact absurd (List.append_eq_nil_iff.1 h.symm).1 (encItem_ne_nil b)
  | a :: _, [], _, _, h => by
    simp only [encode, List.flatMap_nil, List.flatMap_cons] at h
    exact absurd (List.append_eq_nil_iff.1 h).1 (encItem_ne_nil a)
  | a :: t₁, b :: t₂, h₁, h₂, h => by
    simp only [encode, List.flatMap_cons] at h
    obtain ⟨hab, ht⟩ := encItem_prefix (h₁ a (by simp)) (h₂ b (by simp)) h
    subst hab
    rw [encode_inj (fun x hx => h₁ x (by simp [hx])) (fun x hx => h₂ x (by simp [hx])) ht]

/-- **Digest equality ⇔ state equality**, for a hash injective on the streams. -/
theorem digest_eq_iff {D : Type} (H : List UInt8 → D)
    (hinj : ∀ x y, H x = H y → x = y) (l₁ l₂ : List (Key × Entry))
    (h₁ : ∀ a ∈ l₁, Bounded a) (h₂ : ∀ a ∈ l₂, Bounded a) :
    digest H l₁ = digest H l₂ ↔ l₁ = l₂ :=
  ⟨fun h => encode_inj h₁ h₂ (hinj _ _ h), fun h => by rw [h]⟩

/-! ## Mirrored tests (`digest.rs` inline tests) -/

-- `length_prefixes_prevent_field_boundary_collisions`.
example : encode [("ab", ⟨"ab", some [], ⟨1, 1, 100⟩⟩)] ≠
    encode [("a", ⟨"a", some [98], ⟨1, 1, 100⟩⟩)] := by decide
-- `an_empty_value_differs_from_a_tombstone`.
example : encode [("k", ⟨"k", some [], ⟨1, 1, 100⟩⟩)] ≠
    encode [("k", ⟨"k", none, ⟨1, 1, 100⟩⟩)] := by decide
-- `tombstones_are_part_of_the_state`.
example : encode [("a", ⟨"a", none, ⟨1, 1, 100⟩⟩)] ≠ encode [] := by decide
-- `metadata_differences_are_visible`.
example : encode [("k", ⟨"k", some [118], ⟨1, 1, 100⟩⟩)] ≠
    encode [("k", ⟨"k", some [118], ⟨1, 1, 101⟩⟩)] := by decide

end Wavekv.Digest
