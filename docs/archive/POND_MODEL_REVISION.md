# Pond Model Revision — State as Primary Primitive

> Second Red Team finding applied. The model's primary mathematical
> object is now **State**, not **Bytes**. Bytes are the encoding.
> The kernel stores bytes; Lenses decode bytes into State.

---

## The Revised Model

```
State (primitive — the abstract data of a collection)
  ↓ encode (Lens-specific: JSON, Arrow, Git tree, etc.)
Bytes (physical — stored in kernel as content-addressed blobs)
  ↓ decode (Lens-specific)
State (interpreted — presented to application)
  ↓ f(State)
Physical Structures (acceleration: indexes, stats, packs)
```

### What State IS

State is the abstract mathematical object representing a collection's
data at a point in time. It is:
- **Format-agnostic** (not JSON, not Arrow, not any specific encoding)
- **Lens-defined** (each Lens defines what "State" means for its domain)
- **Immutable** (a State never changes; transitions produce new States)
- **Serializable** (can be encoded to bytes and stored in the kernel)

### What State replaces

| Old concept | New concept |
|---|---|
| "snapshot" (a commit with a tree root) | A serialized State |
| "delta" (a commit with additions/deletions) | A State transition σ: State → State |
| "current state" (HEAD → snapshot) | The State encoded by the latest snapshot commit |
| Physical Structure f(snapshot) | f(State) → artifact |
| History | A sequence of (State, σ) pairs |
| Merge | combine(State_A, State_B) → State_merged |

### What stays the same

- The kernel still stores bytes (the encoding is opaque)
- Lenses still encode/decode
- References still map names to hashes
- All existing axioms (A1-A4) hold — they apply to bytes
- All existing Lens laws (L1-L7) hold — they apply to encode/decode
- All existing Physical Structure laws (P1-P4) hold — they apply to f(State)

---

## State Algebra

### Definition

```
State = the abstract data of a collection at a point in time

Operations:
  encode : State → Bytes     (Lens-specific)
  decode : Bytes → State     (Lens-specific)
  transition : State × Operation → State  (Lens-specific)
```

### Laws

**S1 (Round-trip).** For all `s ∈ State`:
```
decode(encode(s)) = s
```
Encoding is lossless. (Same as existing L1.)

**S2 (Determinism).** For all `s ∈ State`:
```
encode(s) is uniquely determined by s
```
Same State → same bytes → same hash. (Same as existing L4.)

**S3 (Transition purity).** For all `s ∈ State` and `σ ∈ Operations`:
```
σ(s) is uniquely determined by (s, σ)
```
State transitions are pure functions. Same inputs → same output.

**S4 (Encoding preservation).** For all reachable `s' = σ(s)`:
```
encode(s') is well-defined
```
Every reachable State is persistable. (Same as existing L3.)

**S5 (Kernel independence).** For all `s ∈ State`:
```
encode(s) is a finite byte string
```
The kernel can store it without knowing the State's structure.
(Same as existing L5.)

### What this simplifies

1. **Commit model:** A commit is `(parent, encode(State) or Δ(State), metadata)`.
   "Snapshot" = commit with full encoded State. "Delta" = commit with
   State transition. No special types needed — just two encoding strategies.

2. **Physical Structures:** `f(State) → artifact`. Not `f(snapshot)`.
   Cleaner, more general.

3. **History:** A sequence of `(State, σ)` pairs. History IS a Physical
   Structure: `f(all_commits) → history_graph`. But "all_commits" is
   just "the sequence of States and transitions."

4. **Merge:** `merge(State_A, State_B, ancestor?) → State_merged`.
   The Lens defines the merge function on States. The kernel records
   the topology.

5. **GC:** Reachability is about States (which blobs encode reachable
   States), not about bytes. This clarifies which blobs can be collected.

---

## Manifest Algebra (NEW — addresses packed storage)

### Definition

```
Manifest = { Hash → PhysicalLocation }
PhysicalLocation = (object_id: Hash, offset: int, length: int)
```

A Manifest maps logical blob hashes to physical locations. When blobs
are packed into a single large object, the Manifest tells the kernel
where to find each blob within the pack.

### Laws

**M1 (Completeness).** Every reachable hash either:
- Has an entry in a Manifest (it's in a pack), OR
- Is a standalone blob (read directly)

**M2 (Immutability).** Manifests are immutable (stored as content-addressed blobs).

**M3 (Indirection).** `read_blob(h)`:
```
if h ∈ Manifest:
    loc = Manifest[h]
    return RangeRead(loc.object_id, loc.offset, loc.length)
else:
    return Get(h)
```

**M4 (Composability).** Manifests can be merged:
```
Manifest₁ ∪ Manifest₂ = { h → loc | h → loc ∈ Manifest₁ ∪ Manifest₂ }
```

### What this enables

1. **Packed storage:** multiple blobs in one object (pack file).
   The Manifest maps each blob hash to its (pack_id, offset, length).

2. **Range reads:** `read_blob(h)` for a packed blob = 1 RANGE read
   (cheaper than 1 GET for large objects).

3. **GC integration:** GC checks the Manifest to determine if a blob
   is in a pack. If the pack is reachable, all blobs in the pack are
   reachable.

4. **Physical layout independence:** the kernel's logical API
   (`write`, `read_blob`) is unchanged. The Manifest is an internal
   indirection that changes HOW blobs are stored, not WHAT they are.

### RTT impact

| Operation | Without Manifest | With Manifest |
|---|---|---|
| Point lookup | 3-4 GETs | 2 GETs + 1 RANGE (if blob is packed) |
| Scan (100 blobs) | 100 GETs | 1 GET (pack) + 1 GET (manifest) = 2 RTTs |
| Write | 1 PUT (blob) | 1 PUT (blob) — packing is deferred |

---

## Revised Algebra Summary

| # | Algebra | Status | Change |
|---|---|---|---|
| 1 | **State** | NEW (fatal gap) | State is the primary primitive. Bytes are the encoding. |
| 2 | **Reference** | Unchanged | Still Ref(name, hash). All roles are naming conventions. |
| 3 | **Merge** | Simplified | merge(State_A, State_B) → State_merged. Lens-defined. |
| 4 | **GC** | Updated | Reachability through States and Manifests. |
| 5 | **RTT** | Updated | Added RANGE to cost vector. Manifest reduces scan RTTs. |
| 6 | **OSN** | Updated | Added OSN9 (conditional writes), OSN10 (eventual consistency). |
| 7 | **Physical Structure** | Simplified | f(State) → artifact. Added "Stateful" subcategory for incremental views. |
| 8 | **Manifest** | NEW (fatal gap) | Logical → physical mapping for packed storage. |
| 9 | **Range Read** | NEW | RangeRead(object, offset, length). Partial object access. |
| 10 | **Concurrency** | NEW | Explicit: C3 (last-writer-wins, losers orphaned). |
| 11 | **Consistency** | NEW | Read-after-write (single-node), eventual (distributed). |

### Eliminated
- **Workspace** — it's an uncommitted delta (implementation pattern over commits)
- **History** — it's a Physical Structure (f(commits) → graph)

### Merged
- History → Physical Structure Taxonomy (as "History Graphs" category)
- Workspace → Commit Model (as "staging = uncommitted delta transition")

---

## Revised Hierarchy

```
State (primitive — abstract data)
  ↓ encode
Bytes → Kernel (Write, Read, Reference — 3 primitives, frozen)
  ↓ Manifest (logical → physical indirection)
Physical Storage (blobs, packs, range reads)
  ↓ decode
State → Lens (interpretation — encode/decode + domain operations)
  ↓ f(State)
Physical Structures (acceleration — indexes, stats, packs, history)
  ↓
Applications (SQL, Git, Notebook, Feature Store)
```

This is cleaner:
1. State is the primary object (not Bytes)
2. Manifest sits between kernel and physical storage
3. Lens is pure interpretation (no staging — that's in the commit model)
4. Physical Structures are f(State) → artifact
5. Everything flows downward

---

## Design Principles (Updated)

The original 6 design principles remain. Add:

7. **Model-driven** — every design choice must answer: "Is this the
   inevitable consequence of the model, or merely one implementation?"
   If it's merely an implementation, document it as such.

8. **Object-store-native** — every operation must have a bounded RTT
   budget. Designs exceeding the budget are rejected. The system must
   work on S3/Azure Blob/GCS without local metadata databases.

9. **Semantic isolation** — semantic metadata (format, domain, schema,
   optimization) never enters the storage kernel. The kernel stores
   bytes and references. Everything else is interpretation.

10. **Falsifiable** — every claim must be executable as a test or
    expressible as a formal property. Untestable claims are rejected.
