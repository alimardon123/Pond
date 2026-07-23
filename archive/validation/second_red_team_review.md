# Second Red Team Review: Attack on the Mathematical Model

**Reviewer role:** Expert storage engine architect with experience at
FoundationDB, Git, Dolt, Iceberg, Pebble/RocksDB, and WarpStream.
**Mandate:** Attack the mathematical model. Not the implementation.
**Task ID:** 51

---

## 0. Overall Verdict: WEAK REJECT

The mathematical model is mostly sound but has **three fatal gaps**
and **five serious issues** that prevent it from being a complete
storage model. The kernel axioms (A1-A4) are solid. The Reference
Algebra is adequate. But the model fails to account for **State** as
a primitive, lacks a **Manifest Algebra** for packed storage, and has
a **circular dependency** between Snapshot and Commit.

The model can survive, but only if three things happen:
1. "State" is introduced as the primary mathematical object (not "Bytes")
2. Manifest Algebra is added (logical → physical mapping)
3. The Snapshot-Commit circularity is broken

---

## 1. Inconsistencies Between Algebras

### 1.1 R2 (last-writer-wins) vs W2 (workspace atomicity) — SERIOUS

**The conflict:** Reference Algebra R2 says `set(name, h₁) ; set(name, h₂)`
results in `resolve(name) = h₂`. Workspace Algebra W2 says `commit(ws)`
is atomic — either all changes commit or none.

**The problem:** If two Workspaces commit simultaneously, both call
`reference(name, their_commit_hash)`. R2 says one wins. But the loser's
Workspace believed its commit was atomic (W2). The loser's commit
blob exists in the object store (orphaned), but the reference doesn't
point to it. This is not a data corruption — but it IS a violation of
W2's atomicity promise from the loser's perspective.

**Fix:** W2 should be weakened to: "commit is atomic from the
Workspace's perspective — either the reference is updated OR the
commit is orphaned (detectable via GC)." The model should explicitly
acknowledge that concurrent commits produce orphans.

### 1.2 P3 (Physical Structure independence) vs packed storage — FATAL

**The conflict:** Physical Structure Law P3 says computing a structure
does not modify the source snapshot. But packed storage (§6 of the
formal algebras) introduces a **manifest** that maps logical blob
hashes to physical locations within a pack file. The manifest IS a
Physical Structure — but it ALSO changes how the kernel reads blobs.

**The problem:** After packing, `read_blob(hash)` must consult the
manifest to find the physical location. This means the manifest is
not independent of the kernel — it's a **kernel-level concern**, not
a Physical Structure. P3 is violated.

**Fix:** Introduce a **Manifest Algebra** that sits between the kernel
and Physical Structures. The manifest is not a Physical Structure;
it's a **kernel-level indirection** that maps logical hashes to
physical locations.

---

## 2. Redundant Algebras

### 2.1 Workspace is a special case of Reference — SERIOUS

Workspace stores `{add: dict[key, hash], del: set[key]}` in memory.
This is exactly what a delta commit stores. A Workspace IS an
uncommitted delta commit.

**The reduction:** Workspace = "a delta commit that hasn't been
written to the kernel yet." The Workspace algebra (W1-W5) can be
derived from the commit model:
- W1 (isolation) = "uncommitted changes aren't visible" (trivially true — they're in memory)
- W2 (atomicity) = "commit writes all staged changes as one commit" (already true)
- W3 (savepoint) = "checkpoint the in-memory state" (implementation detail)
- W4 (Lens independence) = "any Lens can stage to the same delta" (trivially true)
- W5 (ephemeral) = "not in the commit history until committed" (trivially true)

**Verdict:** Workspace is NOT a separate algebra. It's an
**implementation pattern** over the existing commit model. The
model should document it as such, not formalize it as a separate
algebra.

### 2.2 History is a special case of Physical Structure — already acknowledged

The model already acknowledges this (§8.6: "History is a Physical
Structure"). History = `f(all_commits) → history_graph`. This is
correct. The History Algebra should be MERGED into the Physical
Structure Taxonomy, not kept separate.

---

## 3. Missing Primitives

### 3.1 "State" is missing — FATAL

**The problem:** The model says "the kernel stores bytes." But every
optimization operates on STATE, not bytes:
- Snapshots are states (Prolly tree roots)
- Deltas are state transitions (additions/deletions)
- Indexes are functions of state
- History is a sequence of states
- Merge combines two states
- GC walks reachability through state

"Bytes" is the ENCODING of state, not the primitive. The model should be:

```
State → encode → bytes → kernel (storage)
bytes → kernel → decode → State → Lens (interpretation)
```

Not:

```
bytes → kernel → Lens (interpretation)
```

**Consequence:** Without "State" as a primitive, the model cannot
distinguish between:
- A blob that IS state (a JSON record)
- A blob that is METADATA about state (a commit, a tree node, an index)
- A blob that is PHYSICAL LAYOUT (a pack file, a manifest)

All three are "bytes" today. But they have different semantics:
- State blobs are interpreted by Lenses
- Metadata blobs are interpreted by the commit model
- Layout blobs are interpreted by the storage backend

**Fix:** Introduce `State` as the primary mathematical object. The
kernel stores bytes (the encoding of state). Lenses decode bytes
into state. Physical Structures are functions of state. This
simplifies several algebras:
- Snapshot = a serialized State
- Delta = a state transition (σ: State → State)
- History = a sequence of (State, σ) pairs
- Physical Structure = f(State) → artifact
- Merge = combine two States via a Lens-defined function

### 3.2 "Manifest" is missing — FATAL

A **manifest** maps logical blob hashes to physical locations:

```
Manifest = { logical_hash → (pack_id, offset, length) }
```

Without manifests, the model cannot express:
- Packed storage (multiple blobs in one object)
- Range reads (partial object access)
- Physical layout optimization

The manifest is NOT a Physical Structure (it changes how the kernel
reads blobs). It's NOT a Reference (it maps hashes, not names).
It's a new primitive: a **logical-to-physical indirection**.

**Fix:** Add Manifest as a kernel-level concept:

```
Manifest : Hash → PhysicalLocation
PhysicalLocation = (object_id, offset, length)
```

The kernel's `read_blob(hash)` first checks the manifest. If the
hash is in a pack, it reads from the pack (1 range read). If not,
it reads the individual blob (1 GET).

### 3.3 "Range Read" is missing — SERIOUS

The model's RTT Calculus counts GETs, PUTs, LISTs, HEADs, and RANGEs.
But Range Read has no formal algebra. Questions unanswered:
- When is a range read cheaper than a full GET?
- Can a range read be parallelized?
- What's the cost of reading tree node N from a packed tree?
- Does range read change the lookup cost model?

**Fix:** Add Range Read Algebra:

```
RangeRead(object_id, offset, length) → bytes
Cost: 1 RANGE (cheaper than 1 GET for large objects)
```

### 3.4 "Lease/Lock" is missing — MINOR

The Reference Algebra mentions locks and leases as "just references."
But the model doesn't define:
- Lease expiry semantics
- Lock fairness
- Deadlock detection

**Fix:** Either formalize leases (with expiry and auto-release) or
explicitly state that locks/leases are an application-level concern,
not a kernel concept.

---

## 4. Hidden Assumptions

### 4.1 Names are assumed unique — SERIOUS

The model assumes `name ∈ String` is unique. But nothing prevents
two different operations from creating the same name with different
hashes. R2 (last-writer-wins) handles this, but the model doesn't
state that names MUST be unique.

**Fix:** Add axiom: "At any time, each name maps to at most one hash."
(This is implied by R2 but should be stated.)

### 4.2 Writes are assumed durable — SERIOUS

Axiom A1 says "if Write(b) = h, then Read(h) = b for all t > t₀."
But on object stores, a PUT can return success before the object is
fully durable (S3 strong consistency helps, but multipart uploads
and cross-region replication have windows).

**Fix:** Add axiom: "Write is durable when it returns." Or weaken A1
to: "Write is eventually durable" and add a consistency model.

### 4.3 Time is assumed monotonic — MINOR

Commits include `timestamp: float`. The Determinism Law (L4) says
"same state → same bytes → same hash." But commit hashes include
timestamps, making them non-deterministic. This is acknowledged but
not formalized.

**Fix:** State that commit identity includes temporal information,
and data identity does not. Two different terms: "data hash"
(deterministic) vs "commit hash" (non-deterministic).

### 4.4 References are assumed atomically visible — FATAL for distributed

The model says `reference(name, hash)` is atomic (R1). But on S3,
there's no atomic conditional write (no CAS). A reference update is
just a PUT — another reader might see the old value.

**Fix:** For single-node: R1 holds. For distributed: R1 must be
weakened to "eventually consistent." The model should state which
axioms hold only in single-node mode.

---

## 5. Circular Definitions

### 5.1 Snapshot ↔ Commit — FATAL

**The circularity:**
- A **snapshot commit** contains a `snapshot` field (the Prolly tree root)
- The Prolly tree root is a hash of the tree's root node
- The tree's root node contains child hashes
- Child hashes point to leaf nodes
- Leaf nodes contain `(key, blob_hash)` pairs
- `blob_hash` points to data blobs

This is not actually circular — it's a tree (acyclic). BUT:
- "Snapshot" is defined as "a commit with a snapshot field"
- "Commit" is defined as "a blob with parent/snapshot/delta fields"
- "Blob" is defined as "bytes written via Write()"

The circularity is: **"snapshot" is defined in terms of "commit,"
which is defined in terms of "blob," which is defined in terms of
"bytes." But "snapshot" is ALSO used to define "state" (the current
state = the latest snapshot). And "state" is used to define Physical
Structures (f(state) → artifact). And Physical Structures include
"history" (which is derived from commits, which are derived from
snapshots).**

**The fix:** Introduce "State" as a primitive that is NOT defined
in terms of commits or blobs. State is the abstract mathematical
object. Commits, snapshots, and blobs are ENCODINGS of state. This
breaks the circularity:

```
State (primitive)
  ↓ encode
Bytes (stored in kernel)
  ↓ decode
State (interpreted by Lens)
```

### 5.2 Lens ↔ Collection ↔ Reference — MINOR

- Lens creates References (to store data)
- References form Collections (naming convention)
- Collections are accessed via Lenses

This is not circular — it's a usage pattern. But the model should
state that Collection is NOT a primitive; it's emergent from
Reference naming.

---

## 6. Missing Algebras

### 6.1 Manifest Algebra — FATAL (must add)

```
Manifest = { Hash → PhysicalLocation }
PhysicalLocation = (object_id: Hash, offset: int, length: int)

Laws:
  M1 (Completeness): Every reachable hash has a manifest entry OR is a standalone blob.
  M2 (Immutability): Manifests are immutable (stored as blobs).
  M3 (Indirection): read_blob(h) = if h ∈ Manifest then RangeRead(Manifest[h]) else Get(h)
  M4 (Composability): Manifests can be merged (union of entries).
```

### 6.2 State Algebra — FATAL (must add)

```
State = the abstract mathematical object representing a collection's data

Operations:
  encode: State → Bytes
  decode: Bytes → State
  transition: State × Operation → State

Laws:
  S1 (Round-trip): decode(encode(s)) = s
  S2 (Determinism): encode(s) is unique
  S3 (Transitions): all operations are pure functions on State

Relationship to existing concepts:
  Snapshot = a serialized State
  Delta = a state transition
  Commit = (parent, State or transition, metadata)
  Physical Structure = f(State) → artifact
```

### 6.3 Concurrency Algebra — SERIOUS (should add)

The model has no concurrency model. Questions:
- What happens when two writers commit simultaneously?
- Is there a compare-and-swap primitive?
- What consistency level does the model guarantee?

```
Concurrency model:
  C1 (Single-writer): only one writer can commit at a time (pessimistic)
  C2 (Optimistic): multiple writers stage, one wins on commit (CAS)
  C3 (No guarantee): last-writer-wins, losers' commits are orphaned

Current model: C3 (implicit). Should be explicit.
```

### 6.4 Consistency Algebra — SERIOUS (should add)

What guarantees does the model provide?
- Read-after-write: yes (single-node) / eventually (distributed)
- Read-after-commit: yes (the reference is updated atomically)
- Monotonic reads: no guarantee
- Consistent prefix: no guarantee

### 6.5 Range Read Algebra — should add

```
RangeRead(object_id, offset, length) → bytes
Cost: 1 RANGE (cheaper than 1 GET for large objects)

Laws:
  RR1 (Partial): RangeRead(id, 0, len) = Get(id)
  RR2 (Composable): RangeRead(id, off1, len1) + RangeRead(id, off1+len1, len2) = RangeRead(id, off1, len1+len2)
```

---

## 7. The "State vs Bytes" Question

### Verdict: "State" should replace "Bytes" as the primary primitive.

**Argument:** The model currently says "the kernel stores bytes."
But:
- Commits are not bytes — they are state transitions
- Snapshots are not bytes — they are serialized states
- Indexes are not bytes — they are functions of state
- History is not bytes — it is a sequence of states
- Merge combines states, not bytes
- GC walks state reachability, not byte reachability

"Bytes" is the physical encoding. "State" is the mathematical object.
The model should be:

```
State (primitive — the abstract data)
  ↓ encode (Lens-specific)
Bytes (physical — stored in kernel)
  ↓ decode (Lens-specific)
State (interpreted — presented to application)
```

**What changes:**
- Kernel axiom A1 becomes: "encode(s) is immutable once written"
- Lens law L1 becomes: "decode(encode(s)) = s" (already exists)
- Physical Structure: "f(State) → artifact" (not "f(snapshot)")
- Commit: "a transition σ: State → State, encoded as bytes"
- History: "a sequence of (State, σ) pairs"

**What stays the same:**
- The kernel still stores bytes (the encoding is opaque)
- Lenses still encode/decode
- References still map names to hashes
- All existing laws hold

**What simplifies:**
- "Snapshot" is no longer a special commit type — it's just a serialized State
- "Delta" is no longer a special commit type — it's just a state transition
- The Tiered Commit Model becomes "store State or store Δ(State)"
- Physical Structures are f(State) → artifact (cleaner than f(snapshot))

---

## 8. Physical Structure Calculus — Additional Counterexamples

### 8.1 Incrementally maintained materialized views — SERIOUS

An incrementally maintained view depends on BOTH the current snapshot
AND the previous view state:

```
view_new = f(snapshot_new, view_old)
```

This is NOT `f(snapshot) → artifact`. It's `f(snapshot, view_old) → artifact`.
The function depends on the view's own history, not just the snapshot.

**Fix:** Classify incrementally maintained views as a separate category:
"Stateful Physical Structures" — they depend on (snapshot, prior_state).
This is different from "Stateless Physical Structures" (f(snapshot) → artifact).

### 8.2 Learned indexes — MINOR

A learned index depends on the snapshot AND a trained model. But if
the model is trained FROM the snapshot, then `f(snapshot) = train(snapshot)
+ build_index(snapshot, model)`. This IS a pure function of the snapshot
— just a complex one. Not a counterexample.

### 8.3 Adaptive compression — MINOR

Same as learned indexes: if the compression dictionary is derived
from the snapshot, it's a pure function. Not a counterexample.

### 8.4 Query plan caches — already classified as Cache

Query plan caches depend on query history (access patterns). Already
correctly classified as Cache (not a Physical Structure).

---

## 9. Object Store Native Definition — Gaps

### 9.1 Missing: Conditional writes — SERIOUS

S3 supports `If-Match` and `If-None-Match` headers. The model's
Reference Algebra has `compare_and_swap` (R3), but OSN doesn't
mention conditional writes. On S3, CAS requires either:
- 2 RTTs (GET + conditional PUT), or
- 1 RTT with `If-Match` (S3 conditional PUT)

**Fix:** Add OSN9: "Conditional writes are supported via If-Match
or equivalent."

### 9.2 Missing: Multipart upload — MINOR

Large objects (>5GB on S3) require multipart upload. The model
doesn't address this. It's an implementation concern, not a model
concern — but OSN should acknowledge it.

### 9.3 Missing: Eventual consistency window — SERIOUS

S3 provides strong read-after-write consistency (since Dec 2020),
but cross-region replication is eventually consistent. The model's
A1 (immutability) assumes reads always return the correct data. For
cross-region setups, this may not hold during the replication window.

**Fix:** Add OSN10: "The system tolerates eventual consistency for
cross-region replication. Reads may return stale data during the
replication window."

---

## 10. Summary of Required Changes

### Eliminate
- **Workspace Algebra** — it's a pattern over commits, not a separate algebra
- **History Algebra** — merge into Physical Structure Taxonomy

### Add (FATAL)
- **State Algebra** — State is the primary primitive, not Bytes
- **Manifest Algebra** — logical → physical mapping for packed storage

### Add (SERIOUS)
- **Concurrency Algebra** — explicit consistency model (C3: last-writer-wins)
- **Consistency Algebra** — read-after-write, monotonic reads, etc.
- **Range Read Algebra** — partial object access

### Merge
- History → Physical Structure Taxonomy (as "History Graphs" category)
- Workspace → Commit Model (as "uncommitted delta" pattern)

### Fix
- Break Snapshot ↔ Commit circularity by introducing State as primitive
- Add Manifest as kernel-level indirection (not a Physical Structure)
- Classify incrementally maintained views as "Stateful Physical Structures"
- Add OSN9 (conditional writes) and OSN10 (eventual consistency tolerance)
- State explicitly: "Names are unique at any point in time"
- State explicitly: "Writes are durable when they return" (single-node)

---

## 11. Final Assessment

The model's FOUNDATION is sound:
- 3 kernel primitives (Write, Read, Reference) — necessary and sufficient
- Content-addressing (A2) — correct and powerful
- Lens separation — genuinely novel and correct
- Reference Algebra — adequate for single-node

The model's STRUCTURE has gaps:
- "Bytes" is the wrong primitive — should be "State"
- Manifest is missing — needed for packed storage
- Concurrency and consistency are undefined
- Workspace and History are over-formalized (redundant)

The model can be fixed. The fixes simplify rather than complicate.
After fixing, the model would be:

```
State (primitive)
  ↓ encode
Bytes → Kernel (Write, Read, Reference)
  ↓ decode
State → Lens (interpretation)
  ↓ f(State)
Physical Structures (acceleration)
  ↓ manifest
Physical Layout (packed objects, range reads)
```

This is cleaner, more honest, and more publishable than the current model.
