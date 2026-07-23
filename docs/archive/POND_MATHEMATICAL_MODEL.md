# The Pond Mathematical Model

> Week 1 of Architecture Falsification. No implementation. Only models.
> Every component must answer: **Is this the inevitable consequence of the
> model, or merely one implementation?**

---

## 0. Foundational Principle

**Pond stores immutable bytes with universal history. Every higher-level
capability is a different Lens over that substrate. Semantic metadata
never enters the storage kernel.**

This is not an implementation choice. It is the model's defining axiom.
Every consequence below follows from this.

---

## 1. Kernel Semantics

### 1.1. Primitives

The kernel provides exactly three operations:

```
Write : bytes → hash
Read  : hash → bytes
Ref   : name × hash → ()
```

where `hash = SHA-256(bytes)` and `name ∈ String`.

### 1.2. Axioms

**A1 (Immutability).** For all `b`, if `Write(b) = h`, then for all
subsequent `Read(h)`, the result is `b`. Formally:

```
∀ b. Write(b) = h ⟹ ∀ t > t₀. Read(h) = b
```

**A2 (Content-addressing).** For all `b₁, b₂`:

```
Write(b₁) = Write(b₂) ⟺ b₁ = b₂
```

Corollary: deduplication is free. Two writes of the same bytes
produce the same hash and occupy one blob.

**A3 (Name mutability).** `Ref(name, h)` is the only mutation. It
atomically updates the name→hash mapping. Last-writer-wins.

```
Ref(name, h₁) ; Ref(name, h₂) ⟹ resolve(name) = h₂
```

**A4 (Referential integrity).** `Ref(name, h)` requires that `h`
already exists (i.e., `h = Write(b)` for some `b`).

```
Ref(name, h) requires ∃ b. Write(b) = h
```

### 1.3. What the kernel does NOT know

The kernel has no concept of:
- format (JSON, Arrow, Parquet, Git tree, JPEG, ...)
- domain (SQL, Git, Notebook, Feature Store, ...)
- structure (table, row, column, tree, graph, ...)
- schema (types, fields, constraints, ...)
- optimization (index, cache, statistics, bloom filter, ...)
- coordination (transaction, lock, consensus, ...)
- policy (retention, GC, access control, ...)

This is not a limitation. It is the model's core design choice.
By knowing nothing about the data, the kernel never needs to be
updated for new formats, domains, or workloads.

### 1.4. Consequences

From A1-A4, the following are inevitable:

- **Dedup**: A2 ⟹ same bytes → one blob.
- **Verifiable integrity**: A1+A2 ⟹ hash = address = checksum.
- **Crash safety for committed data**: A1 ⟹ committed blobs are
  never modified; A3 ⟹ the only crash risk is a lost Ref (uncommitted
  name update), which loses at most one operation.
- **Time travel**: A1 ⟹ old blobs are never deleted; history is
  preserved as long as references to old commits exist.
- **Structural sharing**: A2 ⟹ two versions sharing the same bytes
  share the same blob (no copy needed).

---

## 2. Reference Semantics

### 2.1. Names as the only mutable state

The kernel's name→hash table is the **only mutable state**. Everything
else is immutable. This has deep consequences:

- **Branching** = creating a new name pointing to an existing commit.
  O(1), no data copied. (Consequence of A3.)
- **HEAD** = a name pointing to the latest commit.
- **Snapshot pointer** = a name pointing to the latest snapshot commit.
- **Branch pointer** = a name pointing to a branch's HEAD commit.
- **Index pointer** = a name pointing to an index tree root.
- **Pack pointer** = a name pointing to a packed-object file.

All of these are just `Ref(name, hash)`. The kernel doesn't
distinguish them. The naming convention (`{collection}__snapshot`,
`{collection}__branch__{name}`, `{collection}__index__{name}`) is
a Lens-level convention, not a kernel concept.

### 2.2. Collection as reference namespace

**Challenge: Is Collection actually fundamental?**

A Collection is currently:
- A name (e.g., `analytics/orders`)
- A HEAD reference (`analytics/orders` → commit hash)
- A snapshot reference (`analytics/orders__snapshot` → snapshot commit hash)
- Optional metadata (`analytics/orders__meta` → metadata blob hash)
- Optional branches (`analytics/orders__branch__{name}` → commit hash)
- Optional indexes (`analytics/orders__index__{name}` → index tree hash)

**All of these are just References.** The Collection is not an object
— it is a **reference namespace**: a set of related references that
happen to share a name prefix.

**Conclusion: Collection is not a fundamental architectural layer.
It is a naming convention over References.** The kernel does not
need a "Collection" concept. The Lens (or the application) creates
references with the right names. The Collection is emergent.

This simplifies the hierarchy:

```
Kernel (Bytes, History, Names)
    ↓
Lens (interprets bytes, creates references)
    ↓
Physical Structures (accelerates access)
    ↓
Applications
```

Collection is not a layer. It is a pattern of reference naming.

### 2.3. Snapshot pointer: is it fundamental?

**Challenge: Should the snapshot pointer be a separate Reference,
embedded in the commit, or derivable?**

Options:

**Option A: Separate Reference (current)**
- `{name}__snapshot` → latest snapshot commit hash
- Cost: 1 extra HEAD per Collection
- Benefit: O(1) lookup of the snapshot (1 RTT)
- Drawback: must be updated on every snapshot commit

**Option B: Embedded in commit**
- Each commit stores `snapshot_ancestor` (the hash of the nearest
  snapshot commit in its ancestry)
- Cost: 0 extra references; slightly larger commits
- Benefit: self-describing (the commit knows its own snapshot)
- Drawback: still need to read the commit to find the snapshot

**Option C: Derivable (skip pointers)**
- Commits have skip pointers (like Git's commit-graph)
- Cost: 0 extra references; skip pointers are in the commit
- Benefit: O(log N) history traversal
- Drawback: more complex commit format

**Analysis:** Option A (separate Reference) is the simplest and
gives O(1) snapshot access — critical for object stores where
every RTT costs 5-50ms. The cost (1 extra Ref per snapshot) is
negligible. **Option A is the right choice for object-store-first
design.**

But: the snapshot pointer is NOT a kernel concept. It is a
Lens-level optimization. The kernel stores References; the Lens
decides which references to maintain. Different Lenses could
choose different optimization strategies (some might not need
a snapshot pointer at all).

---

## 3. Commit Semantics

### 3.1. Commit structure

A commit is a blob containing:
- `parent`: hash of the parent commit (or None for the first commit)
- `second_parent`: hash of the second parent (for merge commits, or None)
- `snapshot`: hash of the Prolly tree root (for snapshot commits, or None)
- `delta`: {additions, deletions} (for delta commits, or None)
- `message`: human-readable description
- `timestamp`: wall-clock time
- `index`: commit sequence number

### 3.2. Commit types

**Snapshot commit**: `snapshot ≠ None, delta = None`
- Contains a full Prolly tree root
- O(log N) lookup via the tree
- O(changed_chunks) write (structural sharing)

**Delta commit**: `snapshot = None, delta ≠ None`
- Contains only changed keys
- O(1) write
- O(K) lookup (must walk delta chain to find key)

**Merge commit**: `second_parent ≠ None`
- Always a snapshot commit (contains full merged state)
- Two parents: the current HEAD and the merged branch HEAD
- Preserves branch topology in the commit graph

### 3.3. Tiered Commit Model

The Tiered Commit Model is NOT a kernel concept. It is a Lens-level
commit strategy:

1. First commit: always snapshot
2. Subsequent commits: delta (O(1) write) until threshold
3. Every K deltas: snapshot (O(changed_chunks) write)
4. Merge: always snapshot

The threshold K is a tunable parameter. K=1 means always-snapshot
(fast reads, slow writes). K=∞ means always-delta (fast writes,
slow reads). K=16 is the default (balanced).

**This is one implementation, not the only implementation.** Different
Lenses or applications could choose different K values or different
commit strategies entirely. The kernel doesn't know about tiers.

### 3.4. History

History is a traversal of the commit graph from HEAD backwards.
Each commit points to its parent(s). The traversal is O(history_length)
for a linear walk.

**Challenge: Can history become logarithmic?**

Currently: O(N) for N commits (linear walk).

Possible approaches:
1. **Skip pointers** (like Git's commit-graph): each commit stores
   a pointer to its grandparent, great-grandparent, etc. O(log N)
   traversal.
2. **Prolly tree of commits**: commits themselves form a Prolly tree,
   keyed by commit hash. O(log N) traversal.
3. **Periodic history snapshots**: a "history snapshot" stores a
   summary of the commit graph at a point in time. O(1) access to
   any historical epoch, then O(within_epoch) for fine-grained access.

This is an open research question. The current O(N) walk is
acceptable for most workloads (history is rarely traversed in full;
usually only the last few commits matter). For very long histories
(millions of commits), skip pointers are the pragmatic answer.

---

## 4. Lens Algebra

### 4.1. Definition

A Lens L is a 4-tuple:

```
L = (E, D, Σ, A)
```

where:
- `E : Object → bytes` (encode: domain object → kernel bytes)
- `D : bytes → Object` (decode: kernel bytes → domain object)
- `Σ` (state space: the set of all possible domain states)
- `A` (algebra: the set of operations the Lens supports)

### 4.2. Laws

**L1 (Round-trip).** For all `o ∈ Σ`:

```
D(E(o)) = o
```

Encoding is lossless. The Lens can persist any domain object and
recover it exactly.

**L2 (Purity of read).** Reading never mutates kernel state:

```
get(key) does not call Write or Ref
```

A Lens may read bytes and decode them, but it must never write
new blobs or update references during a read operation.

**L3 (Encoding preservation).** For every operation `σ ∈ A` and
every state `s ∈ Σ`:

```
E(σ(s)) is well-defined
```

Every reachable state is persistable. No operation produces a state
that cannot be encoded.

**L4 (Determinism).** For all `s ∈ Σ`:

```
E(s) is uniquely determined by s
```

Same state → same bytes → same hash. (Consequence of kernel A2.)

**L5 (Kernel independence).** For all `s ∈ Σ`:

```
E(s) is a finite byte string
```

The kernel can store and retrieve it without any knowledge of the
Lens's structure. The kernel never inspects blob contents.

**L6 (Composition).** If `L₁` and `L₂` are Lenses, then
`L₁ ⊕ L₂` (parallel composition) is also a Lens:

```
E_{L₁⊕L₂}(s₁, s₂) = E_{L₁}(s₁) || E_{L₂}(s₂)
D_{L₁⊕L₂}(b) = (D_{L₁}(b₁), D_{L₂}(b₂))
```

where `||` is byte concatenation and `b = b₁ || b₂`.

### 4.3. What a Lens is NOT

A Lens is NOT:
- A storage owner (bytes belong to the kernel)
- A format (the format is the Lens's choice, not the kernel's)
- A transaction (Lens operations are not atomic across Collections)
- A query engine (Lens provides operations, not query execution)

### 4.4. Lens interpretation

A Lens interprets bytes via `E`/`D`. The interpretation is
**context-based**: the key prefix (e.g., `sql/`, `git/`) tells
the Resolver which codec to use. The Resolver is code, not data.
The kernel never knows which codec is in use.

**Law L7 (Context-based interpretation).** The codec used to
decode a blob is determined by the key (context), not by the
blob itself. Formally:

```
D(key, bytes) = Resolver.decode(key_prefix, bytes)
```

where `Resolver` is a code-level mapping from key prefixes to
codecs. The blob carries no type information.

---

## 5. Physical Structure Algebra

### 5.1. Definition

A Physical Structure P is a 3-tuple:

```
P = (Source, Function, Artifact)
```

where:
- `Source : Snapshot` (the snapshot the structure is derived from)
- `Function : Snapshot → bytes` (a pure function)
- `Artifact : bytes` (the stored result)

### 5.2. Laws

**P1 (Determinism).** For all snapshots `S`:

```
Function(S) is uniquely determined by S
```

Same snapshot → same artifact. (If this fails, the structure is
non-deterministic and cannot be safely cached.)

**P2 (Derivability).** For every artifact `A`:

```
∃ S such that Function(S) = A
```

Every physical structure can be recomputed from its source snapshot.
If the structure is lost, it can be rebuilt. The structure is never
canonical data.

**P3 (Independence).** Computing, updating, or deleting a physical
structure does NOT modify the source snapshot.

```
Function(S) does not modify S
```

(Consequence of kernel A1: immutability.)

**P4 (Composability).** If `P₁ = (S, f₁, A₁)` and `P₂ = (A₁, f₂, A₂)`,
then `P₃ = (S, f₂ ∘ f₁, A₂)` is also a physical structure.

Physical structures can be derived from other physical structures.
The composition `f₂ ∘ f₁` is itself a pure function of the original
snapshot.

### 5.3. Candidates for Physical Structures

| Structure | Source | Function | Artifact |
|---|---|---|---|
| Secondary index | Snapshot | extract keys → tree | Prolly tree |
| Bloom filter | Snapshot | hash all keys → bitmap | Bitmap blob |
| Zone map | Snapshot | min/max per chunk | JSON stats blob |
| Statistics | Snapshot | compute aggregates | JSON stats blob |
| Histogram | Snapshot | bucket values | JSON histogram blob |
| Pack file | Snapshot | collect all blobs → pack | Pack blob |
| Materialized view | Snapshot | transform query | Result bytes |
| Feature vector | Snapshot | compute features | Float array blob |
| Search index | Snapshot | tokenize → inverted index | Index blob |
| Cache | Snapshot | identity (or transform) | Cached bytes |

### 5.4. The hypothesis (to be proven or falsified)

**Hypothesis: Every storage optimization is a Physical Structure.**

If true, this means:
- Indexes, caches, statistics, bloom filters, pack files, materialized
  views, feature vectors, search indexes — all are `f(snapshot) → artifact`.
- The kernel never needs to know what an "index" is. It only knows
  "this artifact is derived from this snapshot via this function."
- Adding a new optimization type requires no kernel changes — just
  a new `Function`.

**This is potentially publishable. But it needs proofs, not intuition.**

### 5.5. Counterexamples to investigate

The Red Team found potential counterexamples:

1. **Learned indexes**: the function depends on the data AND a trained
   model. Is `f(snapshot, model)` still a pure function of snapshot?
   (Answer: if the model is itself derived from the snapshot, then yes.
   If the model is external, then no — but then it's not a Physical
   Structure, it's an external dependency.)

2. **Randomized sketches (HLL, Bloom filters)**: the function involves
   randomization. Is `f(snapshot)` deterministic? (Answer: if the
   random seed is fixed or derived from the snapshot, then yes.
   If the seed is random, then no — but a non-deterministic sketch
   violates P1 and should not be cached as a Physical Structure.)

3. **Caches with eviction policies**: the artifact depends on access
   patterns, not just the snapshot. (Answer: a cache is NOT a pure
   function of the snapshot. It's a function of (snapshot, access_pattern).
   This is a genuine counterexample — caches are NOT Physical Structures
   in the strict sense. They are a different kind of optimization.)

4. **Compression dictionaries**: the dictionary is built from the data
   and then used to compress it. (Answer: if the dictionary is derived
   from the snapshot, then `f(snapshot) = compress(snapshot, dict(snapshot))`
   is a pure function. If the dictionary is external, it's not.)

**Conclusion: The hypothesis holds for most structures (indexes, stats,
bloom filters, zone maps, pack files, materialized views) but NOT for
caches (which depend on access patterns). Caches should be classified
separately — they are not Physical Structures, they are access-pattern
optimizations.**

---

## 6. RTT Budget

Every operation must have a round-trip budget. Designs exceeding the
budget are rejected.

| Operation | Target RTTs | Current | Gap |
|---|---|---|---|
| Point lookup | ≤3 | 4-5 | Need to cache snapshot root in HEAD reference |
| Batch lookup | ≤4 | 4-5 (with pack) | Close |
| Streaming commit | ≤3 | 2-3 | ✓ Met |
| Snapshot commit | ≤5 | O(chunks) | Depends on changed_chunks |
| Branch | ≤2 | 2 | ✓ Met |
| Checkout | ≤3 | 4-9 | Need to optimize snapshot pointer update |
| Merge | ≤8 | ~19 | Need diff-based merge |
| Scan | ≤5 | 4 (with pack) | ✓ Met |
| History(N) | ≤log(N) | O(N) | Need skip pointers |
| Restart | ≤2 | 1-6 | Close |

### 6.1. Lookup optimization path (5 → 3 RTTs)

Current: HEAD(1) → commit(1) → tree_path(log N) → blob(1) = 4-5 RTTs

To reach 3 RTTs:
- **Embed snapshot root in the HEAD reference** (not in a commit blob).
  The reference stores `{commit_hash, snapshot_root}` instead of just
  `commit_hash`. Then: HEAD(1) → tree_path(log N) → blob(1) = 3 RTTs.
- Cost: the reference is larger (64 bytes instead of 32 bytes).
- Benefit: saves 1 GET (the commit blob read) on every lookup.

This is a kernel-level optimization (the reference format changes).
It should be modeled before implementing.

---

## 7. Merge Algebra

### 7.1. Current model

```
merge(branch_name):
  state_A = read_all()  // current HEAD state
  state_B = read_state(branch_HEAD)  // branch state
  merged = state_A ∪ state_B  // union, B wins on conflict
  write snapshot(merged)
```

This is O(|A| + |B|) — reads both full states, writes a full snapshot.
On object storage, this is ~19 RTTs.

### 7.2. What merge should be

Merge should be an **algebra** on snapshots, not a full-state operation.

```
merge(A, B) = diff(A, B) applied to A
```

Where `diff(A, B)` is the set of keys that differ between A and B.
If A and B share the same Prolly tree structure (structural sharing),
the diff is O(changed_chunks), not O(|A| + |B|).

### 7.3. Merge semantics

The current "union, last-writer-wins" is one policy. Others:

- **3-way merge**: find common ancestor C, compute diff(A, C) and
  diff(B, C), apply both. Detect conflicts where both sides changed
  the same key.
- **CRDT merge**: for commutative operations (counters, sets), merge
  is deterministic and conflict-free.
- **Domain-specific merge**: the Lens defines merge semantics. SQL
  might use row-level merge; Git might use tree-level merge; Feature
  Store might use timestamp-based merge.

**Merge semantics are a Lens-level concern, not a kernel concern.**
The kernel provides the commit graph (parents, second_parent). The
Lens defines how to merge two states. This is consistent with the
"semantic metadata never enters the storage kernel" principle.

---

## 8. Open Questions

These are the questions that need to be answered before the model
is complete:

1. **Should the snapshot root be embedded in the HEAD reference?**
   (Saves 1 RTT per lookup, but makes references larger.)

2. **Can history become O(log N)?**
   (Skip pointers, commit-graph, or Prolly tree of commits.)

3. **Is the Physical Structure hypothesis provable?**
   (Every optimization is f(snapshot) → artifact, except caches.)

4. **What is the merge algebra?**
   (3-way? CRDT? Domain-specific? How does it compose?)

5. **Should Collection be eliminated as a layer?**
   (It's just a reference namespace. The kernel doesn't need it.)

6. **What goes in the Workspace/Transaction layer?**
   (Staging, atomic multi-key writes, cross-Lens transactions.)

7. **How does garbage collection work?**
   (Orphaned blobs, tombstone compaction, pack file GC.)

8. **What are the object-store anomalies?**
   (S3 eventual consistency, partial writes, list-after-put, etc.)

---

## 9. What This Model Proves

The model proves (informally, not formally):

1. **Three primitives are sufficient** for immutable, content-addressed,
   versioned storage with branching and merging. (A1-A4.)

2. **Semantic metadata never enters the storage kernel.** The kernel
   stores bytes and references. All interpretation (format, domain,
   schema, optimization) is in Lenses and Physical Structures. (L5, L7.)

3. **Cross-Lens interoperability is emergent.** Any Lens can read any
   blob via `get_raw` (raw bytes) or via the Resolver (decoded). No
   translation metadata is needed. (L7, P3.)

4. **Physical Structures are deterministic and rebuildable.** Any
   structure can be lost and rebuilt from its source snapshot. (P1, P2.)

5. **History and current state are separable.** The snapshot pointer
   decouples current-state access (O(log N)) from history access
   (O(history_length)). (§2.3, §3.3.)

What the model does NOT prove:
- That three primitives are **necessary** (lower bound). (This was
  proven separately in FORMAL_ALGEBRA.md.)
- That the Physical Structure hypothesis is **universally true**.
  (Caches are a counterexample. §5.5.)
- That the merge algebra is **correct** for all workloads. (Current
  merge is naive; domain-specific merge is an open question. §7.)
- That the system is **safe under concurrent writers**. (No ACID
  transactions. §8.6.)
