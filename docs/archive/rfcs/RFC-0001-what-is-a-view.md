# RFC-0001: What Is a Lens?

## Status

Draft — the biggest unanswered research question in Pond.

## Abstract

The kernel has 3 primitives (Write, Read, Reference), 5 storage laws,
7 composition laws, and a lower-bound proof. Views have 14 working
implementations, an independent implementation challenge, and a
View Author's Guide. But "View" itself remains intuitive — never
formally defined.

This RFC attempts to define what a Lens IS, mathematically. If we
can define it, we've discovered something more valuable than another
backend: a formal interface between the substrate and the
interpretation layer.

---

## 1. The Problem

Right now, "View" means "code that calls Write/Read/Reference." That's
an implementation description, not a mathematical definition. It's
like saying "a program is something that runs on a CPU" — true but
unhelpful.

The question: can we define View as an algebraic structure? Can we
specify the Lens interface so precisely that two independently-written
Views that satisfy the same specification are guaranteed to produce
the same observable behavior?

---

## 2. Proposed Definition

A **View** V is a 5-tuple:

```
V = (State, Encode, Decode, Commit, Resolve)
```

where:

### State
The View's internal state (in-memory, not kernel state). This is
what the Lens tracks between kernel calls — staging areas, caches,
schema metadata, pending writes, etc.

```
State = View-specific type
```

### Encode: ViewData → Bytes
Transforms Lens-level data (rows, vectors, files, graph nodes, etc.)
into bytes suitable for `kernel.Write`. The View chooses the format
(Parquet, JSON, raw floats, length-prefixed records, etc.).

```
Encode : ViewData → Bytes
```

This is a pure function. Same ViewData always produces the same Bytes.

### Decode: Bytes → ViewData
Inverse of Encode. Transforms bytes from `kernel.Read` back into
Lens-level data.

```
Decode : Bytes → ViewData
```

This is a pure function. `Decode(Encode(d)) = d` for all d.

### Commit: State × [ViewData] → State × Hash
Takes the current View state and a batch of pending ViewData (staged
changes), encodes each into bytes, calls `kernel.Write` for each,
builds a Tree (View-specific structure) referencing the written blobs,
creates a Commit blob (View-specific metadata), calls `kernel.Write`
for the Tree and Commit, calls `kernel.Reference` to update the name,
and returns the new State (with staging cleared).

```
Commit : State × List<ViewData> → State × Hash
```

This is NOT a pure function — it calls kernel mutations (Write, Reference).
However, it IS deterministic given the same kernel state and inputs
(Deterministic Apply invariant from the Formal Spec).

### Resolve: State × Query → ViewResult
Takes the current View state and a query (read by name, read by hash,
list, search, traverse, etc.), resolves the name/hash via the kernel,
reads the relevant blobs via `kernel.Read`, decodes them via Decode,
and returns Lens-level results.

```
Resolve : State × Query → ViewResult
```

This is a pure function of the kernel state and the query (no mutations).
`Resolve` may call `kernel.Read` but must NOT call `kernel.Write` or
`kernel.Reference`.

---

## 3. View Laws (proposed)

Just as the kernel has 5 storage laws and 7 composition laws, Views
should satisfy their own laws:

### View Law 1: Encode/Decode round-trip
```
∀ d ∈ ViewData: Decode(Encode(d)) = d
```
The View's serialization is lossless.

### View Law 2: Resolve is read-only
```
Resolve does not call Write or Reference.
```
Reading from a Lens does not mutate kernel state.

### View Law 3: Commit is deterministic
```
∀ S, batch: Commit(S, batch) produces the same hash
  given the same kernel state and inputs.
```
Two Commits with the same data on the same kernel state produce
the same commit hash (by content-addressing + deterministic encoding).

### View Law 4: Resolve after Commit
```
∀ S, batch, query:
  let (S', h) = Commit(S, batch)
  Resolve(S', query_for_committed_data) returns the committed data
```
After a Commit, the committed data is immediately visible to Resolve.

### View Law 5: Resolve is snapshot-consistent
```
∀ S, query:
  Resolve(S, query) returns data consistent with the kernel state
  at the time Resolve was called. Concurrent commits do not affect
  an in-progress Resolve (content-addressing guarantees this).
```

---

## 4. The View Interface (formalized)

```
interface View<VS, VD, QR, VR> {
    // VS = View State, VD = View Data, QR = Query, VR = View Result

    encode(data: VD) -> bytes
    decode(bytes: bytes) -> VD

    commit(state: VS, batch: List<VD>) -> (VS, Hash)
    resolve(state: VS, query: QR) -> VR
}
```

A View is any implementation of this interface that:
1. Uses ONLY `kernel.Write`, `kernel.Read`, `kernel.Reference` for storage
2. Satisfies the 5 View Laws
3. Does not call `kernel.Reference` inside `resolve` (View Law 2)

---

## 5. Verification: do existing Views satisfy this?

| View | State | Encode | Decode | Commit | Resolve | Satisfies? |
|---|---|---|---|---|---|---|
| SQLView | schema, pending batches | Arrow→Parquet bytes | Parquet→Arrow table | Write blob+tree+commit, Reference | Read by name, walk tree, concat Parquet | ✓ |
| VectorView | dim, pending vectors | struct.pack floats | struct.unpack floats | Write blob+tree+commit, Reference | Read by name, walk tree, linear search | ✓ |
| StreamView | pending records | length-prefix | parse length-prefix | Write blob+tree+commit, Reference | Read by name, walk tree, parse records | ✓ |
| GitView | staged files | raw file bytes | raw file bytes | Write blobs+tree+commit, Reference | Read by name (HEAD), read tree, read file | ✓ |
| GraphView | pending nodes/edges | JSON | JSON parse | Write blobs+adjacency+tree+commit, Reference | Read node/neighbors via tree+blob reads | ✓ |
| MLView | (none — direct) | raw weights / JSON meta | raw / JSON parse | Write weights+meta+tree+commit, Reference | Read weights/meta by (model, step) | ✓ |
| TimeSeriesView | (none — direct) | struct.pack ts+float | struct.unpack | Write blob+tree+commit, Reference | Read series, walk segments | ✓ |
| OCIView | (none — direct) | raw layer / JSON manifest | raw / JSON parse | Write layer+config+manifest+tree+commit, Reference | Pull manifest/layer by name | ✓ |

**All 8 Views satisfy the 5-tuple definition and the 5 View Laws.**

---

## 6. What this definition gives us

### 6.1. Formal View interface
Views can now be specified as an interface, not just "code that calls the kernel." This enables:
- Type-safe View implementations (the interface is checkable)
- Automated View testing (test harnesses that verify View Laws)
- View interop analysis (do two Views with the same Encode/Decode interoperate?)

### 6.2. View Laws as testable invariants
The 5 View Laws are checkable:
- Law 1 (round-trip): `assert decode(encode(d)) == d` for all d
- Law 2 (read-only): instrument the kernel; verify Resolve doesn't call Write/Reference
- Law 3 (deterministic): commit same data twice; verify same hash
- Law 4 (resolve-after-commit): commit data, then resolve; verify data returned
- Law 5 (snapshot-consistent): resolve while committing; verify no torn reads

### 6.3. View equivalence
Two Views V1 and V2 are **equivalent** if:
- They have the same Encode/Decode (same format)
- They have the same Commit structure (same Tree/Commit pattern)
- They have the same Resolve semantics (same query → same result)

This enables View interop: two Views with the same Encode/Decode can
read each other's objects.

### 6.4. View composition
If V1 and V2 are Views, can we compose them? E.g., a "VersionedVectorView"
that combines GitView's versioning with VectorView's embeddings?

Formally: V_compose = (V1.State × V2.State, V1.Encode ∘ V2.Encode, ...) ?

This is an open question. The 5-tuple definition makes it possible
to ask; the answer requires further research.

---

## 7. Open Questions

1. **Is the 5-tuple minimal?** Could Commit and Resolve be merged?
   (Probably not — Commit mutates, Resolve is read-only.)

2. **Is the 5-tuple complete?** Are there View operations that don't
   fit into Encode/Decode/Commit/Resolve? (E.g., GC, compaction,
   schema migration — are these part of the Lens or external?)

3. **Can Views compose?** Is there an algebra of View composition?

4. **Does every workload have a unique 5-tuple?** Or do some workloads
   have multiple valid View definitions?

5. **What's the relationship between View Laws and Kernel Laws?** Do
   View Laws follow from Kernel Laws, or are they independent?

These are research questions. The 5-tuple definition is a starting
point, not a final answer.
