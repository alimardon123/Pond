# Vector Database View — External Validation Report

## What was built

A `VectorView` class extending `IndexedView`, implementing:

| Method | Description |
|---|---|
| `insert(id, vector, metadata)` | Insert/replace a vector, auto-commits |
| `search(query, k=5)` | k-NN search via L2 (Euclidean) linear scan |
| `get(id)` | Retrieve a record by ID |
| `delete(id)` | Delete a vector, auto-commits |
| `list_vectors()` | List all vector IDs |
| `count()` | Number of vectors |
| `create_branch / checkout_branch / merge_branch` | Domain-friendly wrappers over `View` |
| `get_history(limit)` | Commit history |
| `find_by_id(id)` | Index-backed lookup via `find_by("by_id", id)` |

**Binary storage format** (struct-packed, not JSON):

```
[vec_len: u32 LE][float64 × vec_len][id_len: u32 LE][id bytes]
[meta_len: u32 LE][metadata JSON bytes]
```

**Test results:** all 22 checks passed against the scenario (insert 5, search
2, get, delete, branch/insert/merge, history).

---

## 1. Was the SDK sufficient? Could you implement without asking questions?

**Partially sufficient — but only because I was willing to guess.**

The *method signatures* in the spec are clean and self-consistent.  I could
write `VectorView` without asking questions because the surface API of
`IndexedView` (register_index, find_by, commit, get, etc.) is clear enough
to code against.

However, "sufficient" implies I could ship with confidence.  I could not.
To actually *test* my implementation I had to write three mock SDK modules
(`pond_minimal.py`, `auto_index.py`, `view_sdk.py`) — totalling ~250 lines —
because the spec does not describe internal behavior, only signatures.
If I had been handed a real SDK binary with no source, at least six of my
guesses could have been wrong and my code would have silently failed or
crashed at runtime.

**Verdict:** Implementable without questions only by making assumptions;
not safe to ship without verifying those assumptions.

---

## 2. Where was it ambiguous?

| # | Ambiguity | Impact |
|---|---|---|
| A | **How is a kernel obtained?** `PondMinimal` is imported but the spec never says whether `PondMinimal()` *is* the kernel or produces one via a factory method. | I guessed `PondMinimal()` returns a kernel. Could be wrong. |
| B | **Index extractor call signature.** `register_index(name, extractor, …)` — the spec says `Callable` but never says what arguments the extractor receives (data? key+data? key only?). | I guessed `extractor(decoded_data) -> index_key`. If the real SDK passes `(key, data)`, my extractor breaks. |
| C | **`get()` complexity.** The task demands "O(log N) lookups" via an index, but the spec never says `get(key)` is O(N). If `get` is already O(1), the index is redundant; if O(N), it's essential. | Couldn't determine if the index is necessary or ceremonial. |
| D | **Merge semantics.** `merge(name) -> str` — no description of conflict resolution, whether it's union / theirs-wins / ours-wins / 3-way. | I invented a union merge where the merged branch wins on conflict. |
| E | **Index persistence.** "Index operations are METADATA ONLY" — but where are indexes stored? As kernel objects? In-memory? How are they named? | I invented a naming convention `{view_name}:index:{index_name}` and stored indexes as kernel objects. |
| F | **`drop_index` / `unregister_index`.** The kernel has `reference` and `resolve` but **no delete-name primitive**. You can add a name but never remove one. | Impossible to truly drop an index; the name lingers forever. |
| G | **`diff(a, b)` parameters.** Are `a` and `b` commit hashes, branch names, or tags? | Guessed commit hashes. |
| H | **`history()` return shape.** Spec says `list[dict]` but not which keys. | Invented `{"hash", "message", "parent", "branch"}`. |
| I | **`put_raw` semantics.** Does it stage a `key -> blob_hash` mapping, or something else? | Guessed staging. |
| J | **Commit object format.** Not specified at all — how is a snapshot serialized? | Invented a JSON commit with `snapshot`, `parent`, `message`. |

---

## 3. What did you have to invent?

1. **Commit / snapshot representation.** The spec describes version control
   operations (`commit`, `undo`, `history`, `diff`) but never says what a
   commit *is*.  I invented: a commit is a kernel object containing
   `{snapshot: {key: blob_hash}, parent: commit_hash, message: str}`.

2. **Branch naming convention.** I chose `{branch_name}:head` as the kernel
   reference name pointing to the current commit of each branch.

3. **Index storage format.** I store each index as a kernel object:
   `{index_key: [data_key, …]}`.  The spec says updates are "incremental"
   but gives no data structure.

4. **Index reference naming.** `{view_name}:index:{index_name}`.

5. **Binary vector encoding.** The spec says "use struct.pack" but the
   exact format is up to the implementer.  I designed a length-prefixed
   binary layout (see top of report).

6. **Embedding the ID inside the blob.** Because the extractor (ambiguity B)
   likely receives only the decoded *value*, not the view key, I had to
   store the `id` redundantly inside the binary record so the `by_id` index
   extractor can retrieve it.  This wastes bytes but is the only way to
   satisfy "index by ID" given the guessed extractor contract.

7. **Merge strategy.** Union with the merged branch winning conflicts.

8. **Kernel instantiation pattern.** `kernel = PondMinimal()`.

9. **Eager-index rebuild hook.** The spec says eager mode "rebuilds on every
   commit" but doesn't say *how* the view hooks into commit.  I overrode
   `commit()` to rebuild eager indexes after calling `super().commit()`.

10. **Staleness computation.** Spec says `staleness_budget = commits before
    rebuild`, but not how to count.  I walk the parent chain from the
    current commit back to the index's last-built commit.

---

## 4. What was impossible or required guessing?

**Genuinely impossible from the spec alone:**

- **Verifying O(log N) index lookups.** The task requires O(log N) but the
  spec provides no index data-structure details.  My mock uses a Python
  dict (O(1) average), so I *cannot* confirm the real SDK achieves O(log N).
  This is a claim I can only repeat, not verify.

- **Truly dropping an index.** The kernel has `reference(name, hash)` and
  `resolve(name)` but **no `delete_name` or `unreference`**.  Once a name
  is created, it exists forever.  `drop_index` and `unregister_index` are
  therefore unimplementable at the kernel level — they can only stop
  *tracking* the index in the view, not remove its metadata from the store.

- **Knowing whether `get(key)` already provides O(1)/O(log N) access.**
  If it does, the entire "index by ID" requirement is redundant.  If it
  doesn't, the index is essential.  The spec is silent, so I implemented
  the index *and* used `get()` — covering both cases defensively.

**Required heavy guessing:**

- Extractor call convention (data-only vs. key+data).
- Kernel instantiation (`PondMinimal()` vs. factory).
- Merge conflict resolution.
- Internal commit/snapshot/index serialization formats.
- `history()` and `diff()` return shapes.

---

## 5. Developer experience rating: **5 / 10**

**What's good (the 5 points):**

- The **conceptual model is elegant**: content-addressed blobs + mutable
  names + versioned views + auto-indexing is a clean, composable design.
- **Method signatures are well-organized** and grouped logically (write,
  read, version-control, indexing, serialization).  Reading the spec, I
  immediately knew *what operations exist*.
- **The layered architecture** (Kernel → View → IndexedView → CrossView)
  gives a clear mental model of responsibilities.
- **Override points are explicit** (`encode`/`decode`), making custom
  serialization straightforward.
- **Dedup and immutability** are stated as laws, which simplifies
  reasoning about the system.

**What's missing (the 5 points deducted):**

- **−1.5: No type signatures for callbacks.** `extractor: Callable` appears
  three times but the argument list is never specified.  This is the single
  most damaging gap — it determines the entire indexing contract.
- **−1.0: No internal representation documented.** Commits, snapshots,
  branches, and indexes are all described operationally but never
  structurally.  An implementer must reverse-engineer or invent the storage
  format.
- **−1.0: Missing kernel lifecycle.** How to *get* a kernel is unstated.
  This is the first line of every program and it's a guess.
- **−0.5: No complexity guarantees.** "O(log N)" is demanded by the task
  but the spec never states the complexity of `get`, `find_by`, or index
  operations.  Without this, I can't know if my implementation meets
  performance requirements.
- **−0.5: Incomplete mutation API.** The kernel can *create* names but not
  *delete* them, making `drop_index` impossible.  The spec doesn't
  acknowledge this gap.
- **−0.5: Merge is a black box.** "merge branch" with no conflict model is
  unusable for real software.

**Bottom line:** The spec reads like an excellent *architecture overview*
but an incomplete *implementation contract*.  A developer can sketch code
from it, but cannot ship without either reading the source or asking 6–10
clarifying questions.  For a "frozen kernel" with a public SDK, I'd expect
the callback signatures, kernel instantiation, and at least one fully
worked example to be specified.  Bumping the rating to 8/10 would require:
documenting the extractor signature, showing how to obtain a kernel,
specifying merge semantics, and adding a `delete_name` kernel primitive (or
documenting why it's intentionally absent).

---

## Files produced

| File | Purpose |
|---|---|
| `pond-vector/vector_view.py` | The VectorView implementation (spec-only) |
| `pond-vector/pond_minimal.py` | Mock kernel (for testing) |
| `pond-vector/auto_index.py` | Mock View + IndexedView (for testing) |
| `pond-vector/view_sdk.py` | Mock CrossView (for testing) |
| `pond-vector/test_vector.py` | Test harness — all 22 checks pass |
| `validation/vector_report.md` | This report |

## Test output summary

```
  ALL CHECKS PASSED  (22/22 checks)
  - 5 vectors inserted, binary encoding verified (not JSON)
  - k-NN search returns correct nearest 2 with correct L2 distances
  - get by ID works via both View.get and IndexedView.find_by
  - delete removes vector and it disappears from search
  - branch → insert → checkout back → merge brings new vector in
  - history shows 7 commits with insert/delete/merge messages
```
