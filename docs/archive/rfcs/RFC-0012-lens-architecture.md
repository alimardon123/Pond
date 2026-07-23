# RFC-0012: The Lens Architecture

## Status

**Accepted** — the defining architectural contribution of Pond.

Context-based interpretation is the chosen approach (see falsification
results in `experiments/resolver_comparison/falsification_context.py`).
The kernel stores pure bytes — no envelope, no codec registry in the
data path, no blob-level metadata. The interpretation layer lives in
CODE (the resolver), not in DATA (the blob).

TypedBlob (the envelope approach) is deprecated. It remains in
`pond-sdk/typed_blob.py` as an experimental artifact for reference
only. Do not use it in production.

---

## 1. The clarification

The kernel owns exactly three things:

```
Bytes      (immutable, content-addressed blobs)
History    (commit DAG, parent pointers)
Names      (mutable name → hash references)
```

Nothing else. No rows. No columns. No Arrow. No Parquet. No SQL.
No Feature Store. No Git. Just immutable bytes plus references.

Everything above that is **a way of interpreting those bytes**.
Not owning them. Not copying them. Not converting them. Just
interpreting.

This is like Linux: the filesystem stores bytes. It doesn't know
JPEG, MP4, ELF, Python, or SQLite. Applications interpret them.
Pond should feel the same.

---

## 2. The rename: "View" → "Lens"

"View" is a confusing term because it conflates with:
- SQL VIEW statements
- Materialized Views
- Database Views
- Virtual Tables

These are NOT what Pond means. Pond's "Views" are much more
fundamental: they are **interpretation layers** over immutable
bytes, not relational views.

### The new name: Lens

A **Lens** is an interpretation layer over immutable bytes — like
a lens that focuses light differently without changing the light
itself. The bytes don't change; only the way you observe and
manipulate them changes.

```
                Immutable Bytes
                      │
                Commit History
                      │
         ┌────────────┼────────────┐
         │            │            │
     SQL Lens     Git Lens    Feature Lens
         │            │            │
     DuckDB       Git CLI      ML Runtime
```

### Implementation

- `Lens` is an alias for `View` (backward compatible).
- `IndexedLens` = `IndexedView`
- `KeylessLens` = `KeylessView`
- `SemanticLens` = `SemanticView`
- All existing code continues to work. New code should use `Lens`.
- Future RFCs and documentation use "Lens" exclusively.

---

## 3. The open research question

> Can multiple independent domain lenses operate over the same
> immutable byte graph, without metadata duplication, without
> translation writes, while preserving their own semantics?

### The three options

**Option A: Each interpreter owns its own encoding.**

```
SQL encoder    Git encoder    Notebook encoder
```

Simple. But SQL cannot magically understand Git objects. Each lens
writes its own encoding; cross-lens reading only works if the
encodings happen to match.

**Option B: Canonical intermediate representation.**

Everything writes `Universal Object → bytes`. Everyone reads the
same object differently. Very elegant. Very difficult — you'd need
a universal format that every domain agrees on. This is basically
what Arrow tries to be, but Arrow is format-specific (columnar
tables). Git trees, notebook cells, and feature vectors don't
naturally fit a columnar table.

**Option C: Some interpreters intentionally overlap.**

```
Arrow interpreter → shared by DuckDB, Polars, DataFusion
Git interpreter   → used by Git CLI
Feature interpreter → used by ML runtime
```

Clusters of lenses that share an encoding. This is pragmatic —
lenses with similar domain needs (e.g., tabular data) share an
encoding; lenses with different needs (e.g., Git trees) use their
own.

### The answer: Option C with emergent overlap

Pond chooses **Option C**, but with a crucial twist: the overlap
is **not designed — it's emergent**.

The kernel does NOT enforce interpretability. Lenses choose their
encodings based on their domain needs. If two lenses happen to
need the same encoding (e.g., both use JSON), they get mutual
interpretability for free. If they don't, they don't — and that's
fine. They coexist on the same byte graph without interference.

This is exactly like Linux:
- `.py` files are readable by Python (Python chose to interpret
  bytes as Python source).
- `.jpg` files are readable by JPEG viewers (they chose to
  interpret bytes as JPEG).
- The filesystem doesn't enforce that a `.py` file is readable by
  Python. Python reads it because it chose to.
- A JPEG viewer can't read a `.py` file — but it doesn't crash, and
  the file is intact for Python.

### Concrete example

```
Shared byte graph (kernel):
  user:1  → bytes( '{"name":"Alice","age":30}' )   # JSON encoding
  tree:1  → bytes( '100644 blob abc123\tREADME.md\n...' )  # Git tree encoding
  cell:1  → bytes( '{"cell_type":"code","source":"print(1)"}' )  # Notebook encoding

SQL Lens:     reads user:1 as a row, can't read tree:1 or cell:1
Git Lens:     reads tree:1 as a tree, can't read user:1 or cell:1
Notebook Lens: reads cell:1 as a cell, can't read user:1 or tree:1

All three lenses share the same byte graph. No metadata. No
translation. No duplication. Each lens reads what it can; the rest
is opaque bytes.
```

### What this means for execution engines

DuckDB, Polars, DataFusion, Spark, Velox, GlareDB are NOT storage.
They are compute. They consume data. They shouldn't force Pond
into a storage format.

```
bytes
   ↓
lens (interpreter)
   ↓
execution engine
```

NOT:

```
bytes
↓
Arrow
↓
DuckDB
```

Arrow is ONE possible lens encoding — not the kernel. If a lens
chooses to encode data as Arrow IPC, then DuckDB/Polars/DataFusion
can read it natively (zero-copy). If a lens chooses JSON, those
engines can't read it directly — but the lens can provide a
translation layer (e.g., `lens.to_arrow()`).

---

## 4. What this is NOT

- **NOT Apache XTable.** XTable writes metadata for each format
  (Delta → Iceberg metadata → Hudi metadata → manifests). Pond
  writes ZERO extra metadata. The "enablement" is in the code
  (having a Lens instance with the right decoder), not in the data.

- **NOT Delta Uniform.** Delta Uniform writes sidecar metadata
  for each format. Pond writes no sidecars. The bytes are
  format-agnostic; the lens interprets them.

- **NOT a universal format.** Pond does not impose a canonical
  intermediate representation. Each lens chooses its encoding.
  Overlap is emergent, not designed.

- **NOT a translation layer.** Pond does not translate between
  formats. If you want to read JSON data as Arrow, you write a
  lens that decodes JSON and encodes Arrow. That's a lens-level
  concern, not a kernel concern.

---

## 5. The milestone question answered

> Can multiple independent domain lenses operate over the same
> immutable byte graph, without metadata duplication, without
> translation writes, while preserving their own semantics?

**Yes.** The proof is in `pond-sdk/test_lens_architecture.py`:

1. SQL Lens, Git Lens, and Notebook Lens all share the same byte
   graph (same View name → same Prolly tree).
2. Each lens writes its own encoding (JSON, Git tree format,
   notebook JSON).
3. No metadata is written for "enablement." The kernel stores only
   data blobs + the Prolly tree + commit blobs.
4. Each lens reads what it can. SQL Lens reads SQL-formatted blobs.
   Git Lens reads Git-formatted blobs. They can't read each other's
   blobs (different encodings) — but they coexist without
   interference.
5. Branching and history are shared (same commit DAG). If SQL Lens
   branches, Git Lens sees the branch.
6. All lenses see the same keys (same Prolly tree).

This is Pond's defining architectural contribution: **immutable
bytes and history are the only universal substrate, and every
higher-level capability is simply a different lens over that
substrate.**

---

## 6. Relationship to other RFCs

- **Depends on:** RFC-0003 (Kernel — the 3 primitives), RFC-0007
  (View Algebra — the laws lenses must satisfy).
- **Renames:** "View" → "Lens" throughout. RFC-0007's `V = (Σ, A,
  E, D, M)` becomes `L = (Σ, A, E, D, M)` — same algebra, better
  name.
- **Does not modify:** any kernel code, any existing lens code.
  The rename is via aliases (`Lens = View`). All existing code
  continues to work.
- **Closes:** the open research question about mutual
  interpretability (§3 above).

---

## 7. What this means for the roadmap

This clarification does NOT change the Phase F roadmap (evidence,
not features). It clarifies what the evidence is testing:

- **Scale:** can the byte graph handle 10M–100M blobs?
- **History:** can the commit DAG handle millions of commits?
- **Multiple lenses:** can SQL + Git + Notebook + FeatureStore
  lenses all operate on the same byte graph without interference?
- **Failure:** what happens when a lens writes a blob it can't
  later decode? (Answer: the blob is intact; other lenses that
  CAN decode it still work.)

The rename to "Lens" makes the architecture easier to explain to
new users, which directly supports the "independent implementations"
evidence gap (§5 of the Phase F roadmap).
