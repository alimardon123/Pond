# RFC-0006: Layered Architecture

## Status

Draft — formalizing the layer structure discovered through recursive
View composition.

## Abstract

Pond's architecture is not flat. It has distinct layers, each adding
exactly one capability. This RFC formalizes those layers and their
boundaries.

---

## 1. The Layers

```
Layer 0: Storage Calculus
  Write(bytes) → hash
  Read(hash|name) → bytes
  Reference(name, hash)
  Laws: immutability, addressability, name-mutability, references-don't-mutate, backend-independence

Layer 1: State Calculus
  ProllyViewBase: key→blob_hash with versioning
  Adds: delta commits, Prolly trees, snapshots, branching, history, undo, merge
  Uses ONLY: Layer 0 primitives
  Does NOT know: what blobs contain, what keys mean

Layer 2: Access Calculus
  IndexedView / DerivedStructure: derived state for fast access
  Adds: auto-indexing (lazy/eager/incremental), secondary indexes, bloom filters, statistics
  Uses ONLY: Layer 0 + Layer 1
  Does NOT know: what data represents (rows, files, events, vectors)

Layer 3: Domain Calculus
  SQLView, GitView, StreamingView, FeatureStore, NotebookView, SemanticView
  Adds: domain-specific semantics (queries, version control, logs, features, pages, metrics)
  Uses ONLY: Layer 0 + Layer 1 + Layer 2
  Does NOT know: storage backend, Prolly tree internals, index trigger policy
```

---

## 2. Layer Laws

### Layer Law 1: Downward-only dependency
Each layer may use ONLY the layers below it. No upward dependency.
Layer 1 uses Layer 0. Layer 2 uses Layers 0+1. Layer 3 uses Layers 0+1+2.
No layer may skip (Layer 3 cannot bypass Layer 1 and call the kernel directly
for commit/tree operations — it goes through ProllyViewBase).

### Layer Law 2: Single capability per layer
Each layer adds exactly ONE new capability:
- Layer 0: existence (store and retrieve bytes by hash/name)
- Layer 1: versioning (commit history, branching, snapshots)
- Layer 2: optimization (derived structures for fast access)
- Layer 3: meaning (domain-specific interpretation of bytes)

### Layer Law 3: Layer opacity
A layer does not know what the layers above it do. Layer 0 doesn't know
about Prolly trees. Layer 1 doesn't know about indexes. Layer 2 doesn't
know about SQL or features.

### Layer Law 4: Layer substitutability
Any layer can be replaced with an equivalent implementation:
- Layer 0: filesystem → S3 → FDB (already proven)
- Layer 1: ProllyViewBase → ShardedViewBase → future CoW-ProllyViewBase
- Layer 2: IndexedView → future LearnedIndexView
- Layer 3: SQLView → future ArrowSQLView

### Layer Law 5: Composition at Layer 3
Layer 3 Views can compose with each other (RFC-0004). This is the ONLY
layer where cross-component composition happens. Lower layers don't
compose — they're inherited.

---

## 3. What Each Layer Does NOT Do

| Layer | Does NOT do |
|---|---|
| 0 | No structure, no versioning, no indexing, no domain logic |
| 1 | No indexing, no statistics, no domain logic, no format awareness |
| 2 | No SQL, no streaming, no features, no domain semantics |
| 3 | No kernel access bypass, no Prolly tree manipulation, no index policy |

---

## 4. The Package Structure (Phase 3: Modularize)

```
pond-core/           Layer 0: pond_minimal.py (3 primitives, ~140 LOC)
pond-sdk/            Layer 1+2: prolly_view.py, binary_encoding.py, auto_index.py, lens_sdk.py
pond-semantic/       Layer 3: semantic adapters (Ossie, Cube, dbt)
pond-feature-store/  Layer 3: feature_store.py
pond-sql/            Layer 3: sql_view.py
pond-streaming/      Layer 3: streaming_view.py
pond-git/            Layer 3: pond_git.py
pond-notebook/       Layer 3: notebook.py
```

The core repository (`pond-core` + `pond-sdk`) stays remarkably small.
Domain packages are independent and can be developed/released separately.

---

## 5. Admission Rule for Each Layer

A concept enters a layer ONLY if it cannot live in a higher layer:

- **Layer 0**: must be universal, impossible outside kernel, immutable,
  storage-independent, decades-stable (existing 5-criterion rule)
- **Layer 1**: must be about state management (versioning, branching, history)
  — if it's about optimization, it goes in Layer 2
- **Layer 2**: must be a derived structure (f(snapshot)) — if it's about
  domain semantics, it goes in Layer 3
- **Layer 3**: anything domain-specific is allowed (SQL, Git, streaming, etc.)

This prevents layer leakage. An index is Layer 2 (derived structure),
NOT Layer 1 (state management). A SQL parser is Layer 3 (domain), NOT
Layer 2 (optimization).
