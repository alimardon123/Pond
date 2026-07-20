# Pond Package Structure

Per RFC-0006 (Layered Architecture), the repository is organized into
packages matching the architectural layers.

This document also encodes the **removability discipline** (see
`DESIGN_GOALS.md` §3.4 Scalable): every package must be removable
without changing any lower layer. See §3 below.

## Structure

```
pond/
├── DESIGN_GOALS.md             # Six design principles + repo map (READ FIRST)
├── SDK_SPEC.md                 # Authoritative SDK contract (settles 10 validation ambiguities)
├── PACKAGES.md                 # This file
├── README.md
├── worklog.md                  # Append-only research log
│
├── pond-core/                  # Layer 0: Storage Calculus (FROZEN)
│   ├── pond_minimal.py         # 3 primitives: Write, Read, Reference (~140 LOC)
│   └── __init__.py
│
├── pond-sdk/                   # Layers 1+2: State + Access Calculus
│   ├── prolly_view.py          # Layer 1: ProllyViewBase (delta commits, trees, branching)
│   ├── binary_encoding.py      # Binary Prolly tree encoding (metadata optimization)
│   ├── auto_index.py           # Layer 2: IndexedView (auto-indexing, incremental)
│   ├── view_sdk.py             # View base class + CrossView + SemanticView + adapters
│   ├── maintenance.py          # Layer 0.5: tombstone helpers (RFC-0008) + compact_tombstones
│   ├── view_laws.py            # Property-test harness for RFC-0007's 6 View algebra laws
│   └── __init__.py
│
├── pond-sql/                   # Layer 3: Domain — SQL database
├── pond-streaming/             # Layer 3: Domain — Streaming
├── pond-git/                   # Layer 3: Domain — Version control
├── pond-notebook/              # Layer 3: Domain — Knowledge base
├── pond-feature-store/         # Layer 3: Domain — ML Feature Store
├── pond-semantic/              # Layer 3: Domain — Semantic models
├── pond-vector/                # Layer 3: Domain — Vector DB (external validation)
├── pond-arrow/                 # Layer 3: Domain — Arrow IPC adapter (Phase D compatibility)
│   ├── arrow_view.py           # ArrowView (View Algebra + Arrow ecosystem interop)
│   └── run_arrow_view_laws.py  # view_laws.py harness runner for ArrowView
│
├── rfcs/                       # Architecture specifications
│   ├── RFC-0001-what-is-a-view.md          # Draft (superseded by RFC-0007)
│   ├── RFC-0002-elegance-metrics.md
│   ├── RFC-0003-kernel-specification.md    # ACCEPTED (frozen)
│   ├── RFC-0004-view-composition.md
│   ├── RFC-0005-derived-structures.md      # Renamed to Materialization Calculus
│   ├── RFC-0006-layered-architecture.md
│   ├── RFC-0007-view-algebra.md            # ACCEPTED (verified by view_laws.py)
│   ├── RFC-0008-deletion-as-data.md        # Tombstones; no fourth primitive
│   ├── RFC-0009-architecture-metrics.md    # Measurable design metrics
│   └── RFC-0010-arrowview.md               # ACCEPTED (Phase D compatibility adapter)
│
├── docs/                       # Reference documents
│   ├── ... (FORMAL_SPEC, FORMAL_ALGEBRA, NON_GOALS, etc.)
│   └── LIQUID_CLUSTERING_COMPARISON.md  # Databricks Liquid Clustering vs Pond analysis
├── engineering/                # Engineering milestones (concurrency, GC, S3)
├── validation/                 # External validation (vector challenge + report)
├── destruction/                # Historical destruction-phase experiments
└── prototype/                  # Early experimental code (historical)
```

## Package Dependencies

```
pond-core        → (nothing — standalone, 3 primitives)
pond-sdk         → pond-core
pond-sql         → pond-sdk
pond-streaming   → pond-sdk
pond-git         → pond-sdk
pond-notebook    → pond-sdk
pond-feature-store → pond-sdk + pond-semantic (optional)
pond-semantic    → pond-sdk
pond-vector      → pond-sdk
```

No domain package depends on another domain package.
All domain packages depend only on pond-sdk.
pond-sdk depends only on pond-core.
pond-core depends on nothing.

This is a strict dependency DAG with no cycles.

---

## 3. The Removability Discipline

**Rule:** Every package must be removable without changing any lower
layer.

This is the operationalization of Design Goal 3.4 (Scalable) from
`DESIGN_GOALS.md`. It is also the metric C2 (Removability test
failures) from RFC-0009 — a hard constraint, target 0 failures.

### How to test

For each package P, the removability test is:

1. Move `pond-P/` out of the import path (or temporarily rename it).
2. Run the test suites of every package at a *lower* layer than P.
3. If any lower-layer test fails, the removability test fails —
   P has leaked a dependency upward.

### Concrete examples

| If we delete... | Lower-layer tests that must still pass |
|---|---|
| `pond-feature-store/` | `pond-sdk`, `pond-core` (also `pond-semantic` since FS optionally depends on it, but `pond-semantic`'s own tests must still pass independently) |
| `pond-semantic/` | `pond-sdk`, `pond-core` (and `pond-feature-store` if FS does not hard-depend on Semantic — see the `optional` annotation above) |
| `pond-vector/` | `pond-sdk`, `pond-core` |
| `pond-sql/` | `pond-sdk`, `pond-core` |
| `pond-sdk/` | `pond-core` only |
| `pond-core/` | (nothing — but deleting it deletes the project) |

### What this implies for new packages

A new package P may depend on lower layers (kernel, SDK) but must
not be depended on by them. If a feature in `pond-sdk` "needs" to
call into `pond-feature-store`, the feature belongs in
`pond-feature-store`, not in `pond-sdk`. Move it down, not up.

### What this implies for the kernel

The kernel (`pond-core`) is the most removable-of-all package in
principle (nothing depends up to it) and the least removable in
practice (everything depends down to it). This asymmetry is why the
kernel is FROZEN: any change to `pond-core` ripples to every other
package. The kernel changes only via an Accepted RFC that passes
the Admission Rule (see `rfcs/README.md`).

---

## 4. Adding a new package

Before adding a new `pond-X` package:

1. **Justify against the design goals** (`DESIGN_GOALS.md` §3).
   Which goal does it serve? Which does it potentially conflict with?
2. **Verify the removability test** will pass. Will deleting
   `pond-X` break any lower layer? If yes, do not add it; the
   dependency is wrong.
3. **Specify its View algebra** (RFC-0007). What is its
   `(Σ, A, E, D, M)` 5-tuple? Does it satisfy the six laws?
4. **Specify its materializations** (RFC-0005). What derived
   structures does it maintain? Are they all rebuildable from
   snapshots?
5. **Update this file** (`PACKAGES.md`) and `DESIGN_GOALS.md` §5.4
   if the package adds a new layer or responsibility.
6. **Add an entry to `worklog.md`** documenting the addition.

---

## 5. Removing a package

Removing a package is healthy when the package has been superseded
or its functionality has been absorbed into a lower layer. The
removability discipline makes this safe: if the package was
properly designed, removing it requires no changes to lower layers.

If removing a package *does* require lower-layer changes, that is a
bug — the package leaked a dependency upward. Fix the leak first,
then remove the package.
