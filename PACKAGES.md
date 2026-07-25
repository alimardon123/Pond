# Pond Package Structure

Pond is organized into a clean layer hierarchy. Every package is
removable without breaking any lower layer (Design Goal 3.4 Scalable).

## Structure (current)

```
pond_repo/
│
├── README.md                    # 5-minute intro (start here)
├── DESIGN_GOALS.md              # 7 design principles + roadmap
├── PACKAGES.md                  # This file
├── SDK_SPEC.md                  # Authoritative SDK contract
├── POND.md                      # One-page "What is Pond?"
├── worklog.md                   # Append-only research log
│
├── pond-core/                   # Layer 0: Storage Kernel (FROZEN)
│   └── pond_minimal.py          # 3 primitives: Write, Read, Ref (~140 LOC)
│
├── pond-sdk/                    # Layers 1+2: Lens SDK + Physical Structures
│   ├── pond_lens.py             # PondLens — shared namespace base (no format awareness)
│   ├── keyvalue_lens.py         # KeyValueLens — app-facing KV lens (ProllyTreeIndex backing)
│   ├── lens_sdk.py              # Backward-compat shim (re-exports keyvalue_lens)
│   ├── lens_query.py            # Lazy query API (.where/.select/.join)
│   ├── prolly_view.py           # ProllyLensBase (tiered commits, ProllyTreeIndex)
│   ├── binary_encoding.py       # Binary Prolly tree encoding
│   ├── collection.py            # Collection (reference namespace)
│   ├── auto_index.py            # Auto-indexing (Physical Structure)
│   ├── maintenance.py           # Tombstone helpers (RFC-0008)
│   ├── architecture_laws.py     # 12 executable architecture laws
│   ├── lens_laws.py             # RFC-0007 Lens algebra property tests
│   ├── run_lens_laws_ci.py      # CI runner for Lens contracts
│   └── test_*.py                # Tests
│
├── lenses/                      # Layer 3: Lens implementations
│   ├── lakehouse/               # DuckDB Lakehouse (flagship)
│   │   └── lakehouse.py         # PondLakehouse + LakehouseLens
│   └── vector/                  # Vector DB Lens
│       ├── vector_view.py       # VectorView (ANN search)
│       ├── auto_index.py        # Mock auto-index for testing
│       ├── mock_kernel.py       # In-memory mock kernel for tests
│       ├── lens_sdk.py          # Mock CrossView helpers
│       └── test_vector.py       # Tests
│
├── services/                    # Cross-cutting services (on the kernel)
│   ├── transport/               # Transport Layer (§17 algebra)
│   │   ├── transport.py         # Reference (zlib + XOR)
│   │   └── transport_production.py  # Production (zstd + AES-GCM)
│   ├── schema/                  # Schema Registry (§18 algebra)
│   │   └── schema_registry.py   # Versioned schemas, backward/forward compat
│   └── replication/             # Replication Coordinator (§16 algebra)
│       └── replication_coordinator.py  # Primary-secondary + 2PC
│
├── pond-labs/                   # Experiments and demos
│   ├── feature_store_lens.py    # Feature Store Lens (point-in-time joins)
│   ├── interop_demo.py          # Bidirectional Lens interop (killer demo)
│   └── loc_benchmark.py         # LOC saved: 81% reduction vs from-scratch
│
├── docs/                        # Documentation
│   ├── README.md                # Doc index (start here)
│   ├── POND_WHITEPAPER.md       # The contribution (20 pages)
│   ├── WHERE_POND_FAILS.md      # Honest scope + Lens roadmap
│   ├── POND_FORMAL_ALGEBRAS.md  # 17 algebras, 10 axioms
│   ├── POND_PHASE_Q_BENCHMARKS.md
│   ├── POND_PHASE_Q_REVIEW_PACKET.md
│   ├── LENS_AUTHORS_GUIDE.md
│   ├── LENS_INTERPRETATION_CONTRACT.md
│   ├── LENS_INTEROP_SPEC.md
│   ├── GETTING_STARTED.md
│   ├── NON_GOALS.md
│   ├── POSTMORTEM_PROLLY_TREE_BUG.md
│   ├── DELETE_90_PERCENT.md
│   └── archive/                 # Historical docs (15 files)
│       └── rfcs/                # 13 RFCs
│
├── scripts/                     # Test and benchmark scripts
│   ├── phase_l_*.py             # Verification (491 property tests, Git diffs)
│   ├── phase_n_*.py             # Proofs + untested laws (23 tests)
│   ├── phase_o_*.py             # Remaining laws + hazards (61 tests)
│   ├── phase_p_real_differentials.py  # Real Dolt + Iceberg diffs
│   └── phase_q_benchmarks.py    # Head-to-head vs Git/Dolt/Iceberg
│
├── tla/                         # TLA+ formal specification
│   ├── PondKernel.tla           # 6 invariants, 56 reachable states
│   ├── PondKernel.cfg
│   └── README.md
│
└── archive/                     # Historical code (preserved for reference)
    ├── prototype/               # Early experimental code
    ├── libraries/               # Older SDK versions
    ├── applications/            # Older Lens implementations
    ├── engineering/             # Engineering experiments
    ├── destruction/             # Adversarial destruction tests
    ├── experiments/             # Older performance benchmarks
    ├── validation/              # External validation reports
    ├── pond-semantic/           # Stub (3 lines, never implemented)
    ├── pond-git/                # Broken imports (references archive/prototype)
    ├── pond-notebook/           # Broken imports (references archive/prototype)
    ├── pond-sql/                # Broken imports (references archive/prototype)
    ├── pond-streaming/          # Broken imports (references archive/prototype)
    ├── pond-arrow/              # Broken imports (references archive/prototype)
    ├── pond-feature-store/      # Older feature store (superseded by pond-labs/)
    └── pond_rfc1.py             # RFC-0001 PDF generator (1967 lines, archived)
```

## Package Dependencies

```
pond-core              → (nothing — standalone, 3 primitives)
pond-sdk               → pond-core
lenses/lakehouse       → pond-core + duckdb + pyarrow
lenses/vector          → pond-sdk (uses mock_kernel for testing)
services/transport     → pond-core
services/schema        → pond-core
services/replication   → pond-core
pond-labs              → pond-core + lenses/lakehouse + (pyarrow, duckdb)
```

No Lens depends on another Lens. All Lenses depend only on
`pond-sdk`. `pond-sdk` depends only on `pond-core`. `pond-core`
depends on nothing. This is a strict dependency DAG with no cycles.

## The Removability Discipline

**Rule:** Every package must be removable without changing any lower
layer. (Design Goal 3.4 Scalable.)

### How to test

For each package P:
1. Move the package out of the import path.
2. Run the test suites of every package at a *lower* layer than P.
3. If any lower-layer test fails, P has leaked a dependency upward.

## Adding a new package

Before adding a new package:

1. **Justify against the 7 design goals** (`DESIGN_GOALS.md` §3).
2. **Verify the removability test** will pass.
3. **Specify its Lens algebra** (RFC-0007). What is its 5-tuple?
4. **Specify its Physical Structures** (RFC-0005).
5. **Update this file** and `DESIGN_GOALS.md`.
6. **Add an entry to `worklog.md`**.

## Archive policy

The `archive/` directory contains historical code and docs preserved
for reference. Nothing in `archive/` is needed to use or understand
Pond. It exists so that the project's evolution is traceable.

Several packages were archived during the reorganization because
they had broken imports (referencing `prototype/` and `libraries/`
which were themselves archived) or were superseded by newer
implementations in `pond-labs/`. These include: pond-semantic,
pond-git, pond-notebook, pond-sql, pond-streaming, pond-arrow,
pond-feature-store.
