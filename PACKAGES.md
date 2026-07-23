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
│   ├── lens_sdk.py              # Lens base class, CrossLens, SemanticLens
│   ├── lens_query.py            # Lazy query API (.where/.select/.join)
│   ├── prolly_view.py           # ProllyViewBase (tiered commits, trees)
│   ├── binary_encoding.py       # Binary Prolly tree encoding
│   ├── collection.py            # Collection (reference namespace)
│   ├── auto_index.py            # Auto-indexing (Physical Structure)
│   ├── maintenance.py           # Tombstone helpers (RFC-0008)
│   ├── architecture_laws.py     # 12 executable architecture laws
│   ├── lens_laws.py             # RFC-0007 Lens algebra property tests
│   ├── run_lens_laws_ci.py      # CI runner for Lens contracts
│   └── test_*.py                # Tests
│
├── pond-arrow/                  # Layer 3: Arrow IPC Lens
├── pond-feature-store/          # Layer 3: ML Feature Store (older)
├── pond-git/                    # Layer 3: Git Lens
├── pond-lakehouse/              # Layer 3: DuckDB Lakehouse (flagship)
├── pond-notebook/               # Layer 3: Notebook Lens
├── pond-semantic/               # Layer 3: Semantic Models Lens
├── pond-sql/                    # Layer 3: SQL Lens
├── pond-streaming/              # Layer 3: Streaming Lens
├── pond-vector/                 # Layer 3: Vector DB Lens
│
├── pond-schema/                 # Cross-cutting: Schema Registry (§18 algebra)
├── pond-replication/            # Cross-cutting: Replication Coordinator (§16)
├── pond-transport/              # Cross-cutting: Transport Layer (§17)
│   ├── transport.py             # Reference (zlib + XOR)
│   └── transport_production.py  # Production (zstd + AES-GCM)
│
├── pond-labs/                   # Modern experiments and demos
│   ├── feature_store_lens.py    # Newer Feature Store Lens (point-in-time join)
│   ├── interop_demo.py          # Bidirectional Lens interop (the killer demo)
│   └── loc_benchmark.py         # LOC saved: 81% reduction vs from-scratch
│
├── docs/                        # Documentation
│   ├── README.md                # Doc index (start here)
│   ├── POND_WHITEPAPER.md       # The contribution (20 pages)
│   ├── WHERE_POND_FAILS.md      # Honest scope
│   ├── POND_FORMAL_ALGEBRAS.md  # 17 algebras, 10 axioms
│   ├── POND_PHASE_Q_BENCHMARKS.md
│   ├── POND_PHASE_Q_REVIEW_PACKET.md
│   ├── LENS_AUTHORS_GUIDE.md
│   ├── LNS_INTERPRETATION_CONTRACT.md
│   ├── LENS_INTEROP_SPEC.md
│   ├── GETTING_STARTED.md
│   ├── NON_GOALS.md
│   ├── POSTMORTEM_PROLLY_TREE_BUG.md
│   ├── DELETE_90_PERCENT.md
│   └── archive/                 # Historical docs (15 files)
│       └── rfcs/                # 13 RFCs
│
├── scripts/                     # Test and benchmark scripts
│   ├── phase_l_*.py             # Phase L: verification (491 tests)
│   ├── phase_n_*.py             # Phase N: proofs + untested laws
│   ├── phase_o_*.py             # Phase O: remaining laws + hazards
│   ├── phase_p_real_differentials.py
│   ├── phase_q_benchmarks.py    # Phase Q: head-to-head vs Git/Dolt/Iceberg
│   └── pond_rfc1.py             # RFC-0001 reference
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
    └── validation/              # External validation reports
```

## Package Dependencies

```
pond-core          → (nothing — standalone, 3 primitives)
pond-sdk           → pond-core
pond-arrow         → pond-sdk
pond-feature-store → pond-sdk
pond-git           → pond-sdk
pond-lakehouse     → pond-sdk + duckdb + pyarrow
pond-notebook      → pond-sdk
pond-semantic      → pond-sdk
pond-sql           → pond-sdk
pond-streaming     → pond-sdk
pond-vector        → pond-sdk
pond-schema        → pond-core
pond-replication   → pond-core
pond-transport     → pond-core
pond-labs          → pond-core + pond-lakehouse + (pyarrow, duckdb)
```

No Lens package depends on another Lens. All Lenses depend only on
`pond-sdk`. `pond-sdk` depends only on `pond-core`. `pond-core`
depends on nothing. This is a strict dependency DAG with no cycles.

## The Removability Discipline

**Rule:** Every package must be removable without changing any lower
layer. (Design Goal 3.4 Scalable.)

### How to test

For each package P:
1. Move `pond-P/` out of the import path.
2. Run the test suites of every package at a *lower* layer than P.
3. If any lower-layer test fails, P has leaked a dependency upward.

### Concrete examples

| If we delete... | Lower-layer tests that must still pass |
|---|---|
| `pond-lakehouse/` | `pond-sdk`, `pond-core` |
| `pond-feature-store/` | `pond-sdk`, `pond-core` |
| `pond-vector/` | `pond-sdk`, `pond-core` |
| `pond-sdk/` | `pond-core` only |
| `pond-core/` | (nothing — but deleting it deletes the project) |

## Adding a new package

Before adding a new `pond-X` package:

1. **Justify against the 7 design goals** (`DESIGN_GOALS.md` §3).
   Which goal does it serve? Which does it potentially conflict with?
2. **Verify the removability test** will pass.
3. **Specify its Lens algebra** (RFC-0007). What is its 5-tuple?
4. **Specify its Physical Structures** (RFC-0005). What derived
   structures does it maintain? Are they all rebuildable?
5. **Update this file** and `DESIGN_GOALS.md` if the package adds
   a new layer or responsibility.
6. **Add an entry to `worklog.md`** documenting the addition.

## Removing a package

Removing a package is healthy when it has been superseded or its
functionality has been absorbed into a lower layer. The removability
discipline makes this safe: if the package was properly designed,
removing it requires no changes to lower layers.

If removing a package *does* require lower-layer changes, that is a
bug — the package leaked a dependency upward. Fix the leak first,
then remove the package.

## Archive policy

The `archive/` directory contains historical code and docs preserved
for reference. Nothing in `archive/` is needed to use or understand
Pond. It exists so that the project's evolution is traceable.

If you are looking for:
- Early prototypes → `archive/prototype/`
- Older SDK versions → `archive/libraries/`
- Older Lens implementations → `archive/applications/`
- Adversarial destruction tests → `archive/destruction/`
- External validation reports → `archive/validation/`
- RFCs → `docs/archive/rfcs/`

If you can't find something, it's probably in `archive/`.
