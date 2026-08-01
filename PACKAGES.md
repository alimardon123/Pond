# Pond Package Structure

Pond is organized into a clean layer hierarchy. Every package is
removable without breaking any lower layer (Design Goal 3.4 Scalable).

## Structure (current)

```
pond_repo/
│
├── README.md                    # 5-minute intro (start here)
├── DESIGN_GOALS.md              # 7 design principles + roadmap
├── REPO_ORGANIZATION.md         # Folder rules, naming, promotion process
├── PACKAGES.md                  # This file
├── SDK_SPEC.md                  # Authoritative SDK contract
├── KNOWLEDGE_GRAPH.md           # Navigational map of the repo
├── worklog.md                   # Append-only research log
│
├── pond-core/                   # Layer 0: Storage Kernel (FROZEN)
│   └── kernel.py                # 3 primitives: Write, Read, Ref (~199 LOC)
│
├── pond-sdk/                    # Layer 1: Storage infrastructure + extensions
│   ├── base_lens.py             # PondLens — shared namespace base (no format awareness)
│   ├── prolly_tree.py           # ProllyLensBase + ProllyTree (universal storage backend)
│   ├── collection_metadata.py   # CollectionMetadata — data-side metadata manager
│   ├── row_query.py             # LensQuery — lazy row-level query builder
│   ├── uuid7.py                 # UUIDv7 time-ordered UUID for _rowid
│   ├── binary_encoding.py       # Binary Prolly tree encoding
│   ├── maintenance.py           # Tombstone helpers (RFC-0008)
│   ├── collection.py            # Collection metadata (namespace, type, source)
│   └── extensions/              # Optional modules (data-side, not lens-side)
│       ├── indexing/            # Collection-level indexing
│       │   ├── base.py          # CollectionIndexerInterface (abstract)
│       │   ├── collection_index.py # CollectionIndexer (RECOMMENDED)
│       │   └── 
│       ├── semantic/            # Semantic model management
│       │   ├── base.py          # SemanticModelAdapter (abstract)
│       │   └── ossie.py         # SemanticMixin + OssieAdapter
│       └── physical_structures/ # Pruning, zone maps, bloom filters, stats
│           ├── base.py          # PhysicalStructure (abstract)
│           ├── bloom_filter.py
│           ├── statistics.py
│           ├── zone_map.py
│           ├── pruning.py       # ZoneMap, PruningPredicate, ColumnPredicate
│           ├── zone_map_index.py # ZoneMapIndex (ProllyTreeIndex of zone maps)
│           └── pruning_reader.py # PruningReader (generic pruning reader)
│
├── lenses/                      # Layer 2: Production Lens implementations
│   ├── keyvalue/                # KeyValueLens (KV storage over ProllyTreeIndex)
│   │   └── keyvalue_lens.py     # extends PondLens directly
│   ├── lakehouse/               # LakehouseLens (DuckDB + Parquet row groups)
│   │   └── lakehouse_lens.py    # extends PondLens directly
│   └── vector/                  # VectorLens (packed binary vector storage)
│       ├── vector_lens.py       # extends PondLens directly
│       └── test_vector.py       # Tests
│
├── services/                    # Cross-cutting services (on the kernel only)
│   ├── transport/               # Transport Layer (compression + encryption)
│   │   ├── transport.py         # Reference (zlib + XOR)
│   │   └── transport_production.py # Production (zstd + AES-GCM)
│   ├── schema/                  # Schema Registry (versioned schemas)
│   │   └── schema_registry.py
│   └── replication/             # Replication Coordinator
│       └── replication_coordinator.py # Primary-secondary + 2PC
│
├── pond-labs/                   # Development & experimental code (NOT production)
│   ├── lenses/                  # Lab lens prototypes
│   │   └── feature_store_lens.py # FeatureStoreLens (extends PondLens directly)
│   ├── tracks/                  # Lab tracks (compat, benchmarks, case studies)
│   ├── demos/                   # Demonstration scripts
│   │   └── interop_demo.py      # Feature Store ↔ Lakehouse interop
│   └── benchmarks/              # Performance benchmarks
│       ├── pruning_benchmark.py # Vortex-style pruning effectiveness
│       ├── overhead_audit.py    # Zone map overhead for all workloads
│       ├── sql_pushdown_benchmark.py # SQL pushdown end-to-end
│       └── loc_benchmark.py     # LOC saved vs from-scratch
│
├── tests/                       # All tests, organized by purpose
│   ├── test_all.py              # Single pytest entry point (24 tests)
│   ├── architecture/            # 18 architecture laws (executable spec)
│   ├── lens_algebra/            # RFC-0007 6-law property tests
│   └── integration/             # Integration tests (pruning, projection, etc.)
│
├── scripts/                     # Verification scripts
├── docs/                        # Documentation (whitepaper, formal algebras, RFCs)
├── tla/                         # TLA+ formal specification
└── archive/                     # Historical code (preserved, NOT active)
```

## Dependency rules

```
pond-core (FROZEN, ~199 LOC)
    ↓
pond-sdk (base_lens, prolly_tree, collection_metadata, extensions/)
    ↓
lenses/ (keyvalue, lakehouse, vector — each extends PondLens directly)
    ↓
pond-labs/ (experimental code, depends on everything)
```

**Rules:**
- No lens depends on another lens. All lenses extend PondLens directly.
- pond-sdk depends only on pond-core.
- Services depend only on pond-core (not on pond-sdk or lenses).
- Extensions are data-side (collection-level), not lens-side.
- pond-labs depends on everything (it's experimental).

## The weekly question

> If I deleted everything except `pond-core` and `pond-sdk`, would the
> architecture still make sense? (DESIGN_GOALS.md §4)

Yes. The kernel and SDK are self-contained. Every lens, service, and
lab code can be removed without affecting lower layers.
