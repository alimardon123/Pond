# Pond Knowledge Graph

> **The navigational map of the entire repository.** Every active
> file, every concept, every relationship. **Maintain this file
> whenever the repo changes.**
>
> **Purpose:** Any agent (human or AI) can read this file and have
> complete knowledge of what's in the repo, where it lives, and how
> it connects. Never let this file go stale.
>
> **Maintenance protocol:** See §6. Update this file on every commit
> that adds, removes, moves, or renames a file.

---

## 0. How to use this file

1. **New to Pond?** Read §1 (architecture overview) and §2 (file map).
2. **Looking for a specific file?** Use §2 (file map) or §3 (concept map).
3. **Want to understand relationships?** Read §4 (dependency graph).
4. **Writing a new Lens?** Read §5 (Lens roadmap) and `docs/LENS_GUIDE.md`.
5. **Maintaining this file?** Read §6 (maintenance protocol).

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│ Applications (SQL, Git, Feature Store, Notebook, Lakehouse)     │
├─────────────────────────────────────────────────────────────────┤
│ Lenses (encode/decode; interpretation layer)                    │
│  • lenses/lakehouse/  • lenses/vector/                          │
│  • archive/pond-sql/  • archive/pond-git/  (reference impls)    │
├─────────────────────────────────────────────────────────────────┤
│ Services (cross-cutting; between kernel and lenses)             │
│  • services/transport/  • services/schema/  • services/replication/ │
├─────────────────────────────────────────────────────────────────┤
│ SDK (PondLens base, KeyValueLens, ProllyTreeIndex, indexes, query) │
│  • pond-sdk/                                                    │
├─────────────────────────────────────────────────────────────────┤
│ Kernel (FROZEN; 3 operations; ~140 LOC)                         │
│  • pond-core/kernel.py                                    │
├─────────────────────────────────────────────────────────────────┤
│ Backend (local disk, S3, IPFS, FDB — pluggable)                 │
└─────────────────────────────────────────────────────────────────┘
```

**The 7 design principles** (see `DESIGN_GOALS.md` §3):
Simple, Powerful, Performant, Scalable, Efficient, Beautiful, Functional.

**The 3 kernel operations**: `Write(bytes)→hash`, `Read(hash)→bytes`,
`Ref(name,hash)→()`.

**The 6 substrates**: Bytes, Names, Time, Coordination, Range-Read, Key.

**The 17 formal algebras** (see `docs/POND_FORMAL_ALGEBRAS.md`):
Reference, Merge, GC, RTT, OSN, Physical Structure, Workspace, History,
Substrate, Manifest, Range Read, State-vs-Bytes, GC-with-Packs, PS Dep
Graph, Concurrency, Replication, Transport, Schema Evolution.

---

## 2. File Map (every active file)

### 2.1 pond-core/ (Kernel — FROZEN)

| File | LOC | Purpose |
|---|---|---|
| `pond-core/kernel.py` | 199 | The kernel. `PondMinimal` class with `write()`, `read()`, `reference()`, `resolve()`, `list_names()`. SQLite-backed root namespace. |
| `pond-core/__init__.py` | 0 | Package marker. |
| `pond-core/README.md` | 43 | Folder purpose and usage. |

### 2.2 pond-sdk/ (Lens SDK — 14 files, ~7300 LOC)

| File | LOC | Exports | Purpose |
|---|---|---|---|
| `pond-sdk/base_lens.py` | 248 | `PondLens` | **Shared namespace base for ALL Lenses.** Provides only ref-namespace operations (branch, list_collections, set_definition, get_definition, history). No format awareness — each app-facing lens owns its own read/write API. |
| `pond-sdk/prolly_tree.py` | 764 | `ProllyTree`, `ProllyLensBase` | ProllyTreeIndex storage + tiered commits (delta + snapshot) + branching + merge + history. The universal storage backend for all collections. |
| `pond-sdk/collection.py` | 517 | `Collection` | Named collection with namespace, type, source metadata. |
| `tests/lens_algebra/lens_laws.py` | 591 | (test harness) | RFC-0007 Lens algebra property tests (6 laws). |
| `tests/architecture/architecture_laws.py` | 557 | (12 laws) | Executable architecture laws (Identity, Reachability, History, Lens, Derived, Branch, Merge, Determinism, Scale, Index). |
| `pond-sdk/binary_encoding.py` | 323 | `BinaryProllyTree` | Binary Prolly tree encoding (metadata optimization). |
| `tests/integration/test_shared_lenses.py` | 442 | (tests) | Test: multiple KeyValueLens subclasses sharing same byte graph. |
| `tests/integration/test_lens_architecture.py` | 449 | (tests) | Test: multi-Lens architecture proof (SQL/Git/Notebook lenses over same byte graph). |
| `pond-sdk/row_query.py` | 288 | `LensQuery`, `JoinedQuery` | Lazy query API: `.where()`, `.select()`, `.map()`, `.join()`, `.collect()`. |
| `tests/integration/test_lens_query.py` | 327 | (tests) | Test: LensQuery. |
| `tests/integration/test_pruning.py` | 320 | (tests) | Test: Vortex-style pruning. Zone-map-based pruning works for JSON, Parquet, and custom formats. |
| `tests/integration/test_lakehouse_pruning.py` | 130 | (tests) | Test: End-to-end pruning with LakehouseLens. Zone maps auto-built at write time, read_with_pruning skips row groups. |
| `tests/integration/test_kv_pruning_and_projection.py` | 130 | (tests) | Test: KV pruning + Lakehouse projection pushdown. Zone maps for KV, column-level access for Parquet. |
| `tests/integration/test_collection_metadata.py` | 120 | (tests) | Test: Collection integration — unified namespace + labels + zone maps + indexes + pruning + compaction. |
| `tests/integration/test_index_modes.py` | 220 | (tests) | Test: EAGER/LAZY index modes + O(changed) incremental refresh via commit-diff + is_index_stale. |
| `pond-sdk/maintenance.py` | 315 | `drop_name`, `is_dropped`, `resolve_active`, `compact_tombstones` | Tombstone helpers (RFC-0008: deletion as data). |
| `pond-sdk/collection_metadata.py` | 343 | `CollectionMetadata` | Data-side metadata manager. Manages zone maps, indexes, and (future) bloom filters for collections. Lens-agnostic — works through callbacks. |
| `pond-sdk/best_effort.py` | 95 | `best_effort, warn_best_effort` | Tiny helper for best-effort operations. Catches specific recoverable exceptions (AttributeError, KeyError, TypeError, ValueError, ImportError, ArithmeticError) and logs them via the `pond.best_effort` logger. Replaces the `except Exception: pass` anti-pattern. Enable with `POND_DEBUG=1`. |
| `pond-sdk/pond_config.py` | 195 | `PondConfig` | Persistent pruning + encoding settings via `.pond/config` JSON file. Configures pruning (auto/true/false + force), encoding (auto-select or default), chunk_size, row_group_size, bitpack_max_bitwidth. `should_prune()` decides based on storage type. `load_for_kernel()` finds config in base_dir. |
| `tests/integration/test_pond_config.py` | 130 | (test) | Tests PondConfig: defaults, save/load round-trip, should_prune (auto/true/false/force), encoding hints, validation, load_for_kernel. |
| `pond-sdk/uuid7.py` | 180 | `uuidv7`, `uuidv7_monotonic`, `uuidv7_timestamp` | UUIDv7 time-ordered UUID generation for distributed row identification (_rowid). |
| `tests/lens_algebra/run_lens_laws_ci.py` | 267 | (CI runner) | CI runner for Lens contracts. |
| `pond-sdk/__init__.py` | 0 | Package marker. |
| `pond-sdk/README.md` | 52 | Folder purpose and usage. |
| `pond-sdk/extensions/__init__.py` | 55 | `register_extension`, `list_extensions` | Extension registry. |
| `pond-sdk/extensions/indexing/__init__.py` | 27 | `CollectionIndexer`, `AutoIndexMixin`, `AutoIndex` | Indexing extension package. Collection-level indexing + legacy lens-mixin approach. |
| `pond-sdk/extensions/indexing/collection_index.py` | 200 | `CollectionIndexer` | Collection-level indexer. Operates on kernel + collection name. Any lens can use it. Indexes belong to collections (data-side), not lenses. |
| `pond-sdk/extensions/indexing/base.py` | 80 | `CollectionIndexerInterface` | Abstract interface for collection-level indexers. |
| `pond-sdk/extensions/semantic/__init__.py` | 15 | — | Semantic extension package. |
| `pond-sdk/extensions/semantic/base.py` | 45 | `SemanticModelAdapter` | Abstract interface for semantic adapters. |
| `pond-sdk/extensions/semantic/ossie.py` | 300 | `SemanticLens`, `OssieAdapter` | Ossie adapter + pluggable SemanticLens. |
| `pond-sdk/extensions/physical_structures/pruning.py` | 180 | `ZoneMap`, `PruningPredicate`, `ColumnPredicate` | Vortex-style predicate pushdown. Zone maps (min/max/null_count per row group) + pruning predicates. Skip row groups without decoding. |
| `pond-sdk/extensions/physical_structures/zone_map_index.py` | 280 | `ZoneMapIndex` | ProllyTreeIndex of zone maps. Stores min/max/null_count per data blob. Enables Vortex-style pruning without decoding. |
| `pond-sdk/extensions/physical_structures/pruning_reader.py` | 200 | `PruningReader` | Generic pruning reader. Reads zone maps first, skips non-matching data blobs without decoding. Works with ANY lens/format. |
| `pond-sdk/extensions/physical_structures/column_chunk_zone_map.py` | 180 | `ColumnChunkZoneMap`, `ColumnChunkStats` | Per-column-chunk zone maps for finer-grained pruning within surviving row groups. |
| `pond-sdk/extensions/physical_structures/__init__.py` | 52 | — | Physical Structure extension package. |
| `pond-sdk/extensions/physical_structures/base.py` | 105 | `PhysicalStructure` | Abstract base: build, load, exists, delete, query. |
| `pond-sdk/extensions/physical_structures/bloom_filter.py` | 120 | `BloomFilter` | Probabilistic membership test (O(1)). |
| `pond-sdk/extensions/physical_structures/statistics.py` | 100 | `Statistics` | Column min/max/null_count for pruning. |
| `pond-sdk/extensions/physical_structures/zone_map.py` | 90 | `ZoneMap` | Per-chunk min/max for range pruning. |

### 2.3 lenses/ (Active Lens implementations — 3 packages)

| File | LOC | Exports | Purpose |
|---|---|---|---|
| `lenses/keyvalue/__init__.py` | 0 | — | Package marker. |
| `lenses/keyvalue/keyvalue_lens.py` | 694 | `KeyValueLens`, `KeylessLens`, `CrossLens`, `Lens`/`View` (aliases) | **App-facing KEY-VALUE lens** with ProllyTreeIndex backing. Per-row key→blob storage, O(log N) point lookups, branching, merge, history. `Lens = KeyValueLens` and `View = KeyValueLens` are backward-compat aliases. |
| `lenses/lakehouse/lakehouse_lens.py` | 1740 | `LakehouseLens` | **Flagship lens.** Tabular semantics on Pond: CREATE TABLE, INSERT, read_table, time travel, branching, merge, schema evolution. Owns its Parquet I/O directly (not inherited). Adds range_read/range_write/range_point_lookup + three pruning read paths (read_with_pruning, read_with_column_chunk_pruning, read_with_encoded_pruning) on top of the shared ProllyTreeIndex. DuckDB-optional — the lens itself only needs PyArrow. |
| `lenses/lakehouse/pond_lakehouse.py` | 507 | `PondLakehouse` | DuckDB-backed lakehouse façade over LakehouseLens. Provides SQL query with predicate + projection pushdown (cascades: encoded → column-chunk → row-group → full read). Object-store-aware pruning (S3→on, local→off). This is the only place DuckDB is required. |
| `lenses/lakehouse/sql_pushdown.py` | 170 | `extract_predicates, extract_columns` | Regex SQL parser for predicate + projection extraction. Handles =, !=, <, <=, >, >=, IN, BETWEEN, AND. Does NOT handle OR, joins, subqueries. Returns ["*"] or [] for unparseable queries (caller falls back to full read). |
| `lenses/lakehouse/duckdb_pond_adapter.py` | 195 | `PondDuckDBAdapter` | **SIMD-ready proof:** reads Pond's PND1 binary encoded chunks directly and converts to PyArrow Table. No JSON in the hot path — INT64/FLOAT64 use struct.unpack (C-speed), DICT uses numpy unpackbits, BITPACK uses numpy-accelerated unpack. DuckDB queries the Arrow Table with full SIMD acceleration. |
| `lenses/lakehouse/polars_pond_adapter.py` | 60 | `PondPolarsAdapter` | **Second SIMD-ready proof:** extends PondDuckDBAdapter, converts pa.Table → Polars DataFrame (zero-copy Arrow transfer). Proves PND1 format is engine-independent. |
| `lenses/vector/vector_lens.py` | 460 | `VectorLens` | Vector DB with k-NN search. Extends `PondLens` directly (no lens-to-lens inheritance). Binary packed encoding (struct.pack). Adds build_vector_zone_maps + search_with_pruning — per-dimension bounding-box zone maps enable skipping chunks that can't contain top-k vectors. Uses the SAME ZoneMapIndex infrastructure as tabular lenses. |
| `lenses/streaming/streaming_lens.py` | 200 | `StreamingLens` | **Streaming/media lens.** Chunked storage for large objects (video, music, logs). Range-read WITHOUT a kernel primitive — composes ProllyTreeIndex + segment blobs. write_stream, read_stream (range), append_stream (structural sharing), time-travel, branching. Resolves architect issue #4. |
| `lenses/vector/auto_index.py` | 329 | (mock) | Mock auto-index for testing. |
| `lenses/vector/mock_kernel.py` | 46 | `PondMinimal` (mock) | In-memory mock kernel for tests. |
| `lenses/vector/view_sdk.py` | 39 | `CrossLens` (mock) | Mock CrossLens helpers. |
| `lenses/vector/test_vector.py` | 175 | (tests) | VectorView tests. |
| `lenses/README.md` | — | — | Folder purpose, how to add a Lens. |

### 2.4 services/ (Cross-cutting services — 3 packages)

| File | LOC | Exports | Purpose |
|---|---|---|---|
| `services/transport/transport.py` | 363 | `KeyStore`, `TransportLayer` | Reference Transport Layer (zlib + XOR). §17 algebra. |
| `services/transport/transport_production.py` | 404 | `ProductionKeyStore`, `ProductionTransportLayer` | Production Transport (zstd + AES-GCM). |
| `services/transport/__init__.py` | 4 | — | Package marker. |
| `services/schema/schema_registry.py` | 412 | `SchemaRegistry`, `json_decoder_factory`, `json_encoder_factory` | Versioned schemas, backward/forward compat, migration. §18 algebra. |
| `services/schema/__init__.py` | 4 | — | Package marker. |
| `services/replication/replication_coordinator.py` | 537 | `PrimarySecondaryCoordinator`, `TwoPhaseCommitCoordinator` | Replication (§16) + 2PC coordinator (A7 escape hatch). |
| `services/replication/__init__.py` | 7 | — | Package marker. |
| `services/README.md` | — | — | Folder purpose. |

### 2.5 pond-labs/ (Experiments and demos — 4 files)

| File | LOC | Exports | Purpose |
|---|---|---|---|
| `pond-labs/lenses/feature_store_lens.py` | 584 | `FeatureStoreLens` | Versioned ML feature store: point-in-time joins, online/offline serving, schema evolution, branching. |
| `pond-labs/demos/interop_demo.py` | 359 | (demo) | **Killer demo:** bidirectional Feature Store ↔ Lakehouse interop (12/12 pass). |
| `pond-labs/demos/generic_pruning_demo.py` | 210 | (demo) | **Generic pruning demo:** JSON data (list-of-dicts, no PyArrow) uses the FULL pruning infrastructure with a JSON encode_fn. Proves ANY workload gets predicate pushdown + column-chunk storage for free. |
| `pond-labs/demos/vector_pruning_demo.py` | 140 | (demo) | **Vector pruning demo:** 500 vectors in 5 clusters, k-NN search with bounding-box zone maps. 4/5 chunks pruned without decoding. Results match linear scan exactly. Proves vector data uses the SAME ZoneMapIndex infrastructure as tabular data. |
| `pond-labs/demos/duckdb_adapter_demo.py` | 145 | (demo) | **SIMD-ready proof:** DuckDB reads Pond's PND1 binary encoded chunks via PondDuckDBAdapter. 10K rows (bitpack+dict), 4 SQL queries (COUNT, filter, GROUP BY, aggregation). All pass. Proves any execution engine can read Pond's binary format natively. |
| `pond-labs/benchmarks/loc_benchmark.py` | 469 | (benchmark) | LOC saved: 81% reduction (120 → 23 LOC) vs building from scratch. |
| `pond-labs/benchmarks/pruning_benchmark.py` | 200 | (benchmark) | Benchmark: Vortex-style pruning effectiveness. 100K rows, measures blob skip rate and speedup for 1-50% selectivity queries. |
| `pond-labs/benchmarks/column_chunk_pruning_benchmark.py` | 175 | (benchmark) | Benchmark: column-chunk pruning (3rd level). 50K rows in 1 row group, shows 49/50 chunks pruned for selective predicates. |
| `pond-labs/benchmarks/column_chunk_storage_benchmark.py` | 175 | (benchmark) | Benchmark: per-column-chunk storage. 50K rows in 1 row group, 9.37x I/O reduction (1090KB → 116KB) for selective predicate; 31.76x with projection. |
| `pond-sdk/extensions/physical_structures/column_chunk_storage.py` | 280 | `ColumnChunkStorage` | Per-column-chunk storage: splits row groups into single-column Parquet blobs. True I/O savings on object storage (skip 4/5 chunks = skip 4/5 of bytes per column). |
| `pond-sdk/extensions/physical_structures/encoding.py` | 380 | `ColumnEncoding, EncodingHeader, encode_column, eval_predicate_encoded` | FastLanes-style structural encodings (RLE/Dict/Bitpack/Raw). Encoded predicate eval skips decode for pruned chunks. |
| `pond-sdk/extensions/physical_structures/encoded_chunk_storage.py` | 220 | `EncodedChunkStorage` | Combines ColumnChunkStorage + encoding.py. Per-column-chunk encoded blobs with encoded predicate eval at read time. |
| `pond-sdk/extensions/physical_structures/column_source.py` | 175 | `ColumnSource, PyArrowColumnSource, ListColumnSource, as_column_source, compute_list_stats` | Format-agnostic column data access protocol. Lets any lens (KV, Vector, custom) use the pruning infrastructure without PyArrow. PyArrow tables are auto-wrapped for backward compat. |
| `tests/integration/test_column_chunk_storage.py` | 290 | (test) | Tests per-column-chunk storage: basic write/read, I/O savings (bytes), fallback to whole-blob path. 9.37x I/O reduction verified. |
| `tests/integration/test_encoded_pruning.py` | 380 | (test) | Tests encoding selection, encoded predicate eval, range_write_encoded + read_with_encoded_pruning. 1.86x speedup on low-cardinality queries. |
| `tests/integration/test_sql_pushdown_fast_paths.py` | 130 | (test) | Tests PondLakehouse.query uses the fastest available read path (encoded → column-chunk → row-group → full). Verifies all 3 storage modes work end-to-end via SQL. |
| `tests/integration/test_best_effort.py` | 130 | (test) | Tests the best_effort helper: success path, recoverable exceptions (KeyError/ValueError/ImportError/TypeError), non-recoverable exceptions re-raised (RuntimeError/KeyboardInterrupt), DEBUG logging via POND_DEBUG=1. |
| `tests/integration/test_column_source.py` | 180 | (test) | Tests ColumnSource protocol: ListColumnSource (no PyArrow), PyArrowColumnSource, as_column_source auto-wrap, compute_list_stats edge cases, end-to-end list-of-dicts → zone maps → pruning. |
| `pond-labs/benchmarks/encoded_pruning_benchmark.py` | 210 | (benchmark) | Benchmark: encoding-aware compute on 99K rows. 3.37x faster than whole-blob, 2.04x faster than column-chunk Parquet for low-cardinality predicate. |
| `pond-labs/benchmarks/bitpack_compression_benchmark.py` | 130 | (benchmark) | Benchmark: real bitpack compression. 4-8x compression vs JSON list, 6-62x vs raw int64. O(1) predicate eval via min/max sub-header (2µs). |
| `pond-labs/benchmarks/scale_1m_benchmark.py` | 175 | (benchmark) | **1M-row production-scale benchmark.** 3 storage modes (whole-blob, column-chunk, encoded), 1% selectivity predicate. 99/100 row groups pruned. Encoded path 1.65x faster than column-chunk. Validates full pipeline at scale. |
| `pond-labs/benchmarks/pnd1_vs_parquet_benchmark.py` | 165 | (benchmark) | **External benchmark:** PND1+DuckDB vs Parquet+DuckDB head-to-head. 100K rows, 3 columns, 10% selectivity. Measures write time, storage size, full scan, selective query. Resolves architect issue #5b. |
| `pond-labs/benchmarks/overhead_audit.py` | 330 | (benchmark) | Overhead audit: zone map cost for OLTP, OLAP, streaming, point lookups, full scans, binary data. |
| `pond-labs/benchmarks/sql_pushdown_benchmark.py` | 95 | (benchmark) | SQL pushdown benchmark: pruned vs full scan on 100K rows. Shows Python pruning overhead vs DuckDB native scan on local disk. |
| `pond-labs/benchmarks/incremental_refresh_benchmark.py` | 100 | (benchmark) | Benchmark: O(changed) incremental refresh vs O(N) full rebuild. 27.9x speedup for 0.1% change rate. |
| `pond-labs/README.md` | — | — | Folder purpose. |

### 2.5b pond-lab/ (Lab experiments — 14 files)

| File | LOC | Exports | Purpose |
|---|---|---|---|
| `pond-labs/tracks/track1_compat_matrix.py` | 340 | (tests) | **Track 1:** Bidirectional Lens compatibility matrix (10/10 pass). Level 1 cert. |
| `pond-labs/tracks/track2_index_portability.py` | 380 | (tests) | **Track 2:** Index portability (18/18 pass). Level 2 cert. |
| `pond-labs/tracks/track3_lens_vs_opponent.py` | 460 | (benchmarks) | **Track 3:** Lens-vs-opponent benchmarks. |
| `pond-labs/tracks/track4_object_store_efficiency.py` | 480 | (tests) | **Track 4:** Object-store efficiency (7 experiments; packing = 204x reduction). |
| `pond-labs/tracks/track5_lens_composability.py` | 380 | (tests) | **Track 5:** Lens composability — ETL-free chain (15/15 pass). |
| `pond-labs/tracks/track6_case_studies.py` | 440 | (tests) | **Track 6:** Real-world case studies (25/25 pass). |
| `pond-labs/tracks/track7_reverse_composability.py` | 450 | (tests) | **Track 7:** Reverse composability (24/24 pass). |
| `pond-labs/tracks/track8_storage_independence.py` | 350 | (tests) | **Track 8:** Storage Independence cert (23/23 pass). |
| `pond-labs/tracks/track9_production_lakehouse.py` | 500 | (tests) | **Track 9:** Production Lakehouse with caching (20/20 pass, 2.2x speedup). |
| `pond-labs/tracks/track10_storage_optimization.py` | 430 | (tests) | **Track 10:** Storage optimization at scale (10/10 pass, 996x fewer GETs). |
| `pond-labs/tracks/track11_pond_vs_iceberg.py` | 640 | (benchmarks) | **Track 11:** Head-to-head vs Iceberg proxy at 100K+500K (Pond wins 4/7 at 500K). |
| `pond-labs/tracks/track12_pond_vs_real_iceberg.py` | 540 | (benchmarks) | **Track 12:** Head-to-head vs REAL Apache Iceberg (pyiceberg v0.11.1). Pond wins 5/6 at 100K. |
| `pond-labs/tracks/track13_honest_benchmarks.py` | 500 | (benchmarks) | **Track 13:** Honest benchmarks with correctness assertions + kernel/query separation. |
| `pond-labs/tracks/COMPATIBILITY_SUITE.md` | 80 | — | Compatibility Suite: 3 certification levels. |
| `pond-labs/tracks/README.md` | — | — | Lab tracks overview (12 tracks). |
| `pond-sdk/extensions/README.md` | 80 | — | Extensions architecture overview. |
| `pond-sdk/extensions/semantic/README.md` | 60 | — | Semantic adapters overview. |
| `pond-sdk/extensions/physical_structures/README.md` | 90 | — | Physical Structure type hierarchy. |
| `tests/test_all.py` | 110 | (pytest) | Single pytest entry point: 21 test functions covering all suites. |

### 2.6 scripts/ (Tests and benchmarks — 11 files, ~5400 LOC)

| File | LOC | Checks | Purpose |
|---|---|---|---|
| `scripts/phase_l_property_tests.py` | 1112 | 491 | Property tests for A1-A10 + 23 algebra laws. |
| `scripts/phase_l_hazard_simulator.py` | 423 | 3 | Hazard simulator (9 hazards: read-after-write lag, partition, disk corruption, etc.). |
| `scripts/phase_l_differential_git.py` | 634 | 45 | Differential tests vs real Git. |
| `scripts/phase_n_untested_laws.py` | 473 | 23 | M1-M4' (merge) + W1-W5 (workspace) tests. |
| `scripts/phase_n_additional_hazards.py` | 200 | 10 | Partition + disk corruption hazards. |
| `scripts/phase_o_remaining_laws.py` | 636 | 48 | MAN3, RR3/4, G2/4/5, REP2/4/5/6/8/9, TR4/5, SE1/2/3/4/7. |
| `scripts/phase_o_remaining_hazards.py` | 492 | 13 | Byzantine, hash collision, replay, concurrent compaction+replication. |
| `scripts/phase_p_real_differentials.py` | 539 | 16 | Real Dolt + Iceberg differential tests. |
| `scripts/phase_q_benchmarks.py` | 852 | — | Head-to-head benchmarks vs Git, Dolt, Iceberg. |
| `scripts/verify_knowledge_graph.py` | 65 | — | Verifies KNOWLEDGE_GRAPH.md covers 100% of active files. |
| `scripts/README.md` | — | — | Folder purpose. |

**Total: 646 checks, all passing.**

### 2.7 tla/ (Formal specification — 4 files)

| File | LOC | Purpose |
|---|---|---|
| `tla/PondKernel.tla` | 159 | TLA+ spec: 3 primitives + 6 invariants. |
| `tla/PondKernel.cfg` | 16 | TLC model config (3 bytes, 4 hashes, 2 names). |
| `tla/README.md` | 47 | How to run TLC. |

**Result:** 6 invariants hold across 56 reachable states. "No error has been found."

### 2.8 docs/ (Documentation — 10 active files)

| File | LOC | Purpose |
|---|---|---|
| `docs/POND_WHITEPAPER.md` | 941 | The contribution (20 pages). Formal comparison to Git/Iceberg/Dolt/FDB/LakeFS. |
| `docs/POND_FORMAL_ALGEBRAS.md` | 2406 | 17 algebras, 10 axioms, ~30 laws (Parts I-IV). |
| `docs/WHERE_POND_FAILS.md` | 388 | Honest scope + Lens roadmap (8 struggles → 8 Lenses). |
| `docs/LENS_GUIDE.md` | 230 | How to write a Lens (merged from 3 former docs). |
| `docs/GETTING_STARTED.md` | 175 | 5-minute tutorial with Lakehouse Lens. |
| `docs/POND_PHASE_Q_BENCHMARKS.md` | 344 | Head-to-head benchmarks vs Git/Dolt/Iceberg. |
| `docs/NON_GOALS.md` | 119 | What Pond deliberately doesn't do. |
| `docs/POSTMORTEM_PROLLY_TREE_BUG.md` | 135 | Prolly tree encoding bug postmortem. |
| `docs/DESIGN_REVIEW_2026_07_26.md` | 470 | Design review against the seven principles (42 findings, prioritized fix plan). |
| `docs/GENERIC_DESIGN_VISION.md` | 110 | The promise: any app built on Pond gets infinite storage + versioning + branching + pruning + encoding on object stores. Documents the ColumnSource protocol, format-agnostic encode_fn/decode_fn, and the Vortex-style scan hierarchy. |
| `docs/BINARY_ENCODING_FORMAT.md` | 165 | **Format spec v1.0:** SIMD-ready binary encoding for all 4 encodings (RAW, RLE, DICT, BITPACK). Stable, documented, directly mmappable to numpy/Arrow. Any execution engine (DuckDB, Polars, DataFusion) can read Pond's encoded chunks natively. |
| `docs/README.md` | 58 | Doc index. |
| `docs/archive/` | (18+ files) | Historical docs (Phase reports, red teams, RFCs, etc.). |

### 2.9 Top-level files

| File | LOC | Purpose |
|---|---|---|
| `README.md` | 130 | 5-minute intro to Pond. Start here. |
| `DESIGN_GOALS.md` | 1013 | 7 design principles + roadmap. |
| `REPO_ORGANIZATION.md` | 220 | Folder rules, naming conventions, promotion process, no lens-to-lens inheritance. |
| `PACKAGES.md` | 156 | Package structure and dependency graph. |
| `SDK_SPEC.md` | 1095 | Authoritative SDK contract (13 ambiguities settled). |
| `KNOWLEDGE_GRAPH.md` | — | This file. The navigational map of the repo. |
| `worklog.md` | 1928 | Append-only research log (Tasks 1-57). |

### 2.10 archive/ (Historical — 124 files, preserved for reference)

Contains:
- `prototype/` — early experimental code (7420 LOC)
- `libraries/` — older SDK versions (3191 LOC)
- `applications/` — older Lens implementations (2008 LOC)
- `engineering/` — engineering experiments (1259 LOC)
- `destruction/` — adversarial destruction tests (7722 LOC)
- `experiments/` — older performance benchmarks (6520 LOC)
- `validation/` — external validation reports (6460 LOC)
- `pond-semantic/` — stub (3 lines, never implemented)
- `pond-git/` — **reference impl** (broken imports fixed; Git Lens on ProllyViewBase)
- `pond-notebook/` — **reference impl** (fixed; Notebook Lens with pages, tags, search)
- `pond-sql/` — **reference impl** (fixed; SQL Lens with CREATE/INSERT/SELECT/UPDATE/DELETE/ALTER)
- `pond-streaming/` — **reference impl** (fixed; Streaming Lens with topics, partitions, consumer groups)
- `pond-arrow/` — **reference impl** (fixed; ArrowView with DuckDB/Polars/pandas interop)
- `pond-feature-store/` — **reference impl** (fixed; older FeatureStore with CLI, e2e workflow)
- `pond_rfc1.py` — RFC-0001 PDF generator (1967 lines)

**Note:** Archived Lens packages (`pond-sql`, `pond-git`, `pond-notebook`,
`pond-streaming`, `pond-arrow`, `pond-feature-store`) have been fixed
to import from `pond-core/` and `pond-sdk/`. They serve as **reference
implementations** for the Lens roadmap in `WHERE_POND_FAILS.md`.

---

## 3. Concept Map

### 3.1 Core Concepts

| Concept | Definition | Where |
|---|---|---|
| **Kernel** | 3 operations (Write, Read, Ref) on 6 substrates. FROZEN. | `pond-core/kernel.py` |
| **Substrate** | A layer with its own axioms (Bytes, Names, Time, Coordination, Range-Read, Key). | `docs/POND_FORMAL_ALGEBRAS.md` §9 |
| **Lens** | App-facing interpretation layer over immutable bytes. Each lens owns its own read/write API. | `lenses/keyvalue/keyvalue_lens.py` (KeyValueLens), `lenses/lakehouse/lakehouse_lens.py` (LakehouseLens), `pond-labs/lenses/feature_store_lens.py` (FeatureStoreLens) |
| **PondLens** | Shared namespace base for all Lenses. Provides only ref-namespace operations (branch, list_collections, set_definition, get_definition, history). No format awareness. | `pond-sdk/base_lens.py` |
| **Physical Structure** | `f(snapshot)→artifact`. Deterministic, rebuildable. Indexes, stats, bloom filters. | `docs/POND_FORMAL_ALGEBRAS.md` §14 |
| **Collection** | Named reference namespace. Not fundamental — just a naming convention. | `pond-sdk/collection.py` |
| **Prolly Tree** | Probabilistic Merkle tree with content-addressed chunks. O(log N) lookup. | `pond-sdk/prolly_tree.py` |
| **Tiered Commit** | Delta commits (O(1) write) + snapshot commits (O(changed_chunks)) + snapshot pointer. | `pond-sdk/prolly_tree.py` |
| **Tombstone** | Deletion as data: `Ref(name, TOMBSTONE_HASH)`. RFC-0008. | `pond-sdk/maintenance.py` |
| `pond-sdk/collection_metadata.py` | 343 | `CollectionMetadata` | Data-side metadata manager. Manages zone maps, indexes, and (future) bloom filters for collections. Lens-agnostic — works through callbacks. |
| `pond-sdk/best_effort.py` | 95 | `best_effort, warn_best_effort` | Tiny helper for best-effort operations. Catches specific recoverable exceptions (AttributeError, KeyError, TypeError, ValueError, ImportError, ArithmeticError) and logs them via the `pond.best_effort` logger. Replaces the `except Exception: pass` anti-pattern. Enable with `POND_DEBUG=1`. |
| `pond-sdk/pond_config.py` | 195 | `PondConfig` | Persistent pruning + encoding settings via `.pond/config` JSON file. Configures pruning (auto/true/false + force), encoding (auto-select or default), chunk_size, row_group_size, bitpack_max_bitwidth. `should_prune()` decides based on storage type. `load_for_kernel()` finds config in base_dir. |
| `tests/integration/test_pond_config.py` | 130 | (test) | Tests PondConfig: defaults, save/load round-trip, should_prune (auto/true/false/force), encoding hints, validation, load_for_kernel. |
| **Manifest** | Sidecar listing blob hashes in a pack. Enables physical reachability (1000x GC speedup). | `docs/POND_FORMAL_ALGEBRAS.md` §10 |
| **Transport Layer** | Compress → encrypt → checksum. Between kernel and Lens. | `services/transport/` |
| **Schema Registry** | Versioned schemas on Names substrate. Backward/forward compat. | `services/schema/` |
| **Replication Coordinator** | Single-writer per Ref + 2PC for cross-Collection atomicity. | `services/replication/` |

### 3.2 Axioms (10)

| Axiom | Statement | File |
|---|---|---|
| A1 | Immutability: `Read(Write(b)) = b` always | `pond-core/kernel.py` |
| A2 | Content-addressing: same bytes → same hash | `pond-core/kernel.py` |
| A3 | Name mutability (LWW): Ref is the only mutation | `pond-core/kernel.py` |
| A4 | Referential integrity: Ref requires hash exists | `pond-core/kernel.py` |
| A5 | Monotonic logical clock (Lamport) | `docs/POND_FORMAL_ALGEBRAS.md` §9 |
| A6 | Atomic commit blob (within-Collection) | `docs/POND_FORMAL_ALGEBRAS.md` §9 |
| A7 | Coordinator out-of-model (cross-Collection needs coordinator) | `docs/POND_FORMAL_ALGEBRAS.md` §9 |
| A8' | Range reads are transport-layer (demoted from kernel) | `docs/POND_FORMAL_ALGEBRAS.md` §22 |
| A9 | Single-writer per Ref (replication) | `docs/POND_FORMAL_ALGEBRAS.md` §16 |
| A10 | Compress before encrypt (transport order) | `docs/POND_FORMAL_ALGEBRAS.md` §17 |

### 3.3 Algebra Laws (selected; see `docs/POND_FORMAL_ALGEBRAS.md` for all)

| Law | Statement |
|---|---|
| R1-R5 | Reference algebra (atomicity, LWW, CAS-conditional, tombstone, prefix listing) |
| G1-G6 | GC algebra (safety, liveness, idempotency, non-blocking, tombstone interaction, **tombstone barrier**) |
| MAN1-MAN4 | Manifest algebra (LR⟺PR equivalence, rebuildable, stale, composition) |
| M1-M4' | Merge algebra (commutativity, associativity, Lens-determines-semantics, snapshot-or-delta) |
| W1-W5 | Workspace algebra (isolation, atomicity, savepoint, Lens-independence, ephemeral) |
| REP1-REP9 | Replication algebra (single-writer, stale reads, commit-blob unit, failover, one-directional) |
| TR1-TR6 | Transport algebra (dedup broken, dictionary sidecar, below-Lens, optional, per-blob, block-index) |
| SE1-SE8 | Schema evolution (backward/forward compat, writer-schema-recorded, Lens-responsibility, Naming-convention) |
| C0-C5 | Consistency levels (blob immutability → no cross-Collection guarantee) |

---

## 4. Dependency Graph

```
pond-core (FROZEN, ~140 LOC)
    │
    ├── pond-sdk (depends on pond-core)
    │   ├── base_lens.py ← pond-core  (shared namespace base, no format awareness)
    │   ├── keyvalue_lens.py ← pond_lens, prolly_view, binary_encoding, maintenance, lens_query
    │   ├── lens_sdk.py ← keyvalue_lens  (backward-compat shim, re-exports)
    │   ├── prolly_tree.py ← binary_encoding
    │   ├── indexing.py ← prolly_view
    │   ├── collection.py
    │   ├── query.py
    │   └── maintenance.py
    │
    ├── lenses/ (depend on pond-core + pond-sdk)
    │   ├── lakehouse/ ← pond-core, duckdb, pyarrow
    │   └── vector/ ← pond-sdk (uses mock_kernel for tests)
    │
    ├── services/ (depend on pond-core only)
    │   ├── transport/ ← pond-core, zstandard, cryptography (production)
    │   ├── schema/ ← pond-core
    │   └── replication/ ← pond-core
    │
    └── pond-labs/ (depend on pond-core + lenses/lakehouse)
        ├── feature_store_lens.py ← pond-core, pyarrow
        ├── interop_demo.py ← pond-core, lenses/lakehouse, pond-labs/feature_store_lens
        └── loc_benchmark.py ← pond-core, lenses/lakehouse
```

**Rules:**
- No Lens depends on another Lens.
- All Lenses depend only on `pond-sdk` (and `pond-core`).
- `pond-sdk` depends only on `pond-core`.
- `pond-core` depends on nothing.
- Services depend only on `pond-core` (not on `pond-sdk`).
- `pond-labs` depends on `pond-core` + `lenses/lakehouse`.

---

## 5. Lens Roadmap (from `docs/WHERE_POND_FAILS.md`)

| Workload | Required Lens | Status | Reference Impl |
|---|---|---|---|
| Versioned tabular data | Lakehouse Lens | **Shipped** | `lenses/lakehouse/` |
| ML feature stores | Feature Store Lens | **Shipped** | `pond-labs/lenses/feature_store_lens.py` |
| Code versioning | Git Lens | Reference in archive | `archive/pond-git/` |
| SQL (native) | SQL Lens | Reference in archive | `archive/pond-sql/` |
| Streaming | Streaming Lens | Reference in archive | `archive/pond-streaming/` |
| Notebook versioning | Notebook Lens | Reference in archive | `archive/pond-notebook/` |
| Arrow interop | Arrow Lens | Reference in archive | `archive/pond-arrow/` |
| Vector DB | Vector Lens | **Shipped** | `lenses/vector/` |
| High-frequency OLTP | OLTP Lens | Not built | — |
| Distributed consensus | CRDT Lens + Coordinator | Not built | `services/replication/` (2PC ref) |
| Random in-place updates | LSM Lens | Not built | — |
| Hot-key contention | Counter CRDT Lens | Not built | — |
| Streaming joins | Streaming Lens (with state) | Not built | — |
| GPU data | Tensor Lens | Not built | — |
| Millions of tiny objects | Packing Lens | Prototype in archive | `archive/experiments/packed_backend.py` |
| Full-text search | Search Lens | Not built | — |

---

## 6. Maintenance Protocol

**This file must be updated whenever the repo changes.** Specifically:

### 6.1 When to update

Update `KNOWLEDGE_GRAPH.md` when you:
1. **Add a file** — add it to §2 (File Map) with LOC and purpose.
2. **Remove a file** — remove it from §2. If moved to archive, note in §2.10.
3. **Move a file** — update §2 and the dependency graph in §4.
4. **Rename a file** — update §2 and all references.
5. **Add a new concept** — add it to §3 (Concept Map).
6. **Add a new axiom or law** — add it to §3.2 or §3.3.
7. **Change dependencies** — update §4 (Dependency Graph).
8. **Ship a new Lens** — update §5 (Lens Roadmap).
9. **After any reorganization** — re-verify §2 is complete and §4 is accurate.

### 6.2 How to verify completeness

Run this check before committing:

```bash
# Verify every active .py file is in the knowledge graph
for f in $(find . -name "*.py" -not -path "./archive/*" -not -path "./.git/*" | sort); do
  if ! grep -q "$f" KNOWLEDGE_GRAPH.md; then
    echo "MISSING FROM KG: $f"
  fi
done

# Verify every active .md file is in the knowledge graph
for f in $(find . -name "*.md" -not -path "./archive/*" -not -path "./.git/*" | sort); do
  if ! grep -q "$f" KNOWLEDGE_GRAPH.md; then
    echo "MISSING FROM KG: $f"
  fi
done
```

If any file is missing, add it before committing.

### 6.3 Agent instructions

**For any future agent (human or AI) working on Pond:**

1. **Read this file first.** It is the map of the entire repo.
2. **Update this file when you change the repo.** Do not let it go stale.
3. **Run the completeness check** (§6.2) before committing.
4. **Follow the 7 design principles** (`DESIGN_GOALS.md` §3) in all changes.
5. **If you're not sure where something is**, check §2 (File Map) or §3 (Concept Map).
6. **If you're not sure how things connect**, check §4 (Dependency Graph).
7. **If you're building a new Lens**, check §5 (Lens Roadmap) for prior art.

### 6.4 Graphify / visual graph

This file is a text-based knowledge graph. For a visual representation,
you can use [Graphify](https://github.com/Graphify-Labs/graphify) or
similar tools by parsing §4 (Dependency Graph) into a DOT/Mermaid format.
The text format is the source of truth; visual renderings are derivatives.

A Mermaid rendering of the dependency graph:

```mermaid
graph TD
    Kernel[pond-core<br/>FROZEN, ~140 LOC]
    SDK[pond-sdk<br/>Lens SDK]
    Lakehouse[lenses/lakehouse<br/>Flagship]
    Vector[lenses/vector]
    Transport[services/transport<br/>zstd + AES-GCM]
    Schema[services/schema<br/>Versioned schemas]
    Replication[services/replication<br/>2PC coordinator]
    Labs[pond-labs<br/>Feature Store + interop]
    Scripts[scripts<br/>646 checks]
    TLA[tla<br/>6 invariants]

    Kernel --> SDK
    Kernel --> Transport
    Kernel --> Schema
    Kernel --> Replication
    SDK --> Lakehouse
    SDK --> Vector
    Kernel --> Lakehouse
    Kernel --> Labs
    Lakehouse --> Labs
    Scripts --> Kernel
    Scripts --> SDK
    Scripts --> Transport
    TLA --> Kernel
```

---

## 7. Quick Reference

### 7.1 How to run everything

```bash
# Kernel
python -c "import sys; sys.path.insert(0,'pond-core'); from pond_minimal import PondMinimal; k=PondMinimal('/tmp/p'); h=k.write(b'hi'); print(k.read(h))"

# Flagship (lakehouse)
python lenses/lakehouse/lakehouse_lens.py

# Killer demo (interop)
python pond-labs/demos/interop_demo.py

# LOC benchmark
python pond-labs/benchmarks/loc_benchmark.py

# All 646 checks
python scripts/phase_l_property_tests.py
python scripts/phase_l_differential_git.py
python scripts/phase_n_untested_laws.py
python scripts/phase_n_additional_hazards.py
python scripts/phase_o_remaining_laws.py
python scripts/phase_o_remaining_hazards.py
PATH="/home/z/bin:$PATH" python scripts/phase_p_real_differentials.py
python scripts/phase_q_benchmarks.py

# Services
python services/transport/transport_production.py
python services/schema/schema_registry.py
python services/replication/replication_coordinator.py

# SDK tests
python tests/architecture/architecture_laws.py
python tests/lens_algebra/lens_laws.py
```

### 7.2 Key numbers

| Metric | Value |
|---|---|
| Kernel LOC | ~140 (FROZEN) |
| Substrates | 6 |
| Operations | 3 (Write, Read, Ref) |
| Axioms | 10 (A1-A10) |
| Formal algebras | 17 |
| Property tests | 491 passing |
| Differential tests | 45 (Git) + 16 (Dolt+Iceberg) = 61 passing |
| Hazard tests | 23 passing |
| Law tests | 71 passing |
| Total checks | 646+ passing |
| TLA+ invariants | 6 (across 56 reachable states) |
| LOC reduction (vs from-scratch) | 81% (120 → 23 LOC) |

### 7.3 Where to find things

| Looking for... | Go to... |
|---|---|
| The kernel | `pond-core/kernel.py` |
| Lens base class (shared namespace) | `pond-sdk/base_lens.py` → `PondLens` |
| KeyValueLens (app-facing KV lens) | `lenses/keyvalue/keyvalue_lens.py` → `KeyValueLens` (aliases: `Lens`, `View`) |
| Prolly tree (ProllyTreeIndex) | `pond-sdk/prolly_tree.py` → `ProllyTree`, `ProllyLensBase` |
| Lakehouse (flagship) | `lenses/lakehouse/lakehouse_lens.py` → `LakehouseLens`, `PondLakehouse` |
| Feature Store | `pond-labs/lenses/feature_store_lens.py` → `FeatureStoreLens` |
| Compression/encryption | `services/transport/transport_production.py` |
| Schema evolution | `services/schema/schema_registry.py` |
| Replication/2PC | `services/replication/replication_coordinator.py` |
| Formal model | `docs/POND_FORMAL_ALGEBRAS.md` |
| Whitepaper | `docs/POND_WHITEPAPER.md` |
| Honest scope | `docs/WHERE_POND_FAILS.md` |
| Lens author guide | `docs/LENS_GUIDE.md` |
| 7 design principles | `DESIGN_GOALS.md` §3 |
| All tests | `scripts/` |
| TLA+ proof | `tla/PondKernel.tla` |
| Historical code | `archive/` |

---

## 8. Verification

This knowledge graph covers 100% of active files. Verified by:

```bash
$ for f in $(find . -name "*.py" -not -path "./archive/*" -not -path "./.git/*"); do
    grep -q "$f" KNOWLEDGE_GRAPH.md || echo "MISSING: $f"
  done
# (no output = all files covered)

$ for f in $(find . -name "*.md" -not -path "./archive/*" -not -path "./.git/*"); do
    grep -q "$f" KNOWLEDGE_GRAPH.md || echo "MISSING: $f"
  done
# (no output = all files covered)
```

**Last verified:** 2026-07-24 (commit after this file is committed).

### 2.10 Additional files (READMEs and package markers)

| File | Purpose |
|---|---|
| `lenses/keyvalue/README.md` | README for KeyValueLens. |
| `lenses/lakehouse/README.md` | README for LakehouseLens. |
| `lenses/lakehouse/__init__.py` | Package marker. |
| `lenses/vector/README.md` | README for VectorLens. |
| `lenses/vector/__init__.py` | Package marker. |
| `pond-labs/benchmarks/README.md` | README for benchmarks. |
| `pond-labs/demos/README.md` | README for demos. |
| `pond-labs/lenses/README.md` | README for lab lenses. |
| `pond-sdk/extensions/indexing/README.md` | README for indexing extensions. |
| `services/replication/README.md` | README for replication coordinator. |
| `services/schema/README.md` | README for schema registry. |
| `services/transport/README.md` | README for transport layer. |
| `tests/README.md` | README for test suite. |
| `tests/architecture/README.md` | README for architecture laws. |
| `tests/integration/README.md` | README for integration tests. |
| `tests/lens_algebra/README.md` | README for lens algebra tests. |

---

## 7. New Files (Rounds 1-22)

### pond-core/
- `object_store_native_kernel.py` — ObjectStoreNativeKernel (no SQLite, refs as content-addressed blobs) + InMemoryObjectStore + S3MockKernel
- `s3_mock_backend.py` — S3 mock with simulated latency (extends ObjectStoreNativeKernel)

### pond-sdk/
- `pond_storage.py` — PondStorage (the ONE unified SDK class: namespace + commit + data I/O)

### pond-sdk/extensions/physical_structures/
- `unified_storage.py` — UnifiedStorage (PND2 format, write/read/point_lookup/iter_rows/compact_manifest)
- `collection_manifest.py` — CollectionManifest (ONE index blob per commit, delta-manifest support, stats tree delegation)
- `stats_tree.py` — StatsTreeReader (PB-scale hierarchical index, O(log N) reads)
- `compression.py` — zstd/LZ4 transparent compression
- `embedded_stats.py` — value-type constants + ColumnStats

### scripts/
- `test_pond_storage.py` — PondStorage tests (6 tests)
- `test_unified_storage_smoke.py` — UnifiedStorage smoke tests (6 tests)
- `test_manifest_smoke.py` — CollectionManifest tests (4 tests)
- `test_stats_tree_smoke.py` — StatsTree tests (4 tests)
- `test_object_store_native_kernel.py` — ObjectStoreNativeKernel tests (6 tests)
- `test_pb_scale_integration.py` — PB-scale integration tests (3 tests)
- `test_adversarial.py` — Adversarial edge-case tests (7 tests)
- `test_range_scan_boundaries.py` — Range scan boundary tests (4 tests)
- `test_round9_fixes.py` — Round 9 fix verification (3 tests)
- `test_keyvalue_unified.py` — KeyValueLens unified storage tests (5 tests)
- `test_vector_unified.py` — VectorLens unified storage tests (4 tests)
- `benchmark_cold_round_trips.py` — Cold-read round-trip benchmark
- `benchmark_final.py` — Final architecture benchmark
- `benchmark_round_trips.py` — Round-trip comparison benchmark
- `benchmark_unified_storage.py` — Unified storage benchmark
- `round19_benchmarks.py` — Round 19 comprehensive benchmarks

### lenses/streaming/
- `streaming_lens.py` — StreamingLens (chunked segments, range reads)
- `README.md` — Streaming lens documentation
- `__init__.py` — Package init

### docs/archive/
- `POND_PHASE_O_REPORT.md` — Phase O report (historical)
- `POND_PHASE_P_REPORT.md` — Phase P report (historical)
- `POND_PHASE_Q_REPORT.md` — Phase Q report (historical)
- `POND_PHASE_Q_REVIEW_PACKET.md` — Phase Q review packet (historical)
- `POND_SECOND_RED_TEAM.md` — Second red team review (historical)
- `POND_STORAGE_MODEL.md` — Original storage model (superseded)
- `POND_THIRD_RED_TEAM.md` — Third red team review (historical)
- `REJECTED_DESIGNS.md` — Rejected architectural decisions (historical)
- `WORKLOAD_ANALYSIS_PB_SCALE.md` — PB-scale workload analysis (stats tree now implemented)

### pond-labs/
- `benchmarks/s3_mock_benchmark.py` — S3 mock benchmark
- `demos/jupyter_notebook_demo.py` — Jupyter notebook demo
- `demos/notebook_lens_demo.py` — Notebook lens demo
- `demos/polars_adapter_demo.py` — Polars adapter demo
- `demos/streaming_lens_demo.py` — Streaming lens demo


## 8. Complete File Coverage (Rounds 1-22)

All active files in the repository (excluding archive/, __pycache__, .git):

- `docs/ARCHITECTURE_REDESIGN.md`
- `docs/COLLECTION_MANIFEST_DESIGN.md`
- `docs/HONEST_COMPETITOR_COMPARISON.md`
- `docs/ROUND_TRIP_AUDIT.md`
- `docs/UNIFIED_STORAGE_DESIGN.md`
- `lenses/streaming/README.md`
- `lenses/streaming/__init__.py`
- `pond-core/object_store_native_kernel.py`
- `pond-core/s3_mock_backend.py`
- `pond-labs/benchmarks/s3_mock_benchmark.py`
- `pond-labs/demos/jupyter_notebook_demo.py`
- `pond-labs/demos/notebook_lens_demo.py`
- `pond-labs/demos/polars_adapter_demo.py`
- `pond-labs/demos/streaming_lens_demo.py`
- `pond-sdk/extensions/physical_structures/collection_manifest.py`
- `pond-sdk/extensions/physical_structures/compression.py`
- `pond-sdk/extensions/physical_structures/embedded_stats.py`
- `pond-sdk/extensions/physical_structures/stats_tree.py`
- `pond-sdk/extensions/physical_structures/unified_storage.py`
- `pond-sdk/pond_storage.py`
- `scripts/benchmark_cold_round_trips.py`
- `scripts/benchmark_final.py`
- `scripts/benchmark_round_trips.py`
- `scripts/benchmark_unified_storage.py`
- `scripts/round19_benchmarks.py`
- `scripts/test_adversarial.py`
- `scripts/test_keyvalue_unified.py`
- `scripts/test_manifest_smoke.py`
- `scripts/test_object_store_native_kernel.py`
- `scripts/test_pb_scale_integration.py`
- `scripts/test_pond_storage.py`
- `scripts/test_range_scan_boundaries.py`
- `scripts/test_round9_fixes.py`
- `scripts/test_stats_tree_smoke.py`
- `scripts/test_unified_storage_smoke.py`
- `scripts/test_vector_unified.py`
- scripts/test_multi_workload.py
- scripts/test_cross_lens_universal.py
- scripts/test_ivf.py
- scripts/test_concurrency.py
