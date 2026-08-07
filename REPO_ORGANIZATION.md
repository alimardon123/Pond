# Pond Repository Organization Rules

> This document codifies the folder structure, naming conventions, and
> promotion process for the Pond repository. Every agent (human or AI)
> working on Pond MUST follow these rules.

---

## 1. Top-level folder structure

```
pond_repo/
├── pond-core/          # Layer 0: Storage Kernel + storage backends (NOT FROZEN)
├── pond-sdk/           # Layers 1+2: Lens SDK + Physical Structures + Extensions
├── lenses/             # Layer 3: Production-ready Lens implementations
├── services/           # Cross-cutting services (transport, schema, replication)
├── pond-labs/          # Development & experimental code (NOT production)
├── pond-rust/          # Cross-language Rust core + Python PyO3 bindings + CLI
│   ├── pond-core/      #   Pure-Rust PND2 codec + C ABI (zero deps)
│   ├── pond-python/    #   PyO3 wrapper (produces pond_rust.so for Python)
│   ├── pond-kernel/    #   Rust storage kernel (3 primitives + ObjectStore trait)
│   └── pond-cli/       #   The 'pond' CLI binary (DuckDB philosophy)
├── sdk-go/             # Go SDK — PND2 codec bindings via cgo (peer to pond-sdk/)
├── pond/               # Pip-installable package shim (re-exports pond-core/pond-sdk)
├── tests/              # All tests, organized by type/purpose
├── scripts/            # Verification scripts (property tests, differentials, hazards)
├── docs/               # Documentation (whitepaper, formal algebras, RFCs)
├── tla/                # TLA+ formal specification
├── archive/            # Historical code (preserved for reference, NOT active)
└── (top-level docs)    # README.md, DESIGN_GOALS.md, PACKAGES.md, etc.
```

---

## 2. Folder rules

### 2.1 `pond-core/` — Storage Kernel + storage backends

**Contains:** The 3-primitive kernel (Write, Read, Ref) plus batch I/O
helpers, plus the production storage backends (LocalFS, S3, in-memory)
and a unified kernel factory.
- `kernel.py` (274 LOC) — `PondMinimal`: `write`, `read`, `read_blob`,
  `write_batch`, `read_blob_batch`, `reference`, `resolve`, `list_names`.
  6 substrates, 3 operations + batch I/O helpers.
- `object_store_native_kernel.py` — `ObjectStoreNativeKernel`,
  `InMemoryObjectStore`, `make_object_store_native_kernel`. Production
  kernel backend (no SQLite — refs are content-addressed blobs).
- `local_fs_object_store.py` — pure local-filesystem store.
- `s3_object_store.py` — real boto3-backed store.
- `s3_mock_backend.py` — latency-injecting S3 mock for benchmarks.
- `make_kernel.py` — `make_kernel(url)`: `file://` or `s3://`.

**Rule:** Not FROZEN. The kernel grew `write_batch` and `read_blob_batch`
in the thread-safety round; these are same-collection performance
primitives (not cross-collection atomicity — see `DESIGN_GOALS.md`
§3.1). Bug fixes and same-collection batch helpers are allowed; new
substrates or new operations require a written rationale.

**Naming:** `kernel.py` (was `pond_minimal.py`).

### 2.2 `pond-sdk/` — Lens SDK + Extensions

**Contains:**
- The shared namespace base (`base_lens.py` → `PondLens`)
- The unified SDK entry point (`pond_storage.py` → `PondStorage`)
- Configuration (`pond_config.py` → `PondConfig`)
- Tombstone helpers (`maintenance.py` → `drop_name`, `is_dropped`, etc.)
- UUIDv7 generation (`uuid7.py` → `uuidv7`, `uuidv7_monotonic`)
- Hybrid Logical Clock (`hlc.py` → `HLC`) — clock-skew-safe LWW for CRDT
- Lazy row query API (`row_query.py` → `LensQuery`)
- Extensions subdirectory (see §3 below)

> **Honesty note (Task 65).** Earlier versions of this document
> claimed `pond-sdk/` contained `prolly_tree.py`, `binary_encoding.py`,
> `collection.py`, and `collection_metadata.py`. **None of those files
> exist in `pond-sdk/`** — they live in `archive/legacy-sdk/` and
> `archive/legacy-extensions/` as historical reference. The actual
> universal storage backend is
> `pond-sdk/extensions/physical_structures/unified_storage.py`
> (5,540 LOC — not "tiny"). It is the only storage backend in the
> production SDK. The legacy ProllyTree/ProllyLensBase machinery
> has been removed from production lenses (see
> `agent-ctx/task-legacy-cleanup-vector-streaming.md`).

**Note:** `KeyValueLens` lives in `lenses/keyvalue/keyvalue_lens.py` (a
production lens package, see §2.3), NOT in `pond-sdk/`.

**Rule:** pond-sdk depends only on pond-core. No lens-to-lens imports.

**Naming convention:**
- Infrastructure files: `{role}.py` (e.g., `kernel.py`, `maintenance.py`)
- Lens files: `{role}_lens.py` (e.g., `keyvalue_lens.py`)
- The file `row_query.py` is NOT named `query.py` to avoid confusion with
  "the query method for data in Pond" — it is a lazy row-level query BUILDER
  for iterating/filtering/joining rows from any iterable lens.

### 2.3 `lenses/` — Production-ready Lens implementations

**Contains:** Lenses that are production-quality and ready for use.
**Current:**
- `lenses/keyvalue/` (`KeyValueLens`, `KeylessLens`)
- `lenses/lakehouse/` (`LakehouseLens`)
- `lenses/streaming/` (`StreamingLens`)
- `lenses/vector/` (`VectorLens`)
- `lenses/oltp/` (`OLTPLens`)

**Rule:**
- Each lens in its own subdirectory: `lenses/{lens_name}/`
- Main file: `lenses/{lens_name}/{lens_name}_lens.py`
- Lenses here extend `PondLens` directly (from `pond-sdk/base_lens.py`),
  **with one documented exception** — `OLTPLens` declares
  `class OLTPLens:` with NO base class, because it is a thin memtable +
  batch-flush wrapper over `PondStorage` and does not need the
  ref-namespace operations `PondLens` provides. See `SDK_SPEC.md` and
  `DESIGN_GOALS.md` Known Gaps.
- `LakehouseLens` is in the same boat — its class declaration does not
  extend `PondLens` either. This is also a documented exception, not a
  bug. (See `SDK_SPEC.md` §Lens hierarchy.)
- **NO lens-to-lens inheritance.** Each production lens owns its own storage
  code, even if that means duplication. This keeps lenses independent and
  removable. (See §4 below.)
- Lenses may use pond-sdk extensions (physical structures, indexing).

### 2.4 `services/` — Cross-cutting services

**Contains:** Services that sit between the kernel and lenses.
**Current:** `services/transport/`, `services/schema/`, `services/replication/`
**Rule:** Services depend only on pond-core (not on pond-sdk or lenses).

### 2.5 `pond-labs/` — Development & experimental code

**Contains:** Code that is NOT yet production-ready. Organized by purpose:
```
pond-labs/
├── lenses/         # Lens prototypes in development (e.g., feature_store_lens.py)
├── tracks/         # Lab tracks (compatibility, benchmarks, case studies)
├── demos/          # Demonstration scripts (e.g., interop_demo.py)
└── benchmarks/     # Performance benchmarks (e.g., loc_benchmark.py)
```
**Rule:**
- Code here is experimental. It may break. It may be deleted.
- **Promotion process:** When code in `pond-labs/` is approved for production,
  it is moved to the appropriate production location:
  - Lenses → `lenses/{lens_name}/`
  - Services → `services/{service_name}/`
  - SDK extensions → `pond-sdk/extensions/`
  - SDK core → `pond-sdk/`
- The promotion is documented in the worklog with a clear rationale.

### 2.5b `pond-rust/` — Cross-language Rust core + Python PyO3 bindings

**Contains:** A Cargo workspace with two crates that provide the canonical
Rust implementation of Pond's PND2 binary format. The C ABI is the
universal interop layer for Go/Java/Node/C/C++/Zig SDK ports.

```
pond-rust/
├── Cargo.toml          # Workspace manifest
├── pond-core/          # Pure-Rust PND2 codec + C ABI (zero external deps)
│   ├── Cargo.toml      #   crate-type = ["staticlib", "cdylib", "rlib"]
│   ├── pond_core.h     #   C ABI header (the contract for cross-language SDKs)
│   └── src/lib.rs      #   pnd2_decode (all encodings) + pnd2_encode_* + C ABI
├── pond-python/        # PyO3 wrapper, produces pond_rust.so for Python
│   ├── Cargo.toml      #   crate-type = ["cdylib"]
│   └── src/lib.rs      #   Thin glue — delegates to pond-core's decoder
└── tests/
    ├── test_c_abi.c    # End-to-end C ABI test (131 checks)
    └── test_blobs/     # Binary PND2 blobs for cross-language compat tests
```

**Rule:** pond-rust/pond-core has ZERO external dependencies (so it can be
statically linked from Go/Java/Node without dragging transitive Rust
crates). pond-rust/pond-python depends on pond-rust/pond-core + PyO3.

**Why split:** Originally a single crate mixed PyO3 with the C ABI, but
the static library contained PyO3's libpython symbol references, breaking
links from C/Go/Java. The split puts the C ABI in a crate with no PyO3
dependency, so `libpond_core.a` is a clean, self-contained static library.

### 2.5c `sdk-go/` — Go SDK (PND2 codec bindings via cgo)

**Contains:** Go bindings for `libpond_core.a`. Peer to `pond-sdk/`
(Python SDK) — both bind to pond-core's storage layer. Currently exposes
PND2 codec operations only (no storage kernel access).

```
sdk-go/
├── go.mod              # Module github.com/pond/pond-go
├── pond/               # Public Go API (import this package)
│   ├── pond.go         #   Result, Column, Encoder, Encode*/Decode
│   └── pond_test.go    #   End-to-end tests + Python-blob cross-lang compat
└── internal/           # Private packages (not importable externally)
    └── cabi/           #   cgo layer over libpond_core.a
        └── cabi.go     #   Direct C function wrappers
```

**Rule:** sdk-go depends on pond-rust/pond-core (via cgo over
`libpond_core.a`) and NOTHING else. It does NOT depend on Python — Go
programs can encode/decode PND2 blobs without any Python runtime.

**Scope:** PND2 codec only (encode + decode). Storage kernel operations
(Write/Read/Ref) require the Python kernel — a future Rust implementation
of the storage kernel would enable full Go storage support. This is
documented honestly in `sdk-go/README.md`.

**Removability test:** Deleting `sdk-go/` breaks no lower layer. The
Python SDK, Rust core, and storage kernel are all unaffected.

### 2.6 `tests/` — All tests, organized by type/purpose

**Contains:** All test files. Organized by test objective:
```
tests/
├── test_all.py          # Single pytest entry point (runs everything)
├── architecture/        # Architecture law tests (executable specification)
│   └── architecture_laws.py
├── lens_algebra/        # RFC-0007 Lens algebra property tests
│   ├── lens_laws.py
│   └── run_lens_laws_ci.py
└── integration/         # Integration tests (multi-lens, cross-lens)
    ├── test_shared_lenses.py
    ├── test_lens_architecture.py
    └── test_lens_query.py
```
**Rule:**
- Every test file lives in `tests/` or a subdirectory.
- Test files are NOT in `pond-sdk/` or `lenses/` — those directories contain
  only production code.
- The single entry point is `tests/test_all.py` (run via `pytest tests/test_all.py`).
- Test subdirectories reflect test PURPOSE, not the code being tested.

### 2.7 `scripts/` — Verification scripts

**Contains:** Property tests, differential tests, hazard simulators, benchmarks.
**Rule:** Scripts that verify the architecture (not tests of specific lenses).

### 2.8 `docs/` — Documentation

**Contains:** Whitepaper, formal algebras, RFCs, guides.
**Rule:** Documentation only. No code.

### 2.9 `archive/` — Historical code

**Contains:** Old code preserved for reference.
**Rule:** NOT active. Do not import from archive in production code.
May have broken imports — acceptable since it's not run.

---

## 3. Extension system

Extensions expand Pond's capabilities. They are **independent and generic** —
they work with any lens/storage/abstraction that meets their interface.

### 3.1 Where extensions live

**Decision: extensions stay inside `pond-sdk/extensions/`.**

Extensions depend on pond-sdk infrastructure (UnifiedStorage, collection
manifests, the kernel). Moving them to repo root would create an upward
dependency (extensions → pond-sdk), but pond-sdk is Layer 1 and
extensions are Layer 2 — downward dependency is correct. Extensions ARE
part of the SDK; they're just optional modules within it.

```
pond-sdk/extensions/
├── indexing/              # Collection-level + vector ANN indexes
│   ├── __init__.py        # Package marker + exports
│   ├── base.py            # CollectionIndexerInterface (abstract)
│   ├── collection_index.py # CollectionIndexer (RECOMMENDED — data-side, no lens dependency)
│   ├── ivf_index.py       # IVFIndex — IVF ANN for vectors (Known Gap: reads ALL vectors; see DESIGN_GOALS.md)
│   └── hnsw_index.py      # HNSWIndex — HNSW graph ANN for vectors
├── maintenance/           # GC + vacuum
│   └── vacuum.py          # GarbageCollector — collect() + vacuum(collections, preserve_days)
├── semantic/              # Semantic model adapters (Ossie, future Cube/dbt)
│   ├── base.py            # SemanticModelAdapter (abstract interface)
│   └── ossie.py           # SemanticMixin + SemanticLens + OssieAdapter
└── physical_structures/   # The actual universal storage backend + derived structures
    ├── __init__.py        # Package marker
    ├── unified_storage.py # UnifiedStorage + PND2 — THE universal storage backend (5,540 LOC)
    ├── collection_manifest.py # CollectionManifest — one manifest blob per commit (PMAN format)
    ├── stats_tree.py      # StatsTreeReader — PB-scale hierarchical stats index
    ├── embedded_stats.py  # ColumnStats + value-type constants
    ├── compression.py     # zstd / LZ4 transparent compression
    ├── encoding.py        # ColumnEncoding — FastLanes-style RLE/Dict/Bitpack/Raw
    ├── column_source.py   # ColumnSource — format-agnostic column access protocol
    └── pond_pack.py       # PondPack — commit+manifest in one PNPK blob (storage-side opt)
```

> **Honesty note (Task 65).** Earlier versions of this section listed
> `pruning.py`, `zone_map_index.py`, `pruning_reader.py`,
> `column_chunk_zone_map.py`, `base.py`, `bloom_filter.py`,
> `statistics.py`, `zone_map.py` as the contents of
> `physical_structures/`. **None of those exist there.** They have
> been moved to `archive/legacy-extensions/` and replaced by the
> `UnifiedStorage` + `CollectionManifest` + `StatsTree` stack listed
> above. The pruning/zone-map machinery still exists conceptually
> inside `unified_storage.py`'s read paths (manifest-level row-group
> pruning via embedded stats + stats tree), but the standalone
> `ZoneMapIndex`/`PruningReader` classes are no longer in production.

**Extension categories:**
- **indexing/** — Collection-level indexing + vector ANN (IVF, HNSW).
  CollectionIndexer (recommended, data-side, no lens dependency).
  Indexes belong to collections, not lenses. Any lens can use any
  collection's indexes.
- **maintenance/** — GC + vacuum. Reclaims space from unreachable blobs.
- **semantic/** — Semantic model management (metrics, dimensions, relationships).
  Composable via SemanticMixin. Supported: KeyValueLens and subclasses.
- **physical_structures/** — The universal storage backend and its
  derived structures. `UnifiedStorage` is the production storage path
  for every lens.

### 3.2 Extension principles

1. **Independent:** Extensions do NOT depend on any specific lens. They
   define a minimal interface and work with any lens that meets it.

2. **Generic:** Extensions are composable via mixins. A lens adds an
   extension's capability by mixing in the mixin:
   ```python
   class MySemanticLens(KeyValueLens, SemanticMixin):
       pass
   ```

3. **Documented interface:** Each extension's docstring MUST list the
   attributes/methods the host lens must expose. Example:
   ```
   GENERIC: works with any lens that exposes:
     - self.kernel         — the PondMinimal kernel
     - self.name           — the collection name
     - self.base           — a persistent ProllyLensBase
     - self.put(key, data) — stage a key→value mapping
     ...
   ```

4. **Storage-agnostic where possible:** Extensions work through the lens's
   API, not through direct kernel access. This makes them compatible with
   any lens that uses ProllyTreeIndex (the unified storage backend).

### 3.3 Extension types

- **Mixins** (`SemanticMixin`): composable with KV-style
  lenses. Add methods via multiple inheritance.
- **Physical Structures** (`BloomFilter`, `Statistics`, `ZoneMap`): derived
  structures stored as kernel blobs. Work with any collection (KV or tabular).
- **Adapters** (`OssieAdapter`): translate between Pond's internal format
  and external standards. Plug into mixins.

### 3.4 Extension metadata

Every extension class declares metadata as class attributes so tooling
can introspect what it supports:

```python
```

This lets tools (and humans) answer:
- "Can I use semantic models with LakehouseLens?" → Not yet (SemanticMixin is KV-only).
- "What storage does SemanticMixin require?" → ProllyTreeIndex.
- "What lens types does BloomFilter work with?" → Any (it's a Physical Structure).

---

## 4. Lens composition rules

**Rule:** Main production lenses SHOULD extend `PondLens` directly with no
dependency on other lenses. However, extending one lens on top of another
IS allowed when it makes sense (e.g., `KeylessLens` extends `KeyValueLens`
to auto-generate UUIDv7 keys — a thin, legitimate variant).

**Principles:**
1. **No extra dependency for main lenses.** KeyValueLens, LakehouseLens,
   VectorLens, StreamingLens, OLTPLens — these should each extend PondLens
   directly and own their storage code. They should NOT import from each
   other. This keeps each main lens independently removable.
2. **Extension is allowed for variants.** A lens that extends another lens
   (like `KeylessLens(KeyValueLens)`) is fine IF:
   - It's documented in the class docstring
   - It overrides a small number of methods (thin wrapper)
   - Removing it doesn't affect any other lens
3. **Composition via PondLens is preferred.** If two lenses share logic,
   the shared logic should go in `PondLens` (the base class) or in an
   extension/mixin — not in a lens-to-lens dependency.

**Exception:** Lenses in `pond-labs/` (experimental) may inherit from
production lenses during prototyping. But before promotion to `lenses/`,
the inheritance must be removed and the lens must own its own code.

**Example:** `FeatureStoreLens` (in `pond-labs/lenses/`) previously
inherited from `LakehouseLens`. After applying this rule, it now extends
`PondLens` directly and has its own ProllyTreeIndex storage code
(duplicated from LakehouseLens). This is intentional.

---

## 5. Naming conventions

### 5.1 Files

- **Kernel:** `kernel.py` (not `pond_minimal.py`)
- **Base lens:** `base_lens.py` (not `pond_lens.py`)
- **Prolly tree:** `prolly_tree.py` (not `prolly_view.py`)
- **Row query:** `row_query.py` (not `query.py` — avoids confusion with
  "the query method for data in Pond")
- **Indexing extension:** `indexing.py` (not `collection_index.py`)
- **Lens files:** `{role}_lens.py` (e.g., `keyvalue_lens.py`, `lakehouse_lens.py`,
  `vector_lens.py`)
- **Test files:** `test_{purpose}.py` (e.g., `test_shared_lenses.py`)

### 5.2 Classes

- **Kernel:** `PondMinimal`
- **Base lens:** `PondLens`
- **Lenses:** `{Name}Lens` (e.g., `KeyValueLens`, `LakehouseLens`, `FeatureStoreLens`)
- **Mixins:** `{Capability}Mixin` (e.g., `SemanticMixin`)
- **Storage:** `ProllyLensBase`, `ProllyTree`
- **Query:** `LensQuery` (the row-level lazy query builder)

### 5.3 Folders

- **Production lenses:** `lenses/{lens_name}/` (e.g., `lenses/lakehouse/`)
- **Lab lenses:** `pond-labs/lenses/` (flat, not per-lens subdirectory)
- **Tests:** `tests/{type}/` (e.g., `tests/architecture/`, `tests/integration/`)
- **Extensions:** `pond-sdk/extensions/{category}/` (e.g., `pond-sdk/extensions/semantic/`)

---

## 6. Promotion process

When code in `pond-labs/` is approved for production:

1. **Document the decision** in `worklog.md` with:
   - What is being promoted
   - Why it's ready (tests pass, design review complete, etc.)
   - Where it's going

2. **Move the code** via `git mv`:
   - Lab lens → `lenses/{lens_name}/`
   - Lab service → `services/{service_name}/`
   - Lab extension → `pond-sdk/extensions/{category}/`

3. **Update imports** across the codebase (use `scripts/update_imports.py`
   as a template).

4. **Remove lens-to-lens inheritance** if any exists (see §4).

5. **Update `KNOWLEDGE_GRAPH.md`** with the new file paths.

6. **Run `tests/test_all.py`** to verify nothing broke.

7. **Update `tests/test_all.py`** if the script path changed.

---

## 7. Dependency rules

```
pond-core (kernel.py 274 LOC + storage backends; NOT FROZEN — gained write_batch / read_blob_batch)
    │
    ├── pond-sdk (depends on pond-core only)
    │   ├── base_lens.py ← pond-core
    │   ├── pond_storage.py ← pond-core, extensions/physical_structures/unified_storage
    │   ├── pond_config.py ← pond-core
    │   ├── maintenance.py ← pond-core
    │   ├── row_query.py ← (no deps)
    │   ├── uuid7.py ← (no deps)
    │   ├── hlc.py ← (no deps)
    │   └── extensions/ ← pond-sdk core + pond-core
    │       ├── indexing/{collection_index,ivf_index,hnsw_index}.py ← pond-core, pond-sdk
    │       ├── maintenance/vacuum.py ← pond-core, pond-sdk
    │       ├── semantic/{base,ossie}.py ← pond-sdk
    │       └── physical_structures/
    │           ├── unified_storage.py ← pond-core, encoding, compression, collection_manifest, stats_tree, pond_pack
    │           ├── collection_manifest.py ← pond-core, embedded_stats, stats_tree
    │           ├── stats_tree.py ← embedded_stats
    │           ├── embedded_stats.py ← (no deps)
    │           ├── compression.py ← zstandard (external)
    │           ├── encoding.py ← (no deps)
    │           ├── column_source.py ← (no deps, optional pyarrow)
    │           └── pond_pack.py ← (no deps)
    │
    ├── lenses/ (depend on pond-core + pond-sdk, NEVER on each other)
    │   ├── keyvalue/ ← pond-sdk (extends PondLens)
    │   ├── lakehouse/ ← pond-core, pond-sdk, duckdb (optional), pyarrow (NO base class)
    │   ├── vector/ ← pond-sdk (extends PondLens)
    │   ├── streaming/ ← pond-sdk (extends PondLens)
    │   └── oltp/ ← pond-sdk (NO base class — thin memtable + batch flush)
    │
    ├── services/ (depend on pond-core only)
    │   ├── transport/ ← pond-core
    │   ├── schema/ ← pond-core
    │   └── replication/ ← pond-core
    │
    └── pond-labs/ (depend on pond-core + pond-sdk + lenses)
        ├── lenses/ ← pond-core, pond-sdk
        ├── tracks/ ← pond-core, pond-sdk, lenses
        ├── demos/ ← pond-core, pond-sdk, lenses
        └── benchmarks/ ← pond-core, pond-sdk, lenses
```

**Rules:**
- No Lens depends on another Lens.
- Lenses depend only on `pond-sdk` (and `pond-core`).
- `pond-sdk` depends only on `pond-core`.
- `pond-core` depends on nothing.
- Services depend only on `pond-core` (not on `pond-sdk`).
- `pond-labs` depends on everything (it's experimental).
- `LakehouseLens` and `OLTPLens` declare no base class — documented
  exceptions (see §2.3 and `SDK_SPEC.md`).

---

## 8. The weekly question

From `DESIGN_GOALS.md` §4:

> Every week, ask: **If I deleted everything except `pond-core` and
> `pond-sdk`, would the architecture still make sense?**

If the answer is "no" — if the architecture only makes sense with a
specific lens or service present — the design has leaked.

These rules exist to keep the answer "yes."
