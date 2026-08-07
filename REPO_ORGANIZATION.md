# Pond Repository Organization Rules

> This document codifies the folder structure, naming conventions, and
> promotion process for the Pond repository. Every agent (human or AI)
> working on Pond MUST follow these rules.

---

## 1. Top-level folder structure

```
pond_repo/
├── core/                    # Language-AGNOSTIC Rust crates
│   ├── kernel/              # 3 primitives + ObjectStore trait + CRDT
│   ├── storage/             # UnifiedStorage (versioning, branching, shards)
│   ├── codec/               # PND2 encode/decode
│   ├── arrow/               # PND2 → Arrow bridge
│   └── s3/                  # S3-compatible object store (SigV4)
├── cli/                     # `pond` CLI binary (application, not core library)
├── bindings/                # Language-specific bindings
│   ├── base/                # Shared cross-language files (pond.h, C tests, blobs)
│   ├── python/
│   │   ├── pyo3/            # PyO3 Rust crate (produces pond.so)
│   │   ├── sdk/             # Python SDK (PondStorage, lenses, extensions)
│   │   └── core/            # Python reference kernel (being migrated to Rust)
│   └── go/                  # Go SDK (cgo wrapper around C ABI)
├── lenses/                  # Workload-specific lenses (rust/python subdirs)
│   ├── base/                # Lens protocol (C ABI placeholder)
│   ├── keyvalue/{python,rust}/
│   ├── lakehouse/{python,rust}/
│   ├── oltp/{python,rust}/
│   ├── streaming/{python,rust}/
│   └── vector/{python,rust}/
├── services/                # Cross-cutting services (transport, schema, replication)
├── pond-labs/               # Experiments and demos
├── tests/                   # All tests (architecture, integration, lens algebra)
├── scripts/                 # Verification scripts (property tests, benchmarks)
├── docs/                    # Documentation
├── tla/                     # TLA+ formal specification
├── archive/                 # Historical code (not active)
└── (top-level docs)         # README.md, DESIGN_GOALS.md, PACKAGES.md, etc.
```

---

## 2. Folder rules

### 2.1 `core/` — Language-agnostic Rust crates

**Contains:** The Rust core implementation. These crates have NO Python
dependency. They are the canonical implementation that all language
bindings call (directly or via C ABI).

**Crates:**

| Crate | Path | Purpose | Deps |
|---|---|---|---|
| `pond_kernel` | `core/kernel/` | 3 primitives (Write, Read, Ref) + `ObjectStore` trait + CRDT (UUIDv7, HLC) + `LocalFSObjectStore` | `sha2` only |
| `pond_storage` | `core/storage/` | `UnifiedStorage` — versioning, branching, shards, merge, history, undo, revert | `pond_kernel`, `serde_json` |
| `pond_codec` | `core/codec/` | PND2 binary format — encode/decode all encodings (RAW/RLE/DICT/BITPACK) × all vtypes (INT64/FLOAT64/STRING/BINARY/NULL) | zero deps |
| `pond_arrow` | `core/arrow/` | PND2 → Apache Arrow direct conversion (near-zero copy) | `arrow`, `pond_codec` |
| `pond_s3` | `core/s3/` | S3-compatible object store (AWS S3, R2, MinIO, etc.) — SigV4 signing from scratch | `pond_kernel`, `pond_storage`, `ureq`, `sha2`, `hex`, `chrono`, `url` |

**Rule:** `core/` crates must not depend on any language-specific binding.
They are pure Rust. The `ObjectStore` trait in `core/kernel/` is the
extension point for storage backends.

**Rule:** `core/kernel/` stays minimal-dep (only `sha2`). Storage backends
that need HTTP/crypto deps (like `core/s3/`) are separate crates.

### 2.2 `cli/` — The `pond` command

**Contains:** The CLI binary. This is an APPLICATION, not a core library.
It depends on `core/kernel`, `core/storage`, and optionally `core/s3`.

**Rule:** The CLI is a thin UI layer over `pond_storage`. No business
logic lives here.

**S3 support:** Enabled by default (`--features s3`). Disable with
`--no-default-features` to build a local-only CLI without the `pond_s3`
dependency.

### 2.3 `bindings/` — Language-specific bindings

**Contains:** Everything that ties the Rust core to a specific language.

#### 2.3a `bindings/base/` — Shared cross-language files

- `pond.h` — The unified C ABI header (kernel + storage + codec + S3)
- `test_c_abi.c` — C ABI tests for codec
- `test_storage_c_abi.c` — C ABI tests for storage
- `generate_test_blobs.py` — Generates PND2 blobs for cross-language tests
- `test_blobs/` — Binary PND2 blobs (used by Go, future Java/Node SDKs)

**Rule:** These files are shared by ALL language SDKs. They define the
contract.

#### 2.3b `bindings/python/` — Python bindings + SDK + reference kernel

```
bindings/python/
├── pyo3/                    # PyO3 Rust crate (produces pond.so)
│   ├── Cargo.toml           # depends on core/codec
│   └── src/lib.rs           # thin PyO3 glue
├── sdk/                     # Python SDK (moved from pond-sdk/)
│   ├── __init__.py
│   ├── base_lens.py         # PondLens base class
│   ├── pond_storage.py      # PondStorage — unified SDK entry point
│   ├── pond_config.py       # PondConfig
│   ├── hlc.py               # Hybrid Logical Clock (CRDT)
│   ├── uuid7.py             # UUIDv7 generation
│   ├── row_query.py         # LensQuery (lazy row-level query builder)
│   ├── maintenance.py       # drop_name, is_dropped, etc.
│   └── extensions/
│       ├── indexing/        # IVF, HNSW, CollectionIndexer
│       ├── maintenance/     # GC, vacuum
│       ├── semantic/        # Ossie semantic adapter
│       └── physical_structures/ # UnifiedStorage, PND2, CollectionManifest
└── core/                    # Python reference kernel (moved from pond-core/)
    ├── __init__.py
    ├── kernel.py            # PondMinimal (3 primitives)
    ├── object_store_native_kernel.py # ObjectStoreNativeKernel
    ├── local_fs_object_store.py
    ├── s3_object_store.py   # Python S3 backend (boto3)
    ├── s3_mock_backend.py
    └── make_kernel.py       # make_kernel(url) — factory
```

**Rule:** `bindings/python/sdk/` depends on `bindings/python/core/` only.
`bindings/python/core/` is the Python reference kernel being migrated to
Rust. `bindings/python/pyo3/` is a thin Rust crate that delegates to
`core/codec`.

#### 2.3c `bindings/go/` — Go SDK

```
bindings/go/
├── go.mod                   # module github.com/pond/pond-go
├── pond/                    # Public Go API (import this package)
│   ├── pond.go              # Result, Column, Encoder, Storage
│   ├── pond_test.go         # Tests
│   └── pond_bench_test.go   # Benchmarks
└── internal/cabi/           # Private cgo layer (wraps pond.h)
    └── cabi.go
```

**Rule:** `bindings/go/` depends on `core/kernel`, `core/storage`,
`core/codec` via cgo over the static libraries. It does NOT depend on
Python.

### 2.4 `lenses/` — Workload-specific lenses

**Contains:** Production-ready lens implementations. Each lens lives in
its own subdirectory with `python/` and `rust/` subdirectories:

```
lenses/{lens_name}/
├── python/          # Python implementation (production)
├── rust/            # Placeholder for future Rust port
└── README.md        # Lens-specific docs
```

**Current lenses:**
- `lenses/keyvalue/` — KeyValueLens, KeylessLens
- `lenses/lakehouse/` — LakehouseLens (with DuckDB SQL)
- `lenses/oltp/` — OLTPLens (memtable + batch flush)
- `lenses/streaming/` — StreamingLens (Kafka-like)
- `lenses/vector/` — VectorLens (with IVF ANN)

**`lenses/base/`** contains the placeholder C ABI header (`pond_lens.h`)
for the future cross-language lens protocol.

**Rule:** No lens-to-lens inheritance. Each production lens owns its own
storage code, even if that means duplication. This keeps lenses
independent and removable.

**Rule:** Lenses depend on `bindings/python/sdk/` (and `bindings/python/core/`),
NEVER on each other.

**Migration plan:** All lenses today are Python-only. When a lens is
ported to Rust, the implementation goes in `rust/`, the C ABI goes in
`lenses/base/pond_lens.h`, and the Python wrapper in `python/` becomes a
thin PyO3 binding. The first lens to be ported will be KeyValueLens.

### 2.5 `services/` — Cross-cutting services

**Contains:** Services that sit between the kernel and lenses.
**Current:** `services/transport/`, `services/schema/`, `services/replication/`
**Rule:** Services depend only on `bindings/python/core/` (not on SDK or lenses).

### 2.6 `pond-labs/` — Development & experimental code

**Contains:** Code that is NOT yet production-ready.
**Rule:** Code here is experimental. It may break. It may be deleted.
**Promotion:** When approved, move to `lenses/`, `services/`, or `bindings/python/sdk/extensions/`.

### 2.7 `tests/` — All tests

```
tests/
├── test_all.py              # Single pytest entry point
├── architecture/            # Architecture law tests (executable spec)
├── lens_algebra/            # RFC-0007 Lens algebra property tests
└── integration/             # Integration tests (multi-lens, cross-lens)
```

### 2.8 `scripts/` — Verification scripts

**Contains:** Property tests, differential tests, hazard simulators, benchmarks.
**Rule:** Scripts that verify the architecture (not tests of specific lenses).

### 2.9 `docs/` — Documentation

**Contains:** Whitepaper, formal algebras, RFCs, guides, design documents.
**Rule:** Documentation only. No code.

### 2.10 `tla/` — TLA+ formal specification

**Contains:** TLA+ specification of the kernel, verified with TLC model checker.

### 2.11 `archive/` — Historical code

**Contains:** Old code preserved for reference.
**Rule:** NOT active. Do not import from archive in production code.

---

## 3. Extension system

Extensions expand Pond's capabilities. They live in
`bindings/python/sdk/extensions/` and are **independent and generic** —
they work with any lens/storage that meets their interface.

```
bindings/python/sdk/extensions/
├── indexing/              # Collection-level + vector ANN indexes
├── maintenance/           # GC + vacuum
├── semantic/              # Semantic model adapters
└── physical_structures/   # Universal storage backend + derived structures
```

---

## 4. Lens composition rules

**Rule:** Main production lenses SHOULD extend `PondLens` directly with no
dependency on other lenses. Extending one lens on top of another IS allowed
for thin variants (e.g., `KeylessLens` extends `KeyValueLens`).

**Principles:**
1. No extra dependency for main lenses.
2. Extension is allowed for thin variants.
3. Composition via `PondLens` (base class) or mixins is preferred.

---

## 5. Naming conventions

### 5.1 Files
- **Kernel:** `kernel.py`
- **Base lens:** `base_lens.py`
- **Lens files:** `{role}_lens.py` (e.g., `keyvalue_lens.py`)
- **Test files:** `test_{purpose}.py`

### 5.2 Classes
- **Kernel:** `PondMinimal`
- **Base lens:** `PondLens`
- **Lenses:** `{Name}Lens` (e.g., `KeyValueLens`)
- **Storage:** `UnifiedStorage`, `PondStorage`

### 5.3 Folders
- **Production lenses:** `lenses/{lens_name}/python/` (and `rust/`)
- **Lab lenses:** `pond-labs/lenses/` (flat)
- **Tests:** `tests/{type}/`
- **Extensions:** `bindings/python/sdk/extensions/{category}/`

---

## 6. Promotion process

When code in `pond-labs/` is approved for production:
1. Document the decision in `worklog.md`
2. Move the code via `git mv`
3. Update imports across the codebase
4. Remove lens-to-lens inheritance if any exists
5. Update `KNOWLEDGE_GRAPH.md`
6. Run `tests/test_all.py`

---

## 7. Dependency rules

```
core/kernel (zero deps except sha2)
    │
    ├── core/storage (depends on core/kernel)
    ├── core/codec (zero deps)
    ├── core/arrow (depends on core/codec + arrow crate)
    └── core/s3 (depends on core/kernel + core/storage + ureq/sha2/hex/chrono/url)
        │
        ├── cli (depends on core/kernel + core/storage + optionally core/s3)
        │
        ├── bindings/python/pyo3 (depends on core/codec + PyO3)
        │   ↑ used by
        ├── bindings/python/sdk (Python SDK — depends on bindings/python/core)
        │   ├── bindings/python/core (Python reference kernel)
        │   └── extensions/ (depend on SDK + core)
        │
        └── bindings/go (depends on core/kernel + core/storage + core/codec via cgo)

lenses/{name}/python/ (depend on bindings/python/sdk + bindings/python/core)
services/ (depend on bindings/python/core only)
pond-labs/ (depends on everything)
```

**Rules:**
- No Lens depends on another Lens.
- Lenses depend only on `bindings/python/sdk/` (and `bindings/python/core/`).
- `bindings/python/sdk/` depends only on `bindings/python/core/`.
- `core/` crates have no Python dependency.
- `core/kernel/` has minimal deps (sha2 only).
- `core/s3/` is a separate crate for S3 HTTP/SigV4 deps.

---

## 8. The weekly question

> Every week, ask: **If I deleted everything except `core/` and
> `bindings/`, would the architecture still make sense?**

If the answer is "no" — if the architecture only makes sense with a
specific lens or service present — the design has leaked.

These rules exist to keep the answer "yes."
