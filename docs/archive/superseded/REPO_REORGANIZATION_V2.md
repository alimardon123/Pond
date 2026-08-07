# Repository Reorganization V2 — Refined Design

> **Date:** 2026-08-07 (revised after user feedback)
> **Purpose:** Refined reorganization addressing 4 questions from the user:
>   1. Why both `core/python/` and `sdk/python/`?
>   2. Should CLI be inside `core/`?
>   3. How to organize extensions/lenses from multiple languages?
>   4. Code review: optimizations, comments, READMEs needed?

---

## 1. Why both `core/python/` and `sdk/python/`?

**Problem**: `core/python/` (PyO3 Rust crate) and `sdk/python/` (Python
SDK with lenses) are both "python" but are fundamentally different things.

**Analysis**: The PyO3 wrapper is a **language binding** — it exposes the
Rust core to Python. It's not part of the language-agnostic core. The
core (kernel, storage, codec, arrow) doesn't know about Python. The PyO3
crate is Python-specific infrastructure.

**Decision**: Move PyO3 wrapper OUT of `core/` into `sdk/python/`.

```
core/                        # Language-AGNOSTIC Rust crates
├── kernel/                  # 3 primitives + ObjectStore + CRDT
├── storage/                 # UnifiedStorage (versioning, branching, shards)
├── codec/                   # PND2 encode/decode
└── arrow/                   # PND2 → Arrow bridge

sdk/                         # Language-SPECIFIC bindings + SDKs
├── base/                    # Shared cross-language files (pond.h, C ABI tests)
├── python/                  # Python SDK
│   ├── pyo3/                # PyO3 Rust crate (produces pond_rust.so)
│   │   ├── Cargo.toml       # depends on core/kernel, core/storage, core/codec
│   │   └── src/lib.rs       # thin PyO3 glue
│   ├── base_lens.py         # Python SDK (lenses, extensions)
│   ├── pond_storage.py
│   └── extensions/
└── go/                      # Go SDK
    ├── pond/                # Public Go API
    └── internal/cabi/       # Private cgo layer
```

The Rust workspace spans multiple directories:
```toml
# Cargo.toml (at repo root)
[workspace]
members = [
    "core/kernel",
    "core/storage",
    "core/codec",
    "core/arrow",
    "cli",
    "sdk/python/pyo3",
]
```

This is clean:
- `core/` has ONLY language-agnostic Rust crates
- `sdk/python/pyo3/` is the Python binding (Rust crate, but in the SDK directory)
- `sdk/python/*.py` is the Python SDK (lenses, extensions)
- No naming collision

---

## 2. Should CLI be inside `core/`?

**No.** The CLI is an **application**, not a library. It links against
the core but IS not the core. It's a user-facing tool.

**Decision**: Move CLI to top-level `cli/`.

```
pond_repo/
├── core/                    # Language-agnostic Rust crates
├── cli/                     # The `pond` CLI binary (application, not core)
│   ├── Cargo.toml           # depends on core/kernel, core/storage
│   ├── src/main.rs
│   ├── tests/cli_integration.rs
│   └── README.md
├── sdk/                     # Language SDKs
├── lenses/                  # Lens implementations
└── ...
```

This follows the Unix philosophy: `core/` is the library, `cli/` is the
application that uses it. Like how `git` has `libgit2/` (library) and
`git` (CLI) as separate things.

---

## 3. How to organize extensions/lenses from multiple languages?

### The problem

We want:
- Rust lenses (the reference implementation)
- Python lenses (call Rust via PyO3, or pure Python for experimental ones)
- Future: C/C++ lenses (loaded as plugins via C ABI)
- Future: Go/Java lenses

Each lens/extension may have BOTH a Rust implementation AND a Python
wrapper. We need a structure that makes this clear.

### The solution: per-lens directories with language subdirectories

```
lenses/                          # Lens implementations
├── keyvalue/                    # KeyValueLens
│   ├── rust/                    # Rust implementation (the reference)
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   ├── python/                  # Python wrapper (calls Rust via PyO3)
│   │   └── keyvalue_lens.py
│   └── README.md                # Documents the lens + both implementations
├── lakehouse/
│   ├── rust/
│   ├── python/
│   └── README.md
├── vector/
│   ├── rust/
│   ├── python/
│   └── README.md
├── streaming/
│   ├── rust/
│   ├── python/
│   └── README.md
└── oltp/
    ├── rust/
    ├── python/
    └── README.md

extensions/                      # Data-side extensions (cross-language)
├── base/                        # Plugin protocol definition
│   ├── pond_plugin.h            # C ABI plugin protocol (Phase 3)
│   └── README.md                # How to write a lens/extension in any language
├── indexing/                    # CollectionIndexer
│   ├── rust/
│   ├── python/
│   └── README.md
├── physical_structures/         # PND2, manifest, pruning, zone maps
│   ├── rust/
│   ├── python/
│   └── README.md
└── semantic/                    # Semantic model adapters
    ├── rust/
    ├── python/
    └── README.md
```

### Why this works:

1. **Each lens is self-contained**: everything about KeyValueLens is in
   `lenses/keyvalue/`. A developer working on KeyValueLens knows exactly
   where to look.

2. **Language implementations are clearly separated**: `rust/` is the
   reference, `python/` is the wrapper. Adding `c/` or `go/` later is
   natural.

3. **Extensions follow the same pattern**: `extensions/indexing/` has
   both `rust/` and `python/` subdirectories.

4. **Plugin protocol lives in `extensions/base/`**: the C ABI plugin
   definition (`pond_plugin.h`) is the contract for cross-language
   extensions. Any language that can produce a shared library with a C
   entry point can write a lens.

5. **No implicit coupling**: each lens directory is independent. Removing
   `lenses/vector/` doesn't affect `lenses/keyvalue/`.

### What about the Python SDK extensions currently in `bindings/python/sdk/extensions/`?

They move to `extensions/`:
- `bindings/python/sdk/extensions/indexing/` → `extensions/indexing/python/`
- `bindings/python/sdk/extensions/semantic/` → `extensions/semantic/python/`
- `bindings/python/sdk/extensions/physical_structures/` → `extensions/physical_structures/python/`

The Rust implementations (when ported) go in the `rust/` subdirectory.

---

## 4. Code review: optimizations, rewriting, comments, READMEs

### 4.1 Code splitting needed

**`core/codec/src/lib.rs` (1,773 LOC)** — too large for one file. Split into:
```
core/codec/src/
├── lib.rs          # Module declarations + public API re-exports (~50 LOC)
├── constants.rs    # PND2_MAGIC, VT_*, ENC_* constants (~30 LOC)
├── parser.rs       # PND2Parser struct + impl (~100 LOC)
├── types.rs        # PondColumn struct + impl (~100 LOC)
├── decode.rs       # pnd2_decode + decode_column + decode_raw/bitpack/dict/rle (~600 LOC)
├── encode.rs       # pnd2_encode_i64/f64/str/multi + EncodeMultiColumn (~300 LOC)
└── c_abi.rs        # All #[no_mangle] extern "C" functions (~700 LOC)
```

**`core/kernel/src/lib.rs` (693 LOC)** — manageable but could be cleaner:
```
core/kernel/src/
├── lib.rs          # Module declarations + PondKernel struct + 3 primitives (~200 LOC)
├── object_store.rs # ObjectStore trait + LocalFSObjectStore (~250 LOC)
├── crdt.rs         # UUIDv7 + HLC (already separate, ~355 LOC)
└── c_abi.rs        # C ABI wrappers (~200 LOC)
```

**`core/storage/src/lib.rs` (452 LOC)** — already well-split into 9 modules.
The C ABI section (~270 LOC) could move to `c_abi.rs`:
```
core/storage/src/
├── lib.rs          # UnifiedStorage struct + ref namespace (~180 LOC)
├── c_abi.rs        # C ABI wrappers (~270 LOC) ← NEW
├── commit.rs       # (existing)
├── manifest.rs     # (existing)
├── branch.rs       # (existing)
├── shard.rs        # (existing)
├── read.rs         # (existing)
├── write.rs        # (existing)
├── transaction.rs  # (existing)
└── maintenance.rs  # (existing)
```

### 4.2 Code optimizations needed

1. **`fill_random` in `crdt.rs`** — uses a simple xorshift PRNG. For
   production, should use the `rand` crate or `/dev/urandom`. Low priority
   (random bits only need to be unique within the same millisecond).

2. **`extract_field` in CLI** — hand-rolled JSON parser. Should use
   `serde_json` properly (the storage layer already does).

3. **`walk_dir` in kernel** — recursive directory walk. Fine for local FS,
   but S3 backend will need a different implementation (list_objects_v2).

4. **`pnd2_encode_i64` computes min/max twice** — once in the pure-Rust
   function and once in the C ABI wrapper. Minor, but worth noting.

### 4.3 Comments and READMEs needed

Every file should have:
- A **file header comment** explaining what the file does and its role
- A **module doc comment** (`//!`) for the crate's `lib.rs`
- **Function doc comments** (`///`) for all public functions
- **Inline comments** for non-obvious logic

Every directory should have:
- A **README.md** explaining the directory's purpose, what's inside,
  and how it relates to the rest of the project

Current state:
- Rust files: mostly well-commented ✓
- Python files: mostly well-commented ✓
- READMEs: many exist but some are stale after the migration ✗
- Missing READMEs: `core/` (no workspace README), `extensions/` (none)

### 4.4 Documentation updates needed

After the reorganization:
1. `REPO_ORGANIZATION.md` — update to reflect the new structure
2. `PACKAGES.md` — update the dependency graph
3. `KNOWLEDGE_GRAPH.md` — update all file paths
4. `README.md` — update the repository structure section
5. `DESIGN_GOALS.md` — update file references
6. `SDK_SPEC.md` — update file references
7. `docs/CROSS_LANGUAGE_SDK_ARCHITECTURE.md` — update paths
8. `docs/MIGRATION_STRATEGY.md` — update paths

---

## Final proposed structure

```
pond_repo/
├── Cargo.toml                    # Rust workspace manifest (at root)
│
├── core/                         # Language-AGNOSTIC Rust crates
│   ├── kernel/                   # 3 primitives (Write, Read, Ref) + ObjectStore + CRDT
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── lib.rs            # PondKernel + 3 primitives
│   │   │   ├── object_store.rs   # ObjectStore trait + LocalFSObjectStore
│   │   │   ├── crdt.rs           # UUIDv7 + HLC
│   │   │   └── c_abi.rs          # C ABI wrappers
│   │   └── README.md
│   ├── storage/                  # UnifiedStorage (versioning, branching, shards, CRDT merge)
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── lib.rs            # UnifiedStorage struct + ref namespace
│   │   │   ├── c_abi.rs          # C ABI wrappers
│   │   │   ├── commit.rs
│   │   │   ├── manifest.rs
│   │   │   ├── branch.rs
│   │   │   ├── shard.rs
│   │   │   ├── read.rs
│   │   │   ├── write.rs
│   │   │   ├── transaction.rs
│   │   │   └── maintenance.rs
│   │   └── README.md
│   ├── codec/                    # PND2 encode/decode (all encodings, all vtypes)
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── lib.rs            # Module declarations + re-exports
│   │   │   ├── constants.rs
│   │   │   ├── parser.rs
│   │   │   ├── types.rs
│   │   │   ├── decode.rs
│   │   │   ├── encode.rs
│   │   │   └── c_abi.rs
│   │   └── README.md
│   └── arrow/                    # PND2 → Arrow bridge
│       ├── Cargo.toml
│       ├── src/lib.rs
│       └── README.md
│
├── cli/                          # The `pond` CLI binary (application, not core)
│   ├── Cargo.toml
│   ├── src/main.rs
│   ├── tests/cli_integration.rs
│   └── README.md
│
├── sdk/                          # Language SDKs (thin wrappers, no logic)
│   ├── base/                     # Shared cross-language files
│   │   ├── pond.h                # Unified C ABI header
│   │   ├── test_storage_c_abi.c  # C ABI test
│   │   ├── test_blobs/           # Binary PND2 blobs for compat tests
│   │   ├── generate_test_blobs.py
│   │   └── README.md             # How to add a new language SDK
│   ├── python/                   # Python SDK
│   │   ├── pyo3/                 # PyO3 Rust crate (→ pond_rust.so)
│   │   │   ├── Cargo.toml
│   │   │   └── src/lib.rs
│   │   ├── base_lens.py
│   │   ├── pond_storage.py
│   │   ├── row_query.py
│   │   ├── pond_config.py
│   │   └── README.md
│   └── go/                       # Go SDK
│       ├── go.mod
│       ├── pond/
│       ├── internal/cabi/
│       └── README.md
│
├── lenses/                       # Lens implementations (Rust + Python wrappers)
│   ├── keyvalue/
│   │   ├── rust/
│   │   ├── python/
│   │   └── README.md
│   ├── lakehouse/
│   ├── vector/
│   ├── streaming/
│   └── oltp/
│
├── extensions/                   # Data-side extensions (cross-language)
│   ├── base/                     # Plugin protocol (C ABI)
│   │   ├── pond_plugin.h
│   │   └── README.md
│   ├── indexing/
│   │   ├── rust/
│   │   ├── python/
│   │   └── README.md
│   ├── physical_structures/
│   │   ├── rust/
│   │   ├── python/
│   │   └── README.md
│   └── semantic/
│       ├── rust/
│       ├── python/
│       └── README.md
│
├── services/                     # Cross-cutting services
├── labs/                         # Experiments (renamed from pond-labs/)
├── tests/                        # Integration tests
├── docs/                         # Documentation
├── scripts/                      # Verification scripts
├── tla/                          # TLA+ formal specification
├── archive/                      # Historical code
└── (top-level docs)              # README.md, DESIGN_GOALS.md, etc.
```

### Summary of changes from V1:

| V1 | V2 | Reason |
|---|---|---|
| `core/python/` | `sdk/python/pyo3/` | PyO3 is a language binding, not core |
| `core/cli/` | `cli/` (top level) | CLI is an application, not core |
| No extensions structure | `extensions/{name}/{rust,python}/` | Multi-language lens/extension support |
| No lens language split | `lenses/{name}/{rust,python}/` | Each lens has Rust ref + Python wrapper |
| `pond.h` in `core/` | `pond.h` in `sdk/base/` | Shared contract for all SDKs |

### Execution plan (5 phases):

1. **Phase 1**: Rename `` → `core/`, rename internal crates (pond-core→codec, pond-kernel→kernel, pond-storage→storage, pond-arrow→arrow, pond-python→sdk/python/pyo3, pond-cli→cli)
2. **Phase 2**: Split large files (codec lib.rs into 7 files, kernel lib.rs into 4 files, storage C ABI into separate file)
3. **Phase 3**: Create `sdk/base/` with shared files (pond.h, C ABI tests, test blobs)
4. **Phase 4**: Move `bindings/go/` → `sdk/go/`, move `bindings/python/sdk/` → `sdk/python/`
5. **Phase 5**: Reorganize `lenses/` and `extensions/` with rust/python subdirectories
6. **Phase 6**: Update all READMEs, comments, and documentation
