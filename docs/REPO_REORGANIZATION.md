# Repository Reorganization — Design Document

> **Date:** 2026-08-07
> **Purpose:** Review current repo structure, identify problems, propose
> a clean reorganization that follows our design principles.

---

## Current structure (problems identified)

```
pond_repo/
├── bindings/python/core/          # Python kernel ← NAME COLLISION with core/codec/
├── bindings/python/sdk/           # Python SDK (reference implementation)
├──           # Rust workspace
│   ├── bindings/python/core/      # PND2 codec ← CONFUSING: same name as Python kernel
│   ├── pond-kernel/    # Storage kernel (3 primitives + ObjectStore + CRDT)
│   ├── pond-storage/   # UnifiedStorage (versioning, branching, shards)
│   ├── pond-arrow/     # Arrow bridge
│   ├── pond-python/    # PyO3 wrapper
│   ├── pond-cli/       # CLI binary
│   ├── pond.h          # Unified C ABI header ← should be shared, not inside 
│   └── tests/          # C ABI tests
├── bindings/go/             # Go SDK ← INCONSISTENT naming (sdk-go vs pond-*)
├── pond/               # Pip shim ← UNCLEAR purpose
├── lenses/             # Python lenses
├── ...
```

### Problems:

1. **Name collision**: `bindings/python/core/` (Python kernel, ~5 files) and `core/codec/` (Rust PND2 codec, 1 file) share the same name but are completely different things. A developer seeing "bindings/python/core" doesn't know which one is meant.

2. **Naming inconsistency**: `bindings/python/core/`, `bindings/python/sdk/`, ``, `bindings/go/`, `pond/` — no consistent convention. Some use `pond-` prefix, some use `sdk-` prefix, one has no prefix.

3. **C ABI header buried**: `pond.h` is inside `` but it's the shared contract for ALL language SDKs. It should be in a shared location.

4. **Kernel 3 primitives not clearly separated**: The `pond-kernel` crate mixes the 3 primitives (Write, Read, Ref) with the ObjectStore trait, LocalFSObjectStore, CRDT utilities, and C ABI wrappers. The 3 primitives ARE the core — everything else is supporting infrastructure.

5. **Go SDK naming**: `bindings/go/` doesn't match the `pond-*` convention and doesn't live under a unified `sdk/` directory.

6. **`pond/` directory unclear**: It's a pip packaging shim but isn't documented in the top-level structure.

7. **No shared SDK base**: Language SDKs need shared files (C ABI header, build scripts, documentation) but there's no `sdk/base/` or equivalent.

---

## Proposed structure

```
pond_repo/
├── core/                           # Rust workspace (the production implementation)
│   ├── Cargo.toml                  # workspace manifest
│   ├── pond.h                      # → symlink to ../sdk/base/pond.h (or keep here, see below)
│   ├── kernel/                     # 3 primitives + ObjectStore trait + CRDT + C ABI
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── lib.rs              # PondKernel (Write, Read, Ref) + ObjectStore trait
│   │   │   │                       # + LocalFSObjectStore + C ABI wrappers
│   │   │   └── crdt.rs             # UUIDv7 + HLC (CRDT utilities)
│   │   └── README.md
│   ├── storage/                    # UnifiedStorage (versioning, branching, shards, CRDT merge)
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── lib.rs              # UnifiedStorage struct + ref namespace + C ABI
│   │   │   ├── commit.rs           # Commit format + history walking
│   │   │   ├── manifest.rs         # CollectionManifest (PMAN binary format)
│   │   │   ├── branch.rs           # branch, checkout, merge (O(conflicting)), undo, revert
│   │   │   ├── shard.rs            # CRDT shards (append, upsert, delete, merge_rows_by_rowid)
│   │   │   ├── read.rs             # read, read_at_snapshot, read_full
│   │   │   ├── write.rs            # write (creates commit)
│   │   │   ├── transaction.rs      # atomic publication (begin_tx, commit_tx, abort_tx)
│   │   │   └── maintenance.rs      # tombstone operations (drop_name, is_dropped, compact)
│   │   └── README.md
│   ├── codec/                      # PND2 encode/decode (all encodings, all vtypes)
│   │   ├── Cargo.toml
│   │   ├── src/lib.rs              # pnd2_decode, pnd2_encode_*, PondResult, PondEncoder
│   │   └── README.md
│   ├── arrow/                      # PND2 → Arrow bridge (near zero-copy)
│   │   ├── Cargo.toml
│   │   ├── src/lib.rs              # pnd2_to_arrow (PND2 → RecordBatch)
│   │   └── README.md
│   ├── cli/                        # pond CLI binary (DuckDB philosophy)
│   │   ├── Cargo.toml
│   │   ├── src/main.rs             # init, write, read, branch, checkout, merge, history, ...
│   │   ├── tests/cli_integration.rs
│   │   └── README.md
│   └── python/                     # PyO3 wrapper (thin — calls kernel + storage + codec)
│       ├── Cargo.toml
│       ├── src/lib.rs              # #[pyfunction] decode, encode (delegates to codec)
│       └── README.md
│
├── sdk/                            # Language SDKs (thin FFI wrappers, no logic)
│   ├── base/                       # Generic cross-language shared files
│   │   ├── pond.h                  # Unified C ABI header (kernel + storage + codec)
│   │   ├── pond_storage_c_abi.c    # C test for storage C ABI
│   │   ├── test_blobs/             # Binary PND2 blobs for cross-language compat tests
│   │   ├── generate_test_blobs.py  # Generates test blobs via Python encoder
│   │   └── README.md               # How to add a new language SDK
│   ├── go/                         # Go SDK (cgo wrapper around pond.h)
│   │   ├── go.mod
│   │   ├── pond/                   # Public Go API (Storage, Result, Column, Encoder)
│   │   ├── internal/cabi/          # Private cgo layer
│   │   ├── pond_test.go            # Tests (codec + storage)
│   │   ├── pond_bench_test.go      # Benchmarks
│   │   └── README.md
│   └── python/                     # Python SDK (lenses, extensions — calls core via PyO3)
│       ├── __init__.py
│       ├── base_lens.py            # PondLens shared base
│       ├── pond_storage.py         # PondStorage (delegates to core via PyO3)
│       ├── row_query.py            # LensQuery (lazy row-level query builder)
│       ├── pond_config.py          # .pond/config (pruning + encoding settings)
│       └── extensions/             # Data-side extensions (indexing, semantic, physical_structures)
│           ├── indexing/
│           ├── semantic/
│           └── physical_structures/
│
├── lenses/                         # Lens implementations (Rust + Python wrappers)
│   ├── keyvalue/
│   ├── lakehouse/
│   ├── vector/
│   ├── streaming/
│   └── oltp/
│
├── services/                       # Cross-cutting services (transport, schema, replication)
├── labs/                           # Experimental code (NOT production)
├── tests/                          # Integration tests (all languages)
├── docs/                           # Documentation
├── scripts/                        # Verification scripts
├── tla/                            # TLA+ formal specification
├── archive/                        # Historical code (NOT active)
└── (top-level docs)                # README.md, DESIGN_GOALS.md, PACKAGES.md, etc.
```

### Key decisions:

1. **`` → `core/`**: The Rust workspace IS the core. Renaming eliminates the `pond-` prefix (which is redundant — everything is Pond) and makes it clear this is the production implementation.

2. **`core/codec/` → `core/codec/`**: The PND2 codec is NOT "the core" — the kernel is. Renaming to `codec/` is self-describing and eliminates the name collision with Python's `bindings/python/core/`.

3. **`core/kernel/` → `core/kernel/`**: The 3 primitives live here. The name `kernel/` is self-describing. The ObjectStore trait and LocalFSObjectStore are also here because they're the kernel's storage backend abstraction.

4. **`bindings/go/` → `sdk/go/`**: All language SDKs live under `sdk/`. This is consistent and extensible — adding `sdk/java/` or `sdk/node/` is natural.

5. **`sdk/base/`**: Shared cross-language files. The C ABI header (`pond.h`), C ABI tests, and test blobs live here. All SDKs include from `sdk/base/pond.h`. This is the "generic unified cross-language files" folder the user asked for.

6. **`bindings/python/sdk/` → `sdk/python/`**: The Python SDK is a language SDK. It goes under `sdk/` with the others. The `pond-` prefix is dropped for consistency.

7. **`bindings/python/core/` (Python kernel) → stays as `bindings/python/core/`**: This is the Python REFERENCE implementation. We keep the name because it's the reference, not the production implementation. It will eventually become a thin wrapper. When it does, it moves to `sdk/python/`.

8. **`pond/` (pip shim) → deleted or merged into `sdk/python/`**: The pip shim was a packaging artifact. It should be part of the Python SDK, not a separate top-level directory.

9. **`pond-labs/` → `labs/`**: Drop the `pond-` prefix. Everything in this repo is Pond.

### What stays the same:
- `lenses/` — lens implementations stay at top level (they're peers to `core/` and `sdk/`)
- `services/` — cross-cutting services stay
- `tests/` — integration tests stay
- `docs/` — documentation stays
- `scripts/` — verification scripts stay
- `tla/` — TLA+ specification stays
- `archive/` — historical code stays

### Kernel 3 primitives separation:

The 3 primitives (Write, Read, Ref) are in `core/kernel/src/lib.rs` as methods on `PondKernel`. They ARE clearly separated from the storage layer (`core/storage/`) and the codec (`core/codec/`). The `ObjectStore` trait is also in the kernel crate because it's the kernel's storage backend abstraction — the kernel uses it, and `LocalFSObjectStore` implements it. This is correct: the kernel owns its storage backend.

The CRDT utilities (`crdt.rs` — UUIDv7 + HLC) are in the kernel crate because they're foundational primitives used by the storage layer. This is also correct: the kernel provides the building blocks, the storage layer composes them.

### Migration plan:

This is a big rename. We should do it in phases:
1. **Phase 1**: Move `` → `core/` and rename internal crates (low risk — just paths)
2. **Phase 2**: Move `bindings/go/` → `sdk/go/` and create `sdk/base/` (medium risk — cgo paths)
3. **Phase 3**: Move `bindings/python/sdk/` → `sdk/python/` (high risk — many Python imports)
4. **Phase 4**: Clean up `bindings/python/core/` (Python reference) and `pond/` (pip shim)

Each phase is a separate commit with tests passing.
