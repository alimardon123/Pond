# Pond — Current Status (August 2026)

> **This document tracks what's done, what's in progress, and what's next.**
> It replaces the archived `MIGRATION_STRATEGY.md` and `NEXT_STEPS_DEEP_REVIEW.md`.

---

## Migration: Python → Rust

Pond is migrating from Python to Rust as the core implementation language.
The Rust core is now the canonical implementation; Python is maintained
for bug fixes only.

### Done (Rust core)

| Component | Path | Status |
|---|---|---|
| Kernel (3 primitives + ObjectStore trait) | `core/kernel/` | ✅ Done |
| CRDT (UUIDv7, HLC, upsert/delete/merge) | `core/kernel/src/crdt.rs` | ✅ Done |
| LocalFSObjectStore | `core/kernel/src/object_store.rs` | ✅ Done |
| S3ObjectStore (SigV4 from scratch) | `core/s3/` | ✅ Done |
| UnifiedStorage (versioning, branching, shards) | `core/storage/` | ✅ Done |
| PND2 codec (all encodings, all vtypes) | `core/codec/` | ✅ Done |
| PND2 → Arrow bridge | `core/arrow/` | ✅ Done |
| CLI (`pond` command, local + S3 + auto-discovery) | `cli/` | ✅ Done |
| C ABI (pond.h — kernel + storage + codec + S3) | `bindings/base/pond.h` | ✅ Done |
| Go SDK (full storage access via cgo) | `bindings/go/` | ✅ Done |
| Python PyO3 wrapper (codec + storage) | `bindings/python/pyo3/` | ✅ Done |
| Parallel S3 batch operations | `core/s3/` | ✅ Done (32 concurrent threads) |
| KeyValueLens (Rust port) | `lenses/keyvalue/rust/` | ✅ Done (core API) |
| StreamingLens (Rust port) | `lenses/streaming/rust/` | ✅ Done (core API) |
| OLTPLens (Rust port) | `lenses/oltp/rust/` | ✅ Done (core API) |

### In Progress (Python still in use)

| Component | Path | Status |
|---|---|---|
| Python reference kernel | `bindings/python/core/` | Maintained (bug fixes only) |
| Python SDK (PondStorage, lenses) | `bindings/python/sdk/` | Maintained (bug fixes only) |
| Lenses (Lakehouse, Vector — Python only) | `lenses/{name}/python/` | Production (Python) |
| base_lens.py (PondLens) | `bindings/python/sdk/base_lens.py` | DEPRECATED — vestigial |

### Not Started (Future)

| Component | Path | Status |
|---|---|---|
| LakehouseLens (Rust port) | `lenses/lakehouse/rust/` | Placeholder |
| VectorLens (Rust port) | `lenses/vector/rust/` | Placeholder |
| Lens C ABI protocol | `lenses/base/pond_lens.h` | Placeholder only |

---

## Test Coverage

| Suite | Count | Status |
|---|---|---|
| Rust unit tests (cargo test --workspace) | 90 | ✅ All pass |
| CLI integration tests | 15 | ✅ All pass |
| S3 unit tests (SigV4, HMAC, URL encoding) | 6 | ✅ All pass |
| S3 mock server tests (moto) | 12 | ✅ All pass |
| S3 real R2 tests (Cloudflare R2) | 7 | ✅ All pass |
| Go SDK tests | 10 | ✅ All pass |
| Python pytest suite | 23 | ✅ All pass (2 skipped) |
| KNOWLEDGE_GRAPH coverage | 240/240 | ✅ 100% |

---

## Storage Backend Support

| Backend | Rust | Python | Notes |
|---|---|---|---|
| Local filesystem | ✅ | ✅ | `core/kernel/LocalFSObjectStore` |
| AWS S3 | ✅ | ✅ (boto3) | `core/s3/S3ObjectStore` (SigV4 from scratch) |
| Cloudflare R2 | ✅ | ✅ (boto3) | Verified against real R2 |
| MinIO | ✅ | ✅ (boto3) | S3-compatible |
| LocalStack | ✅ | ✅ (boto3) | S3-compatible |
| Wasabi | ✅ | ✅ (boto3) | S3-compatible |
| DigitalOcean Spaces | ✅ | ✅ (boto3) | S3-compatible |
| In-memory | ❌ | ✅ | Python only (for testing) |
| GCS | ❌ | ❌ | Future (S3-compatible API works via interop) |

---

## Cross-Language Support

| Language | Status | How |
|---|---|---|
| Python | ✅ Full | PyO3 (codec + storage) + Python SDK (lenses) |
| Rust | ✅ Full | Direct (it's the core) |
| Go | ✅ Full | cgo over C ABI (kernel + storage + codec) |
| C/C++ | ✅ Full | Direct C ABI (`#include "pond.h"`) |

---

## Architecture (Current)

```
Lenses (KV, Vector, Streaming, Lakehouse, OLTP) — Python
  ↓
UnifiedStorage (Rust core) — core/storage/
  ↓
Kernel (3 ops: Write, Read, Ref) — core/kernel/
  ↓
ObjectStore trait
  ├── LocalFSObjectStore (Rust, core/kernel/)
  ├── S3ObjectStore (Rust, core/s3/ — SigV4 from scratch)
  └── InMemoryObjectStore (Python, testing only)
```

---

## Next Steps (Prioritized)

### Tier 1 — Do next
1. **Port KeyValueLens to Rust** — first lens migration, proves the pattern
2. **Parallel S3 batch operations** — `put_blob_batch` / `get_blob_batch` with a thread pool
3. **PyO3 wrapper for storage** — let Python call Rust storage directly (not just codec)

### Tier 2 — Do after
4. Port remaining lenses (Lakehouse, Vector) to Rust
5. Define lens C ABI protocol (`lenses/base/pond_lens.h`)
6. Build Python wheel with maturin (single `pip install pond`)
7. Cross-compilation for release binaries (Linux, macOS, Windows)
8. GCS native backend (currently works via S3-compatible API)

---

## Key Architectural Decisions

1. **Rust core, Python first-class SDK** — Rust is canonical; Python gets PyO3 wrappers
2. **Unified C ABI** — one `pond.h` for all languages (kernel + storage + codec + S3)
3. **SigV4 from scratch** — no AWS SDK dependency, keeps binary small
4. **Synchronous HTTP** — `ureq` not `reqwest`/`tokio` (no async runtime)
5. **S3 as a separate crate** — `core/s3/` has HTTP deps; `core/kernel/` stays minimal
6. **Lens rust/python subdirs** — each lens has both, Python is production today
7. **Cargo features for S3** — `default = ["s3"]`, can disable for local-only CLI
