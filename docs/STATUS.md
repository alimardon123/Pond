# Pond — Current Status (August 2026)

> **This document tracks what's done, what's in progress, and what's next.**
> It replaces the archived `MIGRATION_STRATEGY.md` and `NEXT_STEPS_DEEP_REVIEW.md`.

---

## Migration: Python → Rust

Pond is migrating from Python to Rust as the core implementation language.
The Rust core is now the canonical implementation; Python is maintained
for bug fixes only.

### Design Decision: JSON Storage (Rust) vs PND2 Storage (Python)

The Rust storage layer stores collection data as **JSON arrays** (`[{...}, ...]`)
for simplicity. The Python UnifiedStorage uses **PND2 binary format** (columnar,
compressed, with stats for pruning). Both formats store the same logical data
(rows of JSON objects), so they're compatible at the data level — a Rust-written
collection can be read by Python (as JSON) and vice versa (if Python writes JSON).

**Current state:**
- Rust lenses (KeyValue, Streaming, OLTP) store JSON arrays — simple, correct
- Python UnifiedStorage stores PND2 blobs — production performance (pruning,
  projection, compression)
- Rust PND2 codec exists (decode all encodings, encode RAW only) but is NOT
  wired into the Rust storage write path
- zstd decompression IS supported in the Rust codec (feature flag)

**Future:** Wire PND2 encoding into the Rust storage write path for production
parity. Until then, Rust storage is correct but slower than Python for large
datasets (no pruning, no compression).

**UPDATE (August 2026):** PND2 is now wired into the Rust storage layer!
- `write_rows_i64()` — PND2 encode with auto-encoding (RLE/DICT/BITPACK/RAW)
- `write_rows_i64_packed()` — PND2 + PondPack (commit+manifest in ONE blob)
- `read_rows_i64()` — PND2 decode with predicate pruning + column projection
- IVF Bug 10 FIXED — per-cluster blob references for true I/O reduction

### Done (Rust core)

| Component | Path | Status |
|---|---|---|
| Kernel (3 primitives + ObjectStore trait) | `core/kernel/` | ✅ Done |
| CRDT (UUIDv7, HLC, upsert/delete/merge) | `core/kernel/src/crdt.rs` | ✅ Done |
| LocalFSObjectStore | `core/kernel/src/object_store.rs` | ✅ Done |
| S3ObjectStore (SigV4 from scratch) | `core/s3/` | ✅ Done |
| UnifiedStorage (versioning, branching, shards) | `core/storage/` | ✅ Done |
| PND2 codec — decode (all encodings, all vtypes) | `core/codec/` | ✅ Done |
| PND2 codec — zstd decompression | `core/codec/` | ✅ Done (feature flag) |
| PND2 codec — encode (RLE, DICT, BITPACK, RAW + auto-select) | `core/codec/` | ✅ Done |
| PND2 → Arrow bridge | `core/arrow/` | ✅ Done |
| PND2 storage write path (write_rows_i64) | `core/storage/src/write.rs` | ✅ Done |
| PND2 storage read path (read_rows_i64) | `core/storage/src/read.rs` | ✅ Done (pruning + projection) |
| PondPack (PNPK) format | `core/storage/src/pond_pack.rs` | ✅ Done |
| GarbageCollector / vacuum | `core/storage/src/maintenance.rs` | ✅ Done |
| IVF Index (Bug 10 fixed) | `lenses/vector/rust/` | ✅ Done (per-cluster blob refs) |
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
| Python UnifiedStorage (PND2, 5767 LOC) | `bindings/python/sdk/extensions/physical_structures/` | Production (PND2 format) |
| Lenses (Lakehouse, Vector — Python only) | `lenses/{name}/python/` | Production (Python) |
| base_lens.py (PondLens) | `bindings/python/sdk/base_lens.py` | DEPRECATED — vestigial |

### Not Started (Future — prioritized by impact)

| Component | Path | Priority | Notes |
|---|---|---|---|
| PND2 encoders (RLE, DICT, BITPACK) | `core/codec/src/encode.rs` | HIGH | Only RAW encoder exists |
| PondPack (PNPK) format | `core/storage/` | HIGH | Production commit format (saves 1-2 GETs) |
| Wire PND2 encoding into Rust storage write() | `core/storage/src/write.rs` | HIGH | Currently writes JSON, not PND2 |
| LakehouseLens (Rust port) | `lenses/lakehouse/rust/` | MEDIUM | Most complex lens (DuckDB SQL) |
| VectorLens (Rust port) | `lenses/vector/rust/` | MEDIUM | IVF ANN — fix Bug 10 (reads ALL vectors) |
| GarbageCollector/vacuum | `core/storage/` | MEDIUM | Rust only has tombstone helpers |
| eval_predicate_encoded | `core/codec/` | MEDIUM | Vortex-style pruning without decode |
| StatsTree | `core/storage/` | LOW | PB-scale hierarchical stats (defer) |
| Lens C ABI protocol | `lenses/base/pond_lens.h` | LOW | Placeholder only |

---

## Test Coverage

| Suite | Count | Status |
|---|---|---|
| Rust unit tests (cargo test --workspace) | 117 | ✅ All pass |
| CLI integration tests | 17 | ✅ All pass |
| S3 unit tests (SigV4, HMAC, URL encoding) | 6 | ✅ All pass |
| S3 mock server tests (moto) | 12 | ✅ All pass |
| S3 real R2 tests (Cloudflare R2) | 7 | ✅ All pass |
| Go SDK tests | 10 | ✅ All pass |
| Python pytest suite | 23 | ✅ All pass (2 skipped) |
| KNOWLEDGE_GRAPH coverage | 185/185 | ✅ 100% |

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
Lenses (KV, Vector, Streaming, Lakehouse, OLTP)
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

**Storage format:**
- Rust storage: JSON arrays (simple, correct, no pruning)
- Python UnifiedStorage: PND2 binary (columnar, compressed, with stats)
- Both store the same logical data (rows of JSON objects)

---

## Key Architectural Decisions

1. **Rust core, Python first-class SDK** — Rust is canonical; Python gets PyO3 wrappers
2. **Unified C ABI** — one `pond.h` for all languages (kernel + storage + codec + S3)
3. **SigV4 from scratch** — no AWS SDK dependency, keeps binary small
4. **Synchronous HTTP** — `ureq` not `reqwest`/`tokio` (no async runtime)
5. **S3 as a separate crate** — `core/s3/` has HTTP deps; `core/kernel/` stays minimal
6. **Lens rust/python subdirs** — each lens has both, Python is production today
7. **Cargo features for S3 and zstd** — `default = ["s3", "zstd"]`, can disable
8. **JSON storage in Rust lenses** — simple and correct; PND2 wiring is future work
9. **No base_lens** — lenses own a UnifiedStorage directly (no PondLens base class)
10. **.pond/config lives at storage root** — not in local CWD for remote storage

---

## Known Gaps (from deep audit)

1. **Rust PND2 encoders incomplete** — only RAW; RLE/DICT/BITPACK decoders exist but encoders missing
2. **Rust storage write() doesn't use PND2** — writes JSON, no pruning/projection
3. **PondPack (PNPK) not ported** — production commit format saves 1-2 GETs
4. **IVF Bug 10** — reads ALL vectors, `n_probe` has no effect on I/O (admitted in source)
5. **Rust merge lacks row-level CRDT** — row-group LWW only (acceptable for JSON storage)
6. **SDK_SPEC.md is stale** — 15+ sections describe APIs that no longer exist
7. **Documentation drift** — references to deleted files (BloomFilter, Statistics, etc.)
