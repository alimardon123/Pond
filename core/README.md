# core/ — Language-Agnostic Rust Crates

This directory contains the Rust core of Pond. These crates have NO
Python dependency. They are the canonical implementation that all
language bindings call (directly or via C ABI).

## Crates

| Crate | Path | Purpose | Dependencies |
|---|---|---|---|
| `pond_kernel` | `kernel/` | 3 primitives (Write, Read, Ref) + `ObjectStore` trait + CRDT (UUIDv7, HLC) + `LocalFSObjectStore` | `sha2` only |
| `pond_storage` | `storage/` | `UnifiedStorage` — versioning, branching, shards, merge, history, undo, revert | `pond_kernel`, `serde_json` |
| `pond_codec` | `codec/` | PND2 binary format — encode/decode all encodings (RAW/RLE/DICT/BITPACK) × all vtypes (INT64/FLOAT64/STRING/BINARY/NULL) | zero deps |
| `pond_arrow` | `arrow/` | PND2 → Apache Arrow direct conversion (near-zero copy) | `arrow`, `pond_codec` |
| `pond_s3` | `s3/` | S3-compatible object store (AWS S3, R2, MinIO, etc.) — SigV4 signing from scratch | `pond_kernel`, `pond_storage`, `ureq`, `sha2`, `hex`, `chrono`, `url` |

## Build

```bash
# Build all crates
cargo build --release

# Build specific crate
cargo build -p pond_kernel
cargo build -p pond_storage
cargo build -p pond_codec
cargo build -p pond_arrow
cargo build -p pond_s3

# Build with S3 support in the CLI (default)
cargo build -p pond_cli

# Build local-only CLI (no S3 dependency)
cargo build -p pond_cli --no-default-features
```

## Test

```bash
# All tests
cargo test --workspace

# Specific crate
cargo test -p pond_kernel    # 19 tests (kernel + CRDT + object store)
cargo test -p pond_storage   # 37 tests (versioning + branching + shards)
cargo test -p pond_codec     # 15 tests (PND2 encode/decode)
cargo test -p pond_arrow     # 9 tests (Arrow conversion)
cargo test -p pond_s3        # 6 tests (SigV4 + HMAC + URL encoding)
```

## Architecture

```
Lenses (lenses/) + CLI (cli/)
  ↓ depend on
pond_storage (UnifiedStorage)
  ↓ depends on
pond_kernel (PondKernel + ObjectStore trait)
  ↓ implemented by
LocalFSObjectStore (in pond_kernel)
S3ObjectStore (in pond_s3 — separate crate for HTTP/SigV4 deps)
  ↓ uses
pond_codec (PND2 format) ← pond_arrow (Arrow bridge)
```

## Key Design Decisions

1. **`pond_kernel` stays minimal-dep** (only `sha2`). Storage backends that
   need HTTP/crypto deps (like S3) are separate crates (`pond_s3`).
2. **`pond_codec` has zero deps** — it can be statically linked from any
   language without dragging in transitive crates.
3. **`pond_s3` implements SigV4 from scratch** — no AWS SDK dependency,
   no tokio/async runtime. Uses `ureq` (sync HTTP) + `sha2` + manual HMAC.
4. **All crates produce `rlib` + `staticlib` + `cdylib`** — so they can
   be used by Rust code, statically linked into C/Go/Java, or dynamically
   loaded.

## C ABI

The unified C ABI header is at [`../bindings/base/pond.h`](../bindings/base/pond.h).
It declares functions from `pond_kernel`, `pond_storage`, `pond_codec`, and
`pond_s3` in a single header. All language SDKs include this header.

## Build Artifacts

After `cargo build --release`, the artifacts in `target/release/` are:

| File | Used by |
|---|---|
| `libpond_kernel.a` | Static linking (Go, C, C++, Zig) |
| `libpond_storage.a` | Static linking (pulls in pond_kernel) |
| `libpond_codec.a` | Static linking (zero deps) |
| `libpond_arrow.a` | Static linking (pulls in pond_codec + arrow) |
| `libpond_s3.a` | Static linking (pulls in pond_kernel + pond_storage) |
| `libpond_kernel.so` | Dynamic loading (Java JNI, Node N-API) |
| `libpond_storage.so` | Dynamic loading |
| `libpond_codec.so` | Dynamic loading |
| `libpond_s3.so` | Dynamic loading |
| `pond` | The CLI binary (DuckDB philosophy — single executable) |

## PND2 Format

See the module-level doc comment in [`codec/src/lib.rs`](codec/src/lib.rs)
for the full PND2 binary format specification, or
[`../docs/BINARY_ENCODING_FORMAT.md`](../docs/BINARY_ENCODING_FORMAT.md)
for the column encoding spec.
