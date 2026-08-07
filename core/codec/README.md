# bindings/python/core — Pure-Rust PND2 codec + C ABI

This crate is the language-agnostic core of Pond's binary storage layer.

- **Python** binds to it via the `pond-python` crate (PyO3 wrapper).
- **Go, Java, Node, C, C++, Zig, etc.** bind to it directly via the C ABI
  declared in [`pond_core.h`](pond_core.h).

## Design principles

1. **Zero external dependencies** — so static linking from other languages
   doesn't pull in transitive Rust crates. This is enforced by having no
   `[dependencies]` in `Cargo.toml`.
2. **Pure Rust only** — no PyO3, no async runtime, no I/O.
3. **C ABI is the universal interop layer** — any language that can call
   C functions can use Pond's Rust core.
4. **Explicit ownership** — every heap allocation across the FFI boundary
   is owned by the caller; every `*_free` function documents its contract.

## Public API (Rust)

```rust
use pond_core::{pnd2_decode, pnd2_encode_i64, PondColumn};

// Encode
let values = vec![1i64, 2, 3, 4, 5];
let blob: Vec<u8> = pnd2_encode_i64(&values);

// Decode
let columns: Vec<PondColumn> = pnd2_decode(&blob).expect("valid PND2");
assert_eq!(columns[0].name, "v");
assert_eq!(columns[0].i64_data, values);
```

## C ABI

See [`pond_core.h`](pond_core.h) for the full C API. Build with:

```bash
cargo build --release -p pond_core
# → target/release/libpond_core.a (static)
# → target/release/libpond_core.so (dynamic)
```

## What's here vs. `pond-python`

| Feature | `bindings/python/core` (pure Rust) | `pond-python` (PyO3 wrapper) |
|---------|------------------------|-----------------------------|
| Constants (VT_*, ENC_*, etc.) | ✅ | re-exports from bindings/python/core |
| PND2Parser | ✅ | re-exports from bindings/python/core |
| Pure-Rust decode (all encodings, all vtypes) | ✅ (`pnd2_decode`, `decode_column`) | delegates to bindings/python/core |
| Pure-Rust encode (i64 / f64 / &str) | ✅ | delegates to bindings/python/core |
| C ABI (`extern "C"`) | ✅ | n/a |
| PyO3 `#[pyfunction]` wrappers | n/a | ✅ (`decode`, `encode`) |
| Multi-column Python encode (RAW) | n/a | ✅ |
| zstd decompression | ❌ (returns Err) | ✅ (via Python's `zstandard`) |
| Projection pushdown | n/a (single-shot decode) | ✅ |
| Predicate pushdown | n/a | ✅ |

The C ABI now has FULL parity with Python's decoder for all encodings
(RAW, RLE, DICT, BITPACK) and all value types (INT64, FLOAT64, STRING,
BINARY, NULL). The Python wrapper delegates to pond-core's decoder,
eliminating the duplication that existed before.
