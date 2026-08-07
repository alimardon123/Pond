# pond-python — PyO3 bindings to bindings/python/core

This crate produces the Python extension module `pond_rust.so` (named
`pond_rust` on the Python side). It depends on `bindings/python/core` for all the
PND2 encode/decode logic and adds PyO3 on top to expose it to Python.

## Build

From the workspace root (``):

```bash
./build.sh            # release build
./build.sh debug      # debug build
```

After build:

```bash
PYTHONPATH=target/release python3 -c "import pond_rust; print(pond_rust.__file__)"
# → /path/to/target/release/pond_rust.so
```

`build.sh` also creates a hardlink `pond_rust.so` → `libpond_rust.so`
so the module is importable without the `lib` prefix.

## Python API

```python
import pond_rust

# Encode
result = pond_rust.encode(
    [("id", [1, 2, 3]), ("name", ["a", "b", "c"])],
    n_rows=3,
)
blob = result["blob"]      # bytes — the PND2 blob
stats = result["stats"]    # list of (name, vtype, min, max, null_count)

# Decode (full)
decoded = pond_rust.decode(blob)
# → {"id": [1, 2, 3], "name": ["a", "b", "c"]}

# Decode with column projection (skip unrequested columns)
decoded = pond_rust.decode(blob, columns=["id"])

# Decode with predicate pushdown (filter rows)
decoded = pond_rust.decode(blob, predicates=[("id", ">", 1)])
# → {"id": [2, 3], "name": ["b", "c"]}
```

## What's here vs. `bindings/python/core`

| Feature | `bindings/python/core` (pure Rust) | `pond-python` (this crate) |
|---------|------------------------|-----------------------------|
| Constants (VT_*, ENC_*, etc.) | ✅ | re-exports from bindings/python/core |
| PND2Parser | ✅ | re-exports from bindings/python/core |
| Pure-Rust encode/decode | ✅ (`pnd2_encode_i64`, `pnd2_decode`) | n/a (uses pond-core's) |
| C ABI (`extern "C"`) | ✅ | n/a |
| PyO3 `#[pyfunction]` wrappers | n/a | ✅ (`decode`, `encode`) |
| All encodings (RLE, DICT, BITPACK) | ❌ (RAW only) | ✅ |
| All value types (BINARY, NULL) | ❌ (INT64/FLOAT64/STRING) | ✅ |
| zstd decompression | ❌ (returns Err) | ✅ (via Python's `zstandard`) |

The C ABI is intentionally minimal so it can stay dependency-free. The
Python bindings have full coverage. Future work: extend `bindings/python/core`'s
decoder to share the full implementation with Python.
