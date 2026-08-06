# pond-rust — Rust core + Python bindings for Pond

This directory is a Cargo **workspace** with two crates:

| Crate | Path | Crate types | Purpose |
|-------|------|-------------|---------|
| `pond-core` | [`pond-core/`](pond-core/) | `staticlib`, `cdylib`, `rlib` | Pure-Rust PND2 codec + C ABI. **Zero external dependencies** so it can be statically linked from Go, Java, Node, C, C++, Zig without dragging in transitive crates. |
| `pond-python` | [`pond-python/`](pond-python/) | `cdylib` | PyO3 wrapper around `pond-core`. Produces `pond_rust.so` for `import pond_rust` from Python. |

## Why split?

The original single crate mixed PyO3 (Python bindings) with the C ABI. The static library (`libpond_rust.a`) contained PyO3's libpython symbol references, which broke linking from C/Go/Java/Node — none of those languages have Python at link time.

The split puts the C ABI in a crate with **no PyO3 dependency at all**, so `libpond_core.a` is a clean, self-contained static library. The Python bindings live in a separate crate that depends on `pond-core` and adds PyO3 on top.

## Build

```bash
./build.sh            # release build (default)
./build.sh debug      # debug build
```

After build, artifacts are in `target/release/`:

| File | Used by |
|------|---------|
| `libpond_core.a` | Go (cgo), C, C++, Zig — static linking |
| `libpond_core.so` | Java (JNI), Node (N-API) — dynamic loading |
| `pond_rust.so` | Python — `import pond_rust` (set `PYTHONPATH=target/release`) |
| `libpond_rust.so` | Same as `pond_rust.so` (hardlinked, for tooling that expects `lib` prefix) |

## C ABI header

The C ABI is declared in [`pond-core/pond_core.h`](pond-core/pond_core.h). See that file for the full API documentation. Quick summary:

```c
PondResult* pond_pnd2_decode(const uint8_t* blob, size_t blob_len);
size_t      pond_result_num_columns(const PondResult* result);
const char* pond_result_column_name(const PondResult* result, size_t index);
uint8_t     pond_result_column_vtype(const PondResult* result, size_t index);
size_t      pond_result_column_len(const PondResult* result, size_t index);
const int64_t* pond_result_column_i64(const PondResult* result, size_t index);
const double*  pond_result_column_f64(const PondResult* result, size_t index);
const char* pond_result_column_str(const PondResult*, size_t col, size_t row);
void        pond_result_free(PondResult* result);

int32_t pond_pnd2_encode_i64(const int64_t* values, size_t n,
                              uint8_t** out_blob, size_t* out_blob_len);
void    pond_blob_free(uint8_t* blob, size_t blob_len);
```

## Tests

- **Rust unit tests** (`pond-core/src/lib.rs`): `cargo test -p pond_core`
- **C ABI end-to-end test** (`tests/test_c_abi.c`): compile + run via `tests/test_all.py::test_rust_c_abi` or manually:
  ```bash
  cc tests/test_c_abi.c -Ipond-core target/release/libpond_core.a \
    -lpthread -ldl -lm -o target/test_c_abi && ./target/test_c_abi
  ```
- **Python roundtrip test** (`tests/test_all.py::test_rust_python_roundtrip`): verifies encode → decode → projection → predicate pushdown from Python.

## PND2 format

See the module-level doc comment in [`pond-core/src/lib.rs`](pond-core/src/lib.rs) for the full PND2 binary format specification.

Currently the C ABI handles **RAW encoding** for INT64, FLOAT64, and STRING value types (the common case). The Python bindings handle all encodings (RAW, RLE, DICT, BITPACK) and all value types (INT64, FLOAT64, STRING, BINARY, NULL). Future work: extend the C ABI to share the full decoder with Python via `pond_core::pnd2_decode`.
