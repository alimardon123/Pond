# bindings/go/ — Go SDK for Pond

Go bindings for Pond's Rust core. Links against the static libraries
(`libpond_storage.a`, `libpond_kernel.a`, `libpond_codec.a`) via cgo.

## Scope

The Go SDK provides FULL access to Pond's storage layer:

| Operation | Status |
|---|---|
| Encode single-column (i64 / f64 / str) | ✅ |
| Encode multi-column (i64 + f64 + str mix) | ✅ |
| Decode all encodings (RAW, RLE, DICT, BITPACK) | ✅ |
| Decode all value types (INT64, FLOAT64, STRING, BINARY, NULL) | ✅ |
| Storage: write / read | ✅ |
| Storage: branch / checkout / merge | ✅ |
| Storage: undo / revert / list_branches | ✅ |
| Storage: S3 backend | ✅ (via `libpond_s3.a`) |

## Build

The Go SDK depends on the Rust static libraries being built. From the repo root:

```bash
# 1. Build the Rust core (one-time, or after Rust changes)
cargo build --release

# 2. Build + test the Go SDK
cd bindings/go && go test -v ./...
```

The cgo directives in `internal/cabi/cabi.go` automatically locate the
static libraries via relative paths — no environment variables needed.

## Quick start

```go
package main

import (
    "fmt"
    "github.com/pond/pond-go/pond"
)

func main() {
    // Open storage (local FS)
    store, err := pond.OpenStorage("/var/lib/pond")
    if err != nil { panic(err) }
    defer store.Free()

    // Write
    hash, err := store.Write("users", []byte(`[{"id":1,"name":"alice"}]`), "init")
    if err != nil { panic(err) }
    fmt.Println("commit:", hash)

    // Read
    data, err := store.Read("users")
    if err != nil { panic(err) }
    fmt.Println("data:", string(data))

    // Branch + merge
    store.Branch("users", "dev")
    store.Checkout("users", "dev")
    store.Merge("users", "dev", "main", "merge dev")

    // PND2 codec operations
    enc := pond.NewEncoder(3)
    enc.AddInt64Column("id", []int64{1, 2, 3})
    enc.AddFloat64Column("score", []float64{1.5, 2.5, 3.5})
    enc.AddStringColumn("name", []string{"alice", "bob", "carol"})
    blob, err := enc.Build()
    if err != nil { panic(err) }
    enc.Free()

    r, err := pond.Decode(blob)
    if err != nil { panic(err) }
    defer r.Free()
    for _, col := range r.Columns {
        fmt.Printf("%s: %s, %d values\n", col.Name, col.Vtype, col.Len())
    }
}
```

## Architecture

```
                     Rust core (language-agnostic)
                     ┌──────────────────────────────────────┐
                     │  core/kernel/   (PondKernel)          │
                     │  core/storage/  (UnifiedStorage)      │
                     │  core/codec/    (PND2 format)         │
                     │  core/s3/       (S3ObjectStore)       │
                     └────────────┬─────────────┬───────────┘
                                  │             │
                  ┌───────────────▼───┐  ┌──────▼──────────┐
                  │  bindings/python/ │  │  bindings/go/   │
                  │  (PyO3 + SDK)     │  │  (cgo)          │
                  │                   │  │                 │
                  │  - Lenses         │  │  - Storage      │
                  │  - Extensions     │  │  - Codec        │
                  └───────────────────┘  └─────────────────┘
```

The Go SDK demonstrates **Storage-Independence** (Design Goal 3.8): blobs
produced by Go are byte-compatible with blobs produced by Python. Either
language can encode; either can decode.

## Memory ownership

Go's GC doesn't track C-allocated memory. The Go SDK handles this in two ways:

1. **Blobs returned by Encode/Build** are copied into Go-owned slices.
   The C allocation is freed immediately. Callers can hold the Go slice
   indefinitely — no Free needed.

2. **Decoded Result handles** wrap a C `PondResult*`. The C memory is
   freed when you call `result.Free()`. After Free, all derived slices
   and strings are invalid. The high-level `Column.Values` fields are
   always copies (safe to hold after Free), but the underlying handle
   still needs Free to avoid leaking C memory.

## Package layout

```
bindings/go/
├── go.mod
├── README.md            # this file
├── pond/                # public Go API (import this package)
│   ├── pond.go          # Result, Column, Encoder, Storage, Encode*/Decode
│   ├── pond_test.go     # end-to-end tests + Python-blob compat tests
│   └── pond_bench_test.go  # benchmarks
└── internal/            # private packages (not importable externally)
    └── cabi/            # cgo layer over libpond_storage.a
        └── cabi.go      # direct C function wrappers (pond.h)
```

The split between `pond/` (public) and `internal/cabi/` (private cgo)
keeps the cgo #include directives out of the public API surface. Users
only see clean Go types.

## Tests

The Go tests verify:

1. **Single-column round-trips** for INT64, FLOAT64, STRING
2. **Multi-column encoder** (INT64 + FLOAT64 + STRING mix)
3. **Cross-language compatibility**: decode Python-generated blobs
   covering all encodings (RAW, RLE, DICT, BITPACK) and all value
   types (INT64, FLOAT64, STRING, BINARY)
4. **Error paths**: empty blob, garbage blob
5. **Storage operations**: write/read, branch/merge, undo

To run:

```bash
# Generate the Python test blobs first (one-time)
PYTHONPATH=bindings/python/sdk:target/release \
    python3 bindings/base/generate_test_blobs.py

# Run the Go tests
cd bindings/go && go test -v ./...
```

## Design principles followed

| Principle | How |
|---|---|
| **Simple** (3.1) | One package, clear API surface. No leaky abstractions. |
| **Performant** (3.3) | All hot-path logic lives in Rust; Go is a thin wrapper. Zero-copy slice sharing for large INT64/FLOAT64 reads. |
| **Scalable** (3.4) | Removability test: deleting `bindings/go/` breaks no lower layer. |
| **Beautiful** (3.6) | Dependencies flow downward only: `bindings/go/` → `core/` (via C ABI) → no further. |
| **Functional** (3.7) | Full storage + codec access for Go callers. |
| **Storage-Independent** (3.8) | PND2 blobs are language-agnostic. Go-produced blobs are byte-compatible with Python-produced blobs. |
