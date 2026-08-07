# sdk-go — Go bindings for Pond's PND2 binary codec

`sdk-go` is a Go SDK that lets Go programs encode and decode Pond's PND2
columnar binary format. It links against `libpond_core.a` (the Rust C ABI
in `../pond-rust/pond-core/`) via cgo.

## Architectural role

`sdk-go` is a **peer to `pond-sdk/`** (the Python SDK). Both bind to
`pond-core`'s storage layer:

```
                     Layer 0 (storage kernel)
                     ┌──────────────────────────────────────┐
                     │  pond-core/kernel.py (Python)         │
                     │  pond-rust/pond-core/ (Rust C ABI)    │
                     └────────────┬─────────────┬───────────┘
                                  │             │
                  ┌───────────────▼───┐  ┌──────▼──────────┐
                  │  pond-sdk/        │  │  sdk-go/         │
                  │  (Python SDK)     │  │  (Go SDK)        │
                  │                   │  │                  │
                  │  - Lenses         │  │  - PND2 codec    │
                  │  - ProllyTree     │  │  - Encoder       │
                  │  - Extensions     │  │  - Decoder       │
                  └───────────────────┘  └──────────────────┘
```

The Go SDK demonstrates **Storage-Independence** (Design Goal 3.8): blobs
produced by Go are byte-compatible with blobs produced by Python. Either
language can encode; either can decode.

## Scope

This SDK currently exposes **PND2 codec operations only**:

| Operation | Status |
|-----------|--------|
| Encode single-column (i64 / f64 / str) | ✅ |
| Encode multi-column (i64 + f64 + str mix) | ✅ |
| Decode all encodings (RAW, RLE, DICT, BITPACK) | ✅ |
| Decode all value types (INT64, FLOAT64, STRING, BINARY, NULL) | ✅ |
| Storage kernel (Write/Read/Ref) | ❌ (Python-only for now) |

Storage kernel operations require the Python `pond-core/kernel.py`. A
future Rust implementation of the storage kernel would enable full Go
storage support — but that's a much larger project (the kernel's
ProllyTree, commit graph, ref CAS, etc.).

## Build

The Go SDK depends on `libpond_core.a` being built. From the repo root:

```bash
# 1. Build the Rust C ABI (one-time, or after Rust changes)
cd pond-rust && cargo build --release -p pond_core && cd ..

# 2. Build + test the Go SDK
cd sdk-go && go test ./...
```

The cgo directives in `internal/cabi/cabi.go` automatically locate the
static library via relative paths — no environment variables needed.

## Quick start

```go
package main

import (
    "fmt"
    "github.com/pond/pond-go/pond"
)

func main() {
    // Encode a 3-column blob
    enc := pond.NewEncoder(3)
    enc.AddInt64Column("id", []int64{1, 2, 3})
    enc.AddFloat64Column("score", []float64{1.5, 2.5, 3.5})
    enc.AddStringColumn("name", []string{"alice", "bob", "carol"})
    blob, err := enc.Build()
    if err != nil { panic(err) }
    enc.Free()

    // Decode it back
    r, err := pond.Decode(blob)
    if err != nil { panic(err) }
    defer r.Free()

    for _, col := range r.Columns {
        fmt.Printf("%s: %s, %d values\n", col.Name, col.Vtype, col.Len())
    }
}
```

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
sdk-go/
├── go.mod
├── README.md            # this file
├── pond/                # public Go API (import this package)
│   ├── pond.go          # Result, Column, Encoder, Encode*/Decode funcs
│   └── pond_test.go     # end-to-end tests + Python-blob compat tests
└── internal/            # private packages (not importable externally)
    └── cabi/            # cgo layer over libpond_core.a
        └── cabi.go      # direct C function wrappers
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

To run:

```bash
# Generate the Python test blobs first (one-time)
cd pond-rust && PYTHONPATH=../pond-sdk:target/release \
    python3 tests/generate_test_blobs.py && cd ..

# Run the Go tests
cd sdk-go && go test -v ./...
```

## Design principles followed

| Principle | How |
|-----------|-----|
| **Simple** (3.1) | One package, one responsibility (PND2 codec). No leaky abstractions. |
| **Performant** (3.3) | Optimization lives in Rust; Go is a thin wrapper. Zero-copy slice sharing for large INT64/FLOAT64 reads (with copy option for safety). |
| **Scalable** (3.4) | Removability test: deleting `sdk-go/` breaks no lower layer. The Python SDK, Rust core, and storage kernel are all unaffected. |
| **Beautiful** (3.6) | Dependencies flow downward only: `sdk-go` → `pond-rust/pond-core` (via C ABI) → no further. |
| **Functional** (3.7) | PND2 codec is the minimum useful capability for Go callers. Storage kernel access is a future addition. |
| **Storage-Independent** (3.8) | PND2 blobs are language-agnostic. Go-produced blobs are byte-compatible with Python-produced blobs. |
