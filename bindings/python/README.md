# bindings/python/ — Python Bindings + SDK + Reference Kernel

This directory contains everything Python-related for Pond:

```
bindings/python/
├── pyo3/      # PyO3 Rust crate (produces pond_rust.so for the codec)
├── sdk/       # Python SDK (PondStorage, lenses, extensions)
└── core/      # Python reference kernel (being migrated to Rust)
```

## Subdirectories

### `pyo3/` — PyO3 Rust Crate

A thin Rust crate that wraps the PND2 codec (in `core/codec/`) and
exposes it to Python as `pond_rust.so`. This gives Python access to
the fast Rust decoder/encoder.

- **Depends on:** `core/codec` (Rust)
- **Produces:** `pond_rust.so` (Python extension module)
- **API:** `pond_rust.decode(blob, columns=None, predicates=None)`

See [`pyo3/README.md`](pyo3/README.md) for details.

### `sdk/` — Python SDK

The Python SDK: `PondStorage`, `PondLens` (base class), `PondConfig`,
`HLC` (Hybrid Logical Clock), `uuid7`, `LensQuery`, and the `extensions/`
subdirectory (indexing, maintenance, semantic, physical_structures).

- **Depends on:** `bindings/python/core/` (the Python reference kernel)
- **Used by:** all Python lenses (in `lenses/{name}/python/`)

### `core/` — Python Reference Kernel

The Python reference implementation of the storage kernel. This is the
ORIGINAL implementation, being migrated to Rust. It's still the
production kernel for Python code today.

- **Files:** `kernel.py` (PondMinimal), `object_store_native_kernel.py`,
  `local_fs_object_store.py`, `s3_object_store.py` (boto3-based),
  `make_kernel.py` (factory: `file://` or `s3://`)
- **Status:** Maintained for bug fixes only. New development happens in Rust.

## Quick Start

```python
import sys, os
sys.path.insert(0, "bindings/python/core")
sys.path.insert(0, "bindings/python/sdk")

from make_kernel import make_kernel
from pond_storage import PondStorage

# Local filesystem
kernel = make_kernel("file:///var/lib/pond")

# OR — S3-compatible (AWS S3, R2, MinIO, etc.)
kernel = make_kernel("s3://my-bucket/prod", region="us-east-1")

storage = PondStorage(kernel)

# Write any workload — same API regardless of backend
storage.write("users", [{"id": 1, "name": "alice"}], key_col="id")

# Read any workload — same API
rows = storage.read("users")

# Version control
storage.branch("users", "dev")
storage.checkout("users", "dev")
storage.append("users", [{"id": 2, "name": "bob"}], key_col="id")
storage.merge("users", "dev")
```

## Migration to Rust

The Python kernel (`core/`) and SDK (`sdk/`) are being migrated to Rust.
The Rust core (`../../core/`) is the canonical implementation. Python
code today calls the Python kernel directly; future Python code will
call the Rust kernel via PyO3 (once the PyO3 wrapper is extended beyond
just the codec).

See [`../../docs/STATUS.md`](../../docs/STATUS.md) for the migration status.

## Rust Acceleration

Python can use the Rust PND2 codec via `pond_rust.so` (built from `pyo3/`):

```python
import pond_rust  # Rust-accelerated PND2 decode/encode

# Decode a PND2 blob (3-5x faster than pure-Python)
result = pond_rust.decode(blob_bytes)
```

The Rust codec is automatically used by the Python SDK when available.
