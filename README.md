# Pond

> *One copy of data on object storage, serving all workloads without
> duplication, with built-in versioning, CRDT concurrency, and
> competitive performance vs specialized systems.*

Pond is a **unified content-addressed storage system** — not another
lakehouse, not another table format, not another Spark.

The core hypothesis: a tiny storage kernel (3 operations, ~200 LOC) is
sufficient for radically different workloads — SQL, vectors, streaming,
KV, Git, notebooks, ML — to be implemented as independent **Lenses** over
a shared immutable substrate, with built-in versioning (branch/merge),
CRDT concurrency (no CAS), and PB-scale performance.

---

## Architecture

```
Lenses (KV, Vector, Streaming, Lakehouse, OLTP)
  ↓ compose
UnifiedStorage (ONE storage engine — Rust core)
  - write / append / read / point_lookup / iter_rows
  - append_shard / upsert_shard / delete_shard (CRDT, no CAS)
  - read_with_shards (two-level merge: row groups + rows)
  - branch / checkout / merge / revert / history / diff
  - gc / vacuum / optimize (Delta/Iceberg parity)
  ↓
Kernel (3 ops: Write, Read, Ref)
  - ObjectStore trait (local FS, S3, GCS, in-memory)
  - PND2 format (ONE binary format for ALL workloads)
  - CollectionManifest (ONE index — flat → StatsTree at PB scale)
  - JSON commit blobs (ONE commit format)
  - Shards (CRDT G-Set) + row-level version vectors
  ↓
Storage Backends
  - LocalFSObjectStore  (Rust, zero deps)
  - S3ObjectStore       (Rust, SigV4 — works with AWS S3, R2, MinIO, etc.)
  - InMemoryObjectStore (Python, for testing)
```

---

## Repository Structure

```
pond_repo/
├── core/                    # Language-AGNOSTIC Rust crates
│   ├── kernel/              # 3 primitives + ObjectStore trait + CRDT
│   ├── storage/             # UnifiedStorage (versioning, branching, shards)
│   ├── codec/               # PND2 encode/decode (all encodings, all vtypes)
│   ├── arrow/               # PND2 → Arrow direct conversion
│   └── s3/                  # S3-compatible object store (SigV4, zero AWS SDK deps)
├── cli/                     # `pond` CLI binary (DuckDB philosophy)
├── bindings/                # Language-specific bindings
│   ├── base/                # Shared C ABI: pond.h, C tests, test blobs
│   ├── python/
│   │   ├── pyo3/            # PyO3 Rust crate (produces pond.so)
│   │   ├── sdk/             # Python SDK (PondStorage, lenses, extensions)
│   │   └── core/            # Python reference kernel (being migrated to Rust)
│   └── go/                  # Go SDK (cgo wrapper around C ABI)
├── lenses/                  # Workload-specific lenses
│   ├── base/                # Lens protocol (C ABI placeholder)
│   ├── keyvalue/
│   │   ├── python/          # KeyValueLens (production)
│   │   └── rust/            # Placeholder for future Rust port
│   ├── lakehouse/{python,rust}/
│   ├── oltp/{python,rust}/
│   ├── streaming/{python,rust}/
│   └── vector/{python,rust}/
├── services/                # Cross-cutting services (transport, schema, replication)
├── pond-labs/               # Experiments and demos
├── tests/                   # All tests (architecture, integration, lens algebra)
├── scripts/                 # Verification scripts (property tests, benchmarks)
├── docs/                    # Documentation
├── tla/                     # TLA+ formal specification
└── archive/                 # Historical code (not active)
```

---

## Quick Start

### Using the Rust CLI (recommended)

```bash
# Build the CLI (with S3 support enabled by default)
cargo build -p pond_cli

# Local filesystem (git-style auto-discovery)
cd /var/lib/pond
pond init                          # creates .pond/ marker
pond write users --json '[{"id":1,"name":"alice"}]' -m "first"
pond read users
pond branch users dev
pond checkout -b users dev
pond merge users dev -m "merge"
pond history users
pond ls

# Works from subdirectories too (like git)
cd /var/lib/pond/subdir
pond read users                    # auto-discovers .pond/

# S3-compatible storage (AWS S3, R2, MinIO, etc.)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
pond init "s3://my-bucket/prod?region=us-east-1"
pond write users --json '[{"id":1}]' -m "first"   # no --root needed!
pond read users

# Cloudflare R2
pond init "s3://bucket/prefix?region=auto&endpoint=https://<account>.r2.cloudflarestorage.com"

# MinIO
pond init "s3://bucket/prefix?region=us-east-1&endpoint=http://localhost:9000"
```

**How it works:** `pond init` creates a `.pond/` marker directory with a
`config` file. Subsequent commands auto-discover it by walking up from CWD
(just like `git` finds `.git/`). No need for `--root` on every command.

`--root` and `POND_ROOT` env var still work as overrides (for scripts/CI).

### Using the Python SDK

```python
import sys, os
sys.path.insert(0, "bindings/python/core")
sys.path.insert(0, "bindings/python/sdk")

from make_kernel import make_kernel
from pond_storage import PondStorage

# Local filesystem (pure files, no SQLite):
kernel = make_kernel("file:///var/lib/pond")

# OR — S3 (boto3, credentials from env):
kernel = make_kernel("s3://my-pond/prod", region="us-east-1")

storage = PondStorage(kernel)

# Write any workload — same API regardless of backend
storage.write("users", [{"id": 1, "name": "alice"}], key_col="id")

# Read any workload — same API
rows = storage.read("users")
row = storage.point_lookup("users", key="1")

# Version control — same API
storage.branch("users", "dev")
storage.checkout("users", "dev")
storage.append("users", [{"id": 2, "name": "bob"}], key_col="id")
storage.merge("users", "dev")

# Concurrent multi-writer — CRDT, no CAS
storage.append_shard("events", [{"id": 1, "event": "click"}], key_col="id")
rows = storage.read_with_shards("events")

# Maintenance
storage.vacuum(preserve_days=7)
storage.optimize()
```

### Using the Go SDK

```go
import "github.com/pond/pond-go/pond"

// Open storage (local FS or S3)
store, _ := pond.OpenStorage("/var/lib/pond")
defer store.Free()

// Write
hash, _ := store.Write("users", []byte(`[{"id":1,"name":"alice"}]`), "initial commit")

// Read
data, _ := store.Read("users")
fmt.Println(string(data))

// Branch + merge
store.Branch("users", "dev")
store.Checkout("users", "dev")
store.Merge("users", "dev", "main", "merge dev")
```

---

## S3-Compatible Storage

Pond's Rust core includes a **from-scratch S3 client** with SigV4 signing.
No AWS SDK dependency — just `sha2` + `ureq` (sync HTTP) + `hex`. This
keeps the binary small and the build fast.

**Supported S3-compatible providers:**
- AWS S3
- Cloudflare R2
- MinIO
- LocalStack
- Wasabi
- DigitalOcean Spaces
- Any S3-compatible API

**URL format:**
```
s3://<bucket>/<prefix>?region=<region>&endpoint=<url>
```

**Credentials** (read from environment):
- `AWS_ACCESS_KEY_ID` (or `AWS_ACCESS_KEY`)
- `AWS_SECRET_ACCESS_KEY` (or `AWS_SECRET_KEY`)
- `AWS_SESSION_TOKEN` (optional, for STS temporary credentials)

**Migration** between local FS and S3 is a straight copy:
```bash
aws s3 sync /var/lib/pond/ s3://my-pond/prod/
aws s3 sync s3://my-pond/prod/ /var/lib/pond/
```
No format conversion needed — blobs and paths use the same layout.

---

## Cross-Language C ABI

One header (`bindings/base/pond.h`) exposes all three layers:

| Layer | Functions | Purpose |
|---|---|---|
| Kernel | `pond_kernel_new/write/read/reference/resolve` | 3 primitives |
| Storage | `pond_storage_new/new_s3/write/read/branch/merge/undo` | Versioning |
| Codec | `pond_pnd2_decode/encode_i64/encode_f64/encode_str` | PND2 format |

Any language that can call C gets full Pond access:
- **Go**: `bindings/go/` (cgo wrapper)
- **Python**: `bindings/python/pyo3/` (PyO3 — calls Rust directly, not C ABI)
- **Java**: future (JNI wrapper around C ABI)
- **Node**: future (N-API wrapper around C ABI)
- **C/C++**: `#include "pond.h"` (direct)

---

## Design Principles

1. **Simple** — ONE storage format, ONE commit format, ONE concurrency model
2. **Powerful** — branch/merge + CRDT + IVF + streaming + GC + optimize
3. **Performant** — O(1) point lookup, O(1) warm writes, O(1) shard writes
4. **Scalable** — linear PUTs, flat GETs, PB-scale via StatsTree
5. **Efficient** — immutable blobs (deduped), O(live) GC, parallel fetch
6. **Beautiful** — shards ARE branches, CRDT = G-Set union, no CAS
7. **Functional** — lakehouse, KV, vector, streaming, notebook, git
8. **Storage-Independent** — no CAS, works on local FS / S3 / R2 / MinIO / GCS

---

## Migration Strategy: Python → Rust

Pond is migrating from Python to Rust as the core implementation language:

- **Rust core** (done): kernel, storage, codec, arrow, S3, CLI
- **Python SDK** (current): PyO3 wrapper for codec; Python kernel + lenses still in use
- **Future**: port lenses to Rust, expose via C ABI, Python becomes thin wrapper

New development happens in Rust. Python is maintained for bug fixes only.

---

## Documentation

- [`DESIGN_GOALS.md`](DESIGN_GOALS.md) — The 8 design principles, in detail
- [`REPO_ORGANIZATION.md`](REPO_ORGANIZATION.md) — Folder structure rules
- [`PACKAGES.md`](PACKAGES.md) — Package dependency graph
- [`KNOWLEDGE_GRAPH.md`](KNOWLEDGE_GRAPH.md) — Every file, its purpose, its exports
- [`SDK_SPEC.md`](SDK_SPEC.md) — SDK API specification
- [`docs/`](docs/) — Design documents, whitepaper, formal algebras, TLA+ spec
