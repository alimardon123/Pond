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
Lenses (KV, Vector, Streaming, Lakehouse)
  ↓ compose
PondStorage (ONE unified SDK)
  - write / append / read / point_lookup / iter_rows
  - append_shard / upsert_shard / delete_shard (CRDT, no CAS)
  - read_with_shards (two-level merge: row groups + rows)
  - branch / checkout / merge / revert / history / diff
  - gc / vacuum / optimize (Delta/Iceberg parity)
  - list_collections / list_namespaces (hierarchical)
  ↓
UnifiedStorage (ONE storage engine)
  - PND2 format (ONE binary format for ALL workloads)
  - CollectionManifest (ONE index — flat → StatsTree at PB scale)
  - JSON commit blobs (ONE commit format)
  - Shards (CRDT G-Set) + row-level version vectors
  - Parallel fetch (~1 RTT wall-clock)
  ↓
Kernel (FROZEN — 3 primitives)
  Write(bytes) → hash  |  Read(hash) → bytes  |  Ref(name, hash) → ()
```

---

## Performance (verified by benchmark)

| Operation | GETs | PUTs | Wall-clock |
|---|---|---|---|
| Cold point lookup | 3 | 0 | ~150ms (50ms S3 RTT) |
| Warm point lookup | 0 | 0 | ~1ms (cached) |
| Warm append (cached) | 0 | 3 | ~8ms |
| Shard append (CRDT) | 0 | 2 | ~0.5ms (warm) |
| Full scan (parallel) | 3+K | 0 | ~1 RTT for fetch phase |
| GC | O(live) | 0 | fast regardless of total storage |

---

## Key Features

### Unified Storage
- ONE format (PND2) for ALL workloads — lakehouse, KV, vector, streaming
- ONE commit format (JSON) — simple, debuggable, no binary encoding
- ONE concurrency model (CRDT shards) — no CAS, no retry, no coordination

### Versioning (git-like)
- `branch(collection, branch_name)` — O(1) ref copy
- `checkout(collection, branch_name)` — switch active branch
- `checkout_new(collection, branch_name)` — branch + checkout in one call
- `merge(collection, source, target)` — three-level CRDT merge
- `revert(collection, commit_hash)` — revert to specific commit
- `history(collection)` — walk commit DAG
- `diff(collection, commit_a, commit_b)` — compare manifests

### Concurrency (CRDT, no CAS)
- `append_shard(collection, rows)` — each writer writes its own shard
- `upsert_shard(collection, rows)` — insert-or-update with _rowid + _version
- `delete_shard(collection, rowids)` — tombstones with version vectors
- `read_with_shards(collection)` — merge HEAD + all shards (CRDT union)
- `compact_shards(collection)` — merge shards into HEAD (idempotent)
- Works on ANY storage (local FS, S3, GCS) — no conditional PUTs needed

### Streaming (Kafka-like)
- `create_topic(collection, n_partitions)` — partitions = branches
- `produce(collection, partition, data)` — append to partition
- `consume(collection, partition, group, max_messages)` — read from offset
- `commit_offset(group, collection, partition, offset)` — at-least-once
- `replay_from(collection, partition, offset)` — time-travel read

### Vector Search (IVF)
- `build_ann_index(collection, n_clusters)` — k-means clustering
- `search(collection, query, k, n_probe)` — auto-accelerated ANN
- 100× reduction at PB scale (10M vectors, 1000 clusters)
- 97% recall (n_probe=5 of 20 clusters)

### Maintenance
- `gc(collection)` — read-only reachability analysis
- `vacuum(collections, preserve_days)` — delete dead blobs
- `optimize(collection)` — compact shards + flatten manifests

### Hierarchical Namespaces
- Collection names with `/` for organization: `dev/events`, `prod/users`
- `list_namespaces(parent)` — browse one level at a time

---

## Quick Start

### Unified kernel factory (recommended)

One entry point for all storage backends. Switch between local FS and S3
by changing one line:

```python
import sys, os
sys.path.insert(0, "pond-core")
sys.path.insert(0, "pond-sdk")
sys.path.insert(0, "pond-sdk/extensions/physical_structures")

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

# ACID transactions — commit markers on top of CRDT
tx = storage.begin_tx()
storage.append_shard("users", [{"id": 3, "name": "carol"}], key_col="id", tx_id=tx)
storage.append_shard("orders", [{"id": 3, "amount": 50.0}], key_col="id", tx_id=tx)
storage.commit_tx(tx)  # both visible atomically

# Maintenance
storage.vacuum(preserve_days=7)
storage.optimize()
```

### Direct store construction (advanced)

If you need finer control over the store object:

```python
# Local FS (pure files, no SQLite):
from local_fs_object_store import LocalFSObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
store = LocalFSObjectStore("/var/lib/pond")

# S3:
from s3_object_store import S3ObjectStore
import boto3
store = S3ObjectStore(boto3.client("s3"), bucket="my-pond", prefix="prod")

# Same kernel, same SDK, same everything:
kernel = ObjectStoreNativeKernel(store)
storage = PondStorage(kernel)
```

### Migrating between local FS and S3

The directory layout mirrors S3's key structure, so migrating is a
straight copy:

```bash
# Local → S3:
aws s3 sync /var/lib/pond/ s3://my-pond/prod/

# S3 → local:
aws s3 sync s3://my-pond/prod/ /var/lib/pond/
```

No format conversion needed — blobs and paths use the same layout.

> **Note**: `PondMinimal` (the old SQLite-backed local kernel) is kept for
> backward compat but should not be used for new code. Use `make_kernel()`
> or `LocalFSObjectStore` / `S3ObjectStore` directly.

---

## Design Principles

1. **Simple** — ONE storage format, ONE commit format, ONE concurrency model
2. **Powerful** — branch/merge + CRDT + IVF + streaming + GC + optimize
3. **Performant** — O(1) point lookup, O(1) warm writes, O(1) shard writes
4. **Scalable** — linear PUTs, flat GETs, PB-scale via StatsTree
5. **Efficient** — immutable blobs (deduped), O(live) GC, parallel fetch
6. **Beautiful** — shards ARE branches, CRDT = G-Set union, no CAS
7. **Functional** — lakehouse, KV, vector, streaming, notebook, git
8. **Storage-Independent** — no CAS, works on local FS / S3 / GCS

---

## Repository Structure

```
pond-core/          Kernel (FROZEN — 3 primitives, ~200 LOC)
pond-sdk/           Unified SDK (PondStorage + UnifiedStorage + extensions)
  extensions/
    physical_structures/  PND2, CollectionManifest, StatsTree, Compression
    indexing/             IVF (vector ANN), CollectionIndexer
    maintenance/          GC/Vacuum
    semantic/             Ossie semantic adapter
lenses/             Workload-specific lenses
  keyvalue/        KeyValueLens
  vector/          VectorLens (with IVF)
  streaming/       StreamingLens (with Kafka-like features)
  lakehouse/       LakehouseLens (with DuckDB SQL)
scripts/           Tests + benchmarks + apps
docs/              Documentation
tests/             Architecture laws + integration tests
```
