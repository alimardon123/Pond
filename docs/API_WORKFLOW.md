# Pond — Full API Workflow

> **Audience**: Application developers using Pond as their storage backbone.
> This document shows the complete end-to-end API surface with working
> examples for every operation.
>
> **Language**: Python (via the `pond` PyO3 module). The same operations are
> available from the Rust CLI (`pond` command), Go SDK, and C ABI — see
> the cross-language section at the end.

---

## 0. The 30-second mental model

```
                    ┌──────────────────────────────────┐
                    │          Storage                 │
                    │  (one connection, any backend)   │
                    └──────────────┬───────────────────┘
                                   │
        ┌──────────────┬───────────┼────────────┬────────────────┐
        ▼              ▼           ▼            ▼                ▼
   Data I/O       Versioning   Indexing    Semantic Layer    Maintenance
   write          branch       build_index  s.layer()         gc_stats
   read           checkout     search_index .add_metrics()    vacuum
   write_rows     merge        lookup_index .add_dimensions()
   read_rows      history      drop_index   .add_relationships()
                                          .add_adapter()
```

Everything is **one `Storage` object**. You create it once, point it at
local disk or S3, and use it for data, versioning, indexing, semantic
layers, and maintenance.

---

## 1. Setup — create a Storage connection

```python
from pond import Storage

# Local filesystem (auto-creates the directory)
s = Storage('/var/lib/pond')

# S3 (AWS S3, Cloudflare R2, MinIO, Wasabi, DigitalOcean Spaces, ...)
s = Storage(
    's3://my-bucket/prod',
    access_key='AKIA...',
    secret_key='secret...',
    region='us-east-1',
    # endpoint='https://<account>.r2.cloudflarestorage.com',  # for R2
    # endpoint='http://localhost:9000',                       # for MinIO
)

# S3 with credentials from env (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
s = Storage('s3://my-bucket/prod?region=us-east-1')
```

**One `Storage` object serves all workloads.** No per-workload clients,
no per-format handles. The storage doesn't know or care whether you're
storing KV pairs, vectors, streaming events, or lakehouse tables.

---

## 2. Data I/O — write and read

### 2.1 Raw bytes (JSON, CSV, Parquet, images — anything)

```python
# Write raw bytes to a collection (creates a commit on the active branch)
commit_hash = s.write('users', b'[{"id":1,"name":"alice"}]', 'init')

# Read raw bytes back from the active branch's HEAD
data = s.read('users')   # → b'[{"id":1,"name":"alice"}]'
```

### 2.2 Structured columns (PND2 — auto-encoded, auto-pruned)

This is the recommended path for tabular data. Pond's PND2 format
auto-selects the best encoding per column (RLE / DICT / BITPACK / RAW),
embeds column statistics, and prunes row groups at read time.

```python
# Write structured columns — auto-detects INT64 / FLOAT64 / STRING
s.write_rows('metrics', [
    ('id',    [1, 2, 3, 4, 5]),
    ('score', [1.5, 2.5, 3.5, 4.5, 5.5]),
    ('name',  ['alice', 'bob', 'carol', 'dave', 'eve']),
], 'init metrics')

# Read all columns
cols = s.read_rows('metrics')
# → {'id': [1,2,3,4,5], 'score': [1.5,...], 'name': ['alice',...]}

# Projection — only decode the columns you need
cols = s.read_rows('metrics', columns=['score', 'name'])

# Predicate pruning — skip row groups whose stats don't match
cols = s.read_rows('metrics', predicates=[('id', '>', 2)])
# → {'id': [3,4,5], 'score': [3.5,4.5,5.5], 'name': ['carol','dave','eve']}

# Combine projection + predicate
cols = s.read_rows('metrics',
                   columns=['name'],
                   predicates=[('id', '=', 3)])
# → {'name': ['carol']}
```

**Supported predicates**: `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`.

**Auto-index acceleration**: if you build a simple index on a column
(see §4.1) and then query with `('col', '=', value)`, the read path
automatically uses the index for O(1) lookup. If the key isn't in the
index, the read returns empty immediately — no row-group scan.

---

## 3. Version control — git for your data

Every `write` / `write_rows` creates a commit. Branches are O(1) ref
copies. Merges use CRDT semantics (no CAS, no conflicts).

```python
# Create a branch from the current HEAD
s.branch('users', 'dev')

# Switch the active branch (subsequent reads/writes go to 'dev')
s.checkout('users', 'dev')

# Create AND checkout in one call (like `git checkout -b`)
s.checkout_new('users', 'feature-x')

# Write on the dev branch
s.write('users', b'[{"id":2,"name":"bob"}]', 'add bob on dev')

# Switch back to main
s.checkout('users', 'main')

# Merge dev into main (or any target branch)
s.merge('users', source='dev', target='main', message='merge dev')

# Walk commit history
for commit in s.history('users', limit=10):
    print(commit)
# → [{'hash': 'abc123', 'parent': 'def456', 'message': 'merge dev', 'index': 3, ...}, ...]

# Undo the last N commits
s.undo('users', steps=2)

# Revert to a specific commit hash (from history())
s.revert('users', commit_hash='abc123...')

# See which branch is active
print(s.get_active_branch('users'))   # → 'main'

# Explicitly set the active branch (alternative to checkout)
s.set_active_branch('users', 'dev')
```

### 3.1 List collections

```python
# List all collections in the storage
for coll in s.ls():
    print(coll)
# → [{'name': 'users', 'head': 'abc123', ...}, {'name': 'metrics', ...}]
```

---

## 4. Indexing — accelerate reads

### 4.1 Simple secondary index (composite multi-key)

A simple index maps key values → rowids. Supports composite keys
(multiple columns joined into one index key).

```python
# Build a single-column index — pass rows explicitly
rows = [
    ('user:1', {'name': 'alice', 'city': 'NYC', 'id': 1}),
    ('user:2', {'name': 'bob',   'city': 'LA',  'id': 2}),
    ('user:3', {'name': 'carol', 'city': 'NYC', 'id': 3}),
]
s.build_index('users', 'by_name', 'simple',
              config={'key_field': 'name'},
              rows=rows)

# Build a composite multi-key index
s.build_index('users', 'by_name_city', 'simple',
              config={'key_fields': ['name', 'city']},
              rows=rows)

# O(1) exact lookup
rowid = s.lookup_index('users', 'by_name', 'alice')
# → 'user:1'

# Auto-acceleration: read_rows with an equality predicate on an indexed
# column will use the index automatically
result = s.read_rows('users', predicates=[('name', '=', 'bob')])
# → uses 'by_name' index internally; returns empty immediately if not found
```

### 4.2 IVF vector index (k-means clusters)

For approximate nearest neighbor (ANN) search on vector collections.

```python
# First, write vectors to a collection (as PND2 columns)
s.write_rows('vectors', [
    ('id', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
    ('vec', [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6], [0.7, 0.8],
             [0.9, 1.0], [1.1, 1.2], [1.3, 1.4], [1.5, 1.6],
             [1.7, 1.8], [1.9, 2.0]]),
], 'init vectors')

# Build an IVF index (k-means clusters)
s.build_index('vectors', 'ann', 'ivf',
              config={'n_clusters': 3, 'metric': 'euclidean'})

# Search — returns [(distance, vector_id), ...] sorted by distance
results = s.search_index('vectors', 'ivf',
                          query=[0.2, 0.3],
                          k=5,
                          n_probe=2)   # clusters to search
# → [(0.14, 1), (0.28, 2), (0.42, 3), ...]
```

### 4.3 HNSW vector index (hierarchical navigable small world)

For O(log N) ANN search — better recall than IVF at small-to-medium scale.

```python
s.build_index('vectors', 'ann', 'hnsw',
              config={'m': 16, 'ef_construction': 200, 'metric': 'l2'})

results = s.search_index('vectors', 'hnsw',
                          query=[0.2, 0.3],
                          k=5,
                          ef=50)   # beam width
# → [(0.14, 1), (0.28, 2), ...]
```

### 4.4 Index management

```python
# List all indexes on a collection
print(s.list_indexes('vectors'))
# → ['ann']

# Drop an index (works for all index types)
s.drop_index('vectors', 'ann')   # → True
```

---

## 5. Semantic Layer — metrics, dimensions, relationships

A **Semantic Layer** is a coherent set of metrics/dimensions/relationships
over Pond collections, exposed to external systems (BI tools, query
engines, AI agents) via one or more adapters.

### Why "layer" (not "model")

The word "model" collides with ML models, which Pond may host in the
future. "Semantic Layer" is the industry-standard term (dbt Semantic
Layer, Cube Semantic Layer, Looker LookML).

### 5.1 Create a layer

```python
# Create a layer with default adapter (['ossie']) and reflection off
m = s.layer('sales')

# Create with explicit adapters + reflection enabled
m = s.layer('sales', adapters=['ossie', 'cube'], enable_reflection=True)
# Note: only 'ossie' is built-in; 'cube' must be registered first.

# List all layers
print(s.layers())   # → ['sales']
```

### 5.2 Batch-add datasets, metrics, dimensions, relationships

```python
# Add datasets (collections that the layer reads from)
m.add_datasets(['orders', 'users', 'products'])

# Add metrics — dict of {name: SQL expression}
m.add_metrics({
    'revenue':         'SUM(orders.amount)',
    'order_count':     'COUNT(orders.id)',
    'avg_order_value': 'revenue / order_count',
})

# Add dimensions — dict of {name: (dataset, field, data_type)}
m.add_dimensions({
    'country':    ('users',  'country',    'string'),
    'order_date': ('orders', 'created_at', 'datetime'),
})

# Add relationships — dict of {name: (from, to, join_condition)}
m.add_relationships({
    'user_orders':     ('users',    'orders',   'users.id = orders.user_id'),
    'product_orders':  ('products', 'orders',   'products.id = orders.product_id'),
})
```

### 5.3 Independent adapter management (multi-adapter)

A layer can be exposed via multiple adapters simultaneously. External
systems query the same layer through whichever adapter they speak.

```python
# List currently enabled adapters
print(m.adapters())   # → ['ossie', 'cube']

# Add another adapter (idempotent — safe to call repeatedly)
m.add_adapter('dbt')

# Remove an adapter (independent of the spec)
m.remove_adapter('cube')   # → True
m.remove_adapter('cube')   # → False (already removed)

print(m.adapters())   # → ['ossie', 'dbt']
```

**Auto-exposure**: there is no explicit "export" step. When you register
an adapter, the layer is queryable via that adapter's protocol. Adapters
read the layer's spec directly from storage at query time.

### 5.4 Reflection (Dremio-style query acceleration)

```python
# Enable/disable reflection (idempotent)
m.enable_reflection()
m.disable_reflection()
```

When reflection is enabled, the layer is registered with the reflection
subsystem so reflection-aware query engines can find it and use it for
query acceleration.

### 5.5 Inspect the layer

```python
# Full overview — returns a dict
info = m.info()
# → {
#     'name': 'sales',
#     'adapters': ['ossie', 'dbt'],
#     'reflection_enabled': True,
#     'datasets': ['orders', 'users', 'products'],
#     'metrics': ['revenue', 'order_count', 'avg_order_value'],
#     'dimensions': ['country', 'order_date'],
#     'relationships': ['user_orders', 'product_orders'],
#   }

# Individual listings
m.datasets()        # → ['orders', 'users', 'products']
m.metrics()         # → ['revenue', 'order_count', 'avg_order_value']
m.dimensions()      # → ['country', 'order_date']
m.relationships()   # → ['user_orders', 'product_orders']
```

### 5.6 Optional one-shot export

`export()` is OPTIONAL. It's for one-shot snapshots (file export,
debugging, migration). Adapters can read the layer's spec directly from
storage at query time — that's the default "auto-exposure" path.

```python
# Export in a specific adapter's format
ossie_spec = m.export('ossie')
# → {'name': 'sales', 'datasets': [...], 'metrics': [...], ...} in Ossie format

# Export using the first adapter in the layer's adapters list
default_spec = m.export()   # equivalent to m.export(m.adapters()[0])
```

### 5.7 Multiple layers coexist

```python
sales   = s.layer('sales',   adapters=['ossie'])
product = s.layer('product', adapters=['ossie'])
finance = s.layer('finance', adapters=['ossie'])

# Each layer is independent
sales.add_datasets(['orders'])
product.add_datasets(['products'])
finance.add_datasets(['invoices'])

print(s.layers())   # → ['sales', 'product', 'finance']

# Each layer's spec is stored separately under semantic_layers/{name}/
```

---

## 6. Maintenance — GC and vacuum

```python
# Read-only reachability analysis (no deletion)
stats = s.gc_stats(compute_size=False)
# → {'live': 150, 'dead': 12, 'dead_size_bytes': -1}

stats = s.gc_stats(compute_size=True)
# → {'live': 150, 'dead': 12, 'dead_size_bytes': 4096}

# Vacuum — delete unreachable blobs with time-travel safety
result = s.vacuum(preserve_days=7, dry_run=True)
# → {'deleted': 0, 'preserved': 12, 'dry_run': True}

# Real vacuum (actually deletes)
result = s.vacuum(preserve_days=7, dry_run=False)
# → {'deleted': 8, 'preserved': 4, 'dry_run': False}
```

`preserve_days` keeps blobs referenced by commits younger than N days,
so time-travel reads still work for recent history. Older unreachable
blobs are deleted.

---

## 7. Complete end-to-end example

```python
from pond import Storage

# === Setup ===
s = Storage('/var/lib/pond')

# === Data: write structured columns ===
s.write_rows('orders', [
    ('id',         [1, 2, 3, 4]),
    ('user_id',    [1, 2, 1, 3]),
    ('amount',     [50.0, 75.0, 20.0, 100.0]),
    ('created_at', [1718000000, 1718000100, 1718000200, 1718000300]),
], 'init orders')

s.write_rows('users', [
    ('id',      [1, 2, 3]),
    ('name',    ['alice', 'bob', 'carol']),
    ('country', ['USA', 'UK', 'USA']),
], 'init users')

# === Version control: branch + merge ===
s.branch('orders', 'dev')
s.checkout('orders', 'dev')
s.write_rows('orders', [
    ('id',         [5]),
    ('user_id',    [2]),
    ('amount',     [200.0]),
    ('created_at', [1718000400]),
], 'add order 5 on dev')
s.checkout('orders', 'main')
s.merge('orders', source='dev', target='main', message='merge dev')

# === Indexing: build a secondary index ===
# (must provide rows for simple indexes — the indexer doesn't auto-read yet)
rows = [(str(r['id']), r) for r in [
    {'id': 1, 'user_id': 1, 'amount': 50.0, 'created_at': 1718000000},
    {'id': 2, 'user_id': 2, 'amount': 75.0, 'created_at': 1718000100},
    {'id': 3, 'user_id': 1, 'amount': 20.0, 'created_at': 1718000200},
    {'id': 4, 'user_id': 3, 'amount': 100.0, 'created_at': 1718000300},
    {'id': 5, 'user_id': 2, 'amount': 200.0, 'created_at': 1718000400},
]]
s.build_index('orders', 'by_user', 'simple',
              config={'key_field': 'user_id'},
              rows=rows)

# O(1) lookup
order_id = s.lookup_index('orders', 'by_user', '1')
print(f"First order for user 1: {order_id}")

# Auto-accelerated read (uses the index)
user1_orders = s.read_rows('orders', predicates=[('user_id', '=', 1)])
print(f"User 1's orders: {user1_orders}")

# === Semantic Layer: define metrics over the data ===
sales = s.layer('sales', adapters=['ossie'], enable_reflection=True)
sales.add_datasets(['orders', 'users'])
sales.add_metrics({
    'total_revenue':   'SUM(orders.amount)',
    'order_count':     'COUNT(orders.id)',
    'avg_order_value': 'total_revenue / order_count',
})
sales.add_dimensions({
    'country':    ('users',  'country',    'string'),
    'order_date': ('orders', 'created_at', 'datetime'),
})
sales.add_relationships({
    'user_orders': ('users', 'orders', 'users.id = orders.user_id'),
})

# Inspect
print(sales.info())

# Optional one-shot export to Ossie format
ossie_spec = sales.export('ossie')
print(f"Ossie spec: {ossie_spec}")

# === Maintenance: GC + vacuum ===
stats = s.gc_stats(compute_size=True)
print(f"GC stats: {stats}")

result = s.vacuum(preserve_days=7, dry_run=True)
print(f"Vacuum (dry run): {result}")
```

---

## 8. Cross-language equivalents

### 8.1 Rust CLI (`pond` command)

```bash
# Init + write + read
pond init /var/lib/pond
pond write users --json '[{"id":1,"name":"alice"}]' -m "init"
pond read users

# Version control
pond branch users dev
pond checkout -b users dev
pond merge users dev -m "merge dev"
pond history users --limit 10
pond undo users 2
pond ls
```

### 8.2 Go SDK

```go
import "github.com/pond/pond-go/pond"

store, _ := pond.OpenStorage("/var/lib/pond")
defer store.Free()

hash, _ := store.Write("users", []byte(`[{"id":1}]`), "init")
data, _  := store.Read("users")

store.Branch("users", "dev")
store.Checkout("users", "dev")
store.Merge("users", "dev", "main", "merge dev")
```

### 8.3 C ABI (`pond.h`)

```c
#include "pond.h"

PondKernel* k = pond_kernel_new("/var/lib/pond");
char hash[65];
pond_kernel_write(k, data, len, hash);

PondStorage* s = pond_storage_new(k);
pond_storage_write(s, "users", data, len, "init", hash);
pond_storage_branch(s, "users", "dev");
pond_storage_merge(s, "users", "dev", "main", "merge dev");
```

### 8.4 Python reference SDK (legacy — being phased out)

```python
import sys
sys.path.insert(0, "bindings/python/core")
sys.path.insert(0, "bindings/python/sdk")

from make_kernel import make_kernel
from pond_storage import PondStorage

kernel = make_kernel("file:///var/lib/pond")
storage = PondStorage(kernel)

storage.write("users", [{"id": 1, "name": "alice"}], key_col="id")
storage.branch("users", "dev")
storage.merge("users", "dev")
```

---

## 9. API reference (quick lookup)

### `Storage` (the main class)

| Method | Signature | Returns | Purpose |
|---|---|---|---|
| `Storage` | `(location, access_key?, secret_key?, region?, endpoint?)` | `Storage` | Create a connection (local FS or S3) |
| `write` | `(collection, data: bytes, message: str)` | `str` (commit hash) | Write raw bytes |
| `read` | `(collection)` | `bytes` | Read raw bytes from HEAD |
| `write_rows` | `(collection, columns: [(name, [values])], message)` | `str` | Write PND2 structured columns |
| `read_rows` | `(collection, columns?, predicates?)` | `dict` | Read with projection + pruning |
| `branch` | `(collection, branch_name)` | `str` | Create a branch |
| `checkout` | `(collection, branch_name)` | `None` | Switch active branch |
| `checkout_new` | `(collection, branch_name)` | `None` | Create + checkout (like `git -b`) |
| `merge` | `(collection, source, target?, message)` | `str` | Merge source → target |
| `history` | `(collection, limit=20)` | `list[dict]` | Walk commit history |
| `undo` | `(collection, steps=1)` | `str` | Undo last N commits |
| `revert` | `(collection, commit_hash)` | `None` | Revert to specific commit |
| `ls` | `()` | `list[dict]` | List all collections |
| `get_active_branch` | `(collection)` | `str` | Get active branch name |
| `set_active_branch` | `(collection, branch_name)` | `None` | Set active branch |
| `build_index` | `(collection, index_name, index_type, config?, rows?)` | `str` | Build index (`simple`/`ivf`/`hnsw`) |
| `lookup_index` | `(collection, index_name, index_key)` | `str?` | O(1) exact lookup (simple indexes) |
| `search_index` | `(collection, index_type, query, k=10, n_probe=10, ef=50)` | `list[(dist, id)]` | ANN search (ivf/hnsw) |
| `drop_index` | `(collection, index_name)` | `bool` | Drop an index |
| `list_indexes` | `(collection)` | `list[str]` | List indexes on a collection |
| `gc_stats` | `(compute_size=False)` | `dict` | Read-only GC analysis |
| `vacuum` | `(preserve_days, dry_run)` | `dict` | Delete unreachable blobs |
| `layer` | `(name, adapters?, enable_reflection=False)` | `SemanticLayer` | Get/create a semantic layer handle |
| `layers` | `()` | `list[str]` | List all semantic layer names |

### `SemanticLayer` (handle returned by `s.layer()`)

| Method | Signature | Returns | Purpose |
|---|---|---|---|
| `add_datasets` | `(datasets: list[str])` | `None` | Batch-add datasets |
| `add_metrics` | `(metrics: dict[str, str])` | `None` | Batch-add metrics `{name: expr}` |
| `add_dimensions` | `(dimensions: dict[str, (dataset, field, type)])` | `None` | Batch-add dimensions |
| `add_relationships` | `(relationships: dict[str, (from, to, join)])` | `None` | Batch-add relationships |
| `info` | `()` | `dict` | Full overview |
| `datasets` | `()` | `list[str]` | List datasets |
| `metrics` | `()` | `list[str]` | List metric names |
| `dimensions` | `()` | `list[str]` | List dimension names |
| `relationships` | `()` | `list[str]` | List relationship names |
| `adapters` | `()` | `list[str]` | List enabled adapters |
| `add_adapter` | `(adapter: str)` | `None` | Add adapter (idempotent) |
| `remove_adapter` | `(adapter: str)` | `bool` | Remove adapter (True if present) |
| `export` | `(adapter=None)` | `dict` | One-shot export in adapter format |
| `enable_reflection` | `()` | `None` | Enable reflection |
| `disable_reflection` | `()` | `None` | Disable reflection |

---

## 10. Storage layout (for debugging)

```
.pond/                                    (local FS) or s3://bucket/prefix/
├── blobs/                                content-addressed blobs (SHA-256 hash)
│   ├── ab/abc123...                      first 2 hex chars = directory
│   └── ...
├── collections/
│   └── {name}/
│       └── _branches/
│           └── {branch}/
│               └── commit                → commit hash (HEAD pointer)
├── collections/{name}/indexes/{idx}      simple index JSON blob
├── collections/{name}/indexes/ivf        IVF index binary blob
├── collections/{name}/indexes/hnsw       HNSW index binary blob
├── semantic_layers/
│   └── {layer}/
│       ├── _meta                         → {name, adapters, enable_reflection}
│       ├── datasets/{ds}                 → {name, source}
│       ├── metrics/{name}                → {name, expression, description, format}
│       ├── dimensions/{name}             → {name, dataset, field, data_type}
│       └── relationships/{name}          → {name, from, to, condition}
└── config                                (optional) PondConfig JSON
```

All paths are kernel refs (mutable name → immutable hash mappings).
The kernel's 3 primitives are: `write(bytes) → hash`, `read(hash) → bytes`,
`reference(name, hash) → ()`.

---

## 11. Design principles (why the API looks like this)

1. **Simple** — ONE storage format (PND2), ONE commit format (JSON), ONE concurrency model (CRDT)
2. **Powerful** — branch/merge + CRDT + IVF + HNSW + streaming + semantic layers + GC
3. **Performant** — O(1) point lookup, O(1) warm writes, O(1) shard writes
4. **Scalable** — linear PUTs, flat GETs, PB-scale via StatsTree
5. **Efficient** — immutable blobs (deduped), O(live) GC, parallel fetch
6. **Beautiful** — shards ARE branches, CRDT = G-Set union, no CAS
7. **Functional** — lakehouse, KV, vector, streaming, semantic, OLTP
8. **Storage-Independent** — no CAS, works on local FS / S3 / R2 / MinIO / GCS

The API surface is deliberately small: **one `Storage` class** with
methods grouped into 5 sections (Data I/O, Versioning, Indexing,
Semantic Layer, Maintenance). Everything else is an implementation
detail.
