# extensions/indexing/

Indexing extensions — IVF (vector ANN), HNSW (graph ANN), CollectionIndexer
(secondary indexes).

## Rust

| Crate | Path | Purpose |
|---|---|---|
| `pond_vector_index` | `rust/` | IVF index — Inverted File for approximate nearest neighbor. Fixes Bug 10 (per-cluster blob references for true I/O reduction). |
| `pond_collection_index` | `collection_index/` | CollectionIndexer — secondary indexes (JSON blob format). Multi-key support. |
| `pond_hnsw_index` | `hnsw_index/` | HNSW index — Hierarchical Navigable Small World. O(log N) ANN. Chunked storage (one blob per layer). |

## Python

Python indexing extensions live at:
`bindings/python/sdk/extensions/indexing/`

| File | Purpose |
|---|---|
| `ivf_index.py` | Python IVF index (has Bug 10 — reads ALL vectors) |
| `hnsw_index.py` | Python HNSW index (pure-Python, 10-100x slower than Rust) |
| `collection_index.py` | Python secondary index (JSON blob format) |
| `base.py` | Abstract CollectionIndexerInterface |

## Test results

| Crate | Tests | Status |
|---|---|---|
| `pond_vector_index` | 5 | ✅ All pass |
| `pond_collection_index` | 6 | ✅ All pass |
| `pond_hnsw_index` | 9 | ✅ All pass |
