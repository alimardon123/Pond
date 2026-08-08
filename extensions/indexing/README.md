# extensions/indexing/

Indexing extensions — IVF (vector ANN), HNSW (graph ANN), CollectionIndexer
(secondary indexes).

## Rust

| File | Purpose |
|---|---|
| `rust/src/lib.rs` | IVFIndex — Inverted File index for approximate nearest neighbor search. Fixes Bug 10 (per-cluster blob references for true I/O reduction). |

## Python

Python indexing extensions live at:
`bindings/python/sdk/extensions/indexing/`

| File | Purpose |
|---|---|
| `ivf_index.py` | Python IVF index (has Bug 10 — reads ALL vectors) |
| `hnsw_index.py` | Python HNSW index (pure-Python, slow) |
| `collection_index.py` | Python secondary index (JSON blob format) |
| `base.py` | Abstract CollectionIndexerInterface |
