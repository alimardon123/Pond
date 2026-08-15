# extensions/indexes/

Index extensions — all share a unified API via Storage.

## Index Types

| Type | Crate | Location | Purpose |
|---|---|---|---|
| `simple` | `pond_simple_index` | `simple/rust/` | Secondary index (key → rowid). Multi-key support. |
| `ivf` | `pond_ivf_index` | `ivf/rust/` | IVF vector ANN. Bug 10 fixed (per-cluster blob refs). |
| `hnsw` | `pond_hnsw_index` | `hnsw/rust/` | HNSW graph ANN. O(log N) search. Chunked storage. |

## Unified API

```python
# Build — same method, different index_type + config
s.build_index('users', 'by_name', 'simple', config={'key_field': 'name'}, rows=rows)
s.build_index('vectors', 'ann', 'ivf', config={'n_clusters': 10, 'metric': 'euclidean'})
s.build_index('vectors', 'ann', 'hnsw', config={'m': 16, 'metric': 'l2'})

# Lookup (exact — simple indexes)
s.lookup_index('users', 'by_name', 'alice')

# Search (approximate — vector indexes)
s.search_index('vectors', 'ivf', query=[0.1, 0.2], k=10)

# Drop, list — same for ALL types
s.drop_index('users', 'by_name')
s.list_indexes('users')
```

## Adding New Index Types

1. Create `extensions/indexes/{name}/rust/` with a new crate
2. Implement the index build/search logic
3. Add the type to Storage's `build_index` / `search_index` dispatch
4. Update this README

Each index type is independent — adding a new one doesn't modify existing code.
