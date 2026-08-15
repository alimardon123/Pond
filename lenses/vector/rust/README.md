# lenses/vector/rust/

Rust implementation of VectorLens — vector storage with ANN search.

## Status

**Implemented (core API).** The following operations are ported:

| Operation | Status | Notes |
|---|---|---|
| `insert(collection, id, vector, metadata)` | ✅ | Buffer vector inserts |
| `commit(collection, message)` | ✅ | Flush buffer to PND2 (id + dim_0..N + metadata) |
| `get_vector(collection, id)` | ✅ | Read single vector by ID |
| `get_all(collection)` | ✅ | Read all vectors |
| `count(collection)` | ✅ | Count vectors |
| `search(collection, query, k, n_probe, ef)` | ✅ | Auto-accelerated: HNSW → IVF → linear scan |
| `build_ivf_index(collection, n_clusters, metric)` | ✅ | Build IVF index (uses extension) |
| `build_hnsw_index(collection, m, ef_construction, metric)` | ✅ | Build HNSW index (uses extension) |

## Architecture

VectorLens is a **LENS** (workload model). It uses two **INDEX extensions**
for acceleration:

```
VectorLens (this lens — workload-specific API)
  ↓ uses for acceleration (optional)
  ├── IVFIndex (extensions/indexing/rust/) — Inverted File index
  └── HNSWIndex (extensions/indexing/hnsw_index/) — HNSW graph index
  ↓ uses for storage
UnifiedStorage (core/storage/) — PND2 encoding, manifest, versioning
```

The indexes are INDEPENDENT extensions that work with ANY collection.
VectorLens provides the vector workload API and optionally uses those
indexes for accelerated search. Without indexes, it falls back to
linear scan.

## Search Algorithm

```
1. Try HNSW index → O(log N) — best for high-recall at low latency
2. Try IVF index   → O(n_probe × cluster_size) — good for large datasets
3. Linear scan     → O(N) — fallback for small collections or no index
```

## Usage

```rust
use pond_vector_lens::VectorLens;
use pond_storage::UnifiedStorage;

let storage = UnifiedStorage::new_local("/var/lib/pond").unwrap();
let lens = VectorLens::new(storage);

// Insert vectors
lens.insert("vectors", "1", &[0.0, 0.0, 0.0], None);
lens.insert("vectors", "2", &[1.0, 1.0, 1.0], Some(r#"{"label":"test"}"#));
lens.commit("vectors", "init").unwrap();

// Search (auto-accelerated: HNSW → IVF → linear scan)
let results = lens.search("vectors", &[0.1, 0.1, 0.1], 5, 10, 50).unwrap();
// → [(distance, vector_id), ...]

// Build IVF index for faster search
lens.build_ivf_index("vectors", 10, "euclidean").unwrap();

// Build HNSW index for O(log N) search
lens.build_hnsw_index("vectors", 16, 200, "l2").unwrap();
```

## Tests

8 tests pass:
- `test_insert_and_commit`: Insert + commit → PND2
- `test_get_all`: Read all vectors back
- `test_count`: Count vectors
- `test_get_vector`: Read single vector by ID
- `test_get_vector_not_found`: Returns None for missing ID
- `test_linear_scan_search`: Search without index (linear scan)
- `test_search_empty_collection`: Search on single-element collection
- `test_metadata_storage`: Verify metadata is stored and retrieved
