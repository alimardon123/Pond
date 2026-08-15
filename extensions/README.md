# extensions/

Extensions add capabilities to Pond. Each extension is independent and
works with any collection via the Storage API.

## Structure

```
extensions/
├── indexes/              # Index extensions
│   ├── ivf/rust/         # IVF index (vector ANN)
│   ├── hnsw/rust/        # HNSW index (vector ANN, O(log N))
│   ├── simple/rust/      # Simple secondary index (key → rowid)
│   └── README.md
├── semantic/             # Semantic model adapters
│   └── rust/             # SemanticModelAdapter trait + definitions
└── README.md             # This file
```

## Indexes

All indexes share a UNIFIED API via Storage:

```python
# Build any index type — same method, different index_type + config
s.build_index('users', 'by_name', 'simple',
    config={'key_field': 'name'},
    rows=[('user:1', {'name': 'alice'})])

s.build_index('vectors', 'ann', 'ivf',
    config={'n_clusters': 10, 'metric': 'euclidean'})

s.build_index('vectors', 'ann', 'hnsw',
    config={'m': 16, 'metric': 'l2'})

# Lookup (exact — for simple indexes)
s.lookup_index('users', 'by_name', 'alice')

# Search (approximate — for vector indexes)
s.search_index('vectors', 'ivf', query=[0.1, 0.2], k=10)

# Drop, list — same for ALL index types
s.drop_index('users', 'by_name')
s.list_indexes('users')
```

| Index Type | Location | Purpose |
|---|---|---|
| `simple` | `indexes/simple/rust/` | Secondary index (key → rowid). Like a DB secondary index. |
| `ivf` | `indexes/ivf/rust/` | Inverted File for vector ANN. Bug 10 fixed (per-cluster blob refs). |
| `hnsw` | `indexes/hnsw/rust/` | HNSW graph for vector ANN. O(log N) search. |

## Semantic

Semantic model adapters translate between Pond's internal definitions
(metrics, dimensions, relationships) and external semantic model formats
(Cube, dbt, Malloy, etc.).

```rust
use pond_semantic::{SemanticDefinitions, SemanticModelAdapter};

let mut defs = SemanticDefinitions::new();
defs.add_metric("revenue", "SUM(amount)");
defs.add_dimension("country", "string");
```
