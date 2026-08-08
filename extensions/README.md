# extensions/

Shared extensions that work with any lens. Extensions add domain-specific
capabilities (indexing, maintenance, semantic) without modifying the core
or lenses.

## Structure

```
extensions/
├── indexing/
│   ├── rust/           # Rust IVF index (Bug 10 fixed)
│   └── README.md
└── README.md           # This file
```

Python extensions live at `bindings/python/sdk/extensions/` (they're
Python-specific and use Python's type system).

## Indexing

| Extension | Rust | Python | Notes |
|---|---|---|---|
| IVFIndex | ✅ `extensions/indexing/rust/` | ✅ `bindings/python/sdk/extensions/indexing/ivf_index.py` | Rust fixes Bug 10 (per-cluster blob refs) |
| HNSWIndex | ❌ | ✅ | Pure-Python; 10-100x slower than Rust would be |
| CollectionIndexer | ❌ | ✅ | Secondary indexes (JSON blob format) |

## Maintenance

| Extension | Rust | Python | Notes |
|---|---|---|---|
| GarbageCollector | ✅ `core/storage/src/maintenance.rs` | ✅ `bindings/python/sdk/extensions/maintenance/vacuum.py` | Rust port complete |
| Tombstones | ✅ `core/storage/src/maintenance.rs` | ✅ `bindings/python/sdk/maintenance.py` | Both languages |

## Semantic

| Extension | Rust | Python | Notes |
|---|---|---|---|
| OssieAdapter | ❌ | ✅ | Defer (placeholder name, not a real spec) |
