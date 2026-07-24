# extensions/physical_structures/

Acceleration structures — each is `f(snapshot) → artifact`.

## Purpose

Physical Structures accelerate access to data. They are deterministic
functions of a snapshot: same snapshot → same structure. They can be
lost without data loss (rebuildable). They can be shared across Lenses
(Track 2 proved this).

Think of them as **LLVM optimization passes**: each pass transforms
intermediate representation for a specific purpose, but the IR stays
the same. Here, the "IR" is the immutable byte graph; Physical
Structures are derived artifacts that accelerate specific access
patterns.

## Type hierarchy

```
PhysicalStructure (abstract base — base.py)
├── BloomFilter     — O(1) probabilistic membership test
├── Statistics      — column min/max/null_count for range pruning
├── ZoneMap         — per-chunk min/max for chunk-granularity pruning
│
└── (built into SDK core, not here)
    └── ProllyTree Index — O(log N) point lookup (auto_index.py)
        This is the "default" Physical Structure, used by every Lens.
        Future index types (HNSW, Trie, etc.) would be added here.
```

## Common interface

Every Physical Structure implements (from `base.py`):

| Method | Purpose |
|---|---|
| `build(kernel, collection, source_data)` | Create from data, store as kernel blob |
| `load(kernel, collection)` | Read from kernel (returns dict or None) |
| `exists(kernel, collection)` | Check if it exists (tombstone-aware) |
| `delete(kernel, collection)` | Tombstone the ref (RFC-0008) |
| `query(kernel, collection, *args)` | Type-specific query |
| `verify(kernel, collection)` | Check validity |

## Naming convention

All structures use: `__{type_name}/{collection}`

| Type | `type_name` | Ref example |
|---|---|---|
| BloomFilter | `bloom` | `__bloom/users` |
| Statistics | `stats` | `__stats/users` |
| ZoneMap | `zonemaps` | `__zonemaps/events` |
| ProllyTree Index | `index` (in auto_index.py) | `{name}__index__{index_name}` |

Any Lens can resolve any of these refs. This is the cross-Lens sharing
contract (Track 2 proved it works).

## Files

| File | Class | Purpose |
|---|---|---|
| `base.py` | `PhysicalStructure` | Abstract base class. Defines the common interface. |
| `bloom_filter.py` | `BloomFilter` | Probabilistic membership test. O(1) query, configurable false positive rate. |
| `statistics.py` | `Statistics` | Column-level min/max/null_count. Used for range pruning (skip chunks where value can't exist). |
| `zone_map.py` | `ZoneMap` | Per-chunk min/max. Finer-grained than Statistics (which covers the whole collection). |

## Usage

```python
from extensions.physical_structures import BloomFilter, Statistics, ZoneMap

# Build (any Lens can build)
BloomFilter.build(kernel, "users", ["user_1", "user_2", "user_3"])
Statistics.build(kernel, "users", table_data)
ZoneMap.build(kernel, "events", {"chunk_0": [1,2,3], "chunk_1": [4,5,6]})

# Query (any Lens can query — Track 2 proved cross-Lens sharing)
BloomFilter.query(kernel, "users", "user_2")     # → True
Statistics.can_prune(stats, "age", 999)           # → True (skip chunk)
ZoneMap.query(kernel, "events", 5, 7)             # → ["chunk_1"]

# Delete (tombstone — doesn't affect other structures)
BloomFilter.delete(kernel, "users")
```

## Adding a new Physical Structure type

1. Create a new file in this directory (e.g., `histogram.py`).
2. Subclass `PhysicalStructure` from `base.py`.
3. Set `type_name` (used in the naming convention).
4. Implement `build()`, `load()`, and `query()`.
5. Add to `__init__.py` re-exports.
6. No existing code needs to change.
