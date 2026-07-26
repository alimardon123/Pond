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
│
├── (pruning infrastructure — standalone, not PhysicalStructure subclasses)
│   ├── ZoneMap (pruning.py)     — per-row-group {min, max, null_count} data class
│   ├── ColumnPredicate           — single-column filter (=, !=, <, >, IN, BETWEEN)
│   ├── PruningPredicate          — combines ColumnPredicates (AND/OR)
│   ├── ZoneMapIndex              — ProllyTreeIndex of zone maps
│   ├── PruningReader             — generic reader with zone-map pruning
│   ├── ColumnChunkZoneMap        — per-column-chunk zone maps (finer pruning)
│   ├── ColumnChunkStorage        — per-column-chunk blob storage (true I/O savings)
│   └── EncodedChunkStorage       — FastLanes-style encoded chunk storage
│
└── (built into SDK core, not here)
    └── ProllyTree Index — O(log N) point lookup (collection_index.py)
        This is the "default" Physical Structure, used by every Lens.
```

Note: an earlier `ZoneMap` PhysicalStructure (in `zone_map.py`) was
deleted as dead code — see `docs/DESIGN_REVIEW_2026_07_26.md` (C3).
The active `ZoneMap` is the `@dataclass` in `pruning.py`.

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
| ProllyTree Index | `index` (in auto_index.py) | `{name}__index__{index_name}` |

Any Lens can resolve any of these refs. This is the cross-Lens sharing
contract (Track 2 proved it works).

## Files

| File | Class | Purpose |
|---|---|---|
| `base.py` | `PhysicalStructure` | Abstract base class. Defines the common interface. |
| `bloom_filter.py` | `BloomFilter` | Probabilistic membership test. O(1) query. |
| `statistics.py` | `Statistics` | Column-level min/max/null_count. |
| `pruning.py` | `ZoneMap`, `ColumnPredicate`, `PruningPredicate` | Vortex-style pruning data structures. `ZoneMap` here is a `@dataclass` (min/max/null_count/row_count/column_chunks). |
| `zone_map_index.py` | `ZoneMapIndex` | ProllyTreeIndex of zone maps. Stores min/max per data blob. |
| `pruning_reader.py` | `PruningReader` | Generic reader with zone-map pruning. Skips blobs without decoding. |
| `column_chunk_zone_map.py` | `ColumnChunkZoneMap`, `ColumnChunkStats` | Per-column-chunk zone maps for finer-grained pruning within surviving row groups. |
| `column_chunk_storage.py` | `ColumnChunkStorage` | Per-column-chunk blob storage (true I/O savings on object storage). |
| `encoding.py` | `ColumnEncoding`, `EncodingHeader`, `encode_column`, `eval_predicate_encoded`, `decode_surviving_values` | FastLanes-style structural encodings (RLE/Dict/Bitpack/Raw). |
| `encoded_chunk_storage.py` | `EncodedChunkStorage` | Combines ColumnChunkStorage + encoding.py. Per-column-chunk encoded blobs with encoded predicate eval at read time. |

## Usage

```python
from extensions.physical_structures import BloomFilter, Statistics

# Build (any Lens can build)
BloomFilter.build(kernel, "users", ["user_1", "user_2", "user_3"])
Statistics.build(kernel, "users", table_data)

# Query (any Lens can query — Track 2 proved cross-Lens sharing)
BloomFilter.query(kernel, "users", "user_2")     # → True
Statistics.can_prune(stats, "age", 999)           # → True (skip chunk)

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
