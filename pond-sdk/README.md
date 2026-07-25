# pond-sdk

The Pond SDK — Layer 1 (storage infrastructure) and Layer 2 (extensions)
on top of the kernel.

## What it is

The SDK provides the shared infrastructure for all Pond lenses:

- **PondLens** (`base_lens.py`) — the shared namespace base for ALL lenses.
  Provides ref-namespace operations: branch, list_collections, history.
  No format awareness — each lens owns its own read/write API.
- **ProllyLensBase** (`prolly_tree.py`) — ProllyTreeIndex storage backend.
  Tiered commits (delta + snapshot), O(log N) lookups, branching, merge.
  The universal storage for all collections.
- **CollectionMetadata** (`collection_metadata.py`) — data-side metadata
  manager. Manages zone maps, indexes, and compaction for collections.
  Lens-agnostic — operates on kernel + collection name.
- **LensQuery** (`row_query.py`) — lazy, composable ROW-LEVEL query builder.
  NOT "the query method" — for SQL use LakehouseLens.query(), for point
  lookups use KeyValueLens.get() or LakehouseLens.range_point_lookup().
- **UUIDv7** (`uuid7.py`) — time-ordered UUID for distributed row
  identification (_rowid). 48-bit Unix ms + 74 random bits.
- **BinaryEncoding** (`binary_encoding.py`) — binary Prolly tree encoding.
- **Maintenance** (`maintenance.py`) — tombstone helpers (RFC-0008).
- **Collection** (`collection.py`) — named collection metadata.

## Extensions (pond-sdk/extensions/)

Optional modules that extend Pond's capabilities. All are data-side
(collection-level), not lens-side. Any lens can use any collection's
metadata.

- **indexing/** — CollectionIndexer (recommended, data-side) +
  AutoIndexMixin (deprecated, lens-side).
- **semantic/** — SemanticMixin + OssieAdapter for semantic models.
- **physical_structures/** — ZoneMap, PruningPredicate, PruningReader,
  BloomFilter, Statistics, ZoneMapIndex.

## Files

| File | Purpose | LOC |
|---|---|---|
| `base_lens.py` | PondLens — shared namespace base for all lenses | 248 |
| `prolly_tree.py` | ProllyLensBase + ProllyTree (universal storage backend) | 764 |
| `collection_metadata.py` | CollectionMetadata — data-side metadata manager | 343 |
| `row_query.py` | LensQuery — lazy row-level query builder | 306 |
| `uuid7.py` | UUIDv7 time-ordered UUID generation | 197 |
| `binary_encoding.py` | Binary Prolly tree encoding | 323 |
| `maintenance.py` | Tombstone helpers (RFC-0008) | 315 |
| `collection.py` | Collection metadata (namespace, type, source) | 518 |
| `extensions/` | Indexing, semantic, physical structures extensions | — |

## Dependencies

- `pond-core/` (the kernel — 3 primitives, ~199 LOC)
- Python stdlib only (no external packages)

## Architecture

```
pond-core (kernel: Write, Read, Ref — FROZEN, ~199 LOC)
    ↓
pond-sdk (base_lens, prolly_tree, collection_metadata, extensions/)
    ↓
lenses/ (keyvalue, lakehouse, vector — each extends PondLens directly)
```

Lenses do NOT inherit from each other. Each production lens extends
PondLens and owns its own storage code. Extensions are data-side
(collection-level), not lens-side.
