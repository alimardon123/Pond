# extensions/physical_structures/

The universal storage backend + derived structures.

## Purpose

Physical Structures are the storage layer that all lenses use. The main
component is `UnifiedStorage` — the production storage engine that encodes
data as PND2 blobs with CollectionManifest for pruning.

## Files

| File | Purpose |
|---|---|
| `unified_storage.py` | **UnifiedStorage** — the production storage backend (PND2 + CollectionManifest + CRDT shards) |
| `collection_manifest.py` | **CollectionManifest** — one manifest blob per commit (PMAN format) with per-column stats |
| `stats_tree.py` | **StatsTreeReader** — PB-scale hierarchical stats index |
| `embedded_stats.py` | **ColumnStats** + value-type constants |
| `compression.py` | zstd / LZ4 transparent compression |
| `encoding.py` | **ColumnEncoding** — RLE/Dict/Bitpack/Raw encoding with auto-selection |
| `column_source.py` | **ColumnSource** — format-agnostic column access protocol |
| `pond_pack.py` | **PondPack** — commit+manifest in ONE blob (saves 1-2 GETs per cold read) |

## Architecture

```
Lens (KeyValue, Lakehouse, etc.)
  ↓ uses
UnifiedStorage (this directory)
  ├── PND2 format (encoding.py + compression.py)
  ├── CollectionManifest (collection_manifest.py)
  ├── StatsTree (stats_tree.py — PB scale)
  ├── PondPack (pond_pack.py — commit+manifest in one blob)
  └── CRDT shards (upsert_shard, delete_shard, read_with_shards)
  ↓ calls
Kernel (Write, Read, Ref)
```

## Rust port status

The Rust core (`core/`) has partial ports of these structures:
- ✅ `CollectionManifest` — in `core/storage/src/manifest.rs`
- ✅ `PondPack` — in `core/storage/src/pond_pack.rs`
- ✅ `encoding` — in `core/codec/src/encode.rs` (RLE, DICT, BITPACK, RAW)
- ✅ `compression` — zstd feature flag in `core/codec/`
- ⚠️ `UnifiedStorage` — in `core/storage/` (partial — JSON storage, PND2 via write_rows_i64)
- ❌ `StatsTree` — not ported (defer until PB-scale workloads)
- ❌ `column_source` — not needed in Rust (uses concrete types)

## Historical note

The following files previously existed in this directory but have been
**deleted** (moved to `archive/legacy-extensions/`):
- `BloomFilter` — removed (stats in manifest provide sufficient pruning)
- `Statistics` — consolidated into `CollectionManifest`
- `ZoneMapIndex` — replaced by `CollectionManifest`
- `PruningReader` — pruning now happens inside `UnifiedStorage.read()`
- `column_chunk_storage.py` — replaced by PND2 row groups
- `encoded_chunk_storage.py` — replaced by PND2 encodings
- `base.py` — removed (PhysicalStructure abstract base is gone)

These are kept in `archive/legacy-extensions/` for historical reference.
