# Pond Architecture Redesign — The Path to Simplicity

> **Date:** 2026-07-30
> **Status:** Phase 1 complete (Tier 1 deletion + PondStorage created)
> **Goal:** Collapse 3 SDK classes into 1, delete ~5800 LOC of dead code,
> reduce lenses from ~3700 LOC to ~900 LOC.

## The Problem

The Pond SDK has too many layers. A lens author today must understand:

1. **PondLens** (base_lens.py, 250 LOC) — namespace ops
2. **ProllyLensBase** (prolly_tree.py, 856 LOC) — commit/branch/history + Prolly tree
3. **UnifiedStorage** (unified_storage.py, 1369 LOC) — PND2 write/read
4. **CollectionManifest** (collection_manifest.py, 905 LOC) — the index
5. **CollectionMetadata** (collection_metadata.py, 463 LOC) — legacy façade
6. **ZoneMapIndex** (zone_map_index.py, 466 LOC) — legacy index

That's 6 classes, ~4300 LOC, with overlapping responsibilities. The lens
author has to know which class to call for which operation — and the answer
is "it depends on what you're doing."

## The Solution: ONE Class

**PondStorage** — a single class with three clear sections:

```
┌─────────────────────────────────────────────────────────────────┐
│  PondStorage                                                     │
│  ┌─────────────┐  ┌───────────────┐  ┌────────────────────────┐ │
│  │ Namespace   │  │ Commit/Branch │  │ Data I/O               │ │
│  │ (list, def) │  │ (history)     │  │ (write/read/lookup)    │ │
│  └─────────────┘  └───────────────┘  └────────────────────────┘ │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Storage Engine (internal — lens authors don't see this)         │
│  ┌─────────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │ Format (PND2)   │  │ Index        │  │ Source             │  │
│  │ + encodings     │  │ (Manifest +  │  │ (ColumnSource)     │  │
│  │ + compression   │  │  StatsTree)  │  │                    │  │
│  └─────────────────┘  └──────────────┘  └────────────────────┘  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Kernel (FROZEN — 3 primitives, no changes)                      │
│  PondMinimal (local disk)  OR  ObjectStoreNativeKernel (S3)     │
└─────────────────────────────────────────────────────────────────┘
```

## What's Been Done (Phase 1)

### Tier 1 Deletion — 1665 LOC removed

Deleted files with ZERO production imports (verified by grep):
- `stats_index.py` (177 LOC) — superseded by CollectionManifest
- `base.py` (108 LOC) — unused PhysicalStructure ABC
- `bloom_filter.py` (131 LOC) — unused by any lens
- `statistics.py` (126 LOC) — unused; manifest has inline stats
- `collection.py` (617 LOC) — unused namespace/labels layer
- `test_stats_index.py`, `test_collection_metadata.py` — tests for deleted code

### PondStorage Created — the unified SDK

`pond-sdk/pond_storage.py` (~300 LOC) — the ONE class lens authors see:
- Section 1: Namespace (list_collections, collection_exists, set_definition, get_definition)
- Section 2: Commit/Branch (commit, branch, checkout, list_branches, merge, undo, history, diff)
- Section 3: Data I/O (write, append, read, read_as_columns, point_lookup, scan_with_pruning)

All 6 tests pass. Verified:
- Cold point lookup: 4 GETs
- Multi-predicate reads: correct results
- Non-destructive append: data preserved across commits
- Cross-workload: tabular + KV + vector on the same storage instance

## What's Next (Phases 2-4)

### Phase 2: Delete Tier 2 Legacy (~2093 LOC)

After migrating LakehouseLens to use UnifiedStorage:
- `column_chunk_storage.py` (279 LOC) — PND2 puts all columns in one blob
- `encoded_chunk_storage.py` (308 LOC) — PND2 auto-encodes per column
- `column_chunk_zone_map.py` (221 LOC) — PND2 stats are inline
- `zone_map_index.py` (466 LOC) — manifest replaces zone-map tree
- `pruning_reader.py` (307 LOC) — manifest inlines predicate eval
- `pruning.py` (249 LOC) — ZoneMap shape unused
- `collection_metadata.py` (463 LOC) — indexer pattern is dead

### Phase 3: Reduce LakehouseLens from 2227 → ~350 LOC

DELETE (legacy storage modes + pruning paths):
- `range_write`, `range_write_column_chunks`, `range_write_encoded` → `storage.write`
- `range_read`, `range_point_lookup` → `storage.read`, `storage.point_lookup`
- `read_with_pruning`, `read_with_column_chunk_pruning`, `read_with_encoded_pruning` → `storage.read`
- `_write_via_prolly`, `_write_via_prolly_to_branch`, `_write_merge_via_prolly` → `storage.write/merge`
- `_read_all_row_groups`, `_decode_blob_to_table` → `PND2.decode`
- `compact_zone_maps`, `attach_indexer`, `_notify_indexers` → delete (dead code)

KEEP (workload-specific):
- `create_table`, `insert`, `read_table`, `read_columns` — thin wrappers over storage
- `query` (DuckDB SQL) — workload-specific
- `branch`, `merge_branch`, `read_branch` — delegate to storage

### Phase 4: Inline ProllyLensBase into PondStorage (~856 LOC → 0)

Move commit/branch/undo/merge/history from ProllyLensBase into PondStorage.
The Prolly tree data-key index is replaced by CollectionManifest's
`find_row_group` / `scan_with_pruning`. Delete `prolly_tree.py` entirely.

## Target Architecture

| Layer | Today | After | Reduction |
|---|---|---|---|
| Kernel | 668 LOC | 668 LOC | 0% (FROZEN) |
| SDK core | 5908 LOC | ~4480 LOC | 24% |
| SDK legacy deleted | 0 | -3758 LOC | — |
| Lenses (3) | 3702 LOC | ~900 LOC | 76% |
| **Total** | **10,278 LOC** | **~6050 LOC** | **41%** |

The complexity reduction is greater than the LOC reduction because the
*number of classes a lens author must understand* drops from 6 to 1.

## Migration Path

Each step is independently shippable:

1. ✅ **Tier 1 deletion** (done) — 1665 LOC, zero impact
2. ✅ **PondStorage created** (done) — unified API, delegates internally
3. **Migrate LakehouseLens to UnifiedStorage** (3-5 days)
4. **Delete Tier 2 legacy** (1 day)
5. **Migrate KV/Vector defaults to unified** (2 days)
6. **Inline ProllyLensBase into PondStorage** (1 week)
7. **Delete prolly_tree.py + base_lens.py** (1 day)
8. **Reorganize physical_structures → storage/** (1 day)

**Total: ~3-4 weeks of focused work.**

## Risk Assessment

- **Safe:** Tier 1 deletion (verified zero imports), PondStorage creation (additive)
- **Medium:** LakehouseLens migration (performance regression risk — PND2 vs Parquet)
- **Higher:** ProllyLensBase deletion (architectural change — manifest replaces Prolly tree)

All steps are reversible until the final deletion.
