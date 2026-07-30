# Collection Manifest — ONE fetch, ALL pruning, ANY workload

**Date:** 2026-07-30
**Status:** Implemented (replaces `ZoneMapIndex` + `StatsIndex` as the primary index path)

## The mandate

> "Make sure we will have less round trips possible with object storage for
> all interactions/access with our storage."

Every interaction with object storage must minimize round trips. This
document specifies how.

## The core idea: ONE STORAGE, MANY INDEXES, ONE MANIFEST

A **CollectionManifest** is a single binary blob, written atomically with
each commit, that contains EVERYTHING a reader needs to decide which data
blobs to fetch:

- Schema (column names + types)
- Sort order (key column, row_group_size, chunk_size)
- Per-row-group entries, each with:
  - `key` (e.g., `"rg/00000000000000009999"` — zero-padded for correct lexicographic ordering)
  - `data_blob_hash` (or `manifest_blob_hash` for column-chunk / encoded storage)
  - `n_rows`
  - **Inline column stats** (min/max/null_count per column)
  - **Inline column-chunk refs** (per-column per-chunk blob hashes + stats)
- Optional hierarchical stats tree root (for PB scale)

The manifest is content-addressed and stored in the commit blob (one
field: `manifest_hash`). Reading the commit gives you the manifest hash;
reading the manifest gives you all stats and all blob hashes.

## Round-trip accounting

### Before (with ZoneMapIndex)

| Interaction                | Round trips                                                    |
| -------------------------- | -------------------------------------------------------------- |
| Point lookup (1 row)       | HEAD + commit + snapshot + log N (tree path) + data = 4+log N  |
| Range scan (K row groups)  | HEAD + commit + snapshot + log N + K data = 4+log N+K          |
| Predicate-pruned read      | HEAD + commit + snapshot + log N + zm_manifest + K = 5+log N+K |
| Insert (N row groups)      | N reads (read_all) + N writes + commit + zm commit = O(N)     |

### After (with CollectionManifest)

| Interaction                | Round trips                                                    |
| -------------------------- | -------------------------------------------------------------- |
| Point lookup (1 row)       | HEAD + commit + manifest + 1 data = **3** (constant)           |
| Range scan (K row groups)  | HEAD + commit + manifest + K data = **3+K**                    |
| Predicate-pruned read      | HEAD + commit + manifest + K surviving = **3+K**               |
| Insert (N row groups)      | N writes + 1 manifest + 1 commit = **N+2** (no read-all)       |

### Why we can't get below 3

The first 3 fetches are irreducible on a content-addressed store:

1. **HEAD ref** — the mutable pointer (cheap, cached by SDK)
2. **commit blob** — immutable; gives us manifest_hash + parent
3. **manifest blob** — gives us all data_blob_hashes + inline stats

These three are tiny (HEAD ~80 bytes, commit ~200 bytes, manifest ~200
bytes/row_group). They can be further reduced by caching HEAD and commit
at the SDK layer (1 fetch amortized for hot collections).

### The data blob is the 4th fetch — and that's the only "real" I/O

Once the manifest is in hand, the reader knows EXACTLY which data blobs
to fetch. Surviving blobs can be fetched in parallel; each is large
(actual data, not metadata). This is the only I/O that's "useful" —
everything else was metadata bookkeeping.

## Binary format (PND1-manifest v1)

```
+-----------------------------+
| Magic (4B): b"PMAN"         |
| Version (1B): 1             |
| n_row_groups (4B uint32)    |
| n_columns (2B uint16)       |
+-----------------------------+
| Schema section:             |
|   For each column:          |
|     name_len (1B)           |
|     name (UTF-8)            |
|     value_type (1B)         |
+-----------------------------+
| Sort order section (12B):   |
|   key_col_len (1B)          |
|   key_col (UTF-8)           |
|   row_group_size (4B)       |
|   chunk_size (4B)           |
+-----------------------------+
| Optional sections bitmap    |
|   (1B bitfield):            |
|   bit 0: has_stats_tree     |
|   bit 1: has_bloom_filter   |
|   bit 2-7: reserved         |
+-----------------------------+
| Row group entries:          |
|   For each row group:       |
|     key_len (2B)            |
|     key (UTF-8)             |
|     blob_hash (32B hex)     |
|     n_rows (4B)             |
|     storage_mode (1B):      |
|       0=whole_blob          |
|       1=column_chunks       |
|       2=encoded             |
|     If column_chunks/encoded:|
|       manifest_blob_hash (32B)|
|     For each column:        |
|       value_type (1B)       |
|       has_min (1B)          |
|       min (8B or var-len)   |
|       max (8B or var-len)   |
|       null_count (4B)       |
|       n_chunks (2B)         |
|       For each chunk:       |
|         chunk_blob_hash (32B)|
|         chunk_min (8B or var)|
|         chunk_max (8B or var)|
|         chunk_null_count (4B)|
|         encoding (1B)       |
|         encoding_meta_len (2B)|
|         encoding_meta (var) |
+-----------------------------+
| Optional: stats_tree_root (32B)|
| Optional: bloom_filter_ref (32B)|
```

Size estimate:
- ~50 bytes per row group (no chunks)
- ~80 bytes per column chunk (with hash + stats)
- For a 100-row-group table with 5 columns × 10 chunks each:
  100 × (50 + 5×80) = 45KB — still ONE fetch on S3.

## Lazy hierarchical stats tree (PB scale)

For PB-scale collections (>10K row groups, >1M chunks), the manifest
blob grows past the S3 single-fetch sweet spot (>5MB). At that point we
split into a **hierarchical stats tree**:

- **Internal nodes**: aggregated min/max/null_count per column across
  all children — same as Prolly tree internal nodes, but with stats
  annotations. Stored as content-addressed blobs.
- **Leaf nodes**: per-row-group stats entries (same as the flat manifest
  entries, but no blob hashes — those live in the manifest).

The manifest's `stats_tree_root` field points to the root. Reads at PB
scale walk O(log N) stats-tree nodes (each fetched once, cached by SDK
via content addressing), pruning subtrees that can't match the predicate.

This is **lazy**: the stats tree is NOT built at write time. It's built
on the first OLAP read that would benefit from it, then cached via
content addressing. Subsequent reads reuse the cached tree.

The build cost is O(N) once; subsequent reads are O(log N). The tree is
content-addressed, so two readers on the same commit share the same
cached tree.

## What this replaces

| Old mechanism                | New mechanism                                 |
| ---------------------------- | --------------------------------------------- |
| `ZoneMapIndex` (460 LOC)     | `CollectionManifest` (manifest blob, ~250 LOC)|
| `StatsIndex` (177 LOC)       | `CollectionManifest` (same blob)              |
| `zone_map_manifest` blob     | `manifest` field in commit (no extra fetch)   |
| Per-row-group zone-map blobs | Inline stats in manifest (no extra fetch)     |
| `add_zone_map` API           | `manifest.add_row_group()` API                |
| `commit_zone_maps` API       | Atomic with `commit`                          |
| `compact_zone_maps` API      | N/A — manifest is rebuilt every commit        |
| `pruning_reader.scan_with_pruning` walking zm tree | Reads manifest, evaluates predicate in memory |

## What stays

- `PruningPredicate` / `ColumnPredicate` — evaluate against stats
- `ColumnSource` — format-agnostic data access
- `encode_fn` / `decode_fn` — lens's format contract
- All 4 encodings + compression — unchanged
- `embedded_stats.py` — stats in chunk blob headers (third-level pruning)
- `ColumnChunkZoneMap` / `ColumnChunkStats` — used by manifest entries

## Genericity

The manifest works for ANY workload:

- **Tabular** (LakehouseLens): columns are table columns; row groups are PK ranges
- **KV** (KeyValueLens): columns are JSON fields; row groups are key ranges
- **Vector** (VectorLens): columns are dimensions + vector_id; stats are bounding boxes
- **Streaming** (StreamingLens): columns are segment metadata; row groups are byte ranges
- **Notebooks** (Notebook lens): columns are cell metadata; row groups are cell ranges

Any lens that can compute per-column min/max can produce manifest entries.

## Implementation order

1. ✅ `collection_manifest.py` — binary format (PND1-manifest v1)
2. ✅ Wire into `LakehouseLens._write_via_prolly` — manifest built atomically with commit
3. ✅ Wire into `LakehouseLens._read_all_row_groups` — read manifest, fetch only data blobs
4. ✅ Wire into `LakehouseLens.read_with_*_pruning` — manifest replaces zm_index
5. ✅ Lazy stats tree (for PB scale — built on first read, cached via content addressing)
6. ✅ Round-trip benchmark test
7. ✅ Backward compatibility: ZoneMapIndex kept for KeyValueLens (legacy path)

## Why this is the right design

- **Minimal round trips**: every read is 3 + K, where K is the number of
  surviving data blobs. No fetch is wasted.
- **Simple**: ONE manifest blob, ONE format, ONE index. No ZoneMapIndex,
  no StatsIndex, no separate manifest blob.
- **Generic**: works for any workload that can produce per-column stats.
- **Performant**: stats are inline, evaluated in memory, no extra fetches.
- **Scalable**: lazy stats tree handles PB scale with O(log N) reads.
- **Beautiful**: one responsibility per layer (commit = version,
  manifest = index, data = payload).
- **Storage-Independent**: the manifest is a kernel blob; it never
  depends on the execution engine.
