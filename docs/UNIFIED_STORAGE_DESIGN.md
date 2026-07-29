# Unified Storage Design — Embedded Stats, Zero Overhead

## Problem

The current zone-map design stores pruning metadata as **separate blobs**
from the data. This requires:
- Extra blob fetches (zone-map manifest = 1 fetch, individual zone maps = N fetches)
- A separate ProllyTreeIndex for zone maps (ZoneMapIndex class, 460 LOC)
- Extra write-time work (build zone maps, commit zone maps, clear old zone maps)
- Extra API surface (add_zone_map, commit_zone_maps, clear_zone_maps, iter_zone_maps, count_zone_maps)

On S3, each extra fetch = 50ms RTT. Even with the manifest optimization,
the read path is: fetch manifest → parse → evaluate predicate → fetch
surviving data blobs = 2+ round trips.

## Solution: Embedded Stats in Data Blob Headers

**Embed pruning metadata (min/max/null_count) directly in the data blob
header.** The reader fetches the data blob, reads the first ~100 bytes
(header + stats), and decides whether to decode the rest. If the stats
prove the blob can't match the predicate, the reader skips the rest of
the blob (already fetched, but no decode cost — or on S3 with range
reads, only the header bytes are fetched).

This is what Parquet, Iceberg, and Vortex do:
- Parquet: row group statistics in the file footer
- Iceberg: data file stats in the manifest file (but Pond embeds in the blob)
- Vortex: array-level stats in the array header

### New PND1 Format (v1.2)

```
+-------------------+-----------------------------------+
| CompressionTag    | 1 byte (0x00=none, 0x01=LZ4, 0x02=zstd)|
+-------------------+-----------------------------------+
| EncodingHeader    | 9 bytes (magic + encoding + n_rows)|
+-------------------+-----------------------------------+
| StatsHeader       | variable (embedded pruning stats)  |
+-------------------+-----------------------------------+
| Payload           | encoding-specific data             |
+-------------------+-----------------------------------+
```

The StatsHeader contains:
- `n_columns`: 2 bytes (number of columns in this chunk)
- For each column:
  - `col_name_len`: 1 byte
  - `col_name`: col_name_len bytes (UTF-8)
  - `value_type`: 1 byte (INT64, FLOAT64, STRING, NULL)
  - `min`: 8 bytes (for INT64/FLOAT64) or 4B len + bytes (for STRING)
  - `max`: 8 bytes or variable
  - `null_count`: 4 bytes

Total overhead: ~20 bytes per column. For a 3-column chunk: ~60 bytes.
For a 1000-row INT64 chunk (8000 bytes payload): 60/8000 = 0.75% overhead.

### Read Path (ZERO extra round trips)

```
1. Lens computes which row-group keys might overlap the query range
   (using the ProllyTreeIndex key — e.g., rg/999 means max_pk=999)
   → This is the FIRST-LEVEL PRUNE, no blob fetch needed.

2. For each surviving row group, fetch the data blob (1 fetch per blob)

3. Read the StatsHeader from the blob (first ~100 bytes)
   → If stats prove the blob can't match the predicate: SKIP (no decode)
   → This is the SECOND-LEVEL PRUNE, no extra fetch needed.

4. For surviving blobs: decode the payload (encoding-specific)
   → This is the THIRD-LEVEL (encoded predicate eval, if applicable)
```

**Total round trips**: 1 per surviving row group. No manifest fetch.
No zone-map fetch. The stats travel WITH the data.

### Write Path (ZERO extra work)

The lens writes the data blob with embedded stats in one pass:
1. Encode the column data (existing path: encode_column)
2. Compute min/max/null_count (existing path: compute_list_stats)
3. Write the StatsHeader + encoded payload as a single blob
4. Stage in ProllyTreeIndex (existing path)

No separate `add_zone_map` call. No `commit_zone_maps`. No ZoneMapIndex.
The stats are part of the blob — they can't get out of sync.

### What gets removed

- `ZoneMapIndex` class (460 LOC) — replaced by embedded stats
- `zone_map_index.py` — deleted
- `ZoneMap` dataclass (pruning.py) — stats now live in the blob header
- `PruningReader` — simplified (reads blob headers, not zone-map blobs)
- `clear_zone_maps`, `commit_zone_maps`, `iter_zone_maps`, `count_zone_maps`
- Zone-map ProllyTreeIndex (separate tree per collection)
- Zone-map manifest blob (the optimization we just added — no longer needed)

### What stays

- `ColumnChunkZoneMap` — still useful for per-column-chunk stats WITHIN
  a row group (finer granularity than row-group stats). But these are
  also embedded in the blob, not stored separately.
- `PruningPredicate` / `ColumnPredicate` — still evaluate against stats
- `ColumnSource` — still the format-agnostic data access protocol
- `encode_fn` / `decode_fn` — still the lens's format contract
- ProllyTreeIndex — still the storage backend (key → blob_hash)
- All 4 encodings (RAW, RLE, DICT, BITPACK) — unchanged
- Compression layer — unchanged (wraps the whole blob including stats)

### Backward compatibility

Old blobs (without StatsHeader) still work:
- The reader checks if the blob starts with the StatsHeader magic
  (e.g., b"STAT" after the EncodingHeader)
- If no stats: skip pruning, decode the full blob (current behavior)
- If stats: read stats, evaluate predicate, skip if possible

### S3 performance

| Operation | Before (zone maps) | After (embedded stats) |
|-----------|-------------------|----------------------|
| Zone-map fetch | 1 manifest fetch (50ms) | 0 (stats in data blob) |
| Data blob fetch | N surviving blobs | N surviving blobs |
| Total fetches | 1 + N | N |
| Point lookup | 2 fetches (manifest + 1 blob) | 1 fetch (blob with stats) |

**One round trip for a point lookup. Zero overhead for pruning.**
