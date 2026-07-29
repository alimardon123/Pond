# Unified Stats Index — One Blob, Two Round Trips, Any Workload

## The insight

The user's concern: "reading many blobs to check stats = same as raw Parquet."

The solution is NOT to embed stats in every data blob (that still requires
N fetches to check N blobs). The solution is a **single stats index blob**
that aggregates min/max/null_count across ALL row groups in the collection.

## How other systems solve this

| System | Index structure | Round trips to find 1 matching row |
|--------|----------------|-----------------------------------|
| Iceberg | Manifest file (lists data files + stats) | 2 (manifest + 1 data file) |
| Delta Lake | Transaction log (_delta_log/) | 2 (log + 1 data file) |
| Parquet | Row-group footer (embedded) | N (must read each file's footer) |
| Database B-tree | Index pages (separate, cached) | O(log N) page reads + 1 data page |
| **Pond (this design)** | **Stats index blob (1 blob, content-addressed)** | **2 (stats blob + 1 data blob)** |

Pond's advantage over Iceberg/Delta: the stats index is content-addressed
and versioned. Time-travel to commit X reads the stats index at commit X.
Branching creates a new stats index for the branch. No transaction log
to replay — just ref resolution.

## Design

### Stats Index Blob

A single JSON blob stored at ref `collections/{name}/stats`. Contains
an entry for every row group in the collection:

```json
[
  {
    "key": "rg/9999",
    "blob_hash": "abc123...",
    "n_rows": 10000,
    "columns": {
      "age": {"min": 0, "max": 99, "null_count": 0},
      "region": {"min": "ASIA", "max": "US", "null_count": 0}
    }
  },
  {
    "key": "rg/19999",
    "blob_hash": "def456...",
    "n_rows": 10000,
    "columns": {
      "age": {"min": 0, "max": 99, "null_count": 0},
      "region": {"min": "ASIA", "max": "US", "null_count": 0}
    }
  }
]
```

Size: ~200 bytes per row group. For 100 row groups: ~20KB — ONE fetch.

### Write Path

When the lens commits a write (via ProllyLensBase.commit), it also
updates the stats index:

1. Encode data chunks (existing path)
2. Compute per-row-group stats (min/max/null_count per column)
3. Build the stats index blob (JSON array of all row groups)
4. Write it to the kernel: `kernel.write(stats_bytes)`
5. Point the ref: `kernel.reference("collections/{name}/stats", stats_hash)`
6. Commit (the stats ref is part of the commit's namespace)

The stats index is **always in sync** with the data — it's updated in
the same commit. No separate `commit_zone_maps` call.

### Read Path (2 round trips total)

```
1. Fetch stats index blob (1 fetch — ~20KB for 100 row groups)
   → Evaluate predicate against ALL row groups' stats
   → Identify the 1-2 surviving row groups

2. Fetch surviving data blobs (1-2 fetches)
   → Decode and return rows

Total: 2-3 round trips, regardless of collection size.
```

For a point lookup (1 row group): **2 round trips** (stats + 1 data blob).
For a 1% selectivity query (1 of 100 row groups): **2 round trips**.
For a full scan (no predicate): **1 round trip** (skip stats, fetch all).

### What about per-column-chunk pruning?

Per-column-chunk stats (finer than row-group) can be embedded in the
data blob header (the `embedded_stats.py` module we already built).
This gives a THIRD level of pruning:

1. Stats index blob → skip non-matching row groups (2 round trips)
2. Data blob header → skip non-matching column chunks (0 extra fetch)
3. Encoded predicate eval → skip non-matching rows (0 extra fetch)

But the stats index blob is the KEY innovation — it's the single fetch
that eliminates 99% of data blob fetches.

### S3 performance

| Query type | Without stats index | With stats index |
|-----------|--------------------|-----------------|
| Point lookup | 100 fetches (scan all) | 2 fetches (stats + 1 blob) |
| 1% selectivity | 100 fetches | 3 fetches (stats + 1 blob) |
| 10% selectivity | 100 fetches | 12 fetches (stats + 11 blobs) |
| Full scan | 100 fetches | 100 fetches (skip stats) |

### What gets removed

- `ZoneMapIndex` class (460 LOC) — replaced by a single stats blob
- Zone-map ProllyTreeIndex (separate tree per collection)
- `add_zone_map`, `commit_zone_maps`, `clear_zone_maps`, `iter_zone_maps`
- Zone-map manifest blob (the previous optimization — superseded)
- Per-row-group zone-map blobs (N small blobs → 1 stats blob)

### What stays

- `PruningPredicate` / `ColumnPredicate` — evaluate against stats
- `ColumnSource` — format-agnostic data access
- `encode_fn` / `decode_fn` — lens's format contract
- All 4 encodings + compression — unchanged
- `embedded_stats.py` — optional third-level pruning in blob headers

### Generic

The stats index works for ANY workload:
- **Tabular**: columns are table columns (age, region, etc.)
- **KV**: columns are JSON fields (id, name, timestamp)
- **Vector**: columns are dimensions (dim_0, dim_1, ...)
- **Streaming**: columns are segment metadata (start_byte, end_byte)
- **Notebooks**: columns are metadata (author, created_at, tags)

Any lens that can compute min/max per column can use the stats index.
The stats index doesn't know or care what format the data is in.

### Versioning

The stats index is stored at `collections/{name}/stats` — a kernel ref.
Each commit updates this ref. Time-travel to commit X:
`kernel.resolve("collections/{name}/stats")` at commit X returns the
stats index as of that commit. Branching creates a new stats ref for
the branch. No replay, no transaction log.

### Simplicity

The entire stats index is:
- ONE blob (JSON array)
- ONE ref (`collections/{name}/stats`)
- Updated in ONE write per commit
- Fetched in ONE read per query

No ZoneMapIndex class. No ProllyTreeIndex for zone maps. No manifest.
No `add_zone_map` API. Just one blob.
