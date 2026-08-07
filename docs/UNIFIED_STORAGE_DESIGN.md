# Unified Storage — ONE format, ONE write path, ONE read path

**Date:** 2026-07-30
**Status:** Design + implementation
**Mandate:** "Simpler storage solution that unifies all workloads in same storage format regardless of use with no overhead for writes and reads."

## The problem we're solving

After the manifest work, Pond has:
- **3 write modes**: `range_write` (whole-blob Parquet), `range_write_column_chunks` (per-column Parquet blobs), `range_write_encoded` (per-column encoded blobs)
- **4+ read modes**: `read_table`, `read_with_pruning`, `read_with_column_chunk_pruning`, `read_with_encoded_pruning`, `read_table_via_manifest`, `read_with_pruning_via_manifest`, `range_point_lookup_via_manifest`
- **2 index types**: `ZoneMapIndex` (legacy), `CollectionManifest` (new)
- **Multiple "storage modes"**: `STORAGE_WHOLE_BLOB`, `STORAGE_COLUMN_CHUNKS`, `STORAGE_ENCODED`

This is too many choices. The user wants ONE format, ONE write, ONE read — with zero overhead.

## The unified design

### ONE format: PND2 (Pond Blob v2)

A single binary blob format for **every** workload:

```
+--------------------------------+
| Magic (4B): b"PND2"            |
| Version (1B): 2                |
| Flags (1B):                    |
|   bit 0: has_stats             |
|   bit 1: compressed            |
|   bit 2-7: reserved            |
| n_rows (4B uint32)             |
| n_columns (2B uint16)          |
+--------------------------------+
| Schema section:                |
|   For each column:             |
|     name_len (1B)              |
|     name (UTF-8)               |
|     value_type (1B)            |  1=INT64, 2=FLOAT64, 3=STRING, 4=NULL, 5=BINARY
|     encoding (1B)              |  0=raw, 1=rle, 2=dict, 3=bitpack
+--------------------------------+
| Stats section (if has_stats):  |
|   For each column:             |
|     has_min (1B)               |
|     min (8B or var-len)        |
|     max (8B or var-len)        |
|     null_count (4B)            |
+--------------------------------+
| Compression tag (1B)           |  0=none, 1=lz4, 2=zstd (if compressed flag)
+--------------------------------+
| Payload:                       |
|   For each column:             |
|     payload_len (4B)           |
|     encoded bytes (variable)   |
+--------------------------------+
```

**ONE blob per row group.** All columns in one blob. Stats in the header. Compression transparent. Encoding auto-selected per column.

### ONE write path

```python
def write(collection, rows, key_col=None, row_group_size=10_000):
    """
    rows: a ColumnSource (any lens can produce one — pa.Table, list[dict], etc.)
    """
    # 1. Split rows into row groups of `row_group_size` rows each
    # 2. For each row group:
    #    a. For each column:
    #       - Auto-select encoding (RLE/DICT/BITPACK/RAW)
    #       - Encode values → encoded_bytes + enc_meta
    #       - Compute stats (min/max/null_count) DURING encode (free, one pass)
    #    b. Build PND2 blob: header + schema + stats + compressed payload
    #    c. Write blob to kernel → blob_hash
    # 3. Build manifest with all row-group blob hashes + inline stats
    # 4. Commit (HEAD → commit → manifest → blobs)
```

No choices. No modes. No separate index updates. Stats are computed during encode (zero overhead — same loop that encodes also tracks min/max/null_count).

### ONE read path

```python
def read(collection, predicates=None, columns=None, commit_hash=None):
    """
    Returns rows (as a ColumnSource-compatible structure).
    """
    # 1. Resolve HEAD → commit_hash (SQLite, free)
    # 2. Fetch commit blob (S3 GET #1)
    # 3. Resolve manifest ref (SQLite, free)
    # 4. Fetch manifest blob (S3 GET #2 — has ALL stats + blob hashes)
    # 5. Evaluate predicates IN MEMORY against manifest stats
    #    → K surviving row groups (0 S3 GETs)
    # 6. For each surviving row group (parallel):
    #    a. Fetch blob (S3 GET)
    #    b. Read stats from header (free — already in the blob)
    #    c. Optional: third-level pruning via embedded stats
    #    d. Decompress payload (transparent)
    #    e. For each requested column (projection pushdown):
    #       - Decode the column's encoded bytes
    #       - Optional: Vortex-style predicate eval on encoded form
    # 7. Concatenate row groups → return rows
```

No choices. No modes. Just: manifest → prune → fetch → decode.

### Round trips: 2 + K (the irreducible minimum)

For a content-addressed store:
1. Commit blob (immutable, gives manifest_hash)
2. Manifest blob (immutable, gives blob hashes + stats)
3. K data blobs (parallel, the actual data)

That's the minimum. The unified storage achieves it.

## What gets removed

| Component | Status |
| --- | --- |
| `range_write` (whole-blob Parquet) | Replaced by `write_unified` |
| `range_write_column_chunks` | Replaced by `write_unified` |
| `range_write_encoded` | Replaced by `write_unified` |
| `read_with_pruning` | Replaced by `read_unified` |
| `read_with_column_chunk_pruning` | Replaced by `read_unified` |
| `read_with_encoded_pruning` | Replaced by `read_unified` |
| `STORAGE_WHOLE_BLOB` / `STORAGE_COLUMN_CHUNKS` / `STORAGE_ENCODED` | Single mode now |
| `ColumnChunkStorage` class | One blob per row group, no per-chunk blobs |
| `EncodedChunkStorage` class | Encoding is automatic, no separate class |
| `ColumnChunkZoneMap` class | Stats are inline in the PND2 blob |
| `ZoneMapIndex` class | Manifest replaces it |
| `StatsIndex` class | Manifest replaces it |
| `PruningReader` class | `read_unified` does pruning inline |
| `encode_fn` / `decode_fn` (lens-owned) | PND2 owns the format; lens provides a `ColumnSource` |

## What stays

- **Kernel** (FROZEN — 3 primitives)
- **CollectionManifest** (the index — one blob per commit)
- **stats_tree.py** (PB-scale hierarchical index)
- **encoding.py** (4 encodings — used internally by PND2)
- **compression.py** (zstd/LZ4 — transparent layer)
- **column_source.py** (format-agnostic data access — any lens produces one)
- **embedded_stats.py** (the format spec — now inside every PND2 blob)
- **PruningPredicate / ColumnPredicate** (predicate evaluation)
- **PondLens base class** (shared namespace ops)
- **All 5 lenses** (Lakehouse, KV, Vector, Streaming, FeatureStore) — they all just provide a ColumnSource

## Genericity — works for ANY workload

The PND2 format is columnar. ANY workload that can be expressed as columns works:

| Workload | Columns | Why it works |
| --- | --- | --- |
| **Tabular** (Lakehouse) | Table columns (age, region, ...) | Native columnar |
| **KV** (KeyValue) | JSON fields (id, name, timestamp) | Each field is a column |
| **Vector** (Vector) | Dimensions (dim_0, dim_1, ...) + vector_id | Each dimension is a column |
| **Streaming** (video/music/logs) | One BINARY column "data" + metadata columns (start_byte, end_byte) | BINARY value type = raw bytes |
| **Notebooks** | Cell metadata (author, created_at, tags) + cell content (BINARY) | Cell content as BINARY column |
| **Feature Store** | Feature columns + entity_id + timestamp | Native columnar |
| **Git** | File path, blob content (BINARY), commit metadata | File content as BINARY column |

The `BINARY` value type (new in PND2) stores raw bytes for non-columnar data (video, music, file content). It uses RAW encoding with no compression at the column level (the blob-level zstd still applies).

## SIMD-ready

PND2 is designed for SIMD execution engines:
- INT64/FLOAT64 columns are contiguous 8-byte arrays (directly castable to numpy/Arrow)
- Null bitmap uses Arrow convention (1=null, 0=valid)
- Encoded forms (RLE/DICT/BITPACK) are also binary, not JSON
- No PyArrow conversion needed — readers can `np.frombuffer` directly

## Zero overhead proof

### Write overhead

Old path (range_write_encoded):
1. Encode each column → N_chunks encoded blobs (N writes)
2. Write JSON manifest blob (1 write)
3. Compute ZoneMap (separate pass)
4. Write zone map blob (1 write)
5. Commit zone maps (1 write)
6. Build collection manifest (separate pass)
7. Write manifest blob (1 write)
8. Commit (1 write)

**Total: N + 5 writes**

Unified path:
1. For each row group: encode columns + compute stats (one pass) → 1 PND2 blob (1 write)
2. Build manifest (one pass over the just-written row groups) → 1 manifest blob (1 write)
3. Commit (1 write)

**Total: N_row_groups + 2 writes**

For a 100-row-group table with 5 columns × 10 chunks each:
- Old: 5000 chunk writes + 5 index writes = **5005 writes**
- Unified: 100 row-group writes + 2 index writes = **102 writes**

**~50x fewer writes.**

### Read overhead

Old path (read_with_encoded_pruning):
1. Resolve HEAD (free)
2. Fetch commit (1 GET)
3. Fetch snapshot tree root (1 GET)
4. Walk Prolly tree to find row group keys (log N GETs)
5. Fetch zone map manifest (1 GET)
6. Evaluate predicates against zone maps (in memory)
7. For each surviving row group:
   - Fetch zone map blob (1 GET) — wait, already in manifest?
   - Fetch column-chunk manifest blob (1 GET)
   - For each surviving chunk:
     - Fetch chunk blob (1 GET)
     - Decompress, decode

**Total: 3 + log N + K_row_groups + K_row_groups + K_chunks GETs**

Unified path:
1. Resolve HEAD (free)
2. Fetch commit (1 GET)
3. Fetch manifest (1 GET)
4. Evaluate predicates IN MEMORY against manifest stats
5. For each surviving row group:
   - Fetch PND2 blob (1 GET)
   - Decompress, decode requested columns

**Total: 2 + K_row_groups GETs**

For a 1% selectivity query on 100 row groups (K=1):
- Old: 3 + 7 + 1 + 1 + 10 = **22 GETs** (assuming 10 chunks per column for the predicate column)
- Unified: 2 + 1 = **3 GETs**

**~7x fewer reads.**

## Implementation

`bindings/python/sdk/extensions/physical_structures/unified_storage.py`:
- `PND2` class — encode/decode the PND2 format
- `UnifiedStorage` class — `write()`, `read()`, `point_lookup()`

`lenses/lakehouse/lakehouse_lens.py`:
- New methods: `write_unified()`, `read_unified()`, `point_lookup_unified()`
- These are the new DEFAULT path
- Old methods (`range_write*`, `read_with_*_pruning`) remain as legacy

## Why this is the right design

- **Simple**: ONE format, ONE write, ONE read. No choices.
- **Powerful**: Same pruning, encoding, compression as before — just unified.
- **Performant**: SIMD-ready binary, auto-encoding, transparent compression.
- **Scalable**: Manifest + lazy stats tree handle PB scale.
- **Efficient**: Stats computed during encode (zero overhead). Reads are 2 + K.
- **Beautiful**: One responsibility per layer. Kernel frozen, storage unified, lenses provide ColumnSource.
- **Functional**: Covers ALL workloads (tabular, KV, vector, streaming, notebooks, git, feature store).
- **Storage-Independent**: PND2 is binary; never depends on execution engine.
