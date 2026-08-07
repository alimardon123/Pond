# lenses/streaming/

The **StreamingLens** — chunked storage for large objects (video, music, logs).

## What it is

A streaming/media lens that splits large objects into fixed-size segments.
Each segment is a separate kernel blob. The ProllyTreeIndex maps
segment_number → blob_hash, enabling O(log N) range reads.

## Design decision

Range-read is **NOT a kernel primitive**. It's a Lens pattern. The kernel
stays FROZEN at 3 primitives (Write, Read, Ref). Range-read emerges from
composition: ProllyTreeIndex (segment index) + multiple kernel blobs (segments).

This is the SAME pattern as LakehouseLens:
- Lakehouse: table → row groups → Parquet blobs → ProllyTreeIndex
- Streaming: stream → segments → raw bytes blobs → ProllyTreeIndex

## Capabilities

- `write_stream(name, data, segment_size)` — split into segments, store
- `read_stream(name, start_byte, end_byte)` — range-read (only overlapping segments)
- `append_stream(name, data)` — structural sharing (old segments unchanged)
- `stream_size(name)`, `segment_count(name)`
- `create_branch`, `merge_branch`, `get_history` (versioning)
- Time-travel: `read_stream(name, commit_hash=...)`

## Files

| File | Purpose |
|---|---|
| `streaming_lens.py` | `StreamingLens` |
| `__init__.py` | Package exports |

## Architecture

Extends `PondLens` directly (no lens-to-lens inheritance per
`REPO_ORGANIZATION.md §4`). Owns its ProllyTreeIndex storage code.

## Dependencies

- `bindings/python/core/` (kernel)
- `bindings/python/sdk/` (PondLens, ProllyLensBase)
- Python stdlib only
