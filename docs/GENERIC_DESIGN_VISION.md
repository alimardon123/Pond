# Pond Generic Design Vision

> **Every solution, code, feature, and idea in Pond must be generic
> enough to support ANY workload — notebooks, feature stores, git,
> vectors, music, video — with different data structures and layouts.**

## The promise

Any application built on Pond gets, for free:

- **Infinite storage** — content-addressed blobs on any object store (S3, GCS, local disk)
- **Versioning** — every commit is immutable; time-travel to any point
- **Branching** — create branches for experiments, merge when ready
- **Predicate pushdown** — skip data that can't match the query (Vortex-style)
- **Encoding** — compress data 4-8x with FastLanes-style structural encodings
- **Column-chunk storage** — per-column-chunk blobs for I/O-level savings on object stores
- **Format independence** — the storage layer never depends on the execution engine

## How it works

```
Application Layer (Notebooks, Feature Stores, Git, Vectors, Music, Video)
    ↓ (implements ColumnSource + encode_fn/decode_fn)
Lens Layer (LakehouseLens, KeyValueLens, VectorLens, FutureLens...)
    ↓ (interprets bytes, provides format-specific encode/decode)
Extension Layer (PruningReader, ColumnChunkStorage, EncodedChunkStorage, encoding.py)
    ↓ (format-agnostic — works with any ColumnSource)
SDK Layer (PondLens, ProllyLensBase, CollectionMetadata)
    ↓ (shared namespace + ProllyTreeIndex storage)
Kernel (Write, Read, Ref — immutable bytes + refs)
    ↓
Backend (local disk, S3, IPFS, FDB)
```

## The format-agnostic contract

### ColumnSource protocol

Any lens that can produce columnar data implements this minimal interface:

```python
class ColumnSource(Protocol):
    def column_names(self) -> list[str]: ...
    def num_rows(self) -> int: ...
    def column_slice(self, name: str, start: int, end: int) -> list: ...
    def column_stats(self, name: str) -> tuple[min, max, null_count]: ...
```

Two adapters are provided:
- `PyArrowColumnSource(table)` — for tabular lenses (Lakehouse, Feature Store)
- `ListColumnSource(rows: list[dict])` — for KV-style lenses (KeyValue, Notebook)
- Custom adapters can be written for any data format

### encode_fn / decode_fn

The storage layer calls `encode_fn(col_name: str, values: list) -> bytes`
and `decode_fn(bytes) -> list`. The lens provides its own encoder — no
PyArrow dependency in the storage contract.

| Lens | encode_fn | decode_fn | Example |
|------|-----------|-----------|---------|
| LakehouseLens | Parquet | Parquet | `pq.write_table(pa.Table.from_arrays(...))` |
| KeyValueLens | JSON | JSON | `json.dumps(values).encode()` |
| VectorLens | binary (struct.pack) | binary (struct.unpack) | `struct.pack(f'{n}d', *vector)` |
| Notebook lens | rich text | rich text | custom format |
| Git lens | diff-based | diff-based | custom format |

### The pruning hierarchy (works for ANY format)

```
Level 1: Row-group pruning    — ZoneMap (min/max per row group)
Level 2: Column-chunk pruning — ColumnChunkZoneMap (min/max per chunk)
Level 3: Encoded pruning      — Vortex-style scan on encoded bytes
Level 4: Row-level filtering  — exact match on decoded rows
```

Each level is format-agnostic. A KeyValueLens producing JSON gets the
same pruning infrastructure as a LakehouseLens producing Parquet.

### Vortex-style scan (evaluate predicate without decoding)

For encoded storage (RLE, Dict, Bitpack), the predicate is evaluated
directly on the encoded bytes — no full decode needed:

1. **O(1) min/max prune** — read the encoding header, skip if the
   predicate can't possibly match
2. **O(N) vectorized scan** — walk the encoded bytes, extract each
   value, compare to the predicate, yield only MATCHING ranges
3. **Selective decode** — decode only the bits at surviving positions

This is the Vortex design: scan without decode. The encoded form is
the query target, not an intermediate step.

## Object-store awareness

The pruning infrastructure is designed for object storage (S3, GCS):

- **Per-column-chunk blobs** — skip 4/5 chunks = skip 4/5 of bytes per
  column (9.37x I/O reduction verified)
- **Encoded blobs** — 4-8x compression reduces bytes transferred
- **Zone-map tree** — small JSON blobs (or future binary format) pruned
  before any data blob is fetched
- **Object-store detection** — auto-enable pruning on S3/NFS, disable on
  local disk (DuckDB native scan is faster locally)

Any app built on Pond gets these benefits automatically — the lens
provides the format, Pond provides the storage + pruning + versioning.

## Design principles (from DESIGN_GOALS.md §3)

1. **Simple** — the kernel stays intellectually small (3 primitives, ~140 LOC)
2. **Powerful** — rich behavior emerges from composition
3. **Performant** — optimizations live above the core
4. **Scalable** — lenses and extensions evolve independently
5. **Efficient** — immutable data + rebuildable derived metadata
6. **Beautiful** — one responsibility per layer; dependencies flow downward
7. **Functional** — Pond must do everything users actually need
8. **Storage-Independent** — stored bytes never depend on the execution engine

The generic design vision is the expression of principle 7 (Functional)
and principle 8 (Storage-Independent) applied to the pruning/encoding
infrastructure: ANY workload, ANY format, ANY storage backend.
