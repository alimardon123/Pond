# Universal Storage at PB Scale — The Arrow Question

> **Status:** Design decision (Task 68)
> **Purpose:** Resolve the question "does adopting Arrow lock Pond into
> tabular-only storage?" The answer is NO — if we design it right.
>
> **Context.** The veteran architect's V2 review (§3.6 suggestion (a))
> recommended "PND2 → PND3 with Arrow IPC alignment" to close the 2-4x
> performance gap with DuckDB. The user's concern: "Arrow is tabular.
> Pond's goal is unified storage for ANY workload at PB scale —
> semi-structured, unstructured, any data variant. Does Arrow lock us
> out of that?"

---

## The short answer

**Arrow is a read-path optimization, not a storage-format mandate.**

- **Storage format:** PND2 stays as the universal container. It already
  handles all value types (INT64, FLOAT64, STRING, BINARY, NULL) and
  all encodings (RAW, RLE, DICT, BITPACK). PND2 is NOT tabular-only —
  a BINARY column can hold arbitrary bytes (video segments, git tree
  objects, JSON blobs, images, etc.).
- **Read path:** when a consumer wants Arrow (DuckDB, Polars,
  DataFusion), we decode PND2 directly into Arrow buffers — skipping
  the `list[dict]` intermediate that currently causes the 2-4x
  overhead. This is the "native Arrow path" (Tier 1.1.1).
- **Non-tabular workloads** (KV, streaming, git, notebooks,
  unstructured): the data is stored as PND2 BINARY columns and decoded
  into Python objects / raw bytes, NOT into Arrow. Arrow is optional,
  not required.

Pond's storage stays universal. Arrow is one of several ways to
materialize the data, chosen by the consumer based on what they need.

---

## Why this works

### PND2 is already universal

PND2's value-type system (`VT_INT64`, `VT_FLOAT64`, `VT_STRING`,
`VT_BINARY`, `VT_NULL`) can represent ANY data structure:

| Workload | How it maps to PND2 | Tabular? |
|---|---|---|
| Lakehouse (SQL) | INT64/FLOAT64/STRING columns | ✅ Yes |
| Key-Value | STRING key + BINARY value (JSON or raw bytes) | ⚠️ Semi (2 columns) |
| Vector | BINARY column holding packed float32 arrays | ❌ No (packed bytes) |
| Streaming | INT64 offset + BINARY segment | ⚠️ Semi (2 columns) |
| Git-like | BINARY column holding tree/commit/blob objects | ❌ No (opaque bytes) |
| Graph | STRING src + STRING dst + BINARY attrs | ⚠️ Semi (edge list) |
| Time-series | INT64 timestamp + FLOAT64 values | ✅ Yes |
| Notebooks | STRING cell_id + STRING type + BINARY content | ⚠️ Semi |
| Feature store | INT64 entity_id + INT64 ts + FLOAT64 features | ✅ Yes |
| Semi-structured (JSON) | STRING or BINARY column holding JSON | ❌ No (nested) |
| Unstructured (video, images) | BINARY column holding raw bytes | ❌ No (opaque) |

The "universal" claim is honest: PND2 CAN store all of these. The
question is whether the READ path is efficient for each.

### The read path is adaptive

When you call `pond.read(collection)`:

1. **Default (Python objects):** decode PND2 → `list[dict]`. Works for
   all workloads. Current behavior. ~5M rows/s via PyO3.
2. **Arrow (for query engines):** decode PND2 → Arrow buffers directly.
   Skips the `list[dict]` intermediate. Target: ~50M rows/s. Only
   makes sense for tabular data (INT64/FLOAT64/STRING columns).
3. **Bytes (for streaming/git/unstructured):** decode PND2 → raw bytes.
   No Python object overhead. Makes sense for BINARY-heavy workloads.
4. **Future: Arrow Struct/List (for semi-structured):** decode PND2
   BINARY (JSON) → Arrow Struct/List. Nested but typed. Makes sense
   for JSON-heavy workloads.

The consumer chooses the materialization. The storage format doesn't
change — only the decode path does.

### Why we don't need PND3 (Arrow-IPC as the storage format)

The veteran suggested "PND3 with Arrow IPC alignment" — meaning the
storage format itself would be Arrow-IPC. This would:
- ✅ Get zero-copy Arrow reads for free
- ❌ Make non-tabular data (BINARY columns) second-class citizens
- ❌ Require reimplementing the entire format + all encoders/decoders
- ❌ Break the "one format for all workloads" principle

The native Arrow **read path** (Tier 1.1.1) gets 80% of the benefit
(zero-copy for typed columns) without the cost (PND2 stays as the
container, BINARY columns stay first-class).

**Decision: PND2 stays as the storage format. The native Arrow path is
a decode optimization, not a format change.**

---

## How each workload reads data (the adaptive read path)

### Tabular (Lakehouse, Feature Store, Time-Series)

```
PND2 blob (INT64/FLOAT64/STRING columns)
    ↓ pond_core::decode_to_arrow()     [Tier 1.1.1 — new]
Arrow RecordBatch (zero-copy)
    ↓ DuckDB / Polars / DataFusion
Query result
```

The decode skips `list[dict]` entirely. Arrow buffers are constructed
directly from PND2 column payloads. For INT64/FLOAT64, this is a
memcpy. For STRING, it's a pointer array + concatenated bytes.

### Key-Value

```
PND2 blob (STRING key + BINARY value)
    ↓ pond_core::decode()              [existing]
list[dict] (key → bytes)
    ↓ lens.point_lookup(key)
bytes (the value)
```

Arrow is NOT used here — the value is opaque bytes, not a typed
column. The consumer gets bytes, not Arrow.

### Streaming

```
PND2 blob (INT64 offset + BINARY segment)
    ↓ pond_core::decode_range(start, end)   [new — Tier 1.1.3]
list[(offset, bytes)]
    ↓ lens.read_stream(start, end)
bytes (the requested range)
```

Arrow is NOT used — segments are raw bytes. The read path supports
range reads (only decode the segments that overlap the requested range).

### Git-like (trees, commits, blobs)

```
PND2 blob (BINARY column holding serialized tree/commit objects)
    ↓ pond_core::decode()
list[bytes]
    ↓ lens.parse_tree() / lens.parse_commit()
Tree / Commit objects
```

Arrow is NOT used — git objects are opaque bytes with their own
serialization format.

### Semi-structured (JSON, XML)

Two options:
1. **Store as STRING (JSON text):** decode to `list[str]`, parse JSON
   in Python. Simple, universal, slower.
2. **Store as BINARY (packed JSON):** decode to `list[bytes]`, parse
   JSON in Rust (serde_json). Faster, still universal.
3. **Future: decode to Arrow Struct/List:** parse JSON into Arrow's
   nested types. Fastest for query engines, but requires schema
   inference. Tier 2 work.

### Unstructured (video, images, raw blobs)

```
PND2 blob (BINARY column)
    ↓ pond_core::decode()
list[bytes]
    ↓ consumer writes to file / streams to network
bytes
```

Arrow is NOT used. The data is opaque bytes. PND2's BINARY column is
exactly the right abstraction — it's a length-prefixed byte array,
which is what unstructured data IS.

---

## PB-scale considerations

The user explicitly asked about PB scale. Here's how the design scales:

### 1. Blob-level: content-addressed, deduped, parallel-fetchable

Every PND2 blob is content-addressed (SHA-256 hash). At PB scale:
- Dedup is free (same bytes → same hash → one copy)
- Parallel fetch (N blobs fetched across K connections)
- No metadata explosion (each blob is self-describing)

### 2. Collection-level: manifest + StatsTree

Each collection has a manifest (list of blob hashes + per-blob stats).
At PB scale:
- Manifest is a B-tree (StatsTree) for O(log N) lookup
- Manifest itself is content-addressed and deduped
- Manifests chain via commit hashes (git-like history)

### 3. Workload-level: adaptive read path

The read path chooses the decode strategy based on the workload:
- Tabular → Arrow (zero-copy, vectorized)
- Bytes → raw bytes (no overhead)
- Range → range scan (only fetch needed blobs)

This means a PB-scale lakehouse query doesn't pay the "decode
everything to dict" tax — it gets Arrow directly. A PB-scale streaming
read doesn't pay the "parse everything as tabular" tax — it gets raw
bytes.

### 4. Format-level: PND2 is extensible

PND2's header has a `version` byte. Future encodings (e.g., dictionary
for high-cardinality strings, bit-packing for low-cardinality ints,
run-length for sorted data) can be added without breaking existing
readers. The format is designed to evolve.

---

## Design principles compliance

| Principle | How this design serves it |
|---|---|
| Simple (3.1) | One storage format (PND2). Read path is adaptive but the format doesn't change. |
| Powerful (3.2) | Any data structure (tabular, KV, streaming, git, unstructured) stores in PND2. |
| Performant (3.3) | Native Arrow path for tabular; raw bytes for non-tabular. No intermediate conversion tax. |
| Scalable (3.4) | Content-addressed blobs, parallel fetch, manifest B-tree. PB-scale by design. |
| Efficient (3.5) | Dedup is free. Decode is O(requested) not O(total). |
| Beautiful (3.6) | One format, adaptive reads. Arrow is a view, not a mandate. |
| Functional (3.7) | All workload types supported. Arrow doesn't lock out non-tabular. |
| Storage-Indep (3.8) | PND2 is a custom format, but it's open and documented. Any engine can read it via the C ABI. |

---

## Conclusion

**Arrow is NOT a threat to universal storage.** It's a read-path
optimization for tabular workloads. The storage format (PND2) stays
universal — it handles all data types including raw bytes. The read
path is adaptive — consumers choose Arrow, bytes, or Python objects
based on what they need.

The native Arrow path (Tier 1.1.1) will close the 2-4x performance gap
with DuckDB for lakehouse workloads, WITHOUT sacrificing Pond's ability
to store non-tabular data (KV, streaming, git, unstructured).

**This is the beautiful design: one format, adaptive reads, universal
storage.**
