# Pond Binary Encoding Format Specification

> **Version 1.1** — Stable, documented, SIMD-ready.
>
> Any execution engine (DuckDB, Polars, DataFusion, Arrow compute) can
> read Pond's encoded chunk blobs natively. The format is raw binary —
> no JSON, no Python intermediaries, no pointer chasing. Bytes are
> directly mmappable to numpy/Arrow buffers for AVX2/AVX-512 vectorized
> operations.
>
> **v1.1 changes**: Added compression prefix (1 byte) and null bitmap
> for RAW encoding. Backward compatible — v1.0 readers check the
> compression byte and treat 0x00 as uncompressed v1.0.

## Overview

Every encoded chunk blob has the same top-level structure:

```
+-------------------+-----------------------------------+
| CompressionTag    | 1 byte (0x00=none, 0x01=LZ4, 0x02=zstd)|
+-------------------+-----------------------------------+
| EncodingHeader    | 9 bytes (magic + encoding + n_rows)|
+-------------------+-----------------------------------+
| Payload           | encoding-specific (see below)      |
+-------------------+-----------------------------------+
```

**Note**: The CompressionTag is OUTSIDE the PND1 format. Readers
decompress first (if needed), then parse the EncodingHeader + Payload
as v1.0 PND1. This keeps the PND1 format spec stable — compression
is a transparent wrapper.
```

The EncodingHeader is 9 bytes:
- `magic`: 4 bytes — always `b"PND1"` (identifies Pond-encoded blobs)
- `encoding`: 1 byte — `0=RAW, 1=RLE, 2=DICT, 3=BITPACK`
- `n_rows`: 4 bytes — uint32 little-endian (number of values in this chunk)

## Value type tags

All encodings use a 1-byte value type tag to indicate the data type:

| Tag | Type     | Size per value          |
|-----|----------|-------------------------|
| 1   | INT64    | 8 bytes (signed int64)  |
| 2   | FLOAT64  | 8 bytes (IEEE 754 double) |
| 3   | STRING   | 4 bytes length + UTF-8 bytes |
| 4   | NULL     | 0 bytes (all values are null) |

INT64 and FLOAT64 values are **contiguous** — an array of N values is
exactly N×8 bytes, directly castable to `numpy.frombuffer` or
`Arrow.Array.from_buffers`. This is the key to SIMD-ready access.

## Encoding 0: RAW

Uncompressed contiguous values with null bitmap. The fallback when no
structural encoding applies.

```
+------------+-------------------------------------------+
| value_type | 1 byte (INT64=1, FLOAT64=2, STRING=3, NULL=4)|
+------------+-------------------------------------------+
| null_bitmap| ceil(n_rows / 8) bytes (1=null, 0=valid)  |
|            | (absent if no nulls — detected by size)   |
+------------+-------------------------------------------+
| values     | N × 8 bytes (INT64/FLOAT64, 0 for nulls) |
|            | OR N × (4B length + UTF-8 bytes) (STRING) |
|            | (nulls produce no bytes for STRING)       |
+------------+-------------------------------------------+
```

The null bitmap uses Arrow convention: bit 1 = null, bit 0 = valid.
For INT64/FLOAT64, null values are stored as 0 in the values array —
the bitmap is authoritative. For STRING, nulls produce no bytes —
the bitmap marks their position.

**Null bitmap detection**: The reader detects whether a bitmap is
present by checking if `len(payload) - 1 - bitmap_size == n_rows * 8`
(for INT64/FLOAT64). If the math works out, the bitmap is present.

**SIMD access**: For INT64/FLOAT64, the values region is directly
castable to a numpy array:
```python
arr = np.frombuffer(payload[1 + bitmap_size:], dtype=np.int64)
```

## Encoding 1: RLE (Run-Length Encoding)

Stores `[value, run_length]` pairs. Great for sorted/low-cardinality columns.

```
+--------+------------+-------------------------------------------+
| n_runs | value_type | [value + run_length(4B)] × n_runs         |
| 4 bytes| 1 byte     |                                           |
+--------+------------+-------------------------------------------+
```

Each run is:
- `value`: 8 bytes (INT64/FLOAT64) or 4B length + UTF-8 (STRING)
- `run_length`: 4 bytes (uint32 little-endian)

**SIMD access**: An engine can scan the `run_length` array to find
which runs overlap a query range, then only touch the corresponding
values. The run_lengths are at fixed offsets (every 12 bytes for
INT64), enabling vectorized range computation.

## Encoding 2: DICT (Dictionary Encoding)

Stores unique values + bitpacked codes. Great for strings/categoricals.

```
+----------+------------+-------------------+----------------+------------------+
| n_unique | value_type | [value] × n_unique| code_bitwidth  | packed_codes     |
| 4 bytes  | 1 byte     |                   | 1 byte         | variable         |
+----------+------------+-------------------+----------------+------------------+
```

- `n_unique`: uint32 — number of unique values in the dictionary
- `value_type`: 1 byte — type of dictionary values
- `[value] × n_unique`: contiguous dictionary values (same encoding as RAW)
- `code_bitwidth`: 1 byte — number of bits per code (1-64)
- `packed_codes`: `ceil(n_rows × code_bitwidth / 8)` bytes — bitpacked codes

The packed_codes use **little-endian bit order** (LSB first within each
byte), matching the BITPACK encoding. Codes are offset-shifted to
non-negative (they start at 0 by construction).

**SIMD access**: An engine can:
1. Unpack codes with `numpy.unpackbits(bitorder='little')` + matrix multiply
2. Vectorized lookup: `dict_values[codes]` via numpy fancy indexing
3. Or scan the dictionary values first, find matching codes, then scan
   the packed codes for those code values

## Encoding 3: BITPACK

Packs small-range integers into minimal bits. Great for ages, status codes, etc.

```
+----------+--------+--------+--------+--------+---------------------------+
| bitwidth | offset | min    | max    | packed body                      |
| 1 byte   | 8 bytes| 8 bytes| 8 bytes| ceil(n_rows × bitwidth / 8) bytes|
+----------+--------+--------+--------+---------------------------+
```

- `bitwidth`: 1 byte — number of bits per value (1-64)
- `offset`: int64 — subtract from each stored value to get the original
- `min`: int64 — minimum original value (for O(1) pruning)
- `max`: int64 — maximum original value (for O(1) pruning)
- `packed body`: `ceil(n_rows × bitwidth / 8)` bytes — bitpacked values

The packed body uses **little-endian bit order** (LSB first within each
byte). Value `i` occupies bits `[i × bitwidth, (i+1) × bitwidth)` of
the packed body.

**SIMD access**:
- For byte-aligned bitwidths (8, 16, 32, 64): `np.frombuffer` directly
- For non-byte-aligned: `np.unpackbits(bitorder='little')` + reshape + matrix multiply
- O(1) predicate prune via min/max in the sub-header (read 16 bytes, no scanning)

## Predicate evaluation (Vortex-style)

All encodings support predicate evaluation **without full decode**:

| Encoding | Prune Level 1 (O(1)) | Prune Level 2 (O(N) scan) |
|----------|----------------------|---------------------------|
| RAW      | No (no stats)        | No (must decode)          |
| RLE      | No                   | Walk runs, check run_value |
| DICT     | No                   | Scan dictionary, find matching codes, scan packed codes |
| BITPACK  | min/max in sub-header| Vectorized scan on packed bytes |

The Vortex design: evaluate the predicate on the **encoded form** to
get surviving row ranges, then **decode only the surviving positions**.
For selective predicates, this avoids materializing the full decoded list.

## Endianness

All multi-byte integers are **little-endian** (x86 native, Arrow native).
No byte-swapping needed on x86/ARM little-endian platforms.

## Version detection

The 4-byte magic `b"PND1"` identifies Pond-encoded blobs. The `1` in
`PND1` is the format version. Future format changes would use `PND2`,
`PND3`, etc. Readers MUST check the magic and reject unknown versions.

## Stability promise

This format is **frozen** as of version 1.0. Future changes will:
1. Bump the version byte in the magic (`PND2`)
2. Keep backward-compatible readers for `PND1`
3. Be documented in an RFC

Any execution engine that implements a reader for this spec will
continue to work with Pond-encoded blobs across all future Pond versions
that produce `PND1` blobs.

## Generic design

This format is **workload-agnostic**:
- LakehouseLens (Parquet) → encode_fn wraps Parquet per-column
- KeyValueLens (JSON) → encode_fn wraps JSON per-column
- VectorLens (binary) → encode_fn wraps struct.pack per-dimension
- Notebook lens (rich text) → encode_fn wraps custom format
- Git lens (diffs) → encode_fn wraps diff format

The encoding layer never touches the lens's format — the lens provides
`encode_fn(col_name, values) -> bytes` and `decode_fn(bytes) -> list`.
The encoding layer provides the structural encoding (RAW, RLE, DICT,
BITPACK) on top of whatever the lens produces.
