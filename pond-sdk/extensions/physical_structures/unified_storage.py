"""
UnifiedStorage — ONE format, ONE write path, ONE read path.

THE MANDATE:
  "Simpler storage solution that unifies all workloads in same storage
   format regardless of use with no overhead for writes and reads."

THE DESIGN:
  ONE binary blob format (PND2) for EVERY workload:
    - Tabular (Lakehouse): table columns
    - KV (KeyValue): JSON fields as columns
    - Vector: dimensions as columns
    - Streaming: BINARY column for raw bytes + metadata columns
    - Notebooks: cell metadata + BINARY column for cell content
    - Git: file path + BINARY column for file content
    - Feature Store: feature columns + entity_id + timestamp

  ONE write path:
    write(collection, rows, key_col, row_group_size)
    - Splits rows into row groups
    - For each row group: encodes columns (auto-selects best encoding),
      computes stats (during encode — zero overhead), compresses,
      writes ONE PND2 blob
    - Builds manifest with all blob hashes + inline stats
    - Commits atomically

  ONE read path:
    read(collection, predicates, columns, commit_hash)
    - Fetches commit + manifest (2 S3 GETs)
    - Evaluates predicates IN MEMORY against manifest stats
    - Fetches K surviving blobs (K S3 GETs)
    - Decompresses + decodes only requested columns (projection pushdown)
    - Total: 2 + K S3 GETs (the irreducible minimum)

PND2 FORMAT:
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
  |     value_type (1B)            |
  |     encoding (1B)              |
  +--------------------------------+
  | Stats section (if has_stats):  |
  |   For each column:             |
  |     has_min (1B)               |
  |     min (8B or var-len)        |
  |     max (8B or var-len)        |
  |     null_count (4B)            |
  +--------------------------------+
  | Compression tag (1B)           |
  +--------------------------------+
  | Payload:                       |
  |   For each column:             |
  |     payload_len (4B)           |
  |     encoded bytes (variable)   |
  +--------------------------------+

WHAT THIS REPLACES:
  - 3 write modes (range_write, range_write_column_chunks, range_write_encoded)
  - 4+ read modes (read_table, read_with_*_pruning, etc.)
  - STORAGE_WHOLE_BLOB / STORAGE_COLUMN_CHUNKS / STORAGE_ENCODED
  - ColumnChunkStorage, EncodedChunkStorage classes
  - ColumnChunkZoneMap class (stats are inline in PND2)
  - ZoneMapIndex, StatsIndex classes (manifest replaces them)
  - PruningReader class (read_unified does pruning inline)
  - encode_fn/decode_fn lens-owned contract (PND2 owns the format)

WHAT STAYS:
  - Kernel (FROZEN — 3 primitives)
  - CollectionManifest (the index — one blob per commit)
  - stats_tree.py (PB-scale hierarchical index)
  - encoding.py (4 encodings — used internally by PND2)
  - compression.py (zstd/LZ4 — transparent layer)
  - column_source.py (format-agnostic data access)
  - PruningPredicate / ColumnPredicate (predicate evaluation)
  - All 5 lenses (they just provide a ColumnSource)
"""

from __future__ import annotations

import struct
import os
import sys
import json
from dataclasses import dataclass, field
from typing import Optional, Any, Iterator, Callable

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "pond-core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal  # noqa: E402

# Reuse the existing encoding + compression + manifest + column_source
from encoding import (  # noqa: E402
    ColumnEncoding, encode_column, decode_column,
    eval_predicate_encoded, decode_surviving_values,
    _detect_value_type, EncodingHeader,
    VALUE_TYPE_INT64, VALUE_TYPE_FLOAT64, VALUE_TYPE_STRING, VALUE_TYPE_NULL,
)
from compression import (  # noqa: E402
    compress_blob, decompress_blob,
    COMPRESSION_NONE, COMPRESSION_ZSTD,
)
from column_source import (  # noqa: E402
    ColumnSource, as_column_source, compute_list_stats,
    PyArrowColumnSource, ListColumnSource,
)
from collection_manifest import (  # noqa: E402
    CollectionManifest, RowGroupEntry, ColumnStatsEntry,
    STORAGE_WHOLE_BLOB,  # reuse as "unified" storage mode
    build_manifest_from_zone_map,
)


# ---------------------------------------------------------------------------
# PND2 format constants
# ---------------------------------------------------------------------------

_PND2_MAGIC = b"PND2"
_PND2_VERSION = 2

# Flags
_FLAG_HAS_STATS = 0x01
_FLAG_COMPRESSED = 0x02

# New value type for raw bytes (video, music, file content, etc.)
VALUE_TYPE_BINARY = 5


# ---------------------------------------------------------------------------
# PND2 blob — encode/decode
# ---------------------------------------------------------------------------

@dataclass
class PND2Column:
    """One column's metadata in a PND2 blob."""
    name: str
    value_type: int
    encoding: int
    min: Any = None
    max: Any = None
    null_count: int = 0
    payload: bytes = b""  # encoded bytes (after decompression)


class PND2:
    """Encode/decode the PND2 unified blob format.

    ONE blob per row group. All columns in one blob. Stats inline.
    Compression transparent. Encoding auto-selected per column.

    Lifecycle:
      1. PND2.encode(source, key_col) → bytes, [RowGroupEntry stats]
         (encode columns, compute stats during encode, compress)
      2. kernel.write(bytes) → blob_hash
      3. (later) bytes = kernel.read_blob(blob_hash)
      4. PND2.decode(bytes, columns=None) → dict[col_name, list[values]]
         (decompress, decode only requested columns — projection pushdown)
    """

    # ------------------------------------------------------------------
    # Encode — write side
    # ------------------------------------------------------------------

    @staticmethod
    def encode(source: ColumnSource,
                encoding_hints: Optional[dict[str, str]] = None,
                compress: bool = True) -> tuple[bytes, list[tuple[str, int, Any, Any, int]]]:
        """Encode a ColumnSource as a PND2 blob.

        Args:
            source: a ColumnSource (PyArrow table, list[dict], etc.)
            encoding_hints: optional dict {col_name: "auto"|"rle"|"dict"|"bitpack"|"raw"}
            compress: if True (default), compress the payload with zstd

        Returns:
            Tuple of (pnd2_bytes, column_stats_list) where column_stats_list
            is [(name, value_type, min, max, null_count), ...] — used to
            build the manifest entry without re-decoding.
        """
        if encoding_hints is None:
            encoding_hints = {}

        n_rows = source.num_rows()
        col_names = source.column_names()

        # Encode each column + compute stats (single pass per column)
        columns_meta: list[PND2Column] = []
        for col_name in col_names:
            values = source.column_slice(col_name, 0, n_rows)
            hint = encoding_hints.get(col_name, "auto")

            # Detect value type (special-case BINARY for raw bytes)
            vtype = _detect_value_type_with_binary(values)

            # Encode — encode_column picks the best encoding
            # For BINARY values, force RAW encoding (no RLE/DICT/BITPACK)
            if vtype == VALUE_TYPE_BINARY:
                # _encode_binary_raw returns a full PND1 chunk blob (header + payload).
                # Extract just the payload (skip the 9-byte PND1 header) for
                # storage in PND2. The encoding code is always RAW for BINARY.
                full_blob, enc_meta = _encode_binary_raw(values, hint="raw")
                encoding_code = ColumnEncoding.RAW
                encoded_bytes = full_blob[EncodingHeader.SIZE:]
            else:
                # encode_column returns (full_pnd1_blob, meta) — extract the payload
                full_blob, enc_meta = encode_column(values, hint=hint)
                enc_name = enc_meta.get("encoding", "raw")
                if isinstance(enc_name, str):
                    encoding_code = {"raw": 0, "rle": 1, "dict": 2,
                                      "bitpack": 3}.get(enc_name, 0)
                else:
                    encoding_code = int(enc_name)
                # Skip the 9-byte PND1 header to get just the payload
                encoded_bytes = full_blob[EncodingHeader.SIZE:]

            # Compute stats (one pass over values)
            if vtype == VALUE_TYPE_BINARY:
                # Binary columns have no min/max (raw bytes)
                mn, mx, null_count = None, None, sum(1 for v in values if v is None)
            else:
                mn, mx, null_count = compute_list_stats(values)

            columns_meta.append(PND2Column(
                name=col_name,
                value_type=vtype,
                encoding=encoding_code,
                min=mn,
                max=mx,
                null_count=null_count,
                payload=encoded_bytes,
            ))

        # Build the PND2 bytes
        return PND2._build_blob(columns_meta, n_rows, compress), \
               [(c.name, c.value_type, c.min, c.max, c.null_count) for c in columns_meta]

    @staticmethod
    def _build_blob(columns: list[PND2Column], n_rows: int,
                     compress: bool) -> bytes:
        """Build the PND2 binary blob from column metadata + payloads."""
        # Build the inner payload (schema + stats + per-column payloads)
        inner = bytearray()

        # Schema section
        for col in columns:
            name_bytes = col.name.encode("utf-8")
            inner += struct.pack("<B", len(name_bytes))
            inner += name_bytes
            inner += struct.pack("<BB", col.value_type, col.encoding)

        # Stats section (always present — zero overhead since we compute
        # them during encode anyway)
        for col in columns:
            has_min = col.min is not None and col.max is not None
            inner += struct.pack("<B", 1 if has_min else 0)
            if has_min:
                inner += _encode_pnd2_value(col.value_type, col.min)
                inner += _encode_pnd2_value(col.value_type, col.max)
            inner += struct.pack("<I", col.null_count)

        # Per-column payloads
        for col in columns:
            inner += struct.pack("<I", len(col.payload))
            inner += col.payload

        # Compress the inner payload (transparent)
        if compress and len(inner) > 64:
            try:
                import zstandard as zstd
                compressed = zstd.compress(bytes(inner))
                if len(compressed) + 1 < len(inner):
                    payload = struct.pack("<B", COMPRESSION_ZSTD) + compressed
                    flags = _FLAG_HAS_STATS | _FLAG_COMPRESSED
                else:
                    payload = struct.pack("<B", COMPRESSION_NONE) + bytes(inner)
                    flags = _FLAG_HAS_STATS
            except ImportError:
                payload = struct.pack("<B", COMPRESSION_NONE) + bytes(inner)
                flags = _FLAG_HAS_STATS
        else:
            payload = struct.pack("<B", COMPRESSION_NONE) + bytes(inner)
            flags = _FLAG_HAS_STATS

        # Build the final blob: header + payload
        header = bytearray()
        header += _PND2_MAGIC
        header += struct.pack("<BB", _PND2_VERSION, flags)
        header += struct.pack("<IH", n_rows, len(columns))

        return bytes(header) + bytes(payload)

    # ------------------------------------------------------------------
    # Decode — read side
    # ------------------------------------------------------------------

    @staticmethod
    def decode(data: bytes,
                columns: Optional[list[str]] = None,
                predicates: Optional[list[tuple[str, str, Any]]] = None
                ) -> dict[str, list]:
        """Decode a PND2 blob.

        Args:
            data: the PND2 blob bytes
            columns: optional list of column names to decode (projection
                pushdown — other columns are skipped entirely). If None,
                decode all columns.
            predicates: optional list of (column, op, value) tuples for
                Vortex-style predicate eval on the encoded form. Only
                surviving row ranges are decoded.

        Returns:
            Dict mapping column_name → list of values. Columns not in
            `columns` (if specified) are not in the dict.
        """
        if data[:4] != _PND2_MAGIC:
            raise ValueError(f"Not a PND2 blob (magic={data[:4]!r})")

        version, flags = struct.unpack("<BB", data[4:6])
        if version != _PND2_VERSION:
            raise ValueError(f"Unsupported PND2 version: {version}")
        n_rows, n_columns = struct.unpack("<IH", data[6:12])
        pos = 12

        # Compression tag
        compression_tag = data[pos]; pos += 1

        # Decompress if needed — `inner` is the decompressed bytes (a NEW
        # bytes object). After this, we parse `inner` starting at pos=0.
        if compression_tag == COMPRESSION_NONE:
            inner = data[pos:]
        elif compression_tag == COMPRESSION_ZSTD:
            import zstandard as zstd
            inner = zstd.decompress(data[pos:])
        else:
            # LZ4 or unknown — try zstd as fallback
            try:
                import zstandard as zstd
                inner = zstd.decompress(data[pos:])
            except Exception:
                inner = data[pos:]

        # Parse `inner` from position 0 (NOT `pos` — that was for `data`)
        pos = 0

        # Parse schema
        schema: list[tuple[str, int, int]] = []  # (name, value_type, encoding)
        for _ in range(n_columns):
            name_len = inner[pos]; pos += 1
            name = inner[pos:pos+name_len].decode("utf-8"); pos += name_len
            vtype, enc = struct.unpack("<BB", inner[pos:pos+2]); pos += 2
            schema.append((name, vtype, enc))

        # Parse stats (skip if we don't need them — but they're cheap to parse)
        stats: dict[str, tuple[Any, Any, int]] = {}
        for name, vtype, enc in schema:
            has_min = inner[pos]; pos += 1
            if has_min:
                mn, pos = _decode_pnd2_value(vtype, inner, pos)
                mx, pos = _decode_pnd2_value(vtype, inner, pos)
            else:
                mn = mx = None
            null_count = struct.unpack("<I", inner[pos:pos+4])[0]; pos += 4
            stats[name] = (mn, mx, null_count)

        # Parse per-column payloads
        payloads: dict[str, tuple[int, bytes]] = {}  # name → (encoding, bytes)
        for name, vtype, enc in schema:
            payload_len = struct.unpack("<I", inner[pos:pos+4])[0]; pos += 4
            payload_bytes = inner[pos:pos+payload_len]; pos += payload_len
            payloads[name] = (enc, payload_bytes)

        # Determine which columns to decode (projection pushdown)
        if columns is None:
            columns_to_decode = [s[0] for s in schema]
        else:
            columns_to_decode = [c for c in columns if c in payloads]

        # Determine surviving row ranges (for Vortex-style eval)
        # Find the first predicate column that exists in this blob
        surviving_ranges: Optional[list[tuple[int, int]]] = None
        pred_col_name: Optional[str] = None
        if predicates:
            for col_name, op, val in predicates:
                if col_name in payloads:
                    pred_col_name = col_name
                    enc, payload_bytes = payloads[col_name]

                    # Find this column's value_type
                    pred_vtype = VALUE_TYPE_NULL
                    for s_name, s_vtype, s_enc in schema:
                        if s_name == col_name:
                            pred_vtype = s_vtype
                            break

                    if pred_vtype == VALUE_TYPE_BINARY:
                        # BINARY columns don't support encoded predicate eval;
                        # decode all values and filter in Python
                        all_vals = _decode_binary_raw(payload_bytes, n_rows)
                        surviving = []
                        range_start = None
                        for pos, v in enumerate(all_vals):
                            if _binary_value_matches(v, op, val):
                                if range_start is None:
                                    range_start = pos
                            else:
                                if range_start is not None:
                                    surviving.append((range_start, pos))
                                    range_start = None
                        if range_start is not None:
                            surviving.append((range_start, len(all_vals)))
                        surviving_ranges = surviving
                        if not surviving_ranges:
                            return {c: [] for c in columns_to_decode}
                    else:
                        # eval_predicate_encoded expects a PND1 chunk blob
                        # (EncodingHeader + payload). Reconstruct it.
                        pnd1_blob = EncodingHeader(enc, n_rows).to_bytes() + payload_bytes
                        result = eval_predicate_encoded(pnd1_blob, col_name, op, val)
                        if result is not None:
                            surviving_ranges, _ = result
                            # Bitpack eval may produce ranges that extend past
                            # the declared n_rows (due to byte-boundary padding).
                            # Truncate any range end to n_rows.
                            surviving_ranges = [(s, min(e, n_rows))
                                                  for s, e in surviving_ranges
                                                  if s < n_rows]
                            if not surviving_ranges:
                                # All rows pruned — return empty lists
                                return {c: [] for c in columns_to_decode}
                    break  # only one predicate column drives the ranges

        # Decode the requested columns
        result: dict[str, list] = {}
        for col_name in columns_to_decode:
            # Find this column's value_type from the schema
            col_vtype = VALUE_TYPE_NULL
            for s_name, s_vtype, s_enc in schema:
                if s_name == col_name:
                    col_vtype = s_vtype
                    break

            enc, payload_bytes = payloads[col_name]

            # BINARY columns use a custom decode (decode_column doesn't
            # know about VALUE_TYPE_BINARY)
            if col_vtype == VALUE_TYPE_BINARY:
                values = _decode_binary_raw(payload_bytes, n_rows)
                # Apply surviving ranges if applicable
                if surviving_ranges is not None and pred_col_name is not None:
                    surviving_values = []
                    for start, end in surviving_ranges:
                        surviving_values.extend(values[start:end])
                    values = surviving_values
                result[col_name] = values
                continue

            # Non-BINARY: reconstruct PND1 chunk blob for decode_column
            pnd1_blob = EncodingHeader(enc, n_rows).to_bytes() + payload_bytes

            if surviving_ranges is not None and pred_col_name is not None:
                # Decode only the surviving ranges
                values = decode_surviving_values(pnd1_blob, surviving_ranges)
            else:
                values = decode_column(pnd1_blob)
                # Bitpack decode may return more values than n_rows (pads to
                # the next byte boundary). Truncate to the declared n_rows.
                if enc == 3 and len(values) > n_rows:  # 3 = BITPACK
                    values = values[:n_rows]
            result[col_name] = values

        return result

    @staticmethod
    def peek_header(data: bytes) -> Optional[tuple[int, list[tuple[str, int, int]], dict[str, tuple[Any, Any, int]]]]:
        """Peek at a PND2 blob's header — schema + stats, NO payload decode.

        This is the INLINE-SHARD reader: when a shard ref points directly
        to a PND2 blob (single-row-group shard, no PMAN manifest), the
        reader uses this method to build a pseudo RowGroupEntry from the
        header stats without decoding the column payloads.

        Returns:
            Tuple of (n_rows, schema, stats) where:
              - n_rows: int (rows in this blob)
              - schema: list of (name, value_type, encoding) tuples
              - stats: dict[name → (min, max, null_count)]
            Returns None if not a valid PND2 blob.
        """
        if data[:4] != _PND2_MAGIC:
            return None
        version, flags = struct.unpack("<BB", data[4:6])
        if version != _PND2_VERSION:
            return None
        n_rows, n_columns = struct.unpack("<IH", data[6:12])
        pos = 12
        compression_tag = data[pos]; pos += 1

        # Decompress if needed
        if compression_tag == COMPRESSION_NONE:
            inner = data[pos:]
        elif compression_tag == COMPRESSION_ZSTD:
            try:
                import zstandard as zstd
                inner = zstd.decompress(data[pos:])
            except Exception:
                return None
        else:
            return None

        # Parse `inner` from position 0
        ipos = 0

        # Parse schema
        schema: list[tuple[str, int, int]] = []
        for _ in range(n_columns):
            name_len = inner[ipos]; ipos += 1
            name = inner[ipos:ipos+name_len].decode("utf-8"); ipos += name_len
            vtype, enc = struct.unpack("<BB", inner[ipos:ipos+2]); ipos += 2
            schema.append((name, vtype, enc))

        # Parse stats (skip if absent — return empty tuple per column)
        stats: dict[str, tuple[Any, Any, int]] = {}
        if flags & _FLAG_HAS_STATS:
            for name, vtype, enc in schema:
                has_min = inner[ipos]; ipos += 1
                if has_min:
                    mn, ipos = _decode_pnd2_value(vtype, inner, ipos)
                    mx, ipos = _decode_pnd2_value(vtype, inner, ipos)
                else:
                    mn = mx = None
                null_count = struct.unpack("<I", inner[ipos:ipos+4])[0]; ipos += 4
                stats[name] = (mn, mx, null_count)
        else:
            for name, _vt, _enc in schema:
                stats[name] = (None, None, 0)

        return n_rows, schema, stats

    @staticmethod
    def peek_stats(data: bytes) -> Optional[dict[str, tuple[Any, Any, int]]]:
        """Peek at the stats in a PND2 blob header without decoding payloads.

        Useful for third-level pruning: fetch the blob, peek at stats,
        decide whether to decode. Returns None if not a PND2 blob or
        no stats.
        """
        header = PND2.peek_header(data)
        if header is None:
            return None
        _n_rows, _schema, stats = header
        return stats


# ---------------------------------------------------------------------------
# UnifiedStorage — the ONE write path + ONE read path
# ---------------------------------------------------------------------------

class UnifiedStorage:
    """ONE write path, ONE read path, ANY workload.

    Replaces:
      - range_write (whole-blob Parquet)
      - range_write_column_chunks (per-column Parquet blobs)
      - range_write_encoded (per-column encoded blobs)
      - read_with_pruning / read_with_column_chunk_pruning / read_with_encoded_pruning

    Usage (write):
        storage = UnifiedStorage(kernel)
        commit_hash = storage.write("users", table, key_col="id",
                                      row_group_size=10_000)

    Usage (read):
        storage = UnifiedStorage(kernel)
        # Full scan
        rows = storage.read("users")
        # Predicate-pruned
        rows = storage.read("users", predicates=[("age", ">", 30)])
        # Projection + predicate
        rows = storage.read("users",
                              predicates=[("region", "=", "US")],
                              columns=["id", "age"])
        # Point lookup
        rows = storage.point_lookup("users", key="12345")
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel
        self._manifest_cache: dict[str, CollectionManifest] = {}
        # Cache the manifest HASH alongside the manifest object so we
        # don't need a separate resolve() call to get the hash.
        self._manifest_hash_cache: dict[str, str] = {}
        # Cache the HEAD commit hash per collection so append doesn't
        # need to resolve(HEAD) on every write.
        self._head_cache: dict[str, str] = {}
        # Cache the next commit index per collection so append doesn't
        # need to read the parent commit blob just for the index number.
        self._commit_index_cache: dict[str, int] = {}
        # Cache the delta chain depth per collection so the compaction
        # check doesn't need to walk the parent chain on every append.
        self._delta_chain_depth_cache: dict[str, int] = {}
        # Cache the schema per collection so append_shard doesn't need
        # to read the existing manifest just for schema columns.
        self._schema_cache: dict[str, tuple] = {}  # collection → (columns, key_col, rg_size)
        # Active branch per collection (set by checkout, cleared by undo/merge)
        self._active_branches: dict[str, str] = {}
        # HLC instance shared across all upsert_shard/delete_shard calls.
        # A single clock ensures the logical counter is monotonic across
        # calls within the same process — without this, two calls in the
        # same millisecond would produce identical _version strings and
        # the second update would be silently lost during CRDT merge.
        # _merge_rows_by_rowid also calls self._hlc.observe() on every
        # remote _version it sees, so the local clock stays ahead of
        # remote clocks (prevents skew-related LWW inversion).
        try:
            from hlc import HLC
            self._hlc = HLC()
        except ImportError:
            self._hlc = None

    # ------------------------------------------------------------------
    # Manifest ref helper
    # ------------------------------------------------------------------

    def _manifest_ref(self, collection: str) -> str:
        """The manifest ref path for the ACTIVE branch.

        Each branch owns its own manifest ref so checkout is pure
        in-memory pointer swap (no ref mutation, no race). The active
        branch is resolved via _get_active_branch (defaults to 'main').
        """
        branch = self._get_active_branch(collection)
        return f"collections/{collection}/branches/{branch}/manifest"

    def _manifest_ref_for_branch(self, collection: str, branch: str) -> str:
        """The manifest ref path for a SPECIFIC branch (not the active one).

        Used by checkout (to invalidate the old branch's cache) and merge
        (to write the merge result to the target branch's manifest ref).
        """
        return f"collections/{collection}/branches/{branch}/manifest"

    @staticmethod
    def _head_ref(collection: str) -> str:
        """DEPRECATED: HEAD ref is eliminated.

        Kept for backward compat with external callers (e.g. lakehouse_lens).
        Returns the default branch's commit ref (branches/main/commit).
        NOTE: This is a static method and cannot resolve the in-memory active
        branch — it returns the 'main' branch ref as a fallback. Callers that
        need the true active commit ref should use _active_commit_ref() instead.
        """
        return f"collections/{collection}/branches/main/commit"

    def _active_commit_ref(self, collection: str) -> str:
        """The ref for the currently active branch's commit (replaces HEAD).

        With the HEAD ref eliminated, the 'current commit' is whatever the
        active branch points at. The active branch is tracked in-memory via
        _active_branches[collection] (set by checkout), defaulting to 'main'.
        """
        branch = self._get_active_branch(collection)
        return self._branch_ref(collection, branch)

    def _load_manifest(self, collection: str,
                        manifest_hash: Optional[str] = None,
                        skip_cache: bool = False
                        ) -> Optional[CollectionManifest]:
        """Load the manifest for a collection (cached).

        If manifest_hash is provided, loads that specific manifest (for
        time-travel reads — no ref mutation, no race condition).
        If manifest_hash is None, resolves the current manifest ref.

        Round 26 caching strategy:
        - skip_cache=False (READS): verify cached hash matches current ref
          (1 GET). Handles multi-writer scenarios correctly.
        - skip_cache=True (WRITES): trust the cache blindly (0 GETs).
          The write path is single-writer — the cache is authoritative.
        """
        # If a specific manifest hash is requested, load it directly
        if manifest_hash is not None:
            try:
                return CollectionManifest.load(self.kernel, manifest_hash)
            except (ValueError, KeyError):
                return None

        # skip_cache=True (WRITE path): trust the cache blindly — 0 GETs
        if skip_cache and collection in self._manifest_cache:
            return self._manifest_cache[collection]

        # skip_cache=False (READ path): verify freshness — 1 GET
        if not skip_cache and collection in self._manifest_cache:
            # Check BOTH the dedicated path store (used by concurrent writers)
            # and the root ref (used by legacy writers). The dedicated path
            # is authoritative if it exists.
            if hasattr(self.kernel, 'get_path'):
                current_hash = self.kernel.get_path(self._manifest_ref(collection))
                if current_hash is None:
                    # Fall back to root ref
                    current_hash = self.kernel.resolve(self._manifest_ref(collection))
            else:
                current_hash = self.kernel.resolve(self._manifest_ref(collection))
            cached_hash = self._manifest_hash_cache.get(collection)
            if current_hash == cached_hash:
                return self._manifest_cache[collection]
            # Stale cache — fall through to re-read
            self._invalidate_manifest_cache(collection)

        # Resolve the manifest hash — check dedicated path first (concurrent
        # writers use set_path), then fall back to root ref (legacy writers).
        if hasattr(self.kernel, 'get_path'):
            resolved_hash = self.kernel.get_path(self._manifest_ref(collection))
            if resolved_hash is None:
                resolved_hash = self.kernel.resolve(self._manifest_ref(collection))
        else:
            resolved_hash = self.kernel.resolve(self._manifest_ref(collection))
        if resolved_hash is None:
            return None

        try:
            manifest = CollectionManifest.load(self.kernel, resolved_hash)
            self._manifest_cache[collection] = manifest
            self._manifest_hash_cache[collection] = resolved_hash
            return manifest
        except (ValueError, KeyError):
            return None

    def _get_cached_manifest_hash(self, collection: str) -> Optional[str]:
        """Return the cached manifest hash for a collection (0 GETs).

        Returns None if the manifest is not cached. Call _load_manifest
        first to populate the cache.
        """
        return self._manifest_hash_cache.get(collection)

    def _invalidate_manifest_cache(self, collection: str) -> None:
        """Invalidate ALL caches for a collection (used by undo/checkout/merge
        where the HEAD changed externally and we must re-read)."""
        self._manifest_cache.pop(collection, None)
        self._manifest_hash_cache.pop(collection, None)
        self._head_cache.pop(collection, None)
        self._commit_index_cache.pop(collection, None)
        self._delta_chain_depth_cache.pop(collection, None)
        self._schema_cache.pop(collection, None)

    def _update_caches_after_write(self, collection: str,
                                     manifest: CollectionManifest,
                                     manifest_hash: str,
                                     commit_hash: str,
                                     commit_index: int,
                                     is_delta: bool = False) -> None:
        """Update ALL caches after a write/append — enables O(1) warm writes.

        Instead of invalidating the cache (which forces the next write to
        re-read from storage), we UPDATE the cache with the new values.
        The next write to the same collection uses 0 GETs.
        """
        self._manifest_cache[collection] = manifest
        self._manifest_hash_cache[collection] = manifest_hash
        self._head_cache[collection] = commit_hash
        self._commit_index_cache[collection] = commit_index + 1
        # Cache the schema so append_shard doesn't need to read the manifest
        self._schema_cache[collection] = (
            manifest.columns if manifest else [],
            manifest.key_col if manifest else "",
            manifest.row_group_size if manifest else 10_000,
        )
        # Track delta chain depth for compaction check (0 GETs vs walking chain)
        if is_delta:
            self._delta_chain_depth_cache[collection] = \
                self._delta_chain_depth_cache.get(collection, 0) + 1
        else:
            # Flat manifest — reset chain depth to 0
            self._delta_chain_depth_cache[collection] = 0

    # ------------------------------------------------------------------
    # VERSION CONTROL — manifest-based commit/branch/merge/history
    #
    # This replaces ProllyLensBase. The commit blob is a simple JSON
    # dict stored as a kernel blob:
    #
    #   {
    #     "parent": "<parent_commit_hash or null>",
    #     "second_parent": "<merge_parent or null>",
    #     "manifest": "<manifest_hash>",
    #     "message": "...",
    #     "timestamp": 1234.5,
    #     "index": 42
    #   }
    #
    # The commit chain is: HEAD ref → commit blob → manifest blob → data blobs
    # Branches are just ref copies. Merges create a commit with two parents.
    # History walks parent pointers. No ProllyTree needed.
    # ------------------------------------------------------------------

    @staticmethod
    def _branch_ref(collection: str, branch: str) -> str:
        """Branch commit ref path.

        Each branch owns its own commit ref at
        collections/{c}/branches/{branch}/commit. Shards live alongside
        at collections/{c}/branches/{branch}/shards/{uuid}, and the
        manifest at collections/{c}/branches/{branch}/manifest — all
        under the single branches/ namespace (no separate branch-refs/).
        """
        return f"collections/{collection}/branches/{branch}/commit"

    def _write_commit_blob(self, collection: str,
                            manifest_hash: str,
                            parent: Optional[str] = None,
                            second_parent: Optional[str] = None,
                            message: str = "",
                            index: int = 0) -> str:
        """Write a commit blob and update HEAD.

        The commit blob is a JSON dict linking to the manifest hash.
        This is the ONE commit format for ALL workloads — no more
        JSON encoding (replaces the old BinaryProllyTree format).

        If the collection has an active branch (set by checkout), the
        branch ref is also updated to point to the new commit.
        """
        import json as _json
        import time as _time

        commit = {
            "parent": parent,
            "second_parent": second_parent,
            "manifest": manifest_hash,
            "message": message or f"commit #{index}",
            "timestamp": _time.time(),
            "index": index,
        }
        commit_bytes = _json.dumps(commit, sort_keys=True).encode()
        commit_hash = self.kernel.write(commit_bytes)
        # Update the active branch's commit ref (replaces the old HEAD ref).
        # The active branch is tracked in-memory via _active_branches; this
        # single ref update is the only ref mutation needed on commit.
        active = self._active_commit_ref(collection)
        self.kernel.reference(active, commit_hash)
        # Update the active branch's manifest ref (per-branch manifest).
        # Each branch has its own manifest ref so checkout is a pure
        # in-memory pointer swap — no sync needed.
        self.kernel.reference(self._manifest_ref(collection), manifest_hash)
        return commit_hash

    def _read_commit_blob(self, commit_hash: str) -> Optional[dict]:
        """Read and decode a commit blob.

        Returns None for JSON decode errors or missing blobs (expected
        for legacy/foreign commits). Re-raises network errors and OOM.
        """
        import json as _json
        try:
            raw = self.kernel.read_blob(commit_hash)
            return _json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError, KeyError, ValueError):
            return None  # expected: not a JSON commit, or blob missing

    def _commit_index(self, collection: str) -> int:
        """Get the next commit index for a collection."""
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            return 0
        commit = self._read_commit_blob(head)
        if commit is None:
            return 0
        return commit.get("index", 0) + 1

    def branch(self, collection: str, branch_name: str) -> str:
        """Create a branch — O(1) ref copy.

        Copies BOTH the commit ref AND the manifest ref from the active
        branch to the new branch. Each branch owns its own manifest ref
        (per-branch manifests), so a new branch needs its own manifest
        ref pointing at the source branch's current manifest — otherwise
        reads on the new branch see no HEAD manifest (only shards).
        """
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            raise KeyError(f"Collection '{collection}' not found")
        self.kernel.reference(self._branch_ref(collection, branch_name), head)
        # Also copy the manifest ref so the new branch has a starting manifest.
        source_branch = self._get_active_branch(collection)
        source_manifest = self.kernel.resolve(
            self._manifest_ref_for_branch(collection, source_branch))
        if source_manifest is not None:
            self.kernel.reference(
                self._manifest_ref_for_branch(collection, branch_name),
                source_manifest)
        return head

    def _sync_branch_manifest_to_head(self, collection: str) -> None:
        """Sync the active branch's manifest ref to match its commit's manifest.

        Used by undo/revert — they rebind the active branch's commit ref
        to an older commit, so the manifest ref must be rebound to that
        commit's manifest hash (otherwise reads see the pre-undo manifest).

        NOT needed by checkout — each branch has its own manifest ref, so
        switching the active branch is enough (the new branch's manifest
        ref already points at the right manifest).
        """
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            return
        commit = self._read_commit_blob(head)
        if commit and commit.get("manifest"):
            self.kernel.reference(self._manifest_ref(collection),
                                   commit["manifest"])
        self._invalidate_manifest_cache(collection)

    def checkout(self, collection: str, branch_name: str) -> None:
        """Checkout a branch — set the active branch IN-MEMORY ONLY.

        No storage mutation: each branch owns its own commit ref AND its
        own manifest ref, so switching the active branch is a pure pointer
        swap. Reads automatically pick up the new branch's manifest ref.

        Sets the active branch so subsequent commits/append/shard writes
        go to this branch (git-like behavior).
        """
        h = self.kernel.resolve(self._branch_ref(collection, branch_name))
        if h is None:
            raise ValueError(f"Branch '{branch_name}' does not exist")
        # Invalidate the OLD branch's manifest ref cache (so a future
        # checkout back to it re-reads fresh data).
        old_branch = self._get_active_branch(collection)
        if hasattr(self.kernel, 'invalidate_path_cache'):
            self.kernel.invalidate_path_cache(
                self._manifest_ref_for_branch(collection, old_branch))
        # Switch the active branch IN-MEMORY ONLY.
        self._active_branches[collection] = self._branch_ref(collection, branch_name)
        # Invalidate the NEW branch's manifest ref cache so the next read
        # re-reads from storage (rather than seeing a stale cached value).
        if hasattr(self.kernel, 'invalidate_path_cache'):
            self.kernel.invalidate_path_cache(self._manifest_ref(collection))
        # Invalidate ALL UnifiedStorage-level caches for this collection
        # so the next read sees the new branch's data.
        self._invalidate_manifest_cache(collection)

    def checkout_new(self, collection: str, branch_name: str) -> str:
        """Create a branch AND checkout — like `git checkout -b`.

        Combines branch() + checkout() in one call:
          1. Creates the branch from current HEAD
          2. Checks it out (sets active branch)

        Args:
            collection: collection name
            branch_name: name of the new branch

        Returns:
            The new branch's HEAD commit hash.
        """
        self.branch(collection, branch_name)
        self.checkout(collection, branch_name)
        return self.kernel.resolve(self._active_commit_ref(collection))

    def list_branches(self, collection: str) -> list[str]:
        """List all branches for a collection.

        Branch state lives at collections/{c}/branches/{branch}/ — a branch
        is identified by having a `commit` (or `manifest`) file inside its
        directory. We list all refs under branches/ and collect the unique
        branch names (ignoring `shards/` subpaths).
        """
        prefix = f"collections/{collection}/branches/"
        branches = set()
        for n in self.kernel.list_names():
            if not n.startswith(prefix):
                continue
            # n is like collections/{c}/branches/{branch}/commit
            # or collections/{c}/branches/{branch}/manifest
            # or collections/{c}/branches/{branch}/shards/{uuid}
            rest = n[len(prefix):]
            parts = rest.split("/")
            if len(parts) >= 2:
                branch_name = parts[0]
                if parts[1] in ("commit", "manifest"):
                    branches.add(branch_name)
        return sorted(branches)

    def merge(self, collection: str, source_branch: str,
              target_branch: Optional[str] = None,
              message: str = "") -> str:
        """Merge a source branch into a target branch.

        Args:
            collection: collection name
            source_branch: the branch to merge FROM
            target_branch: the branch to merge INTO. If None, uses the
                currently active branch (backward compat).
            message: commit message for the merge

        THREE-LEVEL MERGE:
          1. Row-group level: union target HEAD + source HEAD + all shards from both
          2. Row level: dedup by _rowid, latest _version wins
          3. Branch level: writes merge commit with two parents, clears shards

        Also merges both branches' shards into the target's HEAD and clears
        the shards from both branches.
        """
        branch_name = source_branch  # for backward compat with internal code
        # Treat empty string as None — some callers (e.g. keyvalue_lens.merge)
        # pass the message string positionally, which lands in target_branch.
        # Empty/None → use the active branch (backward compat).
        if not target_branch:
            target_branch = self._get_active_branch(collection)

        branch_head = self.kernel.resolve(
            self._branch_ref(collection, branch_name))
        if branch_head is None:
            raise ValueError(f"Branch '{branch_name}' does not exist")

        # The parent of the merge commit is the TARGET branch's HEAD —
        # NOT the currently active HEAD. If you're on `dev` and call
        # merge("events", "feature1", "main"), the merge goes INTO `main`,
        # so `main`'s commit is the first parent (git semantics).
        target_head = self.kernel.resolve(
            self._branch_ref(collection, target_branch))
        if target_head is None:
            # Target branch doesn't exist yet — fall back to active branch's
            # commit (backward compat for the no-target-branch case).
            target_head = self.kernel.resolve(self._active_commit_ref(collection))

        # Read both manifests
        head_commit = self._read_commit_blob(target_head) if target_head else None
        branch_commit = self._read_commit_blob(branch_head)

        head_manifest = None
        if head_commit and head_commit.get("manifest"):
            head_manifest = CollectionManifest.load(
                self.kernel, head_commit["manifest"])
        branch_manifest = None
        if branch_commit and branch_commit.get("manifest"):
            branch_manifest = CollectionManifest.load(
                self.kernel, branch_commit["manifest"])

        # Union row group entries from target HEAD + source branch HEAD
        seen: dict[str, RowGroupEntry] = {}
        if head_manifest:
            for rg in head_manifest.scan_with_pruning():
                seen[rg.key] = rg
        if branch_manifest:
            for rg in branch_manifest.scan_with_pruning():
                seen[rg.key] = rg  # branch wins

        # Also merge shards from BOTH branches (target branch
        # and the source branch being merged)
        # schema/key_col from the manifests enable inline-shard pseudo-manifests
        merge_schema = (head_manifest or branch_manifest).columns if \
            (head_manifest or branch_manifest) else None
        merge_key_col = (head_manifest or branch_manifest).key_col if \
            (head_manifest or branch_manifest) else ""
        for shard_manifest in self._parallel_fetch_shard_manifests(
                self._read_shard_index(collection, target_branch),
                schema_columns=merge_schema, key_col=merge_key_col):
            for rg in shard_manifest.scan_with_pruning():
                seen[rg.key] = rg
        for shard_manifest in self._parallel_fetch_shard_manifests(
                self._read_shard_index(collection, branch_name),
                schema_columns=merge_schema, key_col=merge_key_col):
            for rg in shard_manifest.scan_with_pruning():
                seen[rg.key] = rg

        # Build merged manifest
        merged_entries = []
        for rg in sorted(seen.values(), key=lambda r: r.key):
            merged_entries.append({
                "rg_key": rg.key,
                "blob_hash": rg.blob_hash,
                "n_rows": rg.n_rows,
                "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                for c in rg.columns],
            })

        schema = (head_manifest or branch_manifest).columns if \
            (head_manifest or branch_manifest) else []
        key_col = (head_manifest or branch_manifest).key_col if \
            (head_manifest or branch_manifest) else ""

        manifest_hash = self._build_manifest(
            collection, merged_entries, schema, key_col,
            row_group_size=10_000)

        # Write merge commit with TWO parents. We bypass _write_commit_blob
        # because it auto-updates the active branch ref — we need to
        # explicitly update target_branch (and HEAD) here.
        import json as _json
        import time as _time
        commit = {
            "parent": target_head,
            "second_parent": branch_head,
            "manifest": manifest_hash,
            "message": message or f"merge '{branch_name}'",
            "timestamp": _time.time(),
            "index": self._commit_index(collection),
        }
        commit_bytes = _json.dumps(commit, sort_keys=True).encode()
        commit_hash = self.kernel.write(commit_bytes)

        # Update target_branch ref → merge commit (the merge lands on target)
        self.kernel.reference(self._branch_ref(collection, target_branch),
                               commit_hash)
        # No separate HEAD update — the active branch ref is the same as
        # the target_branch ref when target == active. If target != active,
        # we intentionally do NOT move the active branch (the merge landed
        # on a different branch).
        # Update the TARGET branch's manifest ref (per-branch manifests).
        # NB: use _manifest_ref_for_branch (specific branch), NOT
        # _manifest_ref (active branch) — if target != active, writing to
        # the active branch's manifest ref would corrupt the wrong branch.
        self.kernel.reference(
            self._manifest_ref_for_branch(collection, target_branch),
            manifest_hash)
        # Source branch ref is left unchanged (it still points at its own
        # tip — the merge does not fast-forward the source).

        # Clear source + target branch shards via index reset + ref tombstone.
        # Tombstoning (overwriting with empty blob) is REQUIRED so that
        # _list_shards_from_refs — which scans refs as the source of truth
        # to catch concurrent writers — does NOT pick up the absorbed shards.
        # Old shard refs that still point to valid manifests would otherwise
        # be reported as "live" by shard_count and re-merged on next read.
        self._clear_branch_shards(collection, branch_name)
        self._clear_branch_shards(collection, target_branch)

        self._active_branches.pop(collection, None)  # merge detaches from branch
        self._invalidate_manifest_cache(collection)
        return commit_hash

    def _tombstone_shard_refs(self, collection: str, branch: str,
                               shard_hashes: list[str]) -> None:
        """Tombstone shard refs by deleting the path entries.

        This makes resolve() return None for the shard ref, so
        _list_shards_from_refs skips it. The shard BLOB still exists
        in storage (will be cleaned by GC/vacuum later).

        Used by compact_shards and merge to retire absorbed shards.
        """
        if not shard_hashes:
            return
        prefix = self._shards_prefix(collection, branch)
        # List ref-names by their current hash for O(1) lookup
        if hasattr(self.kernel, 'list_paths_with_prefix'):
            candidates = self.kernel.list_paths_with_prefix(prefix)
        else:
            candidates = [n for n in self.kernel.list_names() if n.startswith(prefix)]
        hash_to_name: dict[str, str] = {}
        for name in candidates:
            h = self.kernel.resolve(name)
            if h is not None:
                hash_to_name[h] = name
        # Delete the path entries so resolve() returns None
        for sh in shard_hashes:
            name = hash_to_name.get(sh)
            if name is not None:
                # Overwrite with a sentinel that resolve() will treat as "deleted"
                # We can't actually DELETE paths (no delete_path in the kernel API),
                # so we set it to a known empty-sentinel hash. _list_shards_from_refs
                # will still include it, but read_with_shards' try/except will skip
                # the empty blob when loading the manifest.
                # Better: set the path to None by removing it from the cache and
                # writing a special "deleted" marker.
                # For now: overwrite with empty blob hash. read_with_shards skips
                # corrupt manifests via try/except.
                empty_hash = self.kernel.write(b"")
                self.kernel.reference(name, empty_hash)
                # Invalidate cache so resolve() picks up the new value
                self.kernel.invalidate_path_cache(name)

    def _clear_branch_shards(self, collection: str, branch: str,
                               shard_hashes: Optional[list[str]] = None) -> None:
        """Clear a branch's shards: tombstone refs so listing skips them.

        Unified retire path used by compact_shards and merge. After this:
          - Old shard refs are tombstoned (overwritten with the empty-blob
            hash so _list_shards_from_refs skips them).
        Readers using _list_shards_from_refs see zero live shards because
        tombstoned refs fail CollectionManifest.load and are skipped.

        Args:
            collection: collection name
            branch: branch name
            shard_hashes: if provided, skip the _read_shard_index call and
                use these hashes directly. This avoids re-reading shard
                manifests during compaction (the caller already has them).
        """
        if shard_hashes is None:
            shard_hashes = self._read_shard_index(collection, branch)
        self._tombstone_shard_refs(collection, branch, shard_hashes)

    def undo(self, collection: str, steps: int = 1) -> str:
        """Undo the last N commits — walk parent pointers.

        Clears the active branch (undo is a detached-HEAD operation).
        """
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            raise ValueError("No commits to undo")
        for _ in range(steps):
            commit = self._read_commit_blob(head)
            if commit is None or not commit.get("parent"):
                break
            head = commit["parent"]
        self.kernel.reference(self._active_commit_ref(collection), head)
        self._active_branches.pop(collection, None)  # detach from branch
        # Rebind the active branch's manifest ref to the new HEAD's manifest
        # (undo rewinds the commit ref; the manifest ref must follow).
        self._sync_branch_manifest_to_head(collection)
        return head[:12] if head else ""

    def revert(self, collection: str, commit_hash: str) -> str:
        """Revert HEAD to a specific commit — like `git revert` / `git reset`.

        Points HEAD at the given commit_hash, regardless of how many
        steps back it is. Unlike undo (which walks N steps), revert
        takes an explicit commit hash.

        The commit must be in the collection's history (verified by
        walking the chain). This prevents reverting to an unrelated
        commit from a different collection.

        Args:
            collection: collection name
            commit_hash: the commit hash to revert to

        Returns:
            The commit hash that HEAD now points to.

        Raises:
            ValueError: if the commit is not in the collection's history.
        """
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            raise ValueError(f"Collection '{collection}' has no commits")

        # Verify the commit is in our history (safety check)
        current = head
        found = False
        seen = set()
        while current and current not in seen:
            seen.add(current)
            if current == commit_hash:
                found = True
                break
            commit = self._read_commit_blob(current)
            if commit is None:
                break
            current = commit.get("parent")

        if not found:
            raise ValueError(
                f"Commit {commit_hash[:12]} is not in the history of "
                f"collection '{collection}'")

        # Revert the active branch's commit to the specified commit
        self.kernel.reference(self._active_commit_ref(collection), commit_hash)
        self._active_branches.pop(collection, None)  # detach from branch
        # Rebind the active branch's manifest ref to the reverted commit's manifest
        self._sync_branch_manifest_to_head(collection)
        return commit_hash[:12]

    def history(self, collection: str, limit: int = 100) -> list[dict]:
        """Walk the commit chain from the active branch's commit backwards."""
        head = self.kernel.resolve(self._active_commit_ref(collection))
        if head is None:
            return []

        history: list[dict] = []
        current: Optional[str] = head
        seen: set[str] = set()

        while current and current not in seen and len(history) < limit:
            seen.add(current)
            commit = self._read_commit_blob(current)
            if commit is None:
                history.append({
                    "hash": current,
                    "message": "(undecodable commit)",
                    "parent": None, "second_parent": None,
                    "timestamp": None, "type": "unknown",
                })
                break

            entry_type = "merge" if commit.get("second_parent") else "commit"
            history.append({
                "hash": current,
                "message": commit.get("message", ""),
                "parent": commit.get("parent"),
                "second_parent": commit.get("second_parent"),
                "timestamp": commit.get("timestamp"),
                "manifest": commit.get("manifest"),
                "index": commit.get("index"),
                "type": entry_type,
            })
            current = commit.get("parent")

        return history

    def diff(self, collection: str, commit_a: str, commit_b: str) -> dict:
        """Compute the diff between two commits' manifests."""
        ca = self._read_commit_blob(commit_a) or {}
        cb = self._read_commit_blob(commit_b) or {}
        ma = ca.get("manifest")
        mb = cb.get("manifest")
        if not ma or not mb:
            return {"added": [], "removed": [], "modified": []}

        manifest_a = CollectionManifest.load(self.kernel, ma)
        manifest_b = CollectionManifest.load(self.kernel, mb)

        entries_a = {rg.key: rg for rg in manifest_a.scan_with_pruning()}
        entries_b = {rg.key: rg for rg in manifest_b.scan_with_pruning()}

        added = sorted(entries_b.keys() - entries_a.keys())
        removed = sorted(entries_a.keys() - entries_b.keys())
        modified = sorted(
            k for k in entries_a.keys() & entries_b.keys()
            if entries_a[k].blob_hash != entries_b[k].blob_hash)

        return {"added": added, "removed": removed, "modified": modified}

    # ------------------------------------------------------------------
    # SHARD-BASED CONCURRENCY (CRDT-like, no CAS)
    #
    # The beautiful concurrency model: each writer writes its own shard.
    # No coordination, no retry, no CAS. Readers merge all shards.
    #
    # Why this works:
    #   - Appends are COMMUTATIVE (adding RG1 then RG2 == RG2 then RG1)
    #   - The manifest is a G-Set (Grow-Only Set) of row group entries
    #   - Merge = set union (commutative, associative, idempotent)
    #   - Each shard is an independent immutable blob
    #
    # Architecture:
    #   collections/{name}/branches/{branch}/commit    → commit blob hash
    #   collections/{name}/branches/{branch}/manifest  → manifest blob hash
    #   collections/{name}/branches/{branch}/shards/{uuid} → shard manifest (per writer batch)
    #
    # Write path (append_shard):
    #   1. Writer generates a UUIDv7 (time-ordered, unique)
    #   2. Encodes row groups as PND2 blobs (concurrent-safe — immutable)
    #   3. Writes a shard manifest blob (just the new row groups)
    #   4. Writes collections/{name}/shards/{uuid} → shard_manifest_hash
    #   Done. No CAS, no retry, no coordination.
    #
    # Read path (read_with_shards):
    #   1. Read HEAD manifest (1 GET — the compacted base)
    #   2. List collections/{name}/shards/ (1 LIST)
    #   3. Read all shard manifests (N GETs — parallel, ~1 RTT)
    #   4. Merge: union of all row group entries (CRDT merge)
    #   5. Read surviving data blobs (K GETs — parallel)
    #
    # Compaction (compact_shards):
    #   1. Read HEAD + all shards
    #   2. Merge into one flat manifest
    #   3. Write new compacted HEAD (last-writer-wins OK — rare, idempotent)
    #   4. Clear old shards (delete the shard refs)
    #   Compaction is the ONLY place that needs coordination, and it's
    #   idempotent — multiple compactors produce the same result.
    # ------------------------------------------------------------------

    @staticmethod
    def _shards_prefix(collection: str, branch: str = "main") -> str:
        """Shards live UNDER branches — each branch has its own shard set.

        This enables concurrent work on different branches:
          - 2 writers on feature1 → shards under branches/feature1/shards/
          - 3 writers on main     → shards under branches/main/shards/
          - A writer can switch branches mid-work — next shard goes to
            the new branch automatically (just like git checkout).
        """
        return f"collections/{collection}/branches/{branch}/shards/"

    def _get_active_branch(self, collection: str) -> str:
        """Get the active branch for a collection (default: main)."""
        active = self._active_branches.get(collection)
        if active:
            # active is stored as the full ref path — extract branch name.
            # _branch_ref returns collections/{c}/branches/{branch}/commit,
            # so we strip the prefix and the trailing /commit.
            prefix = f"collections/{collection}/branches/"
            if active.startswith(prefix):
                rest = active[len(prefix):]  # {branch}/commit
                # Strip the trailing /commit (rsplit handles branch names
                # that contain 'commit' as a substring — only the suffix matches).
                return rest.rsplit("/commit", 1)[0]
        return "main"

    def _read_shard_index(self, collection: str, branch: Optional[str] = None) -> list[str]:
        """Read the shard index → list of shard manifest hashes.

        Now a thin delegator to _list_shards_from_refs. Kept for backward
        compat with callers (read_with_shards, compact_shards, etc.).
        Shard discovery is ref-listing only — there is no longer a separate
        index blob to consult.
        """
        if branch is None:
            branch = self._get_active_branch(collection)
        return self._list_shards_from_refs(collection, branch)

    def _list_shards_from_refs(self, collection: str, branch: str) -> list[str]:
        """List shard hashes by scanning refs (source of truth).

        Returns ONLY committed shards (normal shards without tx_id, or
        tentative shards whose transaction has been committed).
        Tentative shards from uncommitted transactions are filtered out.

        Does NOT load each shard manifest to verify it — that's redundant
        because read_with_shards loads them anyway (with try/except for
        corrupt/missing/tombstoned). This saves K GETs per read.
        The tx commit check is KEPT — it's what makes ACID work.
        """
        prefix = self._shards_prefix(collection, branch)
        # Compute the empty-blob hash once (used to detect tombstoned shards)
        import hashlib as _hashlib
        _empty_blob_hash = _hashlib.sha256(b"").hexdigest()
        # Use list_paths_with_prefix for O(matching) listing (not O(total))
        if hasattr(self.kernel, 'list_paths_with_prefix'):
            names = self.kernel.list_paths_with_prefix(prefix)
        else:
            names = [n for n in self.kernel.list_names() if n.startswith(prefix)]
        shard_hashes = []
        committed_tx_cache: set[str] = set()
        checked_tx_cache: set[str] = set()

        for name in names:
            h = self.kernel.resolve(name)
            if h is None:
                continue
            # Skip tombstoned shards (overwritten with empty blob by
            # _tombstone_shard_refs). The empty blob has a known hash —
            # we compute it once and cache it for this call.
            if h == _empty_blob_hash:
                continue

            # Check if this is a tentative shard (has tx_ prefix)
            shard_name = name[len(prefix):]
            if shard_name.startswith("tx_"):
                # Tentative shard — check if transaction is committed
                parts = shard_name.split("_", 2)
                if len(parts) < 3:
                    continue
                tx_id = parts[1]

                # Check commit marker (cached) — KEPT for ACID correctness
                if tx_id not in checked_tx_cache:
                    checked_tx_cache.add(tx_id)
                    tx_ref = f"transactions/{tx_id}"
                    tx_hash = self.kernel.resolve(tx_ref)
                    if tx_hash is not None:
                        committed_tx_cache.add(tx_id)

                # Only include if transaction is committed
                if tx_id in committed_tx_cache:
                    shard_hashes.append(h)
            else:
                # Normal shard (no tx_id) — always visible
                shard_hashes.append(h)
        return shard_hashes

    def _build_pseudo_manifest_from_pnd2(self, blob_hash: str,
                                           blob_bytes: bytes,
                                           schema_columns=None,
                                           key_col: str = "") -> Optional[CollectionManifest]:
        """Build a pseudo CollectionManifest from a direct PND2 data blob.

        Used for INLINE SHARDS: when append_shard() writes a single row
        group, it skips the PMAN shard manifest and points the shard ref
        directly at the PND2 blob. The reader calls this method to build
        a manifest-like object with one RowGroupEntry, populated from
        the PND2 blob's header (schema + stats — NO payload decode).

        Args:
            blob_hash: the PND2 blob's content hash
            blob_bytes: the PND2 blob bytes
            schema_columns: optional schema from the HEAD manifest (used
                for the pseudo-manifest's schema if provided; otherwise
                the PND2 blob's own schema is used)
            key_col: the sort key column name (from HEAD manifest). Used
                to derive the rg_key from the key column's max stat.

        Returns:
            A CollectionManifest with one RowGroupEntry, or None if the
            blob is not a valid PND2 blob.
        """
        header = PND2.peek_header(blob_bytes)
        if header is None:
            return None
        n_rows, pnd2_schema, stats = header
        # pnd2_schema: list[(name, value_type, encoding)]
        # stats: dict[name, (min, max, null_count)]

        # Derive the rg_key from the key column's max stat
        rg_key = "rg/"
        if key_col:
            key_stats = stats.get(key_col)
            if key_stats is not None and key_stats[1] is not None:
                try:
                    rg_key = _format_rg_key(key_stats[1])
                except Exception:
                    rg_key = "rg/"
        elif n_rows > 0:
            # No key_col — use row index as the max pk (matches append_shard's
            # behavior when key_col is None: key_array = list(range(n_rows)))
            rg_key = _format_rg_key(n_rows - 1)

        # Build the RowGroupEntry with per-column stats from the PND2 header
        rg = RowGroupEntry(
            key=rg_key,
            blob_hash=blob_hash,
            n_rows=n_rows,
            storage_mode=STORAGE_WHOLE_BLOB,
        )
        for name, vtype, _enc in pnd2_schema:
            mn, mx, null_count = stats.get(name, (None, None, 0))
            rg.columns.append(ColumnStatsEntry(
                name=name, value_type=vtype, min=mn, max=mx,
                null_count=null_count, chunks=[],
            ))

        # Build the pseudo-manifest. Use the PND2 blob's actual schema
        # (it reflects what's really in the blob — including CRDT columns
        # like _rowid, _version, _deleted that the HEAD manifest's schema
        # might not have if the collection was created via write()).
        final_schema = schema_columns
        if not final_schema:
            final_schema = [(name, vtype) for name, vtype, _ in pnd2_schema]
        else:
            # Merge: ensure CRDT columns from the PND2 schema are present
            # in the pseudo-manifest's schema (so _manifests_have_rowid
            # detects them and triggers row-level compaction when needed).
            existing = {name for name, _ in final_schema}
            for name, vtype, _ in pnd2_schema:
                if name not in existing:
                    final_schema.append((name, vtype))

        manifest = CollectionManifest(self.kernel)
        manifest.set_schema(
            columns=final_schema,
            key_col=key_col or "",
            row_group_size=max(n_rows, 1),
            chunk_size=0,
        )
        manifest.add_row_group(rg)
        return manifest

    def _load_shard_manifest(self, shard_hash: str,
                              schema_columns=None,
                              key_col: str = "") -> Optional[CollectionManifest]:
        """Load a shard as a CollectionManifest, handling BOTH formats.

        Shard refs can point to one of two blob types:
          - b'PMAN' magic: a multi-row-group shard manifest (the classic
            format). Loaded via CollectionManifest.decode().
          - b'PND2' magic: an INLINE shard — a direct data blob for a
            single row group (the new optimized format). Built into a
            pseudo-manifest via _build_pseudo_manifest_from_pnd2().
          - empty/missing: a tombstoned shard (skip).

        Args:
            shard_hash: the blob hash the shard ref points to
            schema_columns: optional HEAD schema (for pseudo-manifest)
            key_col: optional HEAD key_col (for pseudo-manifest rg_key)

        Returns:
            A CollectionManifest, or None for tombstoned/corrupt shards.
        """
        if not shard_hash:
            return None
        try:
            blob_bytes = self.kernel.read_blob(shard_hash)
        except (ValueError, KeyError):
            return None
        if not blob_bytes:
            return None  # tombstoned (empty blob)

        magic = blob_bytes[:4]
        if magic == b'PND2':
            # Inline shard — build pseudo-manifest from PND2 header
            return self._build_pseudo_manifest_from_pnd2(
                shard_hash, blob_bytes, schema_columns, key_col)
        elif magic == b'PMAN':
            # Manifest shard — decode as CollectionManifest
            try:
                return CollectionManifest.decode(self.kernel, blob_bytes)
            except (ValueError, KeyError):
                return None
        else:
            return None  # unknown format

    def _parallel_fetch_shard_manifests(self, shard_hashes: list[str],
                                          schema_columns=None,
                                          key_col: str = "") -> list:
        """Fetch all shard manifests in parallel (~1 RTT wall-clock).

        Without this, fetching N shard manifests takes N × RTT sequentially.
        With this, N manifests are fetched concurrently — wall-clock is
        ~1 RTT regardless of N (bounded by thread pool size).

        Handles BOTH inline (PND2) and manifest (PMAN) shards via
        _load_shard_manifest(). The schema_columns and key_col args are
        used to build pseudo-manifests for inline shards.

        Args:
            shard_hashes: list of shard blob hashes
            schema_columns: optional HEAD schema (for inline shards)
            key_col: optional HEAD key_col (for inline shards)
        """
        if not shard_hashes:
            return []
        if len(shard_hashes) <= 2:
            # Sequential for small N (thread pool overhead > benefit)
            results = []
            for sh in shard_hashes:
                m = self._load_shard_manifest(
                    sh, schema_columns, key_col)
                if m is not None:
                    results.append(m)
            return results

        # Parallel for N > 2
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def fetch_one(sh):
            return self._load_shard_manifest(
                sh, schema_columns, key_col)

        results = []
        with ThreadPoolExecutor(max_workers=min(32, len(shard_hashes))) as pool:
            futures = [pool.submit(fetch_one, sh) for sh in shard_hashes]
            for f in as_completed(futures):
                m = f.result()
                if m is not None:
                    results.append(m)
        return results

    def append_shard(self, collection: str, rows,
                      key_col: Optional[str] = None,
                      row_group_size: int = 10_000,
                      encoding_hints: Optional[dict[str, str]] = None,
                      message: str = "",
                      tx_id: Optional[str] = None) -> str:
        """Concurrent-safe append — NO CAS, NO retry, NO coordination.

        This is the beautiful concurrency model. Each writer writes its
        own shard to a unique path. Readers merge all shards.

        Flow:
          1. Generate UUIDv7 (time-ordered, unique per writer)
          2. Encode row groups as PND2 blobs (immutable, concurrent-safe)
          3. Write a shard manifest blob (just the new row groups)
          4. Write collections/{name}/shards/{uuid} → shard_manifest_hash

        That's it. No CAS, no retry, no reading the current HEAD, no
        coordination with other writers. The shard is immediately
        visible to readers (they list shards on every read).

        Works on ANY storage that supports listing (local FS, S3, GCS).
        No conditional PUTs needed.

        Args:
            collection: collection name (must exist — call write() first)
            rows: new rows to append
            key_col: sort key column (should match existing)
            row_group_size: rows per new row group
            encoding_hints: optional encoding hints
            message: commit message (stored in shard metadata)

        Returns:
            The shard manifest hash.
        """
        # Coerce input
        if isinstance(rows, list):
            source = ListColumnSource(rows)
        elif isinstance(rows, ColumnSource):
            source = rows
        else:
            source = as_column_source(rows)
        n_rows = source.num_rows()

        if n_rows == 0:
            return ""  # nothing to append

        # Use cached schema if available (warm shard append = 0 GETs for schema)
        cached_schema = self._schema_cache.get(collection)
        if cached_schema:
            schema_columns, existing_key_col, existing_rg_size = cached_schema
            if key_col == "":
                key_col = None
            if key_col is None and existing_key_col:
                key_col = existing_key_col
        else:
            # Cold: read existing manifest for schema
            existing_manifest = self._load_manifest(collection, skip_cache=True)
            if existing_manifest is None:
                return self.write(collection, rows, key_col=key_col,
                                    row_group_size=row_group_size,
                                    encoding_hints=encoding_hints,
                                    message=message)
            schema_columns = existing_manifest.columns
            if key_col == "":
                key_col = None
            if key_col is None and existing_manifest.key_col:
                key_col = existing_manifest.key_col
            # Cache the schema for future warm appends
            self._schema_cache[collection] = (
                schema_columns,
                key_col or "",
                existing_manifest.row_group_size,
            )

        if key_col is not None and key_col in source.column_names():
            source = _sort_source_by(source, key_col)
            key_array = source.column_slice(key_col, 0, n_rows)
        elif key_col is not None:
            raise KeyError(f"key column '{key_col}' not in source columns")
        else:
            key_array = list(range(n_rows))

        if not schema_columns and n_rows > 0:
            for col_name in source.column_names():
                sample = source.column_slice(col_name, 0, min(100, n_rows))
                vtype = _detect_value_type_with_binary(sample)
                schema_columns.append((col_name, vtype))

        # If the source has CRDT columns (_rowid, _version, _deleted) that
        # aren't in the existing schema, add them. This ensures the shard
        # manifest's column stats include these columns, which is needed
        # for _manifests_have_rowid() to detect row-level CRDT shards and
        # trigger row-level compaction (instead of manifest-level).
        if n_rows > 0:
            source_cols = set(source.column_names())
            schema_cols = {name for name, _ in schema_columns}
            crdt_cols = {"_rowid", "_version", "_deleted"}
            for col_name in source_cols & crdt_cols - schema_cols:
                sample = source.column_slice(col_name, 0, min(100, n_rows))
                vtype = _detect_value_type_with_binary(sample)
                schema_columns.append((col_name, vtype))

        # Encode new row groups (concurrent-safe — immutable blobs)
        manifest_entries: list[dict] = []
        for start in range(0, n_rows, row_group_size):
            end = min(start + row_group_size, n_rows)
            group_source = _slice_source(source, start, end)
            max_pk = key_array[end - 1]
            rg_key = _format_rg_key(max_pk)
            pnd2_bytes, col_stats = PND2.encode(
                group_source, encoding_hints=encoding_hints)
            blob_hash = self.kernel.write(pnd2_bytes)
            manifest_entries.append({
                "rg_key": rg_key,
                "blob_hash": blob_hash,
                "n_rows": end - start,
                "col_stats": col_stats,
            })

        manifest_entries.sort(key=lambda e: e["rg_key"])

        # INLINE SHARD OPTIMIZATION (single row group):
        # The PND2 blob is self-describing (header has schema + stats), so
        # for N==1 we skip the PMAN shard manifest entirely and point the
        # shard ref DIRECTLY at the PND2 data blob. This saves 1 PUT on
        # the write path and 1 GET on the read path (no separate manifest
        # load — the data blob IS the manifest).
        #
        # The reader detects this by checking the blob's magic bytes:
        #   b'PND2' → inline shard → build pseudo RowGroupEntry from header
        #   b'PMAN' → manifest shard → load via CollectionManifest.decode
        #
        # For N>1, we still build a PMAN shard manifest (needed to group
        # multiple row group entries into one discoverable unit).
        if len(manifest_entries) == 1:
            # Inline shard: ref points directly at the PND2 data blob
            shard_hash = manifest_entries[0]["blob_hash"]
        else:
            # Multi-row-group shard: build a PMAN manifest as before
            shard_manifest = CollectionManifest(self.kernel)
            shard_manifest.set_schema(
                columns=schema_columns,
                key_col=key_col or "",
                row_group_size=row_group_size,
                chunk_size=0,
            )
            for entry in manifest_entries:
                rg = RowGroupEntry(
                    key=entry["rg_key"],
                    blob_hash=entry["blob_hash"],
                    n_rows=entry["n_rows"],
                    storage_mode=STORAGE_WHOLE_BLOB,
                )
                for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                    rg.columns.append(ColumnStatsEntry(
                        name=col_name, value_type=vtype, min=mn, max=mx,
                        null_count=null_count, chunks=[],
                    ))
                shard_manifest.add_row_group(rg)

            # Write the shard manifest blob (does NOT update any ref)
            shard_hash = shard_manifest.commit()

        # Write the shard ref to a unique path (UUIDv7 — time-ordered, unique)
        # This is the KEY: each writer writes to its own path. No conflict.
        try:
            from uuid7 import uuidv7
            shard_id = uuidv7()
        except ImportError:
            import time as _t
            shard_id = f"{_t.time_ns()}_{id(rows)}"

        # Write the shard ref to the ACTIVE BRANCH's shard path
        # If tx_id is set, the shard is TENTATIVE (not visible until
        # the transaction commit marker is written). The tx_id is
        # encoded in the ref path so readers can check it.
        branch = self._get_active_branch(collection)
        if tx_id:
            # Tentative shard: ref path includes tx_id
            shard_ref = f"{self._shards_prefix(collection, branch)}tx_{tx_id}_{shard_id}"
        else:
            # Normal shard: immediately visible
            shard_ref = f"{self._shards_prefix(collection, branch)}{shard_id}"
        self.kernel.reference(shard_ref, shard_hash)

        # The shard ref is the discovery mechanism — _list_shards_from_refs
        # scans refs (with the tx commit-marker check for tentative shards),
        # so there is no separate index to update here.

        # Invalidate manifest cache (so next read picks up the new shard)
        # but PRESERVE the schema cache (schema doesn't change on append)
        self._manifest_cache.pop(collection, None)
        self._manifest_hash_cache.pop(collection, None)
        self._head_cache.pop(collection, None)
        # Keep _schema_cache — it's valid across appends

        return shard_hash

    def list_shards(self, collection: str, branch: Optional[str] = None) -> list[str]:
        """List all shard manifest hashes for a collection's branch.

        Delegates to _read_shard_index, which scans the live shard refs
        (tombstoned refs from compaction are skipped by
        _list_shards_from_refs).
        """
        return self._read_shard_index(collection, branch)

    def read_with_shards(self, collection: str,
                          predicates: Optional[list[tuple[str, str, Any]]] = None,
                          columns: Optional[list[str]] = None,
                          row_filter: Optional[Callable[[dict], bool]] = None,
                          start_key: Optional[str] = None,
                          end_key: Optional[str] = None) -> list[dict]:
        """Read rows from a collection, merging HEAD + all shards.

        TWO-LEVEL MERGE:
          1. Row-group level: dedup by rg_key (shards override HEAD)
          2. Row level: dedup by _rowid, keeping latest _version
             (tombstones suppress rows if their _version is latest)

        If rows have _rowid/_version columns (from upsert_shard/delete_shard),
        the row-level merge handles concurrent updates and deletes correctly.
        If rows don't have _rowid (plain append_shard), all rows are kept
        (insert-only semantics — no conflicts possible).

        Flow:
          1. Read HEAD manifest (1 GET — the compacted base)
          2. List shards (1 LIST)
          3. Read all shard manifests (N GETs — parallel)
          4. Merge row groups: union of entries (dedup by rg_key)
          5. Fetch + decode surviving data blobs (K GETs — parallel)
          6. Merge rows: dedup by _rowid, latest _version wins
        """
        # Read HEAD manifest (the compacted base)
        head_manifest = self._load_manifest(collection)
        # Read all shard hashes via the shard index (1 GET) instead of
        # listing N refs (N+1 GETs). Falls back to listing if no index.
        shard_hashes = self._read_shard_index(collection)

        # Parallel-fetch all shard manifests (~1 RTT wall-clock, not N×RTT)
        # Pass HEAD schema + key_col so inline PND2 shards can be built into
        # pseudo-manifests with the correct rg_key.
        shard_manifests = self._parallel_fetch_shard_manifests(
            shard_hashes,
            schema_columns=(head_manifest.columns if head_manifest else None),
            key_col=(head_manifest.key_col if head_manifest else ""))

        # Level 1 merge: UNION of all row groups (no dedup by rg_key).
        # Row-level CRDT merge (_rowid + _version) handles conflicts.
        # This fixes the bug where concurrent writers with overlapping
        # key ranges would drop data via dedup-by-key.
        merged: list[Any] = []
        if head_manifest:
            merged.extend(head_manifest.scan_with_pruning(predicates, start_key, end_key))
        for sm in shard_manifests:
            merged.extend(sm.scan_with_pruning(predicates, start_key, end_key))

        if not merged:
            return []

        # Fetch + decode data blobs (parallel for large K)
        row_groups = merged
        col_data_list = self._parallel_fetch_and_decode(
            row_groups, columns, predicates)

        # Combine into rows
        all_rows: list[dict] = []
        for col_data in col_data_list:
            if not col_data:
                continue
            # Get the row count from the first column (all should be same length)
            n = len(next(iter(col_data.values())))
            for i in range(n):
                row = {}
                for c, vals in col_data.items():
                    if i < len(vals):
                        row[c] = vals[i]
                    else:
                        row[c] = None
                all_rows.append(row)

        # Level 2 merge: dedup by _rowid, latest _version wins (CRDT)
        # Only applies if rows have _rowid (from upsert_shard/delete_shard)
        has_rowid = any(r.get("_rowid") for r in all_rows)
        if has_rowid:
            key_col = head_manifest.key_col if head_manifest else ""
            all_rows = self._merge_rows_by_rowid(all_rows, key_col=key_col or None)

        # Apply row filter
        if row_filter is not None:
            all_rows = [r for r in all_rows if row_filter(r)]

        return all_rows

    def read_branch_with_shards(self, collection: str, branch: str,
                                  predicates: Optional[list[tuple[str, str, Any]]] = None,
                                  columns: Optional[list[str]] = None) -> list[dict]:
        """Read a branch's full data: branch HEAD commit's manifest + branch's shards.

        This is the branch-aware version of read_with_shards. It does NOT
        mutate HEAD or the active branch — it resolves the branch's commit
        directly and reads its manifest, then merges in the branch's shards.

        Used by LakehouseLens.read_branch() to read a branch's full state
        without checking out (which would race with concurrent readers).

        Args:
            collection: collection name
            branch: branch name to read
            predicates: optional predicate pushdown
            columns: optional projection pushdown

        Returns:
            List of row dicts (HEAD + shards merged, row-level CRDT applied).
        """
        # Resolve the branch's commit
        branch_commit_hash = self.kernel.resolve(
            self._branch_ref(collection, branch))
        if branch_commit_hash is None:
            raise KeyError(f"Branch '{branch}' not found for collection '{collection}'")

        # Read the branch's commit blob → manifest hash
        branch_commit = self._read_commit_blob(branch_commit_hash)
        if branch_commit is None or not branch_commit.get("manifest"):
            return []
        branch_manifest_hash = branch_commit["manifest"]

        # Load the branch's manifest
        try:
            head_manifest = CollectionManifest.load(self.kernel, branch_manifest_hash)
        except (ValueError, KeyError):
            head_manifest = None

        # Read the branch's shards
        shard_hashes = self._read_shard_index(collection, branch)
        shard_manifests = self._parallel_fetch_shard_manifests(
            shard_hashes,
            schema_columns=(head_manifest.columns if head_manifest else None),
            key_col=(head_manifest.key_col if head_manifest else ""))

        # Level 1 merge: UNION of all row groups (no dedup by rg_key).
        merged: list[Any] = []
        if head_manifest:
            merged.extend(head_manifest.scan_with_pruning(predicates))
        for sm in shard_manifests:
            merged.extend(sm.scan_with_pruning(predicates))

        if not merged:
            return []

        # Fetch + decode
        row_groups = merged
        col_data_list = self._parallel_fetch_and_decode(
            row_groups, columns, predicates)

        all_rows: list[dict] = []
        for col_data in col_data_list:
            if not col_data:
                continue
            n = len(next(iter(col_data.values())))
            for i in range(n):
                row = {}
                for c, vals in col_data.items():
                    if i < len(vals):
                        row[c] = vals[i]
                    else:
                        row[c] = None
                all_rows.append(row)

        # Row-level CRDT merge
        has_rowid = any(r.get("_rowid") for r in all_rows)
        if has_rowid:
            key_col = head_manifest.key_col if head_manifest else ""
            all_rows = self._merge_rows_by_rowid(all_rows, key_col=key_col or None)

        return all_rows

    def _read_as_columns_with_shards(self, collection: str,
                                       predicates: Optional[list[tuple[str, str, Any]]] = None,
                                       columns: Optional[list[str]] = None,
                                       shard_hashes: Optional[list[str]] = None
                                       ) -> dict[str, list]:
        """Columnar read merging HEAD + unmerged shards.

        Same merge semantics as read_with_shards, but returns columnar
        data (dict[col_name, list[values]]) instead of row dicts. Used
        by read_as_columns / read_as_arrow so they include unmerged shards.
        """
        if shard_hashes is None:
            shard_hashes = self._read_shard_index(collection)

        head_manifest = self._load_manifest(collection)
        shard_manifests = self._parallel_fetch_shard_manifests(
            shard_hashes,
            schema_columns=(head_manifest.columns if head_manifest else None),
            key_col=(head_manifest.key_col if head_manifest else ""))

        # Level 1 merge: UNION of all row groups (no dedup by rg_key).
        merged: list[Any] = []
        if head_manifest:
            merged.extend(head_manifest.scan_with_pruning(predicates))
        for sm in shard_manifests:
            merged.extend(sm.scan_with_pruning(predicates))

        if not merged:
            return {}

        # Ensure predicate columns are decoded
        eff_columns = list(columns) if columns is not None else None
        if predicates and eff_columns is not None:
            pred_cols = {p[0] for p in predicates}
            missing = pred_cols - set(eff_columns)
            if missing:
                eff_columns = list(dict.fromkeys(eff_columns + list(missing)))

        row_groups = list(merged)
        col_results = self._parallel_fetch_and_decode(
            row_groups, eff_columns, predicates)

        auto_filter = self._build_predicate_filter(predicates)
        result: dict[str, list] = {}
        for col_data in col_results:
            if auto_filter is None:
                for col_name, values in col_data.items():
                    if columns is not None and col_name not in columns:
                        continue
                    if col_name not in result:
                        result[col_name] = list(values)
                    else:
                        result[col_name].extend(values)
            else:
                # Apply filter row by row
                n = max((len(v) for v in col_data.values()), default=0)
                for i in range(n):
                    row = {c: col_data[c][i] if i < len(col_data[c]) else None
                            for c in col_data}
                    if auto_filter(row):
                        for col_name, val in row.items():
                            if columns is not None and col_name not in columns:
                                continue
                            if col_name not in result:
                                result[col_name] = [val]
                            else:
                                result[col_name].append(val)
        return result

    def compact_shards(self, collection: str,
                        target_row_group_size: int = 100_000) -> Optional[str]:
        """Merge all shards into HEAD, then clear the shards.

        This is the ONLY place that needs coordination, and it's idempotent:
        multiple compactors produce the same result (CRDT merge is
        commutative). Last-writer-wins on HEAD is safe here because the
        result is deterministic.

        TWO COMPACTION MODES:
          - Manifest-level (fast path): when no shards have _rowid columns
            (insert-only appends), merge row group ENTRIES (metadata only)
            without reading any data blobs. O(shard_count) GETs, ZERO data I/O.
            Data blobs are immutable and content-addressed — the new manifest
            simply references them directly from HEAD + shards.
            (target_row_group_size is a no-op here — manifest-level compaction
            re-references existing row groups without rewriting them.)
          - Row-level (fallback): when shards have _rowid columns (upserts/
            deletes), decode all rows, apply CRDT merge by _rowid, re-encode
            into new row groups of `target_row_group_size` rows each.
            O(total_rows) data I/O — same as before, but the resulting row
            groups can be larger (default 100K rows) to reduce read
            amplification on the next scan.

        The fast path makes compaction viable at PB scale: merging 16 shards
        each with 10K row groups costs 16 GETs (shard manifests) + 1 PUT
        (merged manifest), regardless of total data volume.

        After compaction:
          - HEAD points to a new flat manifest containing all row groups
          - All shards are cleared (their refs are tombstoned)
          - Reads are fast again (1 manifest, no shard list)

        Args:
            collection: collection name
            target_row_group_size: row group size for row-level compaction
                re-encoding (default 100_000). Larger row groups reduce
                read amplification (fewer blobs per scan) at the cost of
                coarser pruning. Manifest-level compaction ignores this
                parameter (it doesn't re-encode).
        """
        head_manifest = self._load_manifest(collection)
        # Read the live shards via ref-listing (the only source of truth
        # now that the separate shard index has been removed).
        branch = self._get_active_branch(collection)
        shard_hashes = self._read_shard_index(collection, branch)
        if not shard_hashes:
            return None  # nothing to compact

        # Schema + key_col from HEAD, used to build pseudo-manifests for
        # inline (PND2) shards that have no PMAN manifest.
        head_schema = head_manifest.columns if head_manifest else None
        head_key_col = head_manifest.key_col if head_manifest else ""

        # Level 1: merge row group entries (dedup by rg_key, shards override HEAD)
        merged: dict[str, Any] = {}
        if head_manifest:
            for rg in head_manifest.scan_with_pruning():
                merged[rg.key] = rg
        shard_manifests = []
        for sh in shard_hashes:
            sm = self._load_shard_manifest(
                sh, head_schema, head_key_col)
            if sm is not None:
                shard_manifests.append(sm)
                for rg in sm.scan_with_pruning():
                    merged[rg.key] = rg

        if not merged:
            return None

        # Check if any shard has _rowid columns (row-level CRDT needed)
        # by inspecting the schema of HEAD + all shard manifests.
        needs_row_merge = self._manifests_have_rowid(head_manifest, shard_manifests)

        if needs_row_merge:
            return self._compact_shards_row_level(
                collection, head_manifest, shard_hashes, merged,
                target_row_group_size=target_row_group_size)
        else:
            return self._compact_shards_manifest_level(
                collection, head_manifest, shard_hashes, merged,
                target_row_group_size=target_row_group_size)

    def _manifests_have_rowid(self, head_manifest, shard_manifests) -> bool:
        """Check if any manifest has _rowid column (indicating row-level CRDT).

        Checks BOTH the manifest's schema columns AND the row groups' column
        stats. The schema may not include _rowid if the collection was created
        via write() (which doesn't add _rowid) and later upserted via
        upsert_shard (which adds _rowid to the data but not to the manifest's
        schema). The row group column stats always reflect the actual encoded
        columns, so they're the reliable signal.
        """
        manifests = [head_manifest] + shard_manifests
        for m in manifests:
            if m is None:
                continue
            # Check schema columns
            for col_name, _vtype in m.columns:
                if col_name == "_rowid":
                    return True
            # Check row group column stats (more reliable — reflects actual data)
            for rg in m.row_groups:
                for col in rg.columns:
                    if col.name == "_rowid":
                        return True
        return False

    def _compact_shards_manifest_level(self, collection, head_manifest,
                                         shard_hashes, merged,
                                         target_row_group_size: int = 100_000
                                         ) -> Optional[str]:
        """Fast-path compaction: merge manifest entries only, NO data I/O.

        Data blobs are immutable and content-addressed. The new manifest
        simply references the same blob_hash values from HEAD + shards.
        This makes compaction O(shard_count) GETs + O(1) manifest PUT,
        regardless of total data volume — viable at PB scale.

        Note: target_row_group_size is NOT used here — manifest-level
        compaction re-references existing row groups without rewriting them.
        To merge small row groups into larger ones, row-level compaction
        is required (which decodes and re-encodes). The parameter is
        accepted for API symmetry with _compact_shards_row_level.
        """
        schema = (head_manifest.columns if head_manifest else [("value", 4)])
        key_col = (head_manifest.key_col if head_manifest else "")
        rg_size = (head_manifest.row_group_size if head_manifest else 10_000)

        # Build manifest entries from merged row group entries
        # (NO data blob reads — just metadata)
        manifest_entries = []
        for rg in sorted(merged.values(), key=lambda r: r.key):
            manifest_entries.append({
                "rg_key": rg.key,
                "blob_hash": rg.blob_hash,
                "n_rows": rg.n_rows,
                "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                for c in rg.columns],
            })

        manifest_hash, new_manifest = self._build_manifest_with_return(
            collection, manifest_entries, schema, key_col, rg_size)

        # Build stats tree (writer-side, P10 fix)
        try:
            from stats_tree import should_use_stats_tree, build_stats_tree
            if should_use_stats_tree(len(new_manifest.row_groups)):
                stats_root = build_stats_tree(self.kernel, new_manifest.row_groups)
                new_manifest.set_stats_tree_root(stats_root)
                manifest_hash = new_manifest.commit()
                self.kernel.reference(self._manifest_ref(collection), manifest_hash)
        except ImportError:
            pass

        # Write commit
        parent = self._head_cache.get(collection)
        if parent is None:
            parent = self.kernel.resolve(self._active_commit_ref(collection))
        commit_index = self._commit_index_cache.get(collection, 0)
        if commit_index == 0 and parent:
            pc = self._read_commit_blob(parent)
            if pc:
                commit_index = pc.get("index", 0) + 1

        commit_hash = self._write_commit_blob(
            collection, manifest_hash, parent=parent,
            message=f"compact {len(shard_hashes)} shards (manifest-level, {len(manifest_entries)} row groups)",
            index=commit_index)

        # Clear shards
        branch = self._get_active_branch(collection)
        self._clear_branch_shards(collection, branch, shard_hashes=shard_hashes)

        # Update caches
        self._update_caches_after_write(
            collection, new_manifest, manifest_hash, commit_hash,
            commit_index, is_delta=False)

        # Auto-vacuum: delete the tombstoned shard blobs + old shard manifests.
        # Without this, compaction tombstones refs but leaves dead blobs,
        # so object count goes UP instead of down.
        # IMPORTANT: pass new_manifest so we DON'T delete data blobs that
        # are now referenced by the new manifest (inline shards' PND2 blobs
        # are data — they must survive manifest-level compaction).
        try:
            self._auto_vacuum_after_compact(
                collection, shard_hashes, new_manifest=new_manifest)
        except Exception:
            pass  # best-effort — vacuum failure shouldn't break compaction

        return commit_hash

    def _compact_shards_row_level(self, collection, head_manifest,
                                    shard_hashes, merged,
                                    target_row_group_size: int = 100_000
                                    ) -> Optional[str]:
        """Fallback compaction: decode all rows, apply row-level CRDT merge.

        Used when shards have _rowid columns (upserts/deletes). This path
        reads ALL data blobs, merges by _rowid (latest _version wins),
        drops tombstones, and re-encodes into new row groups of
        `target_row_group_size` rows each (default 100K — larger than
        typical write-time row groups, reducing read amplification).

        O(total_rows) data I/O — same as the pre-optimization behavior,
        but the resulting row groups are larger (better for subsequent reads).
        """
        # Fetch + decode ALL rows from merged row groups
        row_groups = list(merged.values())
        col_data_list = self._parallel_fetch_and_decode(row_groups, None, None)
        all_rows: list[dict] = []
        for col_data in col_data_list:
            if not col_data:
                continue
            n = len(next(iter(col_data.values())))
            for i in range(n):
                row = {}
                for c, vals in col_data.items():
                    if i < len(vals):
                        row[c] = vals[i]
                    else:
                        row[c] = None
                all_rows.append(row)

        # Level 2: row-level CRDT merge (dedup by _rowid, drop tombstones)
        has_rowid = any(r.get("_rowid") for r in all_rows)
        if has_rowid:
            key_col = head_manifest.key_col if head_manifest else ""
            all_rows = self._merge_rows_by_rowid(all_rows, key_col=key_col or None)

        # Build the compacted manifest with only LIVE rows
        schema = (head_manifest.columns if head_manifest
                   else [("value", 4)])
        key_col = (head_manifest.key_col if head_manifest else "")
        # Use the adaptive target_row_group_size for re-encoding (defaults
        # to 100K — larger than typical write-time row groups, reducing
        # read amplification on subsequent scans). Fall back to the HEAD
        # manifest's row_group_size only if the caller passed 0.
        rg_size = target_row_group_size if target_row_group_size > 0 else \
            (head_manifest.row_group_size if head_manifest else 10_000)

        # BATCH rows into proper row groups (not 1 row per blob)
        if all_rows:
            manifest_entries = []
            for start in range(0, len(all_rows), max(rg_size, 1)):
                end = min(start + max(rg_size, 1), len(all_rows))
                group_rows = all_rows[start:end]
                group_source = ListColumnSource(group_rows)
                pnd2_bytes, col_stats = PND2.encode(group_source)

                # Use the actual key column value for rg_key
                if key_col and key_col in group_rows[-1]:
                    max_pk = group_rows[-1][key_col]
                else:
                    max_pk = start + len(group_rows) - 1
                rg_key = _format_rg_key(max_pk)

                blob_hash = self.kernel.write(pnd2_bytes)
                manifest_entries.append({
                    "rg_key": rg_key,
                    "blob_hash": blob_hash,
                    "n_rows": end - start,
                    "col_stats": col_stats,
                })
            manifest_hash, new_manifest = self._build_manifest_with_return(
                collection, manifest_entries, schema, key_col, rg_size)
        else:
            manifest_hash, new_manifest = self._build_manifest_with_return(
                collection, [], schema, key_col, rg_size)

        # P10 fix: Build stats tree during compaction (writer-side).
        # This is the RIGHT place — compact has write access, and the
        # result is a flat manifest with all row groups inline (perfect
        # for stats tree construction). Readers find it pre-built.
        try:
            from stats_tree import should_use_stats_tree, build_stats_tree
            if should_use_stats_tree(len(new_manifest.row_groups)):
                stats_root = build_stats_tree(self.kernel, new_manifest.row_groups)
                new_manifest.set_stats_tree_root(stats_root)
                # Re-commit the manifest with the stats tree root
                manifest_hash = new_manifest.commit()
                self.kernel.reference(self._manifest_ref(collection), manifest_hash)
        except ImportError:
            pass

        # Write a new commit pointing to the compacted manifest
        parent = self._head_cache.get(collection)
        if parent is None:
            parent = self.kernel.resolve(self._active_commit_ref(collection))
        commit_index = self._commit_index_cache.get(collection, 0)
        if commit_index == 0 and parent:
            pc = self._read_commit_blob(parent)
            if pc:
                commit_index = pc.get("index", 0) + 1

        commit_hash = self._write_commit_blob(
            collection, manifest_hash, parent=parent,
            message=f"compact {len(shard_hashes)} shards ({len(all_rows)} live rows)",
            index=commit_index)

        # Clear shards: tombstone the absorbed shard refs and reset the index.
        # The tombstone path overwrites each ref with an empty blob so that
        # _list_shards_from_refs (which scans refs as source of truth) skips
        # them. Only the shards that were in the index at compaction time
        # are tombstoned — a concurrent writer that added a new shard after
        # we read the index will still be visible (and will be picked up by
        # the next compaction).
        branch = self._get_active_branch(collection)
        self._clear_branch_shards(collection, branch, shard_hashes=shard_hashes)

        # Update caches
        self._update_caches_after_write(
            collection, new_manifest, manifest_hash, commit_hash,
            commit_index, is_delta=False)

        # Auto-vacuum: delete the tombstoned shard blobs + old shard manifests.
        # Pass new_manifest to protect blobs referenced by the new manifest.
        # This is CRITICAL for inline shards: the shard hash IS the PND2 data
        # blob hash. If row-level compaction re-encodes the SAME data (same
        # rows → same content → same hash), the new manifest references the
        # SAME blob as the old shard. Without protection, vacuum would delete
        # the new manifest's blob — corrupting HEAD.
        try:
            self._auto_vacuum_after_compact(
                collection, shard_hashes, new_manifest=new_manifest)
        except Exception:
            pass

        return commit_hash

    def _auto_vacuum_after_compact(self, collection: str,
                                     shard_hashes: list[str],
                                     new_manifest: Optional[CollectionManifest] = None
                                     ) -> None:
        """Delete dead blobs after compaction (best-effort).

        After compaction, the old shard manifests + their data blobs are
        unreachable (tombstoned refs point to empty blobs). This method
        deletes those dead blobs so object count actually decreases.

        Uses the kernel's delete_blob (maintenance operation, not a kernel
        primitive) to reclaim space.

        Args:
            collection: collection name (unused, kept for API symmetry)
            shard_hashes: the OLD shard hashes (PMAN manifest hashes for
                multi-row-group shards, or PND2 data blob hashes for
                inline single-row-group shards).
            new_manifest: the NEW compacted manifest. If provided, its
                row group blob hashes are PROTECTED from deletion (they
                are now part of HEAD). This is critical for manifest-level
                compaction of inline shards: the shard hash IS the data
                blob hash, and that blob is now referenced by the new
                manifest — deleting it would corrupt HEAD. If None, all
                shard hashes are deleted (used by row-level compaction
                where all data is re-encoded into new blobs).
        """
        import hashlib as _hashlib
        empty_hash = _hashlib.sha256(b"").hexdigest()

        # Build a set of blob hashes referenced by the new manifest.
        # These must NOT be deleted — they are now part of HEAD.
        protected_hashes: set[str] = set()
        if new_manifest is not None:
            for rg in new_manifest.row_groups:
                protected_hashes.add(rg.blob_hash)

        # The shard_hashes are the OLD shard hashes. For PMAN manifest
        # shards, the hash is the manifest blob (its data blobs are
        # separate and not in shard_hashes, so they're safe). For inline
        # PND2 shards, the hash IS the data blob — it must be protected
        # if the new manifest references it.
        for sh in shard_hashes:
            if not sh or sh == empty_hash:
                continue
            if sh in protected_hashes:
                # This blob is now referenced by the new manifest —
                # deleting it would corrupt HEAD. Skip.
                continue
            try:
                self.kernel.store.delete_blob(sh)
            except Exception:
                pass  # best-effort

    # ------------------------------------------------------------------
    # ACID TRANSACTIONS — commit markers on top of CRDT shards
    #
    # ACID = CRDT + commit markers. Same model, thin extension.
    #
    #   tx = storage.begin_tx()
    #   storage.append_shard("users", rows, tx_id=tx)
    #   storage.append_shard("orders", orders, tx_id=tx)
    #   storage.commit_tx(tx)   # 1 PUT — ALL shards become visible
    #
    # Readers automatically filter: shards with tx_id are only visible
    # if the commit marker exists. No coordinator, no 2PC, no CAS.
    # ------------------------------------------------------------------

    @staticmethod
    def _tx_ref(tx_id: str) -> str:
        """The commit marker ref path."""
        return f"transactions/{tx_id}"

    def begin_tx(self) -> str:
        """Begin a transaction. Returns the tx_id.

        The tx_id is a UUIDv7 (time-ordered, unique). Pass it to
        append_shard(tx_id=...) to make shards tentative.

        No storage operation is performed — begin_tx is free.
        The tx_id is just a unique identifier until commit_tx.
        """
        try:
            from uuid7 import uuidv7
            return uuidv7()
        except ImportError:
            import time as _t
            return f"{_t.time_ns():016x}"

    def commit_tx(self, tx_id: str, message: str = "") -> str:
        """Commit a transaction — ALL tentative shards become visible.

        Writes a commit marker (1 PUT + 1 ref). Readers checking
        tentative shards will find this marker and include them.

        This is atomic: the commit marker is a single ref update.
        Crash before = shards invisible. Crash after = all visible.

        Args:
            tx_id: the transaction ID (from begin_tx)
            message: optional commit message

        Returns:
            The commit marker hash.
        """
        import json as _json
        import time as _time

        marker = {
            "tx_id": tx_id,
            "timestamp": _time.time(),
            "message": message,
        }
        marker_bytes = _json.dumps(marker, sort_keys=True).encode()
        marker_hash = self.kernel.write(marker_bytes)
        self.kernel.reference(self._tx_ref(tx_id), marker_hash)
        return marker_hash

    def abort_tx(self, tx_id: str) -> None:
        """Abort a transaction — tentative shards stay invisible.

        Simply don't write the commit marker. Tentative shards remain
        in storage but are invisible to readers (no commit marker).
        GC cleans them up after a configurable timeout.

        This is a no-op: abort = "don't commit". No storage operation.
        """
        pass  # Nothing to do — absence of commit marker = aborted

    def is_tx_committed(self, tx_id: str) -> bool:
        """Check if a transaction has been committed."""
        return self.kernel.resolve(self._tx_ref(tx_id)) is not None

    def shard_count(self, collection: str) -> int:
        """Return the number of unmerged shards for a collection.

        Delegates to _read_shard_index, which scans the live shard refs
        (post-compaction tombstones are skipped by _list_shards_from_refs).
        """
        return len(self._read_shard_index(collection))

    # ------------------------------------------------------------------
    # ROW-LEVEL CRDT — upsert + delete with version vectors
    #
    # The shard CRDT handles INSERT well, but UPDATE and DELETE at the
    # row level need explicit versioning. This section adds:
    #
    #   - upsert_shard: insert-or-update rows with (_rowid, _version)
    #   - delete_shard: row-level tombstones with (_rowid, _version)
    #   - read_with_shards: row-level merge (last-writer-wins by _version)
    #
    # Merge rules (deterministic, eventually consistent):
    #   - INSERT + INSERT (same _rowid): later _version wins
    #   - UPDATE + UPDATE (same _rowid): later _version wins
    #   - DELETE + anything: later _version wins (tombstone if DELETE is later)
    #   - INSERT + INSERT (different _rowid): both kept (no conflict)
    #
    # _rowid: UUIDv7 string (time-ordered, globally unique). Stable across
    #   updates — an UPDATE keeps the same _rowid, bumps _version.
    # _version: UUIDv7 string (time-ordered). Each write generates a new
    #   _version. Merge compares _version strings lexicographically
    #   (UUIDv7 is time-ordered, so lexicographic = chronological).
    # _deleted: bool. If True, this row is a tombstone (deleted at _version).
    #
    # These are REGULAR COLUMNS in PND2 — no format change. The merge
    # logic lives in read_with_shards and compact_shards.
    # ------------------------------------------------------------------

    def upsert_shard(self, collection: str, rows: list[dict],
                      key_col: Optional[str] = None,
                      row_group_size: int = 10_000) -> str:
        """Concurrent-safe upsert (insert-or-update) with row-level CRDT.

        Each row gets a _rowid (stable across updates) and _version
        (new per write). On merge, the row with the later _version wins.

        For NEW rows: caller does NOT provide _rowid — we generate one.
        For UPDATES: caller provides _rowid (from the original read),
                     we generate a new _version.

        Args:
            collection: collection name
            rows: list of row dicts. For updates, include _rowid from
                  the original row. For inserts, omit _rowid.
            key_col: sort key column (for range scans)
            row_group_size: rows per row group

        Returns:
            The shard manifest hash.
        """
        try:
            from uuid7 import uuidv7
        except ImportError:
            import time as _t
            uuidv7 = lambda: f"{_t.time_ns():016x}"

        # B5 fix: use HLC (Hybrid Logical Clock) for _version instead of UUIDv7.
        # HLC is monotonic under clock skew — UUIDv7 is not.
        # The HLC instance is shared across ALL upsert_shard/delete_shard calls
        # (stored as self._hlc). This keeps the logical counter monotonic —
        # two calls in the same millisecond get DIFFERENT _version strings
        # (physical_ms same, logical incremented), so the second update is
        # not silently lost during CRDT merge.
        if self._hlc is not None:
            _gen_version = self._hlc.tick
        else:
            _gen_version = uuidv7
        stamped = []
        for row in rows:
            r = dict(row)
            if "_rowid" not in r or not r["_rowid"]:
                r["_rowid"] = uuidv7()  # new row — generate _rowid (UUIDv7 is fine for identity)
            r["_version"] = _gen_version()  # HLC for version (clock-skew-safe)
            r["_deleted"] = False
            stamped.append(r)

        return self.append_shard(collection, stamped, key_col=key_col,
                                   row_group_size=row_group_size,
                                   message="upsert shard")

    def delete_shard(self, collection: str, rowids: list[str],
                      key_col: Optional[str] = None,
                      row_group_size: int = 10_000,
                      keys: Optional[list[str]] = None) -> str:
        """Concurrent-safe row-level delete with tombstones.

        Each deleted _rowid gets a tombstone row with _deleted=True and
        a new _version. On merge, if the tombstone's _version is later
        than any live row's _version, the row is suppressed.

        Args:
            collection: collection name
            rowids: list of _rowid strings to delete
            key_col: sort key column (for range scans)
            row_group_size: rows per row group
            keys: optional list of key_col values, one per rowid. If
                provided, each tombstone's key_col is set to the actual
                key value (not ""). This avoids rg_key collisions in
                compact_shards/merge when multiple keys are deleted —
                without distinct key_col values, all tombstones land in
                one row group with the same rg_key, and dedup-by-rg_key
                drops all but the last tombstone. If None, tombstones
                get key_col="" (legacy behavior — may lose tombstones
                during compaction when deleting multiple keys).

        Returns:
            The shard manifest hash.
        """
        try:
            from uuid7 import uuidv7
        except ImportError:
            import time as _t
            uuidv7 = lambda: f"{_t.time_ns():016x}"

        # B5 fix: use HLC for _version (shared instance — see upsert_shard)
        if self._hlc is not None:
            _gen_version = self._hlc.tick
        else:
            _gen_version = uuidv7

        tombstones = []
        for i, rid in enumerate(rowids):
            t = {
                "_rowid": rid,
                "_version": _gen_version(),
                "_deleted": True,
            }
            # Set key_col to the actual key value (if provided) so
            # tombstones get distinct rg_keys and survive compaction.
            # Without this, all tombstones share rg_key="rg/" and
            # compact_shards/merge drop all but the last one.
            if keys is not None and i < len(keys) and keys[i] is not None:
                t[key_col or "_key"] = keys[i]
            else:
                t[key_col or "_key"] = ""
            tombstones.append(t)

        return self.append_shard(collection, tombstones, key_col=key_col,
                                   row_group_size=row_group_size,
                                   message="delete shard")

    def _merge_rows_by_rowid(self, all_rows: list[dict],
                              key_col: Optional[str] = None) -> list[dict]:
        """Merge rows by _rowid, keeping the one with the latest _version.

        Tombstones (_deleted=True) suppress the row if their _version is
        the latest. If a live row has a later _version, it overrides the
        tombstone (the delete was superseded by a concurrent update).

        This is the CRDT merge — deterministic and eventually consistent.

        LEGACY ROWS: rows without _rowid (from write(), not upsert_shard)
        are kept as-is — UNLESS there are CRDT rows (with _rowid) AND a
        key_col is provided, in which case legacy rows whose key_col value
        matches a CRDT row's key_col value are dropped (the CRDT row is
        newer and supersedes the legacy snapshot). Legacy rows with unique
        key_col values are kept (they represent data not yet upserted).

        Args:
            all_rows: rows to merge (may include both _rowid-tagged and
                      legacy rows)
            key_col: the sort key column name. Used to dedup legacy rows
                     against CRDT rows. If None, legacy rows are always kept.
        """
        # First pass: separate CRDT rows (with _rowid) from legacy rows.
        # Also observe every remote _version through the shared HLC so the
        # local clock stays ahead of remote clocks. This prevents
        # skew-induced LWW inversions: a writer whose wall clock lags
        # would otherwise generate _version strings that lose to stale
        # remote rows even after observing them. observe() is a no-op
        # for non-HLC strings (legacy UUIDv7 versions).
        latest: dict[str, dict] = {}
        legacy_rows: list[dict] = []
        has_crdt = False
        for row in all_rows:
            rid = row.get("_rowid")
            if rid is None:
                legacy_rows.append(row)
                continue
            has_crdt = True
            ver = row.get("_version", "")
            if self._hlc is not None and ver:
                self._hlc.observe(ver)
            existing = latest.get(rid)
            if existing is None or ver > existing.get("_version", ""):
                latest[rid] = row

        result = []

        if has_crdt and key_col:
            # CRDT mode with key_col: drop legacy rows whose key_col value
            # matches a CRDT row (the CRDT row is newer). Keep legacy rows
            # with unique key_col values (data not yet upserted).
            crdt_keys = set()
            for row in latest.values():
                kv = row.get(key_col)
                if kv is not None:
                    crdt_keys.add(kv)
            for row in legacy_rows:
                kv = row.get(key_col)
                if kv is not None and kv in crdt_keys:
                    continue  # superseded by a CRDT row — drop
                result.append(row)
        else:
            # No CRDT rows, or no key_col to dedup by — keep legacy rows.
            result.extend(legacy_rows)

        # Add surviving CRDT rows (drop tombstones that won)
        for row in latest.values():
            if row.get("_deleted"):
                continue  # tombstone won — suppress this row
            result.append(row)

        return result

    # ------------------------------------------------------------------
    # WRITE — the ONE write path
    # ------------------------------------------------------------------

    def write(self, collection: str, rows,
              key_col: Optional[str] = None,
              row_group_size: int = 10_000,
              encoding_hints: Optional[dict[str, str]] = None,
              message: str = "") -> str:
        """Write rows to a collection as PND2 blobs.

        Args:
            collection: collection name
            rows: a ColumnSource OR PyArrow Table OR list[dict] (KV rows)
            key_col: column to use as the sort key (None = use row index)
            row_group_size: rows per row group (default 10_000)
            encoding_hints: optional dict {col_name: "auto"|"rle"|"dict"|"bitpack"|"raw"}
            message: commit message

        Returns:
            The new HEAD commit hash.
        """
        # Coerce input to a ColumnSource
        if isinstance(rows, list):
            source = ListColumnSource(rows)
        elif isinstance(rows, ColumnSource):
            source = rows
        else:
            source = as_column_source(rows)
        n_rows = source.num_rows()

        # Sort by key_col if specified (empty string = no key col)
        if key_col == "":
            key_col = None
        if key_col is not None and key_col in source.column_names():
            # For PyArrow, we can sort. For ListColumnSource, sort in Python.
            source = _sort_source_by(source, key_col)
            key_array = source.column_slice(key_col, 0, n_rows)
        elif key_col is not None:
            raise KeyError(f"key column '{key_col}' not in source columns")
        else:
            key_array = list(range(n_rows))

        # Build row groups
        manifest_entries: list[dict] = []
        col_names = source.column_names()

        # Detect value types once (from the first chunk)
        schema_columns: list[tuple[str, int]] = []
        if n_rows > 0:
            for col_name in col_names:
                sample = source.column_slice(col_name, 0, min(100, n_rows))
                vtype = _detect_value_type_with_binary(sample)
                schema_columns.append((col_name, vtype))

        # write() has overwrite semantics — the new manifest replaces the
        # old one entirely. No need to delete old row group keys from a
        # ProllyTree (removed — unified architecture uses manifest only).
        # content-addressed (deduped); the old manifest is simply not
        # referenced by the new commit.
        # skip_cache=True: for writes, the cache is authoritative (single-writer)
        existing_manifest = self._load_manifest(collection, skip_cache=True)
        # (existing_manifest is read for schema inheritance if needed;
        #  no deletion of old row group keys is required.)

        if n_rows == 0:
            # Fix (Round 11 Issue #1): empty write must still update the
            # manifest so the collection shows as empty (not stale data).
            manifest = CollectionManifest(self.kernel)
            manifest.set_schema(columns=schema_columns, key_col=key_col or "",
                                 row_group_size=row_group_size, chunk_size=0)
            manifest_hash = self._build_manifest(
                collection, [], schema_columns,
                key_col or "", row_group_size)
            # O(1) warm write: use cached HEAD + commit_index if available
            parent = self._head_cache.get(collection)
            if parent is None:
                parent = self.kernel.resolve(self._active_commit_ref(collection))
            commit_index = self._commit_index_cache.get(collection, 0)
            if commit_index == 0 and parent:
                pc = self._read_commit_blob(parent)
                if pc:
                    commit_index = pc.get("index", 0) + 1
            commit_hash = self._write_commit_blob(
                collection, manifest_hash, parent=parent,
                message=message or "write: empty table",
                index=commit_index)
            self._update_caches_after_write(
                collection, manifest, manifest_hash, commit_hash, commit_index,
                is_delta=False)
            return commit_hash

        for start in range(0, n_rows, row_group_size):
            end = min(start + row_group_size, n_rows)
            group_source = _slice_source(source, start, end)
            max_pk = key_array[end - 1]
            rg_key = _format_rg_key(max_pk)

            pnd2_bytes, col_stats = PND2.encode(
                group_source, encoding_hints=encoding_hints)
            blob_hash = self.kernel.write(pnd2_bytes)

            manifest_entries.append({
                "rg_key": rg_key,
                "blob_hash": blob_hash,
                "n_rows": end - start,
                "col_stats": col_stats,
            })

        n_groups = (n_rows + row_group_size - 1) // row_group_size

        # Build the manifest (one blob, atomically with the commit)
        manifest_hash, new_manifest = self._build_manifest_with_return(
            collection, manifest_entries, schema_columns,
            key_col or "", row_group_size)

        # O(1) warm write: use cached HEAD + commit_index if available
        parent = self._head_cache.get(collection)
        if parent is None:
            parent = self.kernel.resolve(self._active_commit_ref(collection))
        commit_index = self._commit_index_cache.get(collection, 0)
        if commit_index == 0 and parent:
            pc = self._read_commit_blob(parent)
            if pc:
                commit_index = pc.get("index", 0) + 1

        commit_hash = self._write_commit_blob(
            collection, manifest_hash,
            parent=parent,
            message=message or f"unified write: {n_rows} rows in {n_groups} row groups",
            index=commit_index)

        # Update caches (don't invalidate) → next write is O(1)
        self._update_caches_after_write(
            collection, new_manifest, manifest_hash, commit_hash, commit_index,
            is_delta=False)
        return commit_hash

    def append(self, collection: str, rows,
               key_col: Optional[str] = None,
               row_group_size: int = 10_000,
               encoding_hints: Optional[dict[str, str]] = None,
               message: str = "") -> str:
        """Append rows to an existing collection.

        UNIFIED with CRDT shards — no CAS, no HEAD contention.

        This method delegates to append_shard() (the CRDT shard model)
        and auto-compacts when the shard count exceeds a threshold.
        This makes it safe for multi-process use WITHOUT CAS:
          - Each process writes its own shard (no coordination)
          - Readers merge HEAD + all shards (CRDT union)
          - Auto-compaction merges shards into HEAD periodically

        For single-process use (same UnifiedStorage instance), the
        in-memory caches give O(1) warm shard writes (0 GETs).

        Args:
            collection: collection name (must already exist)
            rows: new rows to append
            key_col: sort key column
            row_group_size: rows per new row group
            encoding_hints: optional encoding hints
            message: commit message

        Returns:
            The shard manifest hash.
        """
        # Delegate to append_shard (CRDT — no CAS, no HEAD contention)
        result = self.append_shard(collection, rows, key_col=key_col,
                                     row_group_size=row_group_size,
                                     encoding_hints=encoding_hints,
                                     message=message)

        # Auto-compact when shard count exceeds threshold
        # (bounds read amplification — readers see at most N shards)
        AUTO_COMPACT_THRESHOLD = 16
        if self.shard_count(collection) >= AUTO_COMPACT_THRESHOLD:
            try:
                self.compact_shards(collection)
            except Exception:
                pass  # compaction is best-effort — shards still work

        return result

    def compact_manifest(self, collection: str) -> Optional[str]:
        """Compact a delta-manifest chain into a single flat manifest.

        Fix (Round 11 Issue #2): delta-manifests grow unbounded without
        compaction. After K appends, reads require K extra GETs to walk
        the parent chain. This method walks the chain, collects ALL row
        group entries, and writes a single flat manifest with no parent.

        Should be called periodically (e.g., after every 8 appends, or
        when the chain depth exceeds a threshold).

        Returns:
            The new (compacted) manifest hash, or None if no compaction
            was needed.
        """
        manifest = self._load_manifest(collection)
        if manifest is None or manifest.parent_manifest_hash is None:
            return None  # no delta chain to compact

        # Collect ALL row group entries.
        # Fix (Round 13 Issue #1): call scan_with_pruning() ONCE (it recurses).
        # Fix (Round 18 Issue #1): keep FIRST (NEWEST) for duplicate keys.
        # Fix (Round 19 Issue #2): when BOTH stats_tree_root AND parent_manifest_hash
        # are set, scan_with_pruning delegates to StatsTreeReader and returns
        # early — the parent chain is NOT walked. We must explicitly walk
        # the parent chain to collect ALL entries.
        all_entries: list[dict] = []
        seen_keys: dict[str, dict] = {}

        # Collect entries from the current (delta) manifest
        for rg in manifest.scan_with_pruning():
            if rg.key in seen_keys:
                continue
            seen_keys[rg.key] = {
                "rg_key": rg.key,
                "blob_hash": rg.blob_hash,
                "n_rows": rg.n_rows,
                "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                for c in rg.columns],
            }

        # Fix (Round 19 Issue #2): if the manifest has a stats_tree_root,
        # scan_with_pruning only yielded the delta's entries (from the stats
        # tree). We must ALSO walk the parent chain to get the OLD entries.
        # This is needed because scan_with_pruning early-returns when
        # stats_tree_root is set, skipping the parent_manifest_hash walk.
        if manifest.stats_tree_root and manifest.parent_manifest_hash:
            try:
                parent = CollectionManifest.load(
                    self.kernel, manifest.parent_manifest_hash)
                for rg in parent.scan_with_pruning():
                    if rg.key in seen_keys:
                        continue  # newer version already collected
                    seen_keys[rg.key] = {
                        "rg_key": rg.key,
                        "blob_hash": rg.blob_hash,
                        "n_rows": rg.n_rows,
                        "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                        for c in rg.columns],
                    }
            except (ValueError, KeyError):
                pass  # parent not found — only use delta entries

        all_entries = list(seen_keys.values())

        # Sort by rg_key (fix from Round 9)
        all_entries.sort(key=lambda e: e["rg_key"])

        # Write a flat manifest (no parent, no stats tree)
        schema_columns = manifest.columns
        key_col = manifest.key_col
        row_group_size = manifest.row_group_size

        new_manifest = CollectionManifest(self.kernel)
        new_manifest.set_schema(
            columns=schema_columns,
            key_col=key_col,
            row_group_size=row_group_size,
            chunk_size=0,
        )
        for entry in all_entries:
            rg = RowGroupEntry(
                key=entry["rg_key"],
                blob_hash=entry["blob_hash"],
                n_rows=entry["n_rows"],
                storage_mode=STORAGE_WHOLE_BLOB,
            )
            for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                rg.columns.append(ColumnStatsEntry(
                    name=col_name, value_type=vtype,
                    min=mn, max=mx, null_count=null_count, chunks=[],
                ))
            new_manifest.add_row_group(rg)

        # P10 fix: Build stats tree on write() when the collection is large.
        # This is a writer-side operation — readers find it pre-built.
        # The build is O(N log N) but only when >25K row groups (PB scale).
        try:
            from stats_tree import should_use_stats_tree, build_stats_tree
            if should_use_stats_tree(len(all_entries)):
                stats_root = build_stats_tree(self.kernel, new_manifest.row_groups)
                new_manifest.set_stats_tree_root(stats_root)
        except ImportError:
            pass

        new_hash = new_manifest.commit()
        self.kernel.reference(self._manifest_ref(collection), new_hash)
        self._invalidate_manifest_cache(collection)
        return new_hash

    def _build_manifest_with_return(self, collection: str,
                         entries: list[dict],
                         schema_columns: list[tuple[str, int]],
                         key_col: str,
                         row_group_size: int,
                         parent_manifest_hash: Optional[str] = None
                         ) -> tuple[str, CollectionManifest]:
        """Build the manifest and return (hash, manifest_object).

        The manifest object is returned so callers can cache it for
        O(1) warm writes (avoids re-reading from storage on next write).
        """
        manifest = CollectionManifest(self.kernel)
        manifest.set_schema(
            columns=schema_columns,
            key_col=key_col,
            row_group_size=row_group_size,
            chunk_size=0,
        )

        rg_entries: list[RowGroupEntry] = []
        for entry in entries:
            rg = RowGroupEntry(
                key=entry["rg_key"],
                blob_hash=entry["blob_hash"],
                n_rows=entry["n_rows"],
                storage_mode=STORAGE_WHOLE_BLOB,
            )
            for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                rg.columns.append(ColumnStatsEntry(
                    name=col_name, value_type=vtype, min=mn, max=mx,
                    null_count=null_count, chunks=[],
                ))
            manifest.add_row_group(rg)
            rg_entries.append(rg)

        # P10 fix: StatsTree is NOT built eagerly — lazy on first read.
        if parent_manifest_hash is not None:
            manifest.set_parent_manifest(parent_manifest_hash)

        manifest_hash = manifest.commit()
        self.kernel.reference(self._manifest_ref(collection), manifest_hash)
        return manifest_hash, manifest

    def _build_manifest(self, collection: str,
                         entries: list[dict],
                         schema_columns: list[tuple[str, int]],
                         key_col: str,
                         row_group_size: int,
                         parent_manifest_hash: Optional[str] = None) -> Optional[str]:
        """Build the CollectionManifest for the just-written row groups.

        At PB scale (>25K row groups), the manifest delegates to a
        hierarchical stats tree. The manifest blob itself stays small
        (schema + sort order + stats_tree_root = ~200 bytes), and the
        stats tree provides O(log N) reads via content-addressed nodes.

        For delta-appends (parent_manifest_hash set), the manifest stores
        only the NEW row groups + a pointer to the parent. The reader
        walks the parent chain. This makes append() O(new_row_groups).
        """
        manifest = CollectionManifest(self.kernel)
        manifest.set_schema(
            columns=schema_columns,
            key_col=key_col,
            row_group_size=row_group_size,
            chunk_size=0,  # unified storage has no per-chunk blobs
        )

        # Build RowGroupEntry objects (we need them for both the flat
        # manifest AND the stats tree)
        rg_entries: list[RowGroupEntry] = []
        for entry in entries:
            rg = RowGroupEntry(
                key=entry["rg_key"],
                blob_hash=entry["blob_hash"],
                n_rows=entry["n_rows"],
                storage_mode=STORAGE_WHOLE_BLOB,  # unified mode
            )
            # Build column stats entries from the per-column stats
            for col_name, vtype, mn, mx, null_count in entry["col_stats"]:
                rg.columns.append(ColumnStatsEntry(
                    name=col_name,
                    value_type=vtype,
                    min=mn,
                    max=mx,
                    null_count=null_count,
                    chunks=[],  # no per-chunk stats in unified mode
                ))
            manifest.add_row_group(rg)
            rg_entries.append(rg)

        # P10 fix: StatsTree is NOT built eagerly — lazy on first read.

        # Delta-manifest: set parent pointer for O(1) appends at PB scale
        if parent_manifest_hash is not None:
            manifest.set_parent_manifest(parent_manifest_hash)

        manifest_hash = manifest.commit()
        self.kernel.reference(self._manifest_ref(collection), manifest_hash)
        return manifest_hash

    # ------------------------------------------------------------------
    # READ — the ONE read path
    # ------------------------------------------------------------------

    def read(self, collection: str,
             predicates: Optional[list[tuple[str, str, Any]]] = None,
             columns: Optional[list[str]] = None,
             row_filter: Optional[Callable[[dict], bool]] = None,
             start_key: Optional[str] = None,
             end_key: Optional[str] = None,
             commit_hash: Optional[str] = None,
             manifest_hash: Optional[str] = None) -> list[dict]:
        """Read rows from a collection.

        Args:
            collection: collection name
            predicates: list of (column, op, value) tuples. All ANDed.
            columns: projection pushdown (None = all columns)
            row_filter: exact row-level filter
            start_key: range scan lower bound
            end_key: range scan upper bound
            commit_hash: (unused — use manifest_hash for time-travel)
            manifest_hash: load a specific manifest by hash (for time-travel
                and branch reads). No ref mutation, no race condition.
                Fix (Round 9 Issue #2): replaces the old swap-then-restore pattern.

        Returns:
            List of row dicts.

        Round trips: 3 + K S3 GETs cold (root pointer + root ref + manifest + K data blobs)
        """
        # Live read: if there are unmerged shards, include them via
        # read_with_shards. Time-travel queries (manifest_hash / commit_hash
        # set) skip this and read the snapshot manifest directly.
        if manifest_hash is None and commit_hash is None:
            shard_hashes = self._read_shard_index(collection)
            if shard_hashes:
                return self.read_with_shards(
                    collection, predicates=predicates,
                    columns=columns, row_filter=row_filter,
                    start_key=start_key, end_key=end_key)

        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return []

        # Build a combined row filter: caller's row_filter AND automatic
        # filters for predicates not handled at the encoded-eval level.
        auto_filter = self._build_predicate_filter(predicates)
        combined_filter = self._combine_filters(row_filter, auto_filter)

        # Fix (Round 13 Issue #2): ensure predicate columns are always decoded
        # even if the caller's projection doesn't include them. Without this,
        # the auto_filter sees None for predicate columns not in the projection
        # and silently filters out ALL rows.
        eff_columns = list(columns) if columns is not None else None
        if predicates and eff_columns is not None:
            pred_cols = {p[0] for p in predicates}
            missing = pred_cols - set(eff_columns)
            if missing:
                eff_columns = list(dict.fromkeys(eff_columns + list(missing)))

        # Fix (Round 14 Issue #2): apply row-level key range filter.
        # Fix (Round 15 Issue #2): properly unpad zfill-padded string keys
        # so the comparison works against actual row values.
        key_col_name = manifest.key_col
        if (start_key is not None or end_key is not None) and key_col_name:
            # Parse raw key values from the formatted "rg/..." strings
            # Strip "rg/" prefix and any zfill padding
            def _unpad_rg_key(formatted_key):
                if formatted_key is None:
                    return None
                # Fix (Round 21 Issue #2): only reverse bias encoding for
                # formatted keys (start with "rg/"). Raw numeric keys passed
                # directly by the caller should NOT have bias subtracted.
                if not isinstance(formatted_key, str) or not formatted_key.startswith("rg/"):
                    # Raw key — try int, else return as-is
                    try:
                        return int(formatted_key)
                    except (ValueError, TypeError):
                        return formatted_key
                raw = formatted_key[3:]  # strip "rg/"
                try:
                    # Formatted key — reverse the bias encoding
                    return int(raw) - _INT64_BIAS
                except (ValueError, TypeError):
                    # Non-numeric formatted string — return raw
                    return raw

            raw_start = _unpad_rg_key(start_key)
            raw_end = _unpad_rg_key(end_key)

            def range_filter(row):
                v = row.get(key_col_name)
                if v is None:
                    return False
                try:
                    # Try numeric comparison first
                    v_num = v if isinstance(v, (int, float)) else int(v)
                    if raw_start is not None and isinstance(raw_start, int) and v_num < raw_start:
                        return False
                    if raw_end is not None and isinstance(raw_end, int) and v_num > raw_end:
                        return False
                except (ValueError, TypeError):
                    # Fall back to string comparison
                    sv = str(v)
                    if raw_start is not None:
                        ss = str(raw_start)
                        if sv < ss:
                            return False
                    if raw_end is not None:
                        se = str(raw_end)
                        if sv > se:
                            return False
                return True
            combined_filter = self._combine_filters(combined_filter, range_filter)

        # Fix (Round 24 Issue #2): format raw caller keys to "rg/..." format,
        # matching what point_lookup does internally. This makes the API
        # consistent — callers can pass raw keys (int, string) without
        # needing to know about _format_rg_key.
        if start_key is not None and not (isinstance(start_key, str) and start_key.startswith("rg/")):
            start_key = _format_rg_key(start_key)
        if end_key is not None and not (isinstance(end_key, str) and end_key.startswith("rg/")):
            end_key = _format_rg_key(end_key)

        # Walk surviving row groups via manifest (in-memory pruning — 0 GETs)
        # Fix (Round 21): use parallel fetch for surviving row groups (10-16x
        # latency reduction at PB scale). Same infrastructure as read_as_columns.
        surviving = list(manifest.scan_with_pruning(predicates, start_key, end_key))
        if not surviving:
            return []

        col_results = self._parallel_fetch_and_decode(
            surviving, eff_columns, predicates)

        all_rows: list[dict] = []
        for col_data in col_results:
            # Convert column-oriented data to row-oriented dicts
            row_count = max((len(v) for v in col_data.values()), default=0)
            col_names = list(col_data.keys())
            for i in range(row_count):
                row = {c: col_data[c][i] if i < len(col_data[c]) else None
                        for c in col_names}
                if combined_filter is None or combined_filter(row):
                    # Strip predicate-only columns from the result if the
                    # caller didn't request them
                    if columns is not None and eff_columns != columns:
                        row = {c: row[c] for c in columns if c in row}
                    all_rows.append(row)

        return all_rows

    @staticmethod
    def _build_predicate_filter(
            predicates: Optional[list[tuple[str, str, Any]]]
            ) -> Optional[Callable[[dict], bool]]:
        """Build a row filter that applies ALL predicates.

        PND2.decode only evaluates the first predicate at the encoded
        level. This method builds a Python-level filter for ALL
        predicates (including the first, for safety — the encoded eval
        may not have been able to prune, e.g., for RAW encoding).

        Returns None if no predicates. Returns a function(row_dict) -> bool.
        """
        if not predicates:
            return None

        def filt(row: dict) -> bool:
            for col, op, val in predicates:
                row_val = row.get(col)
                if row_val is None:
                    return False  # NULL never matches
                try:
                    if op == "=" and not (row_val == val): return False
                    elif op == "!=" and not (row_val != val): return False
                    elif op == ">" and not (row_val > val): return False
                    elif op == ">=" and not (row_val >= val): return False
                    elif op == "<" and not (row_val < val): return False
                    elif op == "<=" and not (row_val <= val): return False
                    elif op == "in" and row_val not in val: return False
                    else:
                        pass  # unknown op — don't filter (safe default)
                except TypeError:
                    return False  # type mismatch — row doesn't match
            return True
        return filt

    @staticmethod
    def _combine_filters(
            f1: Optional[Callable], f2: Optional[Callable]
            ) -> Optional[Callable]:
        """Combine two row filters with AND. None = no filter."""
        if f1 is None:
            return f2
        if f2 is None:
            return f1
        def combined(row: dict) -> bool:
            return f1(row) and f2(row)
        return combined

    def read_as_columns(self, collection: str,
                         predicates: Optional[list[tuple[str, str, Any]]] = None,
                         columns: Optional[list[str]] = None,
                         commit_hash: Optional[str] = None,
                         manifest_hash: Optional[str] = None
                         ) -> dict[str, list]:
        """Read rows from a collection as column-oriented data.

        Like read(), but returns dict[col_name, list[values]] instead of
        list[dict]. Faster when the caller wants columnar data (e.g.,
        feeding into PyArrow or numpy).

        Uses PARALLEL blob fetch for surviving row groups (via thread pool).

        Fix (Round 12 Issue #1): applies _build_predicate_filter to ALL
        predicates (not just the first one that PND2.decode evaluates).
        Fix (Round 12 Issue #2): resolves commit_hash to manifest_hash.
        """
        # Fix (Round 12 Issue #2): resolve commit_hash if manifest_hash not provided
        if manifest_hash is None and commit_hash is not None:
            manifest_hash = self._resolve_commit_manifest(collection, commit_hash)

        # Live read: if there are unmerged shards, include them via a
        # shard-aware path. Time-travel queries use the snapshot manifest only.
        if manifest_hash is None and commit_hash is None:
            shard_hashes = self._read_shard_index(collection)
            if shard_hashes:
                return self._read_as_columns_with_shards(
                    collection, predicates=predicates, columns=columns,
                    shard_hashes=shard_hashes)

        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return {}

        # Collect surviving row groups
        surviving = list(manifest.scan_with_pruning(predicates))
        if not surviving:
            return {}

        # Fix (Round 13 Issue #2): ensure predicate columns are always decoded
        eff_columns = list(columns) if columns is not None else None
        if predicates and eff_columns is not None:
            pred_cols = {p[0] for p in predicates}
            missing = pred_cols - set(eff_columns)
            if missing:
                eff_columns = list(dict.fromkeys(eff_columns + list(missing)))

        # PARALLEL fetch: fetch all surviving blobs concurrently.
        col_results = self._parallel_fetch_and_decode(
            surviving, eff_columns, predicates)

        # Fix (Round 12 Issue #1): apply multi-predicate filter.
        auto_filter = self._build_predicate_filter(predicates)

        # Merge column results across row groups, applying the filter
        result: dict[str, list] = {}
        for col_data in col_results:
            if auto_filter is None:
                # No filter needed — merge directly (but strip predicate-only cols)
                for col_name, values in col_data.items():
                    if columns is not None and col_name not in columns:
                        continue  # skip predicate-only columns
                    if col_name not in result:
                        result[col_name] = []
                    result[col_name].extend(values)
            else:
                # Apply filter row-by-row
                row_count = max((len(v) for v in col_data.values()), default=0)
                col_names = list(col_data.keys())
                for i in range(row_count):
                    row = {c: col_data[c][i] if i < len(col_data[c]) else None
                            for c in col_names}
                    if auto_filter(row):
                        # Only include requested columns (strip predicate-only cols)
                        out_cols = columns if columns is not None else col_names
                        for c in out_cols:
                            if c not in result:
                                result[c] = []
                            result[c].append(row.get(c))

        return result

    def _resolve_commit_manifest(self, collection: str,
                                  commit_hash: str) -> Optional[str]:
        """Resolve a commit hash to its manifest hash for time-travel reads.

        With the new manifest-based commit format, the manifest hash is
        stored directly IN the commit blob. We read the commit blob (1
        GET) and extract the "manifest" field.

        Falls back to the legacy ref-based lookup for old collections
        that used the old commit format (now unified to JSON).
        """
        # Manifest hash is in the commit blob (unified architecture)
        commit = self._read_commit_blob(commit_hash)
        if commit and commit.get("manifest"):
            return commit["manifest"]
        return None

    def _parallel_fetch_and_decode(
            self,
            row_groups: list,
            columns: Optional[list[str]],
            predicates: Optional[list[tuple[str, str, Any]]]
            ) -> list[dict[str, list]]:
        """Fetch and decode multiple row groups in parallel.

        Uses a thread pool to fetch blobs concurrently. Each thread:
          1. Calls kernel.read_blob (1 S3 GET)
          2. Calls PND2.decode (CPU work)

        This reduces wall-clock latency from K × RTT to ~1 × RTT for
        the fetch phase, and parallelizes the decode phase across cores.

        For small K (1-2 row groups), the thread pool overhead exceeds
        the benefit — we fall back to sequential.
        """
        if len(row_groups) <= 2:
            # Sequential for small K (thread pool overhead > benefit)
            results = []
            for rg in row_groups:
                blob_bytes = self.kernel.read_blob(rg.blob_hash)
                col_data = PND2.decode(blob_bytes, columns=columns,
                                         predicates=predicates)
                results.append(col_data)
            return results

        # Parallel for large K
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def fetch_and_decode(rg):
            blob_bytes = self.kernel.read_blob(rg.blob_hash)
            return PND2.decode(blob_bytes, columns=columns,
                                 predicates=predicates)

        # Use at most 16 threads (S3 recommends max 50 concurrent per connection)
        max_workers = min(16, len(row_groups))
        results = [None] * len(row_groups)  # preserve order

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(fetch_and_decode, rg): i
                for i, rg in enumerate(row_groups)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                results[idx] = future.result()

        return [r for r in results if r is not None]

    def read_as_arrow(self, collection: str,
                       predicates: Optional[list[tuple[str, str, Any]]] = None,
                       columns: Optional[list[str]] = None) -> "pa.Table":
        """Read rows as a PyArrow Table — ZERO-COPY from PND2 where possible.

        This is the FASTEST read path for tabular workloads:
          1. Manifest pruning (in-memory, 0 GETs)
          2. Parallel blob fetch (K GETs in ~1 RTT wall-clock)
          3. For INT64/FLOAT64 columns: np.frombuffer → pa.array (zero-copy)
          4. For STRING columns: pa.array from Python list (1 copy)

        Returns a pa.Table directly — no list[dict] intermediate.

        Args:
            collection: collection name
            predicates: list of (column, op, value) tuples for pruning
            columns: projection pushdown (None = all columns)

        Returns:
            A pyarrow.Table with the surviving rows.

        Round trips: 3 + K S3 GETs cold (but K blobs fetched in parallel
        → wall-clock ~3 + 1 RTT for the fetch phase).
        """
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError(
                "pyarrow is required for read_as_arrow. "
                "Install with: pip install pyarrow")

        col_data = self.read_as_columns(collection, predicates=predicates,
                                          columns=columns)
        if not col_data:
            return pa.table({})

        # Build Arrow arrays directly from column data
        arrays = []
        names = []
        for col_name, values in col_data.items():
            arrays.append(pa.array(values))
            names.append(col_name)

        return pa.Table.from_arrays(arrays, names=names)

    def point_lookup(self, collection: str, key: str,
                      columns: Optional[list[str]] = None,
                      manifest_hash: Optional[str] = None) -> Optional[dict]:
        """Point lookup — find the single row with the given key.

        Returns the row as a dict, or None if not found.

        Round trips: 2 S3 GETs (manifest + 1 data blob) — O(1) regardless
        of collection scale. When shards exist (unmerged appends) and the
        key isn't found in HEAD, falls back to a shard-aware path that
        checks each shard's manifest for a row group whose key range
        contains the target.
        """
        # Fast path: try HEAD manifest first (preserves the original
        # 4-GET cold-lookup cost for keys that live in HEAD).
        # Time-travel queries (manifest_hash set) use ONLY this path.
        head_result = self._point_lookup_head(
            collection, key, columns=columns, manifest_hash=manifest_hash)

        # Live reads: if HEAD didn't have the key AND there are unmerged
        # shards, the row may be in a shard. Search shards.
        if head_result is None and manifest_hash is None:
            shard_hashes = self._read_shard_index(collection)
            if shard_hashes:
                return self._point_lookup_with_shards(
                    collection, key, columns=columns,
                    shard_hashes=shard_hashes)

        return head_result

    def _point_lookup_head(self, collection: str, key: str,
                            columns: Optional[list[str]] = None,
                            manifest_hash: Optional[str] = None) -> Optional[dict]:
        """Point lookup against the HEAD (or manifest_hash) manifest only."""
        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return None

        # Find the row group with smallest key >= "rg/{key}"
        # Fix (Round 3 Issue #1): use zero-padded key format for correct
        # lexicographic ordering.
        target = _format_rg_key(key)
        rg = manifest.find_row_group(target)
        if rg is None:
            return None

        blob_bytes = self.kernel.read_blob(rg.blob_hash)
        # Use the key column (manifest.key_col) as the predicate for
        # encoded eval — this returns only the surviving row(s) that
        # match the key, not the entire row group.
        #
        # Fix (Round 2 Issue #4): the old code returned the FIRST row of
        # the row group, not the matching row. Now we decode with a
        # predicate on the key column and return the (single) match.
        # Fix (Round 14 Issue #3): always include key_col in decoded columns
        # so the predicate eval and verification work even with RAW encoding.
        key_col = manifest.key_col
        if key_col:
            # Try to coerce the key to the right type for comparison
            try:
                key_val = int(key) if key.lstrip("-").isdigit() else key
            except (ValueError, AttributeError):
                key_val = key
            # Fix (Round 14 Issue #3): ensure key_col is always decoded
            eff_columns = list(columns) if columns is not None else None
            if eff_columns is not None and key_col not in eff_columns:
                eff_columns = eff_columns + [key_col]
            col_data = PND2.decode(blob_bytes, columns=eff_columns,
                                     predicates=[(key_col, "=", key_val)])
        else:
            col_data = PND2.decode(blob_bytes, columns=columns)

        # Convert to row dicts and find the matching one
        row_count = max((len(v) for v in col_data.values()), default=0)
        col_names = list(col_data.keys())
        for i in range(row_count):
            row = {c: col_data[c][i] if i < len(col_data[c]) else None
                    for c in col_names}
            # Verify this row matches the key (defensive — the predicate
            # eval should have already filtered)
            if key_col and key_col in row:
                row_key = row[key_col]
                try:
                    if str(row_key) == str(key) or row_key == int(key):
                        # Fix (Round 14 Issue #3): strip key_col if not in caller's projection
                        if columns is not None and key_col not in columns:
                            row = {c: row[c] for c in columns if c in row}
                        return row
                except (ValueError, TypeError):
                    pass
            # Don't return first row as fallback — that's a bug (R2 fix)
        return None

    def _point_lookup_with_shards(self, collection: str, key: str,
                                    columns: Optional[list[str]],
                                    shard_hashes: list[str]) -> Optional[dict]:
        """Point lookup against HEAD + unmerged shards.

        Searches each shard manifest in parallel for a row group whose key
        range contains the target key. If found, fetches only that row group
        blob and decodes with a key predicate. Falls back to HEAD manifest
        if no shard contains the key.

        Cost: O(shard_count) manifest GETs (parallel, ~1 RTT) + 1 data GET.
        At low shard counts (<16), this is competitive with the no-shard
        path. Compaction keeps shard counts low in steady state.
        """
        # Coerce key to int if numeric (matches point_lookup behavior)
        try:
            key_val = int(key) if key.lstrip("-").isdigit() else key
        except (ValueError, AttributeError):
            key_val = key

        target = _format_rg_key(key)

        # Load HEAD manifest for schema/key_col (needed for inline shards)
        head_manifest = self._load_manifest(collection)
        head_schema = head_manifest.columns if head_manifest else None
        head_key_col = head_manifest.key_col if head_manifest else ""

        # Search shards in parallel for the row group containing this key
        shard_manifests = self._parallel_fetch_shard_manifests(
            shard_hashes,
            schema_columns=head_schema, key_col=head_key_col)

        # Find a candidate row group from any shard
        candidate_blob_hash = None
        candidate_key_col = None
        for sm in shard_manifests:
            rg = sm.find_row_group(target)
            if rg is not None:
                # Verify the key is actually within this rg's column stats
                # (defensive — find_row_group uses formatted key ordering)
                candidate_blob_hash = rg.blob_hash
                candidate_key_col = sm.key_col
                break

        # If not found in shards, try HEAD manifest
        if candidate_blob_hash is None:
            # head_manifest was loaded above for schema/key_col — reuse it
            if head_manifest is None:
                return None
            rg = head_manifest.find_row_group(target)
            if rg is None:
                return None
            candidate_blob_hash = rg.blob_hash
            candidate_key_col = head_manifest.key_col

        if candidate_blob_hash is None:
            return None

        # Fetch + decode with key predicate
        blob_bytes = self.kernel.read_blob(candidate_blob_hash)
        if not blob_bytes:
            return None
        key_col = candidate_key_col
        if key_col:
            eff_columns = list(columns) if columns is not None else None
            if eff_columns is not None and key_col not in eff_columns:
                eff_columns = eff_columns + [key_col]
            col_data = PND2.decode(blob_bytes, columns=eff_columns,
                                     predicates=[(key_col, "=", key_val)])
        else:
            col_data = PND2.decode(blob_bytes, columns=columns)

        row_count = max((len(v) for v in col_data.values()), default=0)
        col_names = list(col_data.keys())
        for i in range(row_count):
            row = {c: col_data[c][i] if i < len(col_data[c]) else None
                    for c in col_names}
            if key_col and key_col in row:
                row_key = row[key_col]
                try:
                    if str(row_key) == str(key) or row_key == int(key):
                        if columns is not None and key_col not in columns:
                            row = {c: row[c] for c in columns if c in row}
                        return row
                except (ValueError, TypeError):
                    pass
        return None

    def scan_with_pruning(self, collection: str,
                           predicates: Optional[list[tuple[str, str, Any]]] = None,
                           manifest_hash: Optional[str] = None
                           ) -> Iterator[tuple[str, str, dict]]:
        """Low-level scan — yields (rg_key, blob_hash, stats_dict) for
        surviving row groups. The caller fetches and decodes the blobs.

        Useful for batch processing or when the caller wants to control
        the decode step.
        """
        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return

        for rg in manifest.scan_with_pruning(predicates):
            stats_dict = {c.name: (c.min, c.max, c.null_count)
                           for c in rg.columns}
            yield (rg.key, rg.blob_hash, stats_dict)

    def iter_rows(self, collection: str,
                  predicates: Optional[list[tuple[str, str, Any]]] = None,
                  columns: Optional[list[str]] = None,
                  batch_size: int = 1000,
                  manifest_hash: Optional[str] = None
                  ) -> Iterator[list[dict]]:
        """Streaming read — yields rows in batches without loading all into memory.

        This is the MEMORY-SAFE read path for large collections. Instead of
        returning list[dict] (which OOMs at 1B rows), this generator yields
        batches of `batch_size` rows at a time.

        Each batch is fetched from one row group (or a slice of one), decoded,
        and yielded. The caller processes the batch and discards it before the
        next batch is fetched.

        Args:
            collection: collection name
            predicates: list of (column, op, value) tuples for pruning
            columns: projection pushdown (None = all columns)
            batch_size: rows per batch (default 1000). Actual batch size
                may be larger if row groups are larger than batch_size.
            manifest_hash: for time-travel reads

        Yields:
            Lists of row dicts (batch_size rows at a time).

        Round trips: 3 + K S3 GETs cold (same as read()), but memory usage
        is O(batch_size) instead of O(total_rows).
        """
        manifest = self._load_manifest(collection, manifest_hash=manifest_hash)
        if manifest is None:
            return

        auto_filter = self._build_predicate_filter(predicates)

        # Fix (Round 14 Issue #1): ensure predicate columns are always decoded
        # even if not in the caller's projection (same fix as read()/read_as_columns())
        eff_columns = list(columns) if columns is not None else None
        if predicates and eff_columns is not None:
            pred_cols = {p[0] for p in predicates}
            missing = pred_cols - set(eff_columns)
            if missing:
                eff_columns = list(dict.fromkeys(eff_columns + list(missing)))

        for rg in manifest.scan_with_pruning(predicates):
            blob_bytes = self.kernel.read_blob(rg.blob_hash)
            col_data = PND2.decode(blob_bytes, columns=eff_columns,
                                     predicates=predicates)

            row_count = max((len(v) for v in col_data.values()), default=0)
            col_names = list(col_data.keys())

            # Yield in batches
            for start in range(0, row_count, batch_size):
                end = min(start + batch_size, row_count)
                batch = []
                for i in range(start, end):
                    row = {c: col_data[c][i] if i < len(col_data[c]) else None
                            for c in col_names}
                    if auto_filter is None or auto_filter(row):
                        # Strip predicate-only columns from the result
                        if columns is not None and eff_columns != columns:
                            row = {c: row[c] for c in columns if c in row}
                        batch.append(row)
                if batch:
                    yield batch


# ---------------------------------------------------------------------------
# Helpers — value encoding for PND2 stats
# ---------------------------------------------------------------------------

def _encode_pnd2_value(value_type: int, value: Any) -> bytes:
    """Encode a single min/max value as binary bytes (PND2 stats section)."""
    if value_type == VALUE_TYPE_INT64:
        return struct.pack("<q", int(value))
    if value_type == VALUE_TYPE_FLOAT64:
        return struct.pack("<d", float(value))
    if value_type == VALUE_TYPE_STRING:
        s = str(value).encode("utf-8")
        return struct.pack("<I", len(s)) + s
    if value_type == VALUE_TYPE_BINARY:
        b = bytes(value) if not isinstance(value, bytes) else value
        return struct.pack("<I", len(b)) + b
    return b""


def _decode_pnd2_value(value_type: int, data: bytes, pos: int) -> tuple[Any, int]:
    """Decode a single min/max value from PND2 stats section."""
    if value_type == VALUE_TYPE_INT64:
        v = struct.unpack("<q", data[pos:pos+8])[0]
        return v, pos + 8
    if value_type == VALUE_TYPE_FLOAT64:
        v = struct.unpack("<d", data[pos:pos+8])[0]
        return v, pos + 8
    if value_type == VALUE_TYPE_STRING:
        slen = struct.unpack("<I", data[pos:pos+4])[0]
        pos += 4
        s = data[pos:pos+slen].decode("utf-8")
        return s, pos + slen
    if value_type == VALUE_TYPE_BINARY:
        slen = struct.unpack("<I", data[pos:pos+4])[0]
        pos += 4
        b = bytes(data[pos:pos+slen])
        return b, pos + slen
    return None, pos


# ---------------------------------------------------------------------------
# Helpers — BINARY value type + source slicing/sorting + key formatting
# ---------------------------------------------------------------------------

# Row group key format: "rg/" + zero-padded numeric key.
# Padding to 20 digits supports up to 10^20 row groups — far beyond any
# realistic workload (1 PB at 100 MB/row group = 10^7 row groups).
# Without padding, lexicographic comparison breaks: "rg/9" > "rg/42"
# because "9" > "4". This silently corrupts point_lookup and range scans
# for collections with >10 row groups.
_RG_KEY_WIDTH = 20

# Fix (Round 20 Issue #1): bias for negative INT64 keys.
# f"{-3:020d}" = "-0000000000000000003" which sorts REVERSE lexicographically
# vs positive numbers. Fix: add INT64_MAX bias so all keys are non-negative.
# -3 → (2^63 - 1) + (-3) = 9223372036854775804 → "rg/09223372036854775804"
# This preserves numeric order in lexicographic comparison.
_INT64_BIAS = 2**63 - 1

def _format_rg_key(max_pk: Any) -> str:
    """Format a row group key with zero-padding for correct lexicographic ordering.

    For numeric keys: bias-encode (add INT64_MAX) then zero-pad to 20 digits.
    This ensures negative numbers sort correctly: -3 < -1 < 0 < 5 < 42.
    For string keys: "rg/" + key (no padding — strings compared as-is)
    For float keys: "rg/" + str(float) (string comparison, prefer INT64)

    Fix (Round 20 Issue #1): negative INT64 keys now sort correctly via
    bias encoding. Previously f"{-3:020d}" = "-000...3" sorted AFTER
    positive numbers lexicographically (because "-" > "0" in ASCII).
    """
    if isinstance(max_pk, int):
        return f"rg/{(max_pk + _INT64_BIAS):0{_RG_KEY_WIDTH}d}"
    if isinstance(max_pk, float):
        return f"rg/{max_pk}"
    try:
        return f"rg/{(int(max_pk) + _INT64_BIAS):0{_RG_KEY_WIDTH}d}"
    except (ValueError, TypeError):
        # Non-numeric string key — use as-is (caller sorted lexicographically)
        return f"rg/{max_pk}"


def _detect_value_type_with_binary(values: list) -> int:
    """Detect value type, including BINARY for raw bytes."""
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            return VALUE_TYPE_INT64
        if isinstance(v, int):
            return VALUE_TYPE_INT64
        if isinstance(v, float):
            return VALUE_TYPE_FLOAT64
        if isinstance(v, bytes):
            return VALUE_TYPE_BINARY
        return VALUE_TYPE_STRING  # default to string
    return VALUE_TYPE_NULL


def _encode_binary_raw(values: list, hint: str = "raw") -> tuple[bytes, dict]:
    """Encode a BINARY column as raw bytes (no RLE/DICT/BITPACK).

    Layout (after the 9-byte EncodingHeader):
      n_values(4B) + [length(4B) + bytes] * n_values
    """
    n_rows = len(values)
    payload = struct.pack("<I", n_rows)
    for v in values:
        if v is None:
            payload += struct.pack("<I", 0xFFFFFFFF)  # null sentinel
        else:
            b = v if isinstance(v, bytes) else bytes(v)
            payload += struct.pack("<I", len(b))
            payload += b

    header = EncodingHeader(ColumnEncoding.RAW, n_rows).to_bytes()
    meta = {"encoding": "raw", "n_rows": n_rows, "value_type": VALUE_TYPE_BINARY,
            "payload_size": len(payload)}
    return header + payload, meta


def _decode_binary_raw(payload: bytes, expected_n_rows: int) -> list:
    """Decode a BINARY column's raw payload.

    Layout: n_values(4B) + [length(4B) + bytes] * n_values

    Args:
        payload: the column's payload bytes (after the PND2 schema/stats
            sections, NOT including any PND1 header)
        expected_n_rows: the declared n_rows from the PND2 header

    Returns:
        List of values (bytes or None for nulls).
    """
    if len(payload) < 4:
        return []
    n_values = struct.unpack("<I", payload[:4])[0]
    pos = 4
    result = []
    for _ in range(n_values):
        if pos + 4 > len(payload):
            break
        (blen,) = struct.unpack("<I", payload[pos:pos+4])
        pos += 4
        if blen == 0xFFFFFFFF:
            result.append(None)  # null sentinel
        elif blen == 0:
            result.append(b"")  # empty bytes (not null)
        else:
            result.append(bytes(payload[pos:pos+blen]))
            pos += blen
    # Pad with None if we ran out of data (defensive)
    while len(result) < expected_n_rows:
        result.append(None)
    return result


def _binary_value_matches(val: Any, op: str, target: Any) -> bool:
    """Check if a BINARY value matches a predicate.

    Supports =, !=, and "in" (target is a list of bytes). Other ops
    return True (can't prune — caller should not filter).
    """
    if op == "=":
        if val is None or target is None:
            return val is None and target is None
        if isinstance(target, str):
            target = target.encode("utf-8")
        return val == target
    if op == "!=":
        if val is None or target is None:
            return not (val is None and target is None)
        if isinstance(target, str):
            target = target.encode("utf-8")
        return val != target
    if op == "in":
        if val is None:
            return False
        targets = [t.encode("utf-8") if isinstance(t, str) else t
                    for t in target]
        return val in targets
    # Unknown op — don't filter (return True so the row survives)
    return True


def _slice_source(source: ColumnSource, start: int, end: int) -> ColumnSource:
    """Slice a ColumnSource — returns a new source with rows [start, end)."""
    # For PyArrowColumnSource, we can slice the underlying table
    if isinstance(source, PyArrowColumnSource):
        return PyArrowColumnSource(source._table.slice(start, end - start))
    # For ListColumnSource, slice the rows list
    if isinstance(source, ListColumnSource):
        return ListColumnSource(source._rows[start:end])
    # Fallback: wrap in a SlicedSource
    return _SlicedSource(source, start, end)


class _SlicedSource:
    """A slice of a ColumnSource — used when the source doesn't natively support slicing."""
    def __init__(self, parent: ColumnSource, start: int, end: int):
        self._parent = parent
        self._start = start
        self._end = end

    def column_names(self) -> list[str]:
        return self._parent.column_names()

    def num_rows(self) -> int:
        return self._end - self._start

    def column_slice(self, name: str, start: int, end: int) -> list:
        return self._parent.column_slice(name,
                                           self._start + start,
                                           self._start + end)

    def column_stats(self, name: str) -> tuple:
        values = self.column_slice(name, 0, self.num_rows())
        return compute_list_stats(values)


def _sort_source_by(source: ColumnSource, key_col: str) -> ColumnSource:
    """Sort a ColumnSource by a column — returns a new sorted source."""
    # For PyArrowColumnSource, use PyArrow's sort_by
    if isinstance(source, PyArrowColumnSource):
        return PyArrowColumnSource(source._table.sort_by(key_col))
    # For ListColumnSource, sort in Python
    if isinstance(source, ListColumnSource):
        rows = sorted(source._rows, key=lambda r: (r.get(key_col) is None, r.get(key_col)))
        return ListColumnSource(rows)
    # Fallback: read all rows, sort, wrap in ListColumnSource
    n = source.num_rows()
    col_names = source.column_names()
    rows = []
    for i in range(n):
        row = {c: source.column_slice(c, i, i+1)[0] for c in col_names}
        rows.append(row)
    rows.sort(key=lambda r: (r.get(key_col) is None, r.get(key_col)))
    return ListColumnSource(rows)
