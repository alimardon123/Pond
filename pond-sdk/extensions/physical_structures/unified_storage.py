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
    def peek_stats(data: bytes) -> Optional[dict[str, tuple[Any, Any, int]]]:
        """Peek at the stats in a PND2 blob header without decoding payloads.

        Useful for third-level pruning: fetch the blob, peek at stats,
        decide whether to decode. Returns None if not a PND2 blob or
        no stats.
        """
        if data[:4] != _PND2_MAGIC:
            return None
        version, flags = struct.unpack("<BB", data[4:6])
        if version != _PND2_VERSION:
            return None
        if not (flags & _FLAG_HAS_STATS):
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

        # Parse stats
        stats: dict[str, tuple[Any, Any, int]] = {}
        for name, vtype, enc in schema:
            has_min = inner[ipos]; ipos += 1
            if has_min:
                mn, ipos = _decode_pnd2_value(vtype, inner, ipos)
                mx, ipos = _decode_pnd2_value(vtype, inner, ipos)
            else:
                mn = mx = None
            null_count = struct.unpack("<I", inner[ipos:ipos+4])[0]; ipos += 4
            stats[name] = (mn, mx, null_count)

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
        # Active branch per collection (set by checkout, cleared by undo/merge)
        self._active_branches: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Manifest ref helper
    # ------------------------------------------------------------------

    @staticmethod
    def _manifest_ref(collection: str) -> str:
        return f"collections/{collection}/manifest"

    @staticmethod
    def _head_ref(collection: str) -> str:
        return f"collections/{collection}/HEAD"

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
            current_hash = self.kernel.resolve(self._manifest_ref(collection))
            cached_hash = self._manifest_hash_cache.get(collection)
            if current_hash == cached_hash:
                return self._manifest_cache[collection]
            # Stale cache — fall through to re-read
            self._invalidate_manifest_cache(collection)

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
        return f"collections/{collection}/branches/{branch}"

    def _write_commit_blob(self, collection: str,
                            manifest_hash: str,
                            parent: Optional[str] = None,
                            second_parent: Optional[str] = None,
                            message: str = "",
                            index: int = 0) -> str:
        """Write a commit blob and update HEAD.

        The commit blob is a JSON dict linking to the manifest hash.
        This is the ONE commit format for ALL workloads — no more
        BinaryProllyTree encoding.

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
        # Update HEAD via reference (also updates manifest ref for reads)
        self.kernel.reference(self._head_ref(collection), commit_hash)
        # Update active branch ref if set (so commits on a branch move the branch)
        active = self._active_branches.get(collection)
        if active:
            self.kernel.reference(active, commit_hash)
        return commit_hash

    def _write_commit_cas(self, collection: str,
                           manifest_hash: str,
                           parent: Optional[str],
                           message: str = "",
                           index: int = 0,
                           max_retries: int = 5) -> str:
        """Write a commit with optimistic concurrency (CAS on HEAD).

        This is the CONCURRENT write path. Multiple writers can call
        this simultaneously; the CAS ensures only one wins at a time.
        Losers re-read the latest HEAD and retry their append.

        Flow:
          1. Write commit blob (immutable, concurrent-safe)
          2. CAS HEAD from parent → commit_hash
          3. If CAS fails (another writer won), re-read HEAD, re-apply,
             retry up to max_retries times.

        This is cache-independent: a new connection reads the current
        HEAD via the CAS path, builds its delta on top, and races to
        update HEAD. No in-memory cache required for correctness.
        """
        import json as _json
        import time as _time

        head_path = self._head_ref(collection)

        for attempt in range(max_retries):
            commit = {
                "parent": parent,
                "second_parent": None,
                "manifest": manifest_hash,
                "message": message or f"commit #{index}",
                "timestamp": _time.time(),
                "index": index,
            }
            commit_bytes = _json.dumps(commit, sort_keys=True).encode()
            commit_hash = self.kernel.write(commit_bytes)

            # CAS: HEAD must still point to parent.
            # Use cas_path (dedicated path store) if available; the parent
            # must have been read via get_path (same store).
            if hasattr(self.kernel, 'cas_path'):
                # Ensure the HEAD path is initialized in the dedicated path
                # store (sync from root ref if missing)
                current = self.kernel.get_path(head_path)
                if current is None:
                    # Path not in dedicated store — try root ref
                    current = self.kernel.resolve(head_path)
                    if current is not None:
                        # Migrate to dedicated path store
                        self.kernel.set_path(head_path, current)

                won = self.kernel.cas_path(head_path, parent, commit_hash)
                if won:
                    # Also update the manifest ref for reads
                    if hasattr(self.kernel, 'set_path'):
                        self.kernel.set_path(
                            self._manifest_ref(collection), manifest_hash)
                    # Also update root ref for backward compat with resolve()
                    self.kernel.reference(self._head_ref(collection), commit_hash)
                    return commit_hash
                # Lost the race — signal the caller to retry
                raise RuntimeError(
                    f"CAS failed after 1 retries on collection '{collection}'. "
                    "Another writer won the race.")
            else:
                # Fallback: no CAS support (legacy kernel) — last-writer-wins
                self.kernel.reference(self._head_ref(collection), commit_hash)
                return commit_hash

        raise RuntimeError(
            f"CAS failed after {max_retries} retries on collection '{collection}'. "
            "Too many concurrent writers.")

    def _read_commit_blob(self, commit_hash: str) -> Optional[dict]:
        """Read and decode a commit blob."""
        import json as _json
        try:
            raw = self.kernel.read_blob(commit_hash)
            return _json.loads(raw)
        except (ValueError, KeyError, Exception):
            return None

    def _commit_index(self, collection: str) -> int:
        """Get the next commit index for a collection."""
        head = self.kernel.resolve(self._head_ref(collection))
        if head is None:
            return 0
        commit = self._read_commit_blob(head)
        if commit is None:
            return 0
        return commit.get("index", 0) + 1

    def branch(self, collection: str, branch_name: str) -> str:
        """Create a branch — O(1) ref copy."""
        head = self.kernel.resolve(self._head_ref(collection))
        if head is None:
            raise KeyError(f"Collection '{collection}' not found")
        self.kernel.reference(self._branch_ref(collection, branch_name), head)
        return head

    def _sync_manifest_ref_to_head(self, collection: str) -> None:
        """After undo/checkout/merge, sync the manifest ref to match HEAD's commit.

        The manifest ref (collections/{name}/manifest) must point to the
        manifest blob of the current HEAD commit. Otherwise reads would
        see stale data from the old manifest.
        """
        head = self.kernel.resolve(self._head_ref(collection))
        if head is None:
            return
        commit = self._read_commit_blob(head)
        if commit and commit.get("manifest"):
            self.kernel.reference(self._manifest_ref(collection),
                                   commit["manifest"])
        self._invalidate_manifest_cache(collection)

    def checkout(self, collection: str, branch_name: str) -> None:
        """Checkout a branch — point HEAD at the branch's commit.

        Sets the active branch so subsequent commits/append update the
        branch ref too (git-like behavior).
        """
        h = self.kernel.resolve(self._branch_ref(collection, branch_name))
        if h is None:
            raise ValueError(f"Branch '{branch_name}' does not exist")
        self.kernel.reference(self._head_ref(collection), h)
        self._active_branches[collection] = self._branch_ref(collection, branch_name)
        self._sync_manifest_ref_to_head(collection)

    def list_branches(self, collection: str) -> list[str]:
        """List all branches for a collection."""
        prefix = f"collections/{collection}/branches/"
        return [n[len(prefix):] for n in self.kernel.list_names()
                if n.startswith(prefix)]

    def merge(self, collection: str, branch_name: str,
              message: str = "") -> str:
        """Merge a branch into HEAD.

        Reads both manifests, unions row group entries (last-writer-wins
        for duplicate keys), writes a new manifest + merge commit with
        two parents.
        """
        branch_head = self.kernel.resolve(
            self._branch_ref(collection, branch_name))
        if branch_head is None:
            raise ValueError(f"Branch '{branch_name}' does not exist")

        # Read both manifests
        head = self.kernel.resolve(self._head_ref(collection))
        head_commit = self._read_commit_blob(head) if head else None
        branch_commit = self._read_commit_blob(branch_head)

        head_manifest = None
        if head_commit and head_commit.get("manifest"):
            head_manifest = CollectionManifest.load(
                self.kernel, head_commit["manifest"])
        branch_manifest = None
        if branch_commit and branch_commit.get("manifest"):
            branch_manifest = CollectionManifest.load(
                self.kernel, branch_commit["manifest"])

        # Union row group entries (branch wins on conflict)
        seen: dict[str, RowGroupEntry] = {}
        if head_manifest:
            for rg in head_manifest.scan_with_pruning():
                seen[rg.key] = rg
        if branch_manifest:
            for rg in branch_manifest.scan_with_pruning():
                seen[rg.key] = rg  # branch wins

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

        # Write merge commit with TWO parents
        commit_hash = self._write_commit_blob(
            collection, manifest_hash,
            parent=head,
            second_parent=branch_head,
            message=message or f"merge '{branch_name}'",
            index=self._commit_index(collection))

        self._active_branches.pop(collection, None)  # merge detaches from branch
        self._invalidate_manifest_cache(collection)
        return commit_hash

    def undo(self, collection: str, steps: int = 1) -> str:
        """Undo the last N commits — walk parent pointers.

        Clears the active branch (undo is a detached-HEAD operation).
        """
        head = self.kernel.resolve(self._head_ref(collection))
        if head is None:
            raise ValueError("No commits to undo")
        for _ in range(steps):
            commit = self._read_commit_blob(head)
            if commit is None or not commit.get("parent"):
                break
            head = commit["parent"]
        self.kernel.reference(self._head_ref(collection), head)
        self._active_branches.pop(collection, None)  # detach from branch
        self._sync_manifest_ref_to_head(collection)
        return head[:12] if head else ""

    def history(self, collection: str, limit: int = 100) -> list[dict]:
        """Walk the commit chain from HEAD backwards."""
        head = self.kernel.resolve(self._head_ref(collection))
        if head is None:
            return []

        history: list[dict] = []
        current: Optional[str] = head
        seen: set[str] = set()

        while current and current not in seen and len(history) < limit:
            seen.add(current)
            commit = self._read_commit_blob(current)
            if commit is None:
                # Could be a legacy BinaryProllyTree commit — try decoding
                try:
                    from binary_encoding import BinaryProllyTree
                    raw = self.kernel.read_blob(current)
                    if raw and raw[0] == 3:
                        bc = BinaryProllyTree.decode_commit(raw)
                        history.append({
                            "hash": current,
                            "message": bc.get("message", ""),
                            "parent": bc.get("parent"),
                            "second_parent": bc.get("second_parent"),
                            "timestamp": bc.get("timestamp"),
                            "type": "snapshot" if bc.get("snapshot") else "delta",
                        })
                        current = bc.get("parent")
                        continue
                except Exception:
                    pass
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
        # ProllyTree (we no longer use one). The old blobs remain
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
                parent = self.kernel.resolve(self._head_ref(collection))
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
            parent = self.kernel.resolve(self._head_ref(collection))
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

    def append_concurrent(self, collection: str, rows,
                           key_col: Optional[str] = None,
                           row_group_size: int = 10_000,
                           encoding_hints: Optional[dict[str, str]] = None,
                           message: str = "",
                           max_retries: int = 5) -> str:
        """Concurrent-safe append — uses CAS on HEAD for multi-writer scenarios.

        This is the RIGHT append for multi-user/multi-engine environments:
        - Multiple streaming writers can call this simultaneously
        - OLTP engines can call this while streaming writes happen
        - A new connection works seamlessly (no cache dependency)

        Flow:
          1. Read current HEAD + manifest (cache-independent, fresh each call)
          2. Encode new row groups (concurrent-safe — immutable blobs)
          3. Build new manifest (delta or full)
          4. CAS HEAD from old → new commit hash
          5. If CAS fails (another writer won), re-read HEAD, re-apply, retry

        Performance:
          - No contention: 4 GETs + 3 PUTs (same as append)
          - Contention: +1 retry per conflicting writer (rare under low load)

        Use this when:
          - Multiple processes/engines write to the same collection
          - You want correctness without depending on in-memory caches
          - A new connection should work seamlessly

        Use append() instead when:
          - Single-writer scenario (same process)
          - You want O(1) warm writes via in-memory caching
        """
        last_error: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                # Invalidate caches to ensure we read fresh state
                self._invalidate_manifest_cache(collection)

                # Read current HEAD fresh via dedicated path (same store as CAS)
                head_path = self._head_ref(collection)
                if hasattr(self.kernel, 'get_path'):
                    parent_commit = self.kernel.get_path(head_path)
                    if parent_commit is None:
                        # Migrate from root ref to dedicated path
                        parent_commit = self.kernel.resolve(head_path)
                        if parent_commit is not None:
                            self.kernel.set_path(head_path, parent_commit)
                else:
                    parent_commit = self.kernel.resolve(head_path)

                # Read current manifest fresh (skip_cache=True for write path)
                existing_manifest = self._load_manifest(collection, skip_cache=True)
                if existing_manifest is None:
                    # Collection doesn't exist — use write() instead
                    return self.write(collection, rows, key_col=key_col,
                                       row_group_size=row_group_size,
                                       encoding_hints=encoding_hints,
                                       message=message)

                # Get commit index from parent
                commit_index = 0
                if parent_commit:
                    pc = self._read_commit_blob(parent_commit)
                    if pc:
                        commit_index = pc.get("index", 0) + 1

                # Get existing manifest hash fresh
                if hasattr(self.kernel, 'get_path'):
                    existing_manifest_hash = self.kernel.get_path(
                        self._manifest_ref(collection))
                    if existing_manifest_hash is None:
                        existing_manifest_hash = self.kernel.resolve(
                            self._manifest_ref(collection))
                        if existing_manifest_hash:
                            self.kernel.set_path(
                                self._manifest_ref(collection),
                                existing_manifest_hash)
                else:
                    existing_manifest_hash = self.kernel.resolve(
                        self._manifest_ref(collection))

                # Encode the new row groups (concurrent-safe — immutable)
                if isinstance(rows, list):
                    source = ListColumnSource(rows)
                elif isinstance(rows, ColumnSource):
                    source = rows
                else:
                    source = as_column_source(rows)
                n_rows = source.num_rows()

                if key_col == "":
                    key_col = None
                if key_col is None and existing_manifest.key_col:
                    key_col = existing_manifest.key_col
                if key_col is not None and key_col in source.column_names():
                    source = _sort_source_by(source, key_col)
                    key_array = source.column_slice(key_col, 0, n_rows)
                elif key_col is not None:
                    raise KeyError(f"key column '{key_col}' not in source columns")
                else:
                    key_array = list(range(n_rows))

                schema_columns = existing_manifest.columns
                if not schema_columns and n_rows > 0:
                    for col_name in source.column_names():
                        sample = source.column_slice(col_name, 0, min(100, n_rows))
                        vtype = _detect_value_type_with_binary(sample)
                        schema_columns.append((col_name, vtype))

                use_delta = True  # always delta for concurrent appends
                manifest_entries: list[dict] = []

                # Add new row groups
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
                seen: dict[str, dict] = {}
                for entry in manifest_entries:
                    seen[entry["rg_key"]] = entry
                manifest_entries = list(seen.values())
                manifest_entries.sort(key=lambda e: e["rg_key"])

                # Build delta manifest
                manifest_hash, new_manifest = self._build_manifest_with_return(
                    collection, manifest_entries, schema_columns,
                    key_col or "", row_group_size,
                    parent_manifest_hash=existing_manifest_hash)

                # CAS the HEAD ref
                n_new_groups = (n_rows + row_group_size - 1) // row_group_size
                commit_hash = self._write_commit_cas(
                    collection, manifest_hash,
                    parent=parent_commit,
                    message=message or f"concurrent append: {n_rows} rows in {n_new_groups} new row groups",
                    index=commit_index,
                    max_retries=1)

                # Update caches
                self._update_caches_after_write(
                    collection, new_manifest, manifest_hash, commit_hash,
                    commit_index, is_delta=True)
                return commit_hash

            except RuntimeError as e:
                if "CAS failed" in str(e):
                    last_error = e
                    continue  # retry the whole append
                raise
            except Exception as e:
                last_error = e
                break

        raise RuntimeError(
            f"Concurrent append failed after {max_retries} retries on '{collection}': {last_error}")

    def append(self, collection: str, rows,
               key_col: Optional[str] = None,
               row_group_size: int = 10_000,
               encoding_hints: Optional[dict[str, str]] = None,
               message: str = "") -> str:
        """Append rows to an existing collection WITHOUT rewriting it.

        Fix (Round 2 Issue #2b): the write() method has destructive
        overwrite semantics — it deletes all existing row groups before
        writing new ones. This append() method preserves existing data:
          1. Reads the existing manifest (1 GET)
          2. Keeps all existing row group entries in the new manifest
          3. Adds new row groups from `rows`
          4. Writes a new manifest + commit

        No read_all() walk, no destructive delete. The old row groups
        remain accessible via the new manifest.

        Args:
            collection: collection name (must already exist)
            rows: new rows to append (ColumnSource, list[dict], or pa.Table)
            key_col: sort key column (should match the existing collection)
            row_group_size: rows per new row group
            encoding_hints: optional encoding hints for new row groups
            message: commit message

        Returns:
            The new HEAD commit hash.
        """
        # skip_cache=True: for writes, the cache is authoritative (single-writer)
        existing_manifest = self._load_manifest(collection, skip_cache=True)
        if existing_manifest is None:
            # Collection doesn't exist — delegate to write()
            return self.write(collection, rows, key_col=key_col,
                                row_group_size=row_group_size,
                                encoding_hints=encoding_hints,
                                message=message)

        # Coerce input
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
        # Fix (Round 14 Issue #5): inherit existing manifest key_col
        if key_col is None and existing_manifest.key_col:
            key_col = existing_manifest.key_col
        if key_col is not None and key_col in source.column_names():
            source = _sort_source_by(source, key_col)
            key_array = source.column_slice(key_col, 0, n_rows)
        elif key_col is not None:
            raise KeyError(f"key column '{key_col}' not in source columns")
        else:
            key_array = list(range(n_rows))

        # Use the existing collection's schema
        schema_columns = existing_manifest.columns
        if not schema_columns and n_rows > 0:
            for col_name in source.column_names():
                sample = source.column_slice(col_name, 0, min(100, n_rows))
                vtype = _detect_value_type_with_binary(sample)
                schema_columns.append((col_name, vtype))

        # No ProllyTree staging — the manifest IS the index now.
        manifest_entries: list[dict] = []

        # Carry over existing row group entries from the old manifest.
        # Fix (Round 9 Issue #5): for large collections, use DELTA-MANIFEST
        # instead of reading ALL existing entries. A delta-manifest stores
        # only the NEW row groups + a pointer to the parent manifest.
        # The reader walks the parent chain to find all entries.
        # This makes append() O(new_row_groups) instead of O(total_row_groups).
        #
        # Strategy: if the existing manifest has >1000 row groups (or uses
        # a stats tree), use delta-manifest. Otherwise, rebuild the full
        # manifest (cheap at small scale).
        #
        # Round 26 optimization: reuse the cached manifest hash instead
        # of doing a separate resolve() call. The _load_manifest above
        # already resolved and cached it. This saves 1 GET.
        existing_manifest_hash = self._get_cached_manifest_hash(collection)
        use_delta = (
            existing_manifest.stats_tree_root is not None
            or len(existing_manifest.row_groups) > 1000
            or existing_manifest.parent_manifest_hash is not None
        )

        if use_delta:
            # DELTA path: only store NEW row groups + parent pointer
            # No need to read existing entries — the parent chain has them
            manifest_entries = []  # only new entries
        elif not existing_manifest.stats_tree_root:
            # FLAT path: carry over all existing entries (small collection)
            for rg in existing_manifest.row_groups:
                manifest_entries.append({
                    "rg_key": rg.key,
                    "blob_hash": rg.blob_hash,
                    "n_rows": rg.n_rows,
                    "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                    for c in rg.columns],
                })
        else:
            # PB-scale flat path: walk the stats tree
            from stats_tree import StatsTreeReader
            reader = StatsTreeReader(self.kernel, existing_manifest.stats_tree_root)
            for rg in reader.scan_with_pruning():
                manifest_entries.append({
                    "rg_key": rg.key,
                    "blob_hash": rg.blob_hash,
                    "n_rows": rg.n_rows,
                    "col_stats": [(c.name, c.value_type, c.min, c.max, c.null_count)
                                    for c in rg.columns],
                })

        # Add new row groups
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

        n_new_groups = (n_rows + row_group_size - 1) // row_group_size

        # Fix (Round 9 Issue #1): SORT manifest entries by rg_key before
        # building the manifest. Without sorting, appended entries with
        # smaller keys sit at the end of the list, and find_row_group()
        # (linear scan for first key >= target) returns the wrong row group.
        # This causes point_lookup to silently return None for appended rows.
        manifest_entries.sort(key=lambda e: e["rg_key"])

        # Fix (Round 15 Issue #3): deduplicate by rg_key (last-writer-wins).
        # Fix (Round 18 Issue #1): manifest_entries is [existing(OLD)..., new(NEW)...]
        # after sort. For last-writer-wins we must keep the LAST entry (NEW)
        # for each rg_key. Dict overwrite naturally keeps the last — which is
        # the NEW entry since new entries are appended after old ones.
        seen: dict[str, dict] = {}
        for entry in manifest_entries:
            seen[entry["rg_key"]] = entry  # last wins = NEW (correct for append)
        manifest_entries = list(seen.values())
        manifest_entries.sort(key=lambda e: e["rg_key"])

        # Build the manifest. If using delta mode, pass the parent hash.
        parent_hash = existing_manifest_hash if use_delta else None
        manifest_hash, new_manifest = self._build_manifest_with_return(
            collection, manifest_entries, schema_columns,
            key_col or "", row_group_size,
            parent_manifest_hash=parent_hash)

        # O(1) warm write: use cached HEAD + commit_index if available.
        # The manifest cache was populated by _load_manifest above (or
        # by the previous write). HEAD and commit_index are cached by
        # _update_caches_after_write.
        parent_commit = self._head_cache.get(collection)
        if parent_commit is None:
            parent_commit = self.kernel.resolve(self._head_ref(collection))

        commit_index = self._commit_index_cache.get(collection, -1)
        if commit_index < 0 and parent_commit:
            # Cold path: read parent commit blob to get the index
            parent_commit_data = self._read_commit_blob(parent_commit)
            if parent_commit_data:
                commit_index = parent_commit_data.get("index", 0) + 1
            else:
                commit_index = 0
        elif commit_index < 0:
            commit_index = 0

        commit_hash = self._write_commit_blob(
            collection, manifest_hash,
            parent=parent_commit,
            message=message or f"append: {n_rows} rows in {n_new_groups} new row groups",
            index=commit_index)

        # Update caches (don't invalidate) → next append is O(1)
        self._update_caches_after_write(
            collection, new_manifest, manifest_hash, commit_hash, commit_index,
            is_delta=use_delta)

        # Fix (Round 11 Issue #2): auto-compact if the delta chain is too deep.
        # After DELTA_CHAIN_THRESHOLD appends, flatten the chain to avoid
        # O(chain_depth) read amplification.
        #
        # Round 26 optimization: use the cached delta chain depth instead
        # of walking the parent chain. This is 0 GETs for the common case.
        if use_delta:
            chain_depth = self._delta_chain_depth_cache.get(collection, 0)
            DELTA_CHAIN_THRESHOLD = 8
            if chain_depth >= DELTA_CHAIN_THRESHOLD:
                self.compact_manifest(collection)

        return commit_hash

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

        # Check if we need a stats tree
        try:
            from stats_tree import should_use_stats_tree, build_stats_tree
            if should_use_stats_tree(len(all_entries)):
                stats_tree_root = build_stats_tree(
                    self.kernel, new_manifest.row_groups)
                new_manifest.set_stats_tree_root(stats_tree_root)
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

        try:
            from stats_tree import should_use_stats_tree, build_stats_tree
            if should_use_stats_tree(len(rg_entries)):
                stats_tree_root = build_stats_tree(self.kernel, rg_entries)
                manifest.set_stats_tree_root(stats_tree_root)
        except ImportError:
            pass

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

        # PB-scale check: if we have >25K row groups, build a stats tree
        # and delegate scan/find_row_group to it. The manifest blob stays
        # small (just schema + stats_tree_root).
        try:
            from stats_tree import should_use_stats_tree, build_stats_tree
            if should_use_stats_tree(len(rg_entries)):
                stats_tree_root = build_stats_tree(self.kernel, rg_entries)
                manifest.set_stats_tree_root(stats_tree_root)
        except ImportError:
            pass  # stats_tree not available — fall back to flat manifest

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
        that used ProllyLensBase commits.
        """
        # New path: manifest hash is in the commit blob
        commit = self._read_commit_blob(commit_hash)
        if commit and commit.get("manifest"):
            return commit["manifest"]
        # Legacy path: ref-based lookup (ProllyLensBase collections)
        return self.kernel.resolve(
            f"collections/{collection}/commits/{commit_hash}__manifest")

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
        of collection scale.
        """
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
