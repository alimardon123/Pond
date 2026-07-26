"""
EncodedChunkStorage — per-column-chunk storage with FastLanes-style encodings.

Combines ColumnChunkStorage (per-column-chunk blobs) with encoding.py
(structural encodings + encoded predicate eval). The result:

  Write side:
    - Splits each row group into per-column-chunk blobs (like ColumnChunkStorage)
    - For each chunk, picks the best encoding (RLE/Dict/Bitpack/Raw)
    - Stores the encoded bytes as a kernel blob
    - Tracks encoding choice + blob_hash in ColumnChunkStats

  Read side (encoding-aware predicate eval):
    - For each surviving chunk blob, peek at the encoding header
    - If the encoding supports direct predicate eval (RLE, Dict, Bitpack),
      evaluate the predicate on the ENCODED form — skip full decode
    - If the predicate can be fully pruned (e.g., bitpack min/max proves
      no matches), skip the chunk entirely
    - Otherwise, decode only the surviving row ranges and yield them

The win: for low-cardinality columns (RLE/Dict) and small-range ints
(Bitpack), we can prune chunks WITHOUT decoding them to PyArrow.

This is FastLanes-style "structural predicates": predicates that operate
on the encoded representation directly, eliminating the decode step for
pruned data.

Storage layout (chunk blob):
  +---------------------+
  | EncodingHeader (9B) |  magic(4) + encoding(1) + n_rows(4)
  +---------------------+
  | Payload (variable)  |  encoding-specific JSON
  +---------------------+

Storage layout (zone map blob's column_chunks stats):
  Each ColumnChunkStats now has:
    blob_hash: str       — kernel blob hash of the encoded chunk
    encoding: int        — 0=raw, 1=rle, 2=dict, 3=bitpack
    encoding_meta: dict  — encoding-specific stats (n_runs, n_unique, etc.)

GENERIC: works with any tabular data the lens can produce as Python lists.
"""

from __future__ import annotations

import os
import sys
import json
from typing import Optional, Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal
from column_chunk_zone_map import ColumnChunkZoneMap, ColumnChunkStats
from column_chunk_storage import ColumnChunkStorage
from encoding import (
    ColumnEncoding, EncodingHeader, encode_column,
    eval_predicate_encoded, decode_column,
)


class EncodedChunkStorage(ColumnChunkStorage):
    """Per-column-chunk storage with FastLanes-style structural encodings.

    Extends ColumnChunkStorage with:
      - Per-column encoding selection (RLE, Dict, Bitpack, Raw)
      - Encoded predicate evaluation (skip decode for pruned chunks)
      - Encoding-aware chunk stats in the zone map blob
    """

    def __init__(self, kernel: PondMinimal):
        super().__init__(kernel)

    def write_row_group_encoded(
            self,
            table,
            row_group_key: str,
            chunk_size: int = 1000,
            encoding_hints: Optional[dict[str, str]] = None) -> tuple[str, ColumnChunkZoneMap]:
        """Split a row group into per-column-chunk ENCODED blobs.

        Like ColumnChunkStorage.write_row_group_column_chunks, but each
        chunk blob is encoded with the best (or hinted) encoding.

        Args:
            table: PyArrow Table (a single row group)
            row_group_key: the ProllyTreeIndex key for this row group
            chunk_size: rows per column chunk (default 1000)
            encoding_hints: optional dict {column_name: "auto"|"rle"|"dict"|"bitpack"|"raw"}
                If None, all columns use "auto".

        Returns:
            Tuple of (manifest_blob_hash, ColumnChunkZoneMap).
            The cczm.column_chunks[col][i] stats include blob_hash,
            encoding, and encoding_meta fields.
        """
        if encoding_hints is None:
            encoding_hints = {}

        n_rows = table.num_rows
        cczm = ColumnChunkZoneMap(row_group_key=row_group_key)
        chunk_hashes_per_col: dict[str, list[str]] = {}

        import pyarrow.compute as pc

        for col_name in table.column_names:
            column = table[col_name]
            chunk_hashes: list[str] = []
            chunk_stats: list[ColumnChunkStats] = []
            hint = encoding_hints.get(col_name, "auto")

            for start in range(0, n_rows, chunk_size):
                end = min(start + chunk_size, n_rows)
                chunk = column.slice(start, end - start)
                values = chunk.to_pylist()

                # Encode the chunk
                encoded_bytes, enc_meta = encode_column(values, hint=hint)
                chunk_blob_hash = self.kernel.write(encoded_bytes)
                chunk_hashes.append(chunk_blob_hash)

                # Build chunk stats with encoding info
                stats = ColumnChunkStats(
                    chunk_index=len(chunk_stats),
                    row_count=end - start,
                    blob_hash=chunk_blob_hash,
                )

                # Min/max/null_count (for row-group zone map compat)
                try:
                    null_count = pc.sum(pc.is_null(chunk)).as_py()
                    stats.null_count = null_count
                    if null_count < len(chunk):
                        stats.min = pc.min(chunk).as_py()
                        stats.max = pc.max(chunk).as_py()
                except Exception:
                    pass

                # Augment with encoding metadata
                # We piggyback on the ColumnChunkStats dataclass by
                # attaching extra attributes via to_dict (which only
                # serializes known fields, so we use a sidecar dict
                # in the zone map blob — see _write_zone_map_with_encoding)
                chunk_stats.append(stats)

            cczm.column_chunks[col_name] = chunk_stats
            chunk_hashes_per_col[col_name] = chunk_hashes

        # Build manifest blob: includes chunk blob hashes + encoding info
        manifest = {
            "row_group_key": row_group_key,
            "row_count": n_rows,
            "chunk_size": chunk_size,
            "column_chunks": chunk_hashes_per_col,
        }
        manifest_bytes = json.dumps(manifest, sort_keys=True,
                                     default=str).encode()
        manifest_blob_hash = self.kernel.write(manifest_bytes)

        # Attach encoding metadata to cczm via a sidecar dict
        # (the ColumnChunkStats dataclass is shared with non-encoded
        # storage; we don't want to bloat it with encoding-only fields.
        # Instead, we'll attach encoding info when serializing cczm.)
        cczm._encoding_meta = self._build_encoding_meta(table, chunk_size,
                                                         encoding_hints)

        return manifest_blob_hash, cczm

    def _build_encoding_meta(self, table, chunk_size: int,
                              encoding_hints: dict) -> dict:
        """Build a sidecar dict mapping (col, chunk_index) → encoding meta.

        This is used at zone-map-write time to embed encoding info in
        the zone map blob alongside the ColumnChunkStats.
        """
        meta: dict[str, list[dict]] = {}
        n_rows = table.num_rows
        for col_name in table.column_names:
            column = table[col_name]
            hint = encoding_hints.get(col_name, "auto")
            chunks_meta = []
            for start in range(0, n_rows, chunk_size):
                end = min(start + chunk_size, n_rows)
                chunk = column.slice(start, end - start)
                values = chunk.to_pylist()
                _, enc_meta = encode_column(values, hint=hint)
                chunks_meta.append(enc_meta)
            meta[col_name] = chunks_meta
        return meta

    def read_column_chunks_encoded(
            self,
            cczm: ColumnChunkZoneMap,
            columns: list[str],
            surviving_chunk_indices: Optional[set[int]] = None,
            predicates: Optional[list[tuple[str, str, Any]]] = None,
            ) -> dict[str, list[list[Any]]]:
        """Read column chunks with encoding-aware predicate eval.

        For each surviving chunk:
          1. Peek at the encoding header
          2. If encoding supports direct predicate eval AND we have a
             predicate for this column, evaluate it on the encoded form
          3. If predicate prunes all rows in the chunk, skip the chunk
          4. Otherwise, decode only the surviving row ranges

        Args:
            cczm: ColumnChunkZoneMap for the row group
            columns: list of column names to read
            surviving_chunk_indices: set of chunk indices to read
                (after column-chunk zone-map pruning). If None, read all.
            predicates: list of (column, op, value) tuples for encoded
                predicate eval. If None, decode full chunks.

        Returns:
            Dict of column_name → list of (chunk_index, surviving_rows)
            pairs. surviving_rows is a list of values (decoded only for
            the rows that survived encoded predicate eval).
        """
        result: dict[str, list[tuple[int, list[Any]]]] = {}

        # Build predicate lookup
        pred_lookup: dict[str, tuple[str, Any]] = {}
        if predicates:
            for col, op, val in predicates:
                pred_lookup[col] = (op, val)

        for col_name in columns:
            if col_name not in cczm.column_chunks:
                continue

            chunk_stats = cczm.column_chunks[col_name]
            chunks_result: list[tuple[int, list[Any]]] = []

            for stats in chunk_stats:
                if surviving_chunk_indices is not None and \
                        stats.chunk_index not in surviving_chunk_indices:
                    continue  # SKIP — chunk pruned by zone-map

                if stats.blob_hash is None:
                    return {}  # fall back to caller

                blob_bytes = self.kernel.read_blob(stats.blob_hash)

                # Try encoded predicate eval
                if col_name in pred_lookup:
                    op, val = pred_lookup[col_name]
                    encoded_result = eval_predicate_encoded(
                        blob_bytes, col_name, op, val)
                    if encoded_result is not None:
                        surviving_ranges, _ = encoded_result
                        if not surviving_ranges:
                            # Chunk fully pruned by encoded eval
                            chunks_result.append((stats.chunk_index, []))
                            continue
                        # Decode only surviving ranges
                        all_values = decode_column(blob_bytes)
                        surviving_values = []
                        for s, e in surviving_ranges:
                            surviving_values.extend(all_values[s:e])
                        chunks_result.append(
                            (stats.chunk_index, surviving_values))
                        continue

                # Fallback: decode the whole chunk
                values = decode_column(blob_bytes)
                chunks_result.append((stats.chunk_index, values))

            result[col_name] = chunks_result

        return result

    @staticmethod
    def has_encoded_storage(zm_dict: dict) -> bool:
        """Check if a zone map blob indicates encoded chunk storage.

        Returns True if column_chunks stats have blob_hash fields AND
        the zone map blob includes _encoding_meta sidecar.
        """
        if not ColumnChunkStorage.has_column_chunk_storage(zm_dict):
            return False
        # Check for encoding meta sidecar (attached by write_row_group_encoded)
        cczm_dict = zm_dict["column_chunks"]
        return "_encoding_meta" in cczm_dict or \
               any("encoding" in chunk for chunks in
                   cczm_dict.get("column_chunks", {}).values()
                   for chunk in chunks)
