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
    eval_predicate_encoded, decode_column, decode_surviving_values,
)
from compression import compress_blob, decompress_blob


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
            table_or_source,
            row_group_key: str,
            chunk_size: int = 1000,
            encoding_hints: Optional[dict[str, str]] = None) -> tuple[str, ColumnChunkZoneMap]:
        """Split a row group into per-column-chunk ENCODED blobs.

        Format-agnostic (design review C4 fix): accepts either a PyArrow
        Table (auto-wrapped) or any ColumnSource. Each chunk is encoded
        with the best (or hinted) FastLanes-style encoding.

        Like ColumnChunkStorage.write_row_group_column_chunks, but each
        chunk blob is encoded with the best (or hinted) encoding.

        Args:
            table_or_source: PyArrow Table OR ColumnSource (a single row
                group's worth of data)
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

        from column_source import as_column_source, compute_list_stats
        source = as_column_source(table_or_source)

        n_rows = source.num_rows()
        cczm = ColumnChunkZoneMap(row_group_key=row_group_key)
        chunk_hashes_per_col: dict[str, list[str]] = {}
        # Sidecar: col_name → list of encoding_meta dicts (one per chunk).
        # Collected during the main encode loop so we don't pay for a
        # second encode pass.
        encoding_meta_per_col: dict[str, list[dict]] = {}

        for col_name in source.column_names():
            chunk_hashes: list[str] = []
            chunk_stats: list[ColumnChunkStats] = []
            chunk_enc_metas: list[dict] = []
            hint = encoding_hints.get(col_name, "auto")

            for start in range(0, n_rows, chunk_size):
                end = min(start + chunk_size, n_rows)
                values = source.column_slice(col_name, start, end)

                # Encode the chunk — enc_meta is reused (no second encode)
                encoded_bytes, enc_meta = encode_column(values, hint=hint)
                # Compress the encoded bytes before writing to the kernel.
                # This is a LENS-LEVEL responsibility (not kernel).
                # The compression layer is transparent: readers decompress
                # before parsing PND1. Compresses with zstd by default.
                compressed_bytes = compress_blob(encoded_bytes)
                chunk_blob_hash = self.kernel.write(compressed_bytes)
                chunk_hashes.append(chunk_blob_hash)
                chunk_enc_metas.append(enc_meta)

                # Build chunk stats with encoding info (format-agnostic)
                mn, mx, null_count = compute_list_stats(values)
                stats = ColumnChunkStats(
                    chunk_index=len(chunk_stats),
                    row_count=end - start,
                    blob_hash=chunk_blob_hash,
                    min=mn,
                    max=mx,
                    null_count=null_count,
                )

                chunk_stats.append(stats)

            cczm.column_chunks[col_name] = chunk_stats
            chunk_hashes_per_col[col_name] = chunk_hashes
            encoding_meta_per_col[col_name] = chunk_enc_metas

        # Build manifest blob: includes chunk blob hashes per column
        manifest = {
            "row_group_key": row_group_key,
            "row_count": n_rows,
            "chunk_size": chunk_size,
            "column_chunks": chunk_hashes_per_col,
        }
        manifest_bytes = json.dumps(manifest, sort_keys=True,
                                     default=str).encode()
        manifest_blob_hash = self.kernel.write(manifest_bytes)

        # Attach encoding metadata sidecar (collected during main loop —
        # no second encode pass). The sidecar is preserved through
        # ColumnChunkZoneMap.to_dict/from_dict.
        cczm._encoding_meta = encoding_meta_per_col

        return manifest_blob_hash, cczm

    def read_column_chunks_encoded(
            self,
            cczm: ColumnChunkZoneMap,
            columns: list[str],
            surviving_chunk_indices: Optional[set[int]] = None,
            predicates: Optional[list[tuple[str, str, Any]]] = None,
            ) -> dict[str, list[list[Any]]]:
        """Read column chunks with encoding-aware predicate eval — Vortex-style.

        The Vortex design: evaluate the predicate on the PREDICATE COLUMN's
        encoded form to determine which ROW POSITIONS survive. Then read ALL
        columns (including non-predicate columns) at those same surviving
        positions. This guarantees all columns have the same number of values
        (the surviving rows) — no misalignment.

        Flow:
          1. Find the predicate column (first column in `predicates` that
             exists in the chunk's column_chunks).
          2. For each surviving chunk:
             a. Evaluate the predicate on the predicate column's encoded form
                → surviving_ranges (list of (start, end) row positions)
             b. If no surviving ranges: skip the chunk (all rows pruned)
             c. For ALL requested columns: decode only the values at the
                surviving_ranges positions
          3. If no predicates: decode full chunks for all columns (standard path)

        This is GENERIC: works for any data format, any column layout, any
        predicate. The predicate column determines which rows survive; all
        other columns are projected to those same rows.

        Args:
            cczm: ColumnChunkZoneMap for the row group
            columns: list of column names to read
            surviving_chunk_indices: set of chunk indices to read
                (after column-chunk zone-map pruning). If None, read all.
            predicates: list of (column, op, value) tuples for encoded
                predicate eval. If None, decode full chunks.

        Returns:
            Dict of column_name → list of (chunk_index, surviving_values)
            pairs. All columns have the same number of values per chunk
            (the surviving rows).
        """
        result: dict[str, list[tuple[int, list[Any]]]] = {}

        # Build predicate lookup
        pred_lookup: dict[str, tuple[str, Any]] = {}
        if predicates:
            for col, op, val in predicates:
                pred_lookup[col] = (op, val)

        # Determine which column drives the predicate eval (if any).
        # The first predicate column that exists in cczm.column_chunks
        # determines the surviving row positions for ALL columns.
        pred_col_name: Optional[str] = None
        for col in columns:
            if col in pred_lookup and col in cczm.column_chunks:
                pred_col_name = col
                break

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

                blob_bytes = decompress_blob(self.kernel.read_blob(stats.blob_hash))

                # VORTEX DESIGN: If this is the predicate column, evaluate
                # the predicate on its encoded form to get surviving_ranges.
                # If this is NOT the predicate column but we have a predicate,
                # we need to read at the SAME surviving_ranges as the
                # predicate column. We get those ranges by evaluating the
                # predicate on the predicate column's blob.
                if pred_col_name is not None:
                    # Get surviving_ranges from the PREDICATE column's blob
                    pred_stats = cczm.column_chunks[pred_col_name][stats.chunk_index]
                    pred_blob_bytes = decompress_blob(self.kernel.read_blob(pred_stats.blob_hash))
                    op, val = pred_lookup[pred_col_name]
                    encoded_result = eval_predicate_encoded(
                        pred_blob_bytes, pred_col_name, op, val)

                    if encoded_result is not None:
                        surviving_ranges, _ = encoded_result
                        if not surviving_ranges:
                            # Chunk fully pruned by encoded eval — ALL columns
                            # get 0 values for this chunk (keeps alignment)
                            chunks_result.append((stats.chunk_index, []))
                            continue

                        # Decode THIS column at the surviving_ranges positions.
                        # For the predicate column: decode_surviving_values
                        #   yields the matching values directly.
                        # For non-predicate columns: decode_surviving_values
                        #   yields the values at the same row positions.
                        # Both produce the same number of values → aligned.
                        surviving_values = decode_surviving_values(
                            blob_bytes, surviving_ranges)
                        chunks_result.append(
                            (stats.chunk_index, surviving_values))
                        continue

                # No predicate for any column in this chunk, or encoded eval
                # not supported — decode the whole chunk.
                values = decode_column(blob_bytes)
                chunks_result.append((stats.chunk_index, values))

            result[col_name] = chunks_result

        return result

    @staticmethod
    def has_encoded_storage(zm_dict: dict) -> bool:
        """Check if a zone map blob indicates encoded chunk storage.

        Returns True if column_chunks stats have blob_hash fields AND
        the zone map blob includes the _encoding_meta sidecar attached
        by write_row_group_encoded.
        """
        if not ColumnChunkStorage.has_column_chunk_storage(zm_dict):
            return False
        # The _encoding_meta sidecar is attached by write_row_group_encoded
        # only. Plain column-chunk storage does not set it.
        cczm_dict = zm_dict["column_chunks"]
        return "_encoding_meta" in cczm_dict
