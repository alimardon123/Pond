"""
ColumnChunkStorage — store each column chunk as a separate content-addressed blob.

This is the storage-side complement to ColumnChunkZoneMap. While
ColumnChunkZoneMap computes pruning statistics (min/max/null_count per
chunk), ColumnChunkStorage actually persists each chunk as its own blob
in the kernel. Combined, they enable TRUE I/O savings on object storage:

  Without ColumnChunkStorage:
    - Row group stored as ONE Parquet blob
    - Column-chunk pruning skips row_filter work but not I/O
    - The whole blob is read even if only 1/5 chunks survive

  With ColumnChunkStorage:
    - Row group stored as N_columns × N_chunks separate Parquet blobs
    - Each blob is a single-column, single-chunk Parquet file
    - Column-chunk pruning skips actual I/O (kernel.read_blob calls)
    - Skip 4/5 chunks = skip 4/5 of bytes per column

Storage layout:
  - The DATA ProllyTreeIndex for the collection maps:
      rg/{max_pk} → manifest_blob_hash
    where the manifest is a small JSON dict:
      {"column_chunks": {column_name: [chunk_blob_hash, chunk_blob_hash, ...]}}
    This preserves read_table() compatibility — the manifest lets the
    reader reconstruct the full row group by reading all chunk blobs.

  - The ZONE MAP blob (managed by ZoneMapIndex) is augmented:
      column_chunks.{column}[i].blob_hash = chunk_blob_hash
    This lets the pruning reader fetch surviving chunk blobs directly
    from the zone map, without reading the manifest.

GENERIC: works with any tabular data the lens can slice into columns.
The current implementation uses PyArrow for column slicing and Parquet
for per-chunk encoding, but the storage contract is format-agnostic —
any lens that can produce (column_name, chunk_bytes) pairs can use it.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Optional, Any, Iterator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal
from column_chunk_zone_map import ColumnChunkZoneMap, ColumnChunkStats


# ---------------------------------------------------------------------------
# ColumnChunkStorage — write/read per-column-chunk blobs
# ---------------------------------------------------------------------------

class ColumnChunkStorage:
    """Manages per-column-chunk blob storage for a collection.

    Used by lenses at write time to split row groups into per-column-chunk
    blobs, and by PruningReader at read time to fetch only surviving
    chunk blobs (real I/O savings on object storage).

    The chunk blobs themselves are stored as kernel blobs (content-addressed).
    A manifest blob is stored in the DATA ProllyTreeIndex for backward-
    compatible full reads. The chunk blob hashes are also embedded in
    the zone map blob's column_chunks stats for direct pruning-reader
    access.
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel

    def write_row_group_column_chunks(
            self,
            table_or_source,
            row_group_key: str,
            chunk_size: int = 1000,
            encode_fn=None) -> tuple[str, ColumnChunkZoneMap]:
        """Split a row group into per-column-chunk blobs and store them.

        Fully format-agnostic: accepts any ColumnSource (PyArrow Table,
        list-of-dicts, or custom adapter). Each chunk is encoded via
        `encode_fn(col_name, values: list) -> bytes` — the lens provides
        its own encoder (Parquet, JSON, binary, rich text, diffs, etc.).
        No PyArrow dependency in the storage contract.

        This enables ANY workload to use column-chunk storage:
          - LakehouseLens: Parquet encoder
          - KeyValueLens: JSON encoder
          - VectorLens: binary encoder
          - Notebook lens: rich-text encoder
          - Git lens: diff-based encoder
        Any app built on Pond gets infinite storage + versioning +
        branching + pruning + encoding on object stores.

        Args:
            table_or_source: PyArrow Table OR ColumnSource (a single row
                group's worth of data)
            row_group_key: the ProllyTreeIndex key for this row group
                (e.g., "rg/999")
            chunk_size: rows per column chunk (default 1000)
            encode_fn: REQUIRED function(col_name: str, values: list) -> bytes
                that encodes a single column's values to the lens's
                preferred format. No default — the lens must supply its
                own encoder.

        Returns:
            Tuple of (manifest_blob_hash, ColumnChunkZoneMap).
        """
        if encode_fn is None:
            raise ValueError(
                "encode_fn is required — pass a function(col_name, values) "
                "-> bytes that encodes a single column's values to your "
                "preferred format (e.g., Parquet, JSON, binary).")

        from column_source import as_column_source, compute_list_stats
        source = as_column_source(table_or_source)

        n_rows = source.num_rows()
        cczm = ColumnChunkZoneMap(row_group_key=row_group_key)

        # column_name → list of chunk blob hashes (one per chunk)
        chunk_hashes_per_col: dict[str, list[str]] = {}

        for col_name in source.column_names():
            chunk_hashes: list[str] = []
            chunk_stats: list[ColumnChunkStats] = []

            for start in range(0, n_rows, chunk_size):
                end = min(start + chunk_size, n_rows)
                values = source.column_slice(col_name, start, end)

                # Encode + write the chunk as its own blob.
                # encode_fn receives (col_name, values: list) — format-agnostic.
                # The lens decides how to encode (Parquet, JSON, binary, etc.).
                chunk_bytes = encode_fn(col_name, values)
                chunk_blob_hash = self.kernel.write(chunk_bytes)
                chunk_hashes.append(chunk_blob_hash)

                # Compute stats for this chunk (format-agnostic)
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

        # Build the manifest blob: lists chunk blob hashes per column
        manifest = {
            "row_group_key": row_group_key,
            "row_count": n_rows,
            "chunk_size": chunk_size,
            "column_chunks": chunk_hashes_per_col,
        }
        manifest_bytes = json.dumps(manifest, sort_keys=True,
                                     default=str).encode()
        manifest_blob_hash = self.kernel.write(manifest_bytes)

        return manifest_blob_hash, cczm

    def read_full_row_group(self, manifest_blob_hash: str,
                             decode_fn=None) -> dict[str, list]:
        """Reconstruct a full row group by reading all chunk blobs.

        Format-agnostic: decode_fn(bytes) -> list returns a list of
        values for one column. The caller (lens) is responsible for
        constructing its native data structure (pa.Table, list[dict],
        etc.) from the returned dict[str, list].

        Args:
            manifest_blob_hash: the manifest blob hash
            decode_fn: function(bytes) -> list that decodes a chunk blob
                to a list of values for one column.

        Returns:
            dict[str, list] mapping column_name → list of all values
            for that column across all chunks.
        """
        if decode_fn is None:
            raise ValueError("decode_fn is required — pass a function(bytes) "
                             "-> list that decodes a chunk blob to values.")

        manifest_bytes = self.kernel.read_blob(manifest_blob_hash)
        manifest = json.loads(manifest_bytes)

        columns: dict[str, list] = {}
        for col_name, chunk_hashes in manifest["column_chunks"].items():
            all_values: list = []
            for chunk_hash in chunk_hashes:
                chunk_bytes = self.kernel.read_blob(chunk_hash)
                all_values.extend(decode_fn(chunk_bytes))
            columns[col_name] = all_values

        return columns

    def read_column_chunks(
            self,
            cczm: ColumnChunkZoneMap,
            columns: list[str],
            surviving_chunk_indices: Optional[set[int]] = None,
            decode_fn=None) -> dict[str, list[list]]:
        """Read specific column chunks for surviving chunk indices.

        Format-agnostic: decode_fn(bytes) -> list returns a list of
        values for one column. Returns dict[str, list[list]] —
        column_name → list of value-lists (one per surviving chunk).

        The caller (lens) is responsible for concatenating the
        value-lists and constructing its native data structure.

        Args:
            cczm: ColumnChunkZoneMap for the row group (with blob_hashes)
            columns: list of column names to read (predicate + projection)
            surviving_chunk_indices: set of chunk indices to read.
                If None, read all chunks.
            decode_fn: function(bytes) -> list that decodes a chunk blob
                to a list of values for one column.

        Returns:
            Dict of column_name → list of value-lists (one per
            surviving chunk, in chunk_index order). Empty dict if
            any column has no blob_hash (caller should fall back).
        """
        if decode_fn is None:
            raise ValueError("decode_fn is required")

        result: dict[str, list[list]] = {}

        for col_name in columns:
            if col_name not in cczm.column_chunks:
                continue

            chunk_stats = cczm.column_chunks[col_name]
            value_lists: list[list] = []

            for stats in chunk_stats:
                if surviving_chunk_indices is not None and \
                        stats.chunk_index not in surviving_chunk_indices:
                    continue  # SKIP — chunk pruned, no I/O

                if stats.blob_hash is None:
                    return {}  # fall back to caller

                chunk_bytes = self.kernel.read_blob(stats.blob_hash)
                values = decode_fn(chunk_bytes)
                value_lists.append(values)

            result[col_name] = value_lists

        return result

    @staticmethod
    def has_column_chunk_storage(zm_dict: dict) -> bool:
        """Check if a zone map blob indicates column-chunk storage.

        Returns True if every column chunk has a blob_hash field set,
        meaning the row group was written with column-chunk storage
        and pruning reader can fetch individual chunk blobs.
        """
        if "column_chunks" not in zm_dict:
            return False
        cczm_dict = zm_dict["column_chunks"]
        if "column_chunks" not in cczm_dict:
            return False
        for col, chunks in cczm_dict["column_chunks"].items():
            for chunk in chunks:
                if chunk.get("blob_hash") is None:
                    return False
        return True
