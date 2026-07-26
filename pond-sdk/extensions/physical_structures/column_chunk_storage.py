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
            table,
            row_group_key: str,
            chunk_size: int = 1000,
            encode_fn=None) -> tuple[str, ColumnChunkZoneMap]:
        """Split a row group into per-column-chunk blobs and store them.

        Args:
            table: PyArrow Table (a single row group)
            row_group_key: the ProllyTreeIndex key for this row group
                (e.g., "rg/999")
            chunk_size: rows per column chunk (default 1000)
            encode_fn: REQUIRED function(column_table) -> bytes that
                encodes a single-column PyArrow Table to your preferred
                format (e.g., Parquet). There is no default — the lens
                must supply its own encoder.

        Returns:
            Tuple of (manifest_blob_hash, ColumnChunkZoneMap).
            The manifest is a small JSON blob listing all chunk blob
            hashes (used for full-row-group reads). The ColumnChunkZoneMap
            has each chunk's blob_hash field populated for pruning.
        """
        if encode_fn is None:
            raise ValueError(
                "encode_fn is required — pass a function(table) -> bytes "
                "that encodes a single-column PyArrow Table to your "
                "preferred format (e.g., Parquet).")

        n_rows = table.num_rows
        cczm = ColumnChunkZoneMap(row_group_key=row_group_key)

        # column_name → list of chunk blob hashes (one per chunk)
        chunk_hashes_per_col: dict[str, list[str]] = {}

        for col_name in table.column_names:
            column = table[col_name]
            chunk_hashes: list[str] = []
            chunk_stats: list[ColumnChunkStats] = []

            for start in range(0, n_rows, chunk_size):
                end = min(start + chunk_size, n_rows)
                chunk = column.slice(start, end - start)

                # Wrap the single column as a one-column Table for encoding
                import pyarrow as pa
                chunk_table = pa.Table.from_arrays([chunk], names=[col_name])

                # Encode + write the chunk as its own blob
                chunk_bytes = encode_fn(chunk_table)
                chunk_blob_hash = self.kernel.write(chunk_bytes)
                chunk_hashes.append(chunk_blob_hash)

                # Compute stats for this chunk
                stats = ColumnChunkStats(
                    chunk_index=len(chunk_stats),
                    row_count=end - start,
                    blob_hash=chunk_blob_hash,
                )

                # Compute min/max/null_count using PyArrow
                try:
                    import pyarrow.compute as pc
                    null_count = pc.sum(pc.is_null(chunk)).as_py()
                    stats.null_count = null_count
                    if null_count < len(chunk):
                        stats.min = pc.min(chunk).as_py()
                        stats.max = pc.max(chunk).as_py()
                except Exception:
                    pass  # type doesn't support min/max

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
                             decode_fn=None) -> Any:
        """Reconstruct a full row group by reading all chunk blobs.

        Used for backward-compatible full reads (read_table). Reads
        every chunk blob for every column and reassembles the table.

        Args:
            manifest_blob_hash: the manifest blob hash
            decode_fn: function(bytes) -> PyArrow Table

        Returns:
            PyArrow Table containing all rows from all chunks.
        """
        if decode_fn is None:
            raise ValueError("decode_fn is required — pass a function(bytes) "
                             "-> PyArrow Table that decodes your format.")

        manifest_bytes = self.kernel.read_blob(manifest_blob_hash)
        manifest = json.loads(manifest_bytes)

        import pyarrow as pa
        columns = {}
        for col_name, chunk_hashes in manifest["column_chunks"].items():
            arrays = []
            for chunk_hash in chunk_hashes:
                chunk_bytes = self.kernel.read_blob(chunk_hash)
                chunk_table = decode_fn(chunk_bytes)
                arrays.append(chunk_table.column(col_name))
            columns[col_name] = pa.concat_arrays(arrays)

        return pa.Table.from_arrays(
            [columns[c] for c in columns],
            names=list(columns.keys()),
        )

    def read_column_chunks(
            self,
            cczm: ColumnChunkZoneMap,
            columns: list[str],
            surviving_chunk_indices: Optional[set[int]] = None,
            decode_fn=None) -> dict[str, list[Any]]:
        """Read specific column chunks for surviving chunk indices.

        This is the read path used by PruningReader when column-chunk
        storage is active. Only fetches the chunk blobs that survived
        column-chunk pruning — real I/O savings.

        Args:
            cczm: ColumnChunkZoneMap for the row group (with blob_hashes)
            columns: list of column names to read (predicate + projection)
            surviving_chunk_indices: set of chunk indices to read.
                If None, read all chunks.
            decode_fn: function(bytes) -> PyArrow Table

        Returns:
            Dict of column_name → list of PyArrow Arrays (one per
            surviving chunk, in chunk_index order). The caller
            concatenates these and builds the result table.
        """
        if decode_fn is None:
            raise ValueError("decode_fn is required")

        result: dict[str, list[Any]] = {}

        for col_name in columns:
            if col_name not in cczm.column_chunks:
                # Column has no chunks (not in schema) — skip
                continue

            chunk_stats = cczm.column_chunks[col_name]
            arrays = []

            for stats in chunk_stats:
                if surviving_chunk_indices is not None and \
                        stats.chunk_index not in surviving_chunk_indices:
                    continue  # SKIP — chunk pruned, no I/O

                if stats.blob_hash is None:
                    # No separate chunk blob — caller should fall back
                    # to full row-group read
                    return {}

                chunk_bytes = self.kernel.read_blob(stats.blob_hash)
                chunk_table = decode_fn(chunk_bytes)
                # chunk_table.column(col_name) returns a ChunkedArray;
                # combine_chunks() to get a single Array for concatenation.
                col_array = chunk_table.column(col_name).combine_chunks()
                arrays.append(col_array)

            result[col_name] = arrays

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
