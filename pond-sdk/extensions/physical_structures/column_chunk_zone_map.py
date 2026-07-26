"""
ColumnChunkZoneMap — per-column-chunk zone maps for finer-grained pruning.

While ZoneMap (in pruning.py) stores min/max per column for an entire row
group, ColumnChunkZoneMap splits each row group into column chunks and
stores min/max per chunk. This enables:

  1. Row-group pruning (existing): skip entire row groups via ZoneMap
  2. Column-chunk pruning (new): within a surviving row group, skip
     individual column chunks that can't match the predicate

This is inspired by Vortex's zoned layout + Parquet's page-level statistics.
The key difference from Parquet: Pond stores column chunks as separate
content-addressed blobs in the ProllyTreeIndex, so skipping a column chunk
means skipping a kernel.read_blob() call — a real I/O saving on object
storage.

Structure:
  ColumnChunkZoneMap:
    row_group_key: str           # e.g., "rg/999"
    column_chunks: dict[str, list[ColumnChunkStats]]
      # column_name → list of per-chunk stats
      # e.g., {"age": [{min: 20, max: 29, offset: 0}, {min: 30, max: 39, offset: 10}, ...]}

Usage (read side):
    # After row-group pruning survives a row group, check column chunks:
    cczm = ColumnChunkZoneMap.from_dict(zm_dict)
    surviving_chunks = cczm.prune_column_chunks("age", predicate)
    # Only read the surviving chunks for this column

GENERIC: works with any data format. The column chunk stats are computed
by the lens at write time (same as ZoneMap but at finer granularity).
"""


from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict


@dataclass
class ColumnChunkStats:
    """Statistics for a single column chunk within a row group."""
    min: Any = None
    max: Any = None
    null_count: int = 0
    row_count: int = 0
    chunk_index: int = 0  # 0-based index within the row group
    blob_hash: Optional[str] = None  # set when chunk is stored as a separate blob

    def to_dict(self) -> dict:
        return {
            "min": self.min,
            "max": self.max,
            "null_count": self.null_count,
            "row_count": self.row_count,
            "chunk_index": self.chunk_index,
            "blob_hash": self.blob_hash,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ColumnChunkStats":
        return cls(
            min=d.get("min"),
            max=d.get("max"),
            null_count=d.get("null_count", 0),
            row_count=d.get("row_count", 0),
            chunk_index=d.get("chunk_index", 0),
            blob_hash=d.get("blob_hash"),
        )


@dataclass
class ColumnChunkZoneMap:
    """Per-column-chunk zone maps for a row group.

    Stores min/max per column chunk (finer than ZoneMap which stores
    min/max per column for the entire row group).

    This enables column-chunk pruning: within a surviving row group,
    skip individual column chunks that can't match the predicate.
    """
    row_group_key: str = ""
    # column_name → list of per-chunk stats (one entry per chunk)
    column_chunks: dict[str, list[ColumnChunkStats]] = field(default_factory=dict)

    @classmethod
    def build(cls, table_or_source, row_group_key: str,
              chunk_size: int = 1000) -> "ColumnChunkZoneMap":
        """Build a ColumnChunkZoneMap from a PyArrow Table or ColumnSource.

        Format-agnostic (design review C4 fix): accepts either a PyArrow
        Table (auto-wrapped) or any ColumnSource. Splits each column into
        chunks of `chunk_size` rows and computes min/max/null_count per
        chunk using the source's column_slice + compute_list_stats.

        Args:
            table_or_source: PyArrow Table OR ColumnSource (a single row
                group's worth of data)
            row_group_key: the ProllyTreeIndex key for this row group
            chunk_size: rows per column chunk (default 1000)
        """
        from column_source import as_column_source, compute_list_stats
        source = as_column_source(table_or_source)

        cczm = cls(row_group_key=row_group_key)
        n_rows = source.num_rows()

        for col_name in source.column_names():
            chunks = []

            for start in range(0, n_rows, chunk_size):
                end = min(start + chunk_size, n_rows)
                values = source.column_slice(col_name, start, end)

                mn, mx, null_count = compute_list_stats(values)
                stats = ColumnChunkStats(
                    chunk_index=len(chunks),
                    row_count=end - start,
                    min=mn,
                    max=mx,
                    null_count=null_count,
                )

                chunks.append(stats)

            cczm.column_chunks[col_name] = chunks

        return cczm

    def to_dict(self) -> dict:
        """Serialize for storage in ProllyTreeIndex."""
        out = {
            "row_group_key": self.row_group_key,
            "column_chunks": {
                col: [s.to_dict() for s in chunks]
                for col, chunks in self.column_chunks.items()
            },
        }
        # Preserve encoding metadata sidecar (set by EncodedChunkStorage)
        encoding_meta = getattr(self, "_encoding_meta", None)
        if encoding_meta is not None:
            out["_encoding_meta"] = encoding_meta
        return out

    @classmethod
    def from_dict(cls, d: dict) -> "ColumnChunkZoneMap":
        """Deserialize from a dict."""
        cczm = cls(
            row_group_key=d.get("row_group_key", ""),
            column_chunks={
                col: [ColumnChunkStats.from_dict(s) for s in chunks]
                for col, chunks in d.get("column_chunks", {}).items()
            },
        )
        # Restore encoding metadata sidecar (if present)
        if "_encoding_meta" in d:
            cczm._encoding_meta = d["_encoding_meta"]
        return cczm

    def prune_column_chunks(self, column: str, op: str,
                            value: Any) -> Optional[list[int]]:
        """Find which chunk indices MIGHT match a predicate.

        Returns the indices of chunks that cannot be pruned (might match).
        Chunks not in the returned list can be skipped for this column.

        Args:
            column: column name
            op: comparison operator (=, !=, <, <=, >, >=, in)
            value: comparison value

        Returns:
            List of chunk indices that might match (0-based), or None if
            this ColumnChunkZoneMap has no stats for the requested column
            (in which case the caller should fall back to reading all
            chunks). Returning None is important — returning [] would
            silently drop the column.
        """
        if column not in self.column_chunks:
            # No stats for this column — caller must fall back to
            # reading all chunks. Returning [] would be wrong: callers
            # treat [] as "no surviving chunks" and skip the column.
            return None

        surviving = []
        for chunk in self.column_chunks[column]:
            if self._chunk_might_match(chunk, op, value):
                surviving.append(chunk.chunk_index)
        return surviving

    @staticmethod
    def _chunk_might_match(chunk: ColumnChunkStats, op: str,
                           value: Any) -> bool:
        """Check if a chunk might match a predicate.

        Returns True if the chunk MIGHT contain matching rows.
        Returns False if the chunk CANNOT match (can be pruned).
        """
        if chunk.min is None or chunk.max is None:
            return True  # no stats — can't prune

        if op == "=":
            return not (value < chunk.min or value > chunk.max)
        elif op == "<":
            return not (chunk.min >= value)
        elif op == "<=":
            return not (chunk.min > value)
        elif op == ">":
            return not (chunk.max <= value)
        elif op == ">=":
            return not (chunk.max < value)
        elif op == "in":
            if not value:
                return False
            v_min = min(value)
            v_max = max(value)
            return not (v_max < chunk.min or v_min > chunk.max)
        else:
            return True  # unknown op — can't prune
