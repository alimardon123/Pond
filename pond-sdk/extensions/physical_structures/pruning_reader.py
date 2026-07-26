"""
PruningReader — wraps any lens's read path with zone-map-based pruning.

This is the GENERIC pruning layer that works with ANY lens. It reads zone
maps first (small, cheap), evaluates the pruning predicate, and only fetches
+ decodes data blobs that MIGHT match.

The PruningReader does NOT know what format the data is in (JSON, Parquet,
binary). It only knows:
  1. How to read zone maps (via ZoneMapIndex)
  2. How to evaluate the pruning predicate (via PruningPredicate)
  3. How to yield data blob hashes for the caller to decode

The CALLER (the lens) is responsible for:
  - Providing a decode function that takes blob bytes → rows
  - Providing a row-level filter function (for exact matching after pruning)

Usage:
    from zone_map_index import ZoneMapIndex
    from pruning import PruningPredicate, ColumnPredicate
    from pruning_reader import PruningReader

    # Set up
    zm_index = ZoneMapIndex(kernel)
    predicate = PruningPredicate([
        ColumnPredicate(column="age", op=">", value=30),
    ])

    # Create a pruning reader
    reader = PruningReader(kernel, zm_index, "users", predicate)

    # Scan — yields rows (only from data blobs that might match)
    for row in reader.scan(decode_fn=lens.decode):
        print(row)  # only rows from non-pruned row groups

    # Scan with exact row-level filtering
    for row in reader.scan(decode_fn=lens.decode,
                           row_filter=lambda r: r.get("age", 0) > 30):
        print(row)  # exactly matching rows, no false positives

PRUNING FLOW (Vortex-style):
  1. Walk zone-map ProllyTree → read small zone-map blobs
  2. Evaluate PruningPredicate.can_prune(zone_map)
  3. If pruned: SKIP (data blob not read, not decoded)
  4. If not pruned: read data blob, decode, yield rows
  5. Optional: apply exact row-level filter on decoded rows

The key win: step 3 skips the ENTIRE data blob — no kernel.read_blob, no
decode. For selective queries (e.g., age > 30 when most row groups have
max(age) < 30), this can skip 90%+ of data blobs.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Optional, Any, Callable, Iterator, Union

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal
from zone_map_index import ZoneMapIndex
from pruning import ZoneMap, PruningPredicate


class PruningReader:
    """Generic pruning reader that works with ANY lens.

    Reads zone maps first, evaluates the pruning predicate, and only
    fetches + decodes data blobs that MIGHT match. The reader is
    format-agnostic — it only works with blob hashes and zone maps.

    The lens provides:
      - decode_fn: bytes → list of rows (or single row for KV)
      - row_filter: optional exact filter on decoded rows
    """

    # Stats schema as a class constant so __init__ and scan() stay in sync.
    _INITIAL_STATS = {
        "total_row_groups": 0,
        "pruned_row_groups": 0,
        "data_blobs_read": 0,
        "rows_yielded": 0,
        "column_chunks_pruned": 0,
    }

    def __init__(self, kernel: PondMinimal,
                 zm_index: ZoneMapIndex,
                 collection: str,
                 predicate: Optional[PruningPredicate] = None):
        """Create a pruning reader.

        Args:
            kernel: the PondMinimal kernel
            zm_index: ZoneMapIndex for reading zone maps
            collection: collection name
            predicate: PruningPredicate for pruning. If None, no pruning
                (all data blobs are read).
        """
        self.kernel = kernel
        self.zm_index = zm_index
        self.collection = collection
        self.predicate = predicate
        # Statistics for performance analysis (reset at the start of scan())
        self.stats = self._INITIAL_STATS.copy()

    def _compute_surviving_chunks(self, zm_dict: dict, cc_predicates: dict
                                    ) -> tuple[Optional[set[int]], Optional[Any]]:
        """Compute the set of chunk indices that survive all predicates.

        Used by scan() to skip individual column chunks within a
        surviving row group. Returns (surviving_chunk_indices, cczm).
        - surviving_chunk_indices is None if no column-chunk pruning is
          possible (no stats, no predicate columns, or extension missing).
        - surviving_chunk_indices is the empty set if all chunks pruned
          (caller should skip the row group entirely).
        - cczm is the deserialized ColumnChunkZoneMap (or None).

        Also updates self.stats["column_chunks_pruned"].
        """
        if not cc_predicates or "column_chunks" not in zm_dict:
            return None, None

        try:
            from column_chunk_zone_map import ColumnChunkZoneMap
        except ImportError:
            return None, None  # extension not available

        cczm = ColumnChunkZoneMap.from_dict(zm_dict["column_chunks"])

        # surviving_chunks_per_col: column_name → set of chunk indices
        # that MIGHT match. We take the INTERSECTION across predicate
        # columns (predicates are ANDed).
        surviving_chunks_per_col: dict[str, set[int]] = {}
        for col, (op, val) in cc_predicates.items():
            chunks = cczm.prune_column_chunks(col, op, val)
            # None means no stats for this column — caller falls back to
            # reading all chunks. Skip the column (don't include in the
            # intersection) so we don't silently drop rows.
            if chunks is None:
                continue
            surviving_chunks_per_col[col] = set(chunks)

        if not surviving_chunks_per_col:
            return None, cczm  # no pruning possible

        # Intersection across predicate columns
        surviving_chunk_indices: Optional[set[int]] = None
        for col, chunks in surviving_chunks_per_col.items():
            if surviving_chunk_indices is None:
                surviving_chunk_indices = set(chunks)
            else:
                surviving_chunk_indices &= chunks

        # Track pruned chunks (per column, for stats)
        for col, chunks in surviving_chunks_per_col.items():
            total_chunks = len(cczm.column_chunks.get(col, []))
            pruned = total_chunks - len(chunks)
            self.stats["column_chunks_pruned"] += pruned

        return surviving_chunk_indices, cczm

    @staticmethod
    def _slice_rows_by_chunks(rows: list, surviving_chunks: set[int],
                                chunk_size: int) -> list:
        """Slice decoded rows to only those that fall in surviving chunks.

        Each chunk holds `chunk_size` consecutive rows starting at
        chunk_index * chunk_size. Rows from pruned chunks are excluded.
        """
        if not surviving_chunks or not rows:
            return rows
        surviving_rows = []
        for ci in sorted(surviving_chunks):
            start = ci * chunk_size
            end = min(start + chunk_size, len(rows))
            if start < end:
                surviving_rows.extend(rows[start:end])
        return surviving_rows

    def scan(self,
             decode_fn: Callable[[bytes], Union[Any, list[Any]]],
             row_filter: Optional[Callable[[Any], bool]] = None,
             start_key: Optional[str] = None,
             end_key: Optional[str] = None,
             columns: Optional[list[str]] = None,
             chunk_size: int = 1000) -> Iterator[Any]:
        """Scan the collection with pruning.

        Args:
            decode_fn: function that takes data blob bytes → row or list of rows.
            row_filter: optional function(row) → bool for exact row-level filtering.
            start_key: optional lower bound on row group keys.
            end_key: optional upper bound on row group keys (inclusive).
            columns: optional list of column names for column-chunk pruning.
                If provided AND the zone map has column_chunks, the reader
                will skip column chunks within surviving row groups that
                can't match the predicate. Rows from pruned chunks are
                never yielded (even before row_filter runs).
            chunk_size: rows per column chunk (must match the chunk_size
                used at write time when building ColumnChunkZoneMap).
                Default 1000.

        Yields:
            Individual rows (dicts) that survive all pruning levels.

        The pruning flow (three levels):
          1. Row-group pruning: skip entire row groups via ZoneMap
          2. Column-chunk pruning: within surviving row groups, skip
             individual column chunks via ColumnChunkZoneMap
          3. Row-level filtering: exact match check on decoded rows
        """
        self.stats = self._INITIAL_STATS.copy()

        # Build column-chunk predicate lookup from the PruningPredicate.
        # Map: column_name → (op, value) for each predicate column.
        cc_predicates: dict[str, tuple[str, Any]] = {}
        if self.predicate and columns:
            for pred in self.predicate.predicates:
                if pred.column in columns:
                    cc_predicates[pred.column] = (pred.op, pred.value)

        # Count total zone maps (for pruned_row_groups stat). When a
        # predicate is active, scan_with_pruning yields only non-pruned
        # row groups, so we need the total count separately.
        if self.predicate is not None:
            try:
                base = self.zm_index._get_base(self.collection)
                total_zone_maps = sum(
                    1 for k in base.read_all().keys()
                    if not k.startswith("_")
                )
            except Exception:
                total_zone_maps = None
        else:
            total_zone_maps = None  # no predicate → no pruning

        # Use verbose scan so we get the zone-map dict alongside the blob
        # hash — this avoids a second zone-map lookup when we want to do
        # column-chunk pruning on surviving row groups.
        for row_group_key, data_blob_hash, zm_dict in self.zm_index.scan_with_pruning(
                self.collection, self.predicate, start_key, end_key,
                verbose=True):

            self.stats["total_row_groups"] += 1
            self.stats["data_blobs_read"] += 1

            # Level 2: column-chunk pruning
            surviving_chunk_indices, cczm = self._compute_surviving_chunks(
                zm_dict, cc_predicates)

            # Defensive: if column-chunk pruning proves nothing survives,
            # skip the whole row group (row-group pruning should already
            # have caught this, but column-chunk can be stricter).
            if surviving_chunk_indices is not None and not surviving_chunk_indices:
                continue

            # Read + decode the data blob (the expensive parts)
            data_bytes = self.kernel.read_blob(data_blob_hash)
            decoded = decode_fn(data_bytes)

            # Normalize to list of rows
            if isinstance(decoded, list):
                rows = decoded
            elif isinstance(decoded, dict):
                rows = [decoded]
            else:
                rows = [decoded]

            # Slice to surviving chunks (if column-chunk pruning is active)
            if surviving_chunk_indices is not None:
                rows = self._slice_rows_by_chunks(rows, surviving_chunk_indices,
                                                    chunk_size)

            # Level 3: row-level filtering
            for row in rows:
                if row_filter is None or row_filter(row):
                    self.stats["rows_yielded"] += 1
                    yield row

        # After the scan, compute pruned_row_groups = total - read
        if total_zone_maps is not None:
            self.stats["pruned_row_groups"] = (
                total_zone_maps - self.stats["total_row_groups"]
            )

    def scan_blob_hashes(self,
                         start_key: Optional[str] = None,
                         end_key: Optional[str] = None) -> Iterator[str]:
        """Scan data blob hashes with pruning, WITHOUT decoding.

        Yields data blob hashes for row groups that MIGHT match the predicate.
        The caller reads and decodes these blobs themselves.

        This is useful when the caller wants to batch reads or use a
        specific decode method.
        """
        for data_blob_hash in self.zm_index.scan_with_pruning(
                self.collection, self.predicate, start_key, end_key):
            self.stats["data_blobs_read"] += 1
            yield data_blob_hash

    def get_stats(self) -> dict:
        """Get pruning statistics from the last scan.

        Returns:
            dict with:
              - total_row_groups: row groups examined (non-pruned)
              - pruned_row_groups: row groups skipped (NOT counted in total)
              - data_blobs_read: data blobs actually read from kernel
              - rows_yielded: rows yielded after filtering
              - column_chunks_pruned: column chunks skipped via column-chunk
                zone-map pruning
        """
        return self.stats.copy()
