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

        # Statistics for performance analysis
        self.stats = {
            "total_row_groups": 0,
            "pruned_row_groups": 0,
            "data_blobs_read": 0,
            "rows_yielded": 0,
            "column_chunks_pruned": 0,
        }

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
        self.stats = {
            "total_row_groups": 0,
            "pruned_row_groups": 0,
            "data_blobs_read": 0,
            "rows_yielded": 0,
            "column_chunks_pruned": 0,
        }

        # Build column-chunk predicate lookup from the PruningPredicate.
        # Map: column_name → (op, value) for each predicate column.
        cc_predicates = {}
        if self.predicate and columns:
            for pred in self.predicate.predicates:
                if pred.column in columns:
                    cc_predicates[pred.column] = (pred.op, pred.value)

        # Use verbose scan so we get the zone-map dict alongside the blob
        # hash — this avoids a second zone-map lookup when we want to do
        # column-chunk pruning on surviving row groups.
        for row_group_key, data_blob_hash, zm_dict in self.zm_index.scan_with_pruning(
                self.collection, self.predicate, start_key, end_key,
                verbose=True):

            self.stats["total_row_groups"] += 1
            self.stats["data_blobs_read"] += 1

            # Compute surviving chunk indices (if column-chunk stats exist
            # and we have predicates on at least one column).
            #
            # surviving_chunks maps column_name → set of chunk indices that
            # MIGHT match. We take the INTERSECTION across all predicate
            # columns: a chunk survives only if it might match for ALL
            # predicates. (We are ANDing the predicates.)
            surviving_chunks_per_col: dict[str, set[int]] = {}
            cczm = None
            if cc_predicates and "column_chunks" in zm_dict:
                try:
                    from column_chunk_zone_map import ColumnChunkZoneMap
                    cczm = ColumnChunkZoneMap.from_dict(zm_dict["column_chunks"])
                    for col, (op, val) in cc_predicates.items():
                        chunks = cczm.prune_column_chunks(col, op, val)
                        # None means no stats for this column — caller must
                        # fall back to reading all chunks (i.e., do not
                        # prune at chunk level). [] means no chunks match.
                        if chunks is None:
                            continue  # skip column-chunk pruning for this col
                        surviving_chunks_per_col[col] = set(chunks)
                except ImportError:
                    cczm = None  # column_chunk_zone_map not available

            # Compute the set of chunk indices that survive ALL predicates
            # (intersection across columns). If empty intersection, the
            # whole row group can be skipped — but row-group pruning should
            # have caught that already via the row-group ZoneMap. We still
            # check defensively.
            surviving_chunk_indices: Optional[set[int]] = None
            if cczm is not None and surviving_chunks_per_col:
                surviving_chunk_indices = None
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

                # If the intersection is empty, skip this row group entirely.
                # (Defensive — row-group pruning should already have caught
                # this, but column-chunk pruning can be stricter.)
                if surviving_chunk_indices is not None and not surviving_chunk_indices:
                    continue

            # Read the data blob (this is the expensive part — large blob)
            data_bytes = self.kernel.read_blob(data_blob_hash)

            # Decode (this is the other expensive part — format-specific decode)
            decoded = decode_fn(data_bytes)

            # Normalize to list of rows
            if isinstance(decoded, list):
                rows = decoded
            elif isinstance(decoded, dict):
                rows = [decoded]
            else:
                rows = [decoded]

            # If column-chunk pruning is active, slice the decoded rows to
            # only those that fall in surviving chunks. Each chunk holds
            # chunk_size consecutive rows starting at chunk_index * chunk_size.
            if surviving_chunk_indices is not None and len(rows) > 0:
                surviving_rows = []
                for ci in sorted(surviving_chunk_indices):
                    start = ci * chunk_size
                    end = min(start + chunk_size, len(rows))
                    if start < end:
                        surviving_rows.extend(rows[start:end])
                rows = surviving_rows

            # Apply exact row-level filter if provided
            for row in rows:
                if row_filter is None or row_filter(row):
                    self.stats["rows_yielded"] += 1
                    yield row

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
