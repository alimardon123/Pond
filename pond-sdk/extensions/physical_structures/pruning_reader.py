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
        }

    def scan(self,
             decode_fn: Callable[[bytes], Union[Any, list[Any]]],
             row_filter: Optional[Callable[[Any], bool]] = None,
             start_key: Optional[str] = None,
             end_key: Optional[str] = None) -> Iterator[Any]:
        """Scan the collection with pruning.

        Args:
            decode_fn: function that takes data blob bytes → row or list of rows.
                For KeyValueLens: decode_fn = lens.decode (returns a dict).
                For LakehouseLens: decode_fn = lambda b: pq.read_table(b).to_pylist()
                (returns a list of dicts).
            row_filter: optional function(row) → bool. Applied AFTER decode
                for exact row-level filtering. This catches false positives
                from zone-map pruning (rows in non-pruned groups that don't
                actually match).
            start_key: optional lower bound on row group keys
            end_key: optional upper bound (documentation only)

        Yields:
            Individual rows (dicts) that survive both zone-map pruning
            and the optional row_filter.

        The pruning flow:
          1. Walk zone-map ProllyTree → read small zone-map blobs
          2. Evaluate PruningPredicate.can_prune(zone_map)
          3. If pruned: SKIP (data blob not read, not decoded)
          4. If not pruned: read data blob, decode, yield rows
          5. If row_filter provided: only yield rows that pass the filter
        """
        self.stats = {
            "total_row_groups": 0,
            "pruned_row_groups": 0,
            "data_blobs_read": 0,
            "rows_yielded": 0,
        }

        # Get data blob hashes from the zone-map index, with pruning
        for data_blob_hash in self.zm_index.scan_with_pruning(
                self.collection, self.predicate, start_key, end_key):

            self.stats["total_row_groups"] += 1
            self.stats["data_blobs_read"] += 1

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

            # Apply exact row-level filter if provided
            for row in rows:
                if row_filter is None or row_filter(row):
                    self.stats["rows_yielded"] += 1
                    yield row

        # Track pruned count (total - read)
        # Note: scan_with_pruning handles the pruning internally;
        # we only see non-pruned blobs. To get the total, we'd need
        # to count zone-map entries separately.
        # For now, pruned = total_zone_maps - data_blobs_read

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
        """
        return self.stats.copy()

    def get_pruning_ratio(self) -> float:
        """Get the fraction of row groups that were pruned (0.0 to 1.0).

        Returns 0.0 if no zone maps exist or no scan has been done.
        """
        # We need the total zone-map count to compute this
        tree_root = self.kernel.resolve(self.zm_index._zm_ref(self.collection))
        if not tree_root:
            return 0.0
        state = ProllyTree.read_all(self.kernel, tree_root)
        total = len([k for k in state.keys() if not k.startswith("_")])
        if total == 0:
            return 0.0
        pruned = total - self.stats["data_blobs_read"]
        return pruned / total
