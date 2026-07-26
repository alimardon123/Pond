"""
ZoneMapIndex — ProllyTreeIndex of zone maps for generic pruning.

DESIGN: Zone maps (min/max/null_count statistics) are stored as SEPARATE
small blobs in the kernel, indexed by a ProllyTreeIndex. At read time:

  1. Walk the zone-map ProllyTree → read small zone-map blobs (cheap, O(log N))
  2. Evaluate PruningPredicate.can_prune(zone_map) → skip non-matching entries
  3. Only read + decode the large data blobs that MIGHT match

This is FULLY GENERIC because:
  - ANY lens that can compute min/max for its data can provide zone maps
  - The zone map format is lens-agnostic (JSON dict of column → min/max)
  - The pruning layer never touches the data bytes — only the zone maps
  - KeyValueLens: zone map = min/max of JSON fields per blob
  - LakehouseLens-style tabular lens: zone map = Parquet row-group statistics
  - VectorLens: zone map = bounding box of vectors

The zone map blob contains:
  {
    "min": {"col1": min_val, "col2": min_val, ...},
    "max": {"col1": max_val, "col2": max_val, ...},
    "null_count": {"col1": N, "col2": N, ...},
    "row_count": N,
    "blob_hash": "abc123..."  # the data blob this zone map describes
  }

Including blob_hash in the zone map means the pruning reader can fetch
the data blob directly — no second ProllyTree lookup needed.

Storage:
  collections/{collection}/zone_maps → ProllyTree root
  The ProllyTree maps: row_group_key → zone_map_blob_hash

Usage (write side — lens does this at write time):
    zm_index = ZoneMapIndex(kernel)
    zm_index.add_zone_map("users", "rg/100", zone_map_dict, data_blob_hash)

Usage (read side — pruning reader):
    zm_index = ZoneMapIndex(kernel)
    pruner = PruningReader(zm_index, kernel, predicate)
    for data_blob_hash in pruner.scan("users"):
        # Only data blobs that MIGHT match the predicate
        data_bytes = kernel.read_blob(data_blob_hash)
        rows = lens.decode(data_bytes)  # decode only surviving blobs
"""

from __future__ import annotations

import json
import os
import sys
from typing import Optional, Any, Iterator
from dataclasses import dataclass, field

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal
from prolly_tree import ProllyTree, ProllyLensBase
from pruning import ZoneMap, PruningPredicate


# ---------------------------------------------------------------------------
# ZoneMapIndex — manages the zone-map ProllyTreeIndex for a collection
# ---------------------------------------------------------------------------

class ZoneMapIndex:
    """Manages zone maps for a collection via a ProllyTreeIndex.

    Zone maps are small JSON blobs containing min/max/null_count statistics
    for each data blob. They are stored in a ProllyTreeIndex keyed by the
    same row_group_key as the data, enabling O(log N) lookup and O(K) range
    scans (K = matching row groups).

    The zone-map ProllyTreeIndex is separate from the data ProllyTreeIndex:
      - Data tree: collections/{collection}/HEAD → maps rg/{max_pk} → data_blob_hash
      - Zone map tree: collections/{collection}/zone_maps → maps rg/{max_pk} → zm_blob_hash

    Both trees use the SAME keys, so you can look up a zone map by the same
    key you'd use to look up the data.
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel
        self._bases: dict[str, ProllyLensBase] = {}

    def _get_base(self, collection: str) -> ProllyLensBase:
        """Get or create the ProllyLensBase for a collection's zone maps."""
        name = f"{collection}__zone_maps"
        if name not in self._bases:
            self._bases[name] = ProllyLensBase(self.kernel, name)
        return self._bases[name]

    @staticmethod
    def _zm_ref(collection: str) -> str:
        """The kernel reference for a collection's zone-map tree root."""
        return f"collections/{collection}/zone_maps"

    # ------------------------------------------------------------------
    # Write: add zone maps (called by lenses at write time)
    # ------------------------------------------------------------------

    def add_zone_map(self, collection: str, row_group_key: str,
                     zone_map: ZoneMap, data_blob_hash: str) -> str:
        """Add a zone map for a row group.

        Args:
            collection: collection name
            row_group_key: the ProllyTreeIndex key (e.g., "rg/100")
            zone_map: ZoneMap with min/max/null_count statistics
            data_blob_hash: the data blob this zone map describes

        Returns:
            The zone-map blob hash.
        """
        # Serialize the zone map + data_blob_hash as a JSON blob
        zm_dict = zone_map.to_dict()
        zm_dict["blob_hash"] = data_blob_hash
        zm_bytes = json.dumps(zm_dict, sort_keys=True, default=str).encode()
        zm_blob_hash = self.kernel.write(zm_bytes)

        # Stage in the zone-map ProllyTreeIndex (cached base)
        base = self._get_base(collection)
        base.stage(row_group_key, zm_blob_hash)
        return zm_blob_hash

    def commit_zone_maps(self, collection: str, message: str = "") -> str:
        """Commit staged zone maps for a collection.

        Returns the zone-map tree root hash.
        """
        base = self._get_base(collection)
        commit_hash = base.commit(message or f"zone maps for {collection}")
        # Also store the tree root as a ref for O(1) lookup
        tree_root = self.kernel.resolve(f"collections/{collection}__zone_maps/HEAD")
        if tree_root:
            self.kernel.reference(self._zm_ref(collection), tree_root)
        return commit_hash

    def build_zone_maps_from_scan(self, collection: str,
                                   scan_fn: Iterator[tuple[str, bytes, int]],
                                   columns: Optional[list[str]] = None) -> str:
        """Build zone maps by scanning data blobs.

        Args:
            collection: collection name
            scan_fn: generator yielding (row_group_key, data_blob_bytes, row_count)
            columns: columns to compute stats for. If None, infers from data.

        Returns:
            The zone-map tree root hash.
        """
        for row_group_key, data_bytes, row_count in scan_fn:
            zm = self._compute_zone_map(data_bytes, row_count, columns)
            # Find the blob hash for this data (we need it for the zone map)
            # The scan_fn provides bytes; we compute the hash
            from kernel import hash_bytes
            data_blob_hash = hash_bytes(data_bytes)
            self.add_zone_map(collection, row_group_key, zm, data_blob_hash)

        return self.commit_zone_maps(collection)

    def _compute_zone_map(self, data_bytes: bytes, row_count: int,
                          columns: Optional[list[str]] = None) -> ZoneMap:
        """Compute a ZoneMap from raw data bytes.

        Tries PyArrow (Parquet) first, then JSON, then gives up.
        """
        # Try Parquet (tabular lens data, e.g., LakehouseLens)
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pq.read_table(pa.BufferReader(data_bytes))
            return ZoneMap.build(table, columns)
        except Exception:
            pass

        # Try JSON (KeyValueLens data — single row)
        try:
            row = json.loads(data_bytes)
            zm = ZoneMap(row_count=1)
            if isinstance(row, dict):
                for col, val in row.items():
                    if columns and col not in columns:
                        continue
                    if val is None:
                        zm.null_count[col] = 1
                    else:
                        zm.min[col] = val
                        zm.max[col] = val
                        zm.null_count[col] = 0
            return zm
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        # Can't compute zone map — return empty (will never prune)
        return ZoneMap(row_count=row_count)

    # ------------------------------------------------------------------
    # Read: scan zone maps with pruning
    # ------------------------------------------------------------------

    def get_zone_map(self, collection: str,
                     row_group_key: str) -> Optional[dict]:
        """Get the zone map for a specific row group key.

        Returns the zone map dict (including blob_hash), or None.
        """
        # Use ProllyLensBase to read the current state (handles commit→snapshot)
        base = self._get_base(collection)
        state = base.read_all()
        zm_blob_hash = state.get(row_group_key)
        if not zm_blob_hash:
            return None
        return json.loads(self.kernel.read_blob(zm_blob_hash))

    def scan_with_pruning(self, collection: str,
                          predicate: Optional[PruningPredicate] = None,
                          start_key: Optional[str] = None,
                          end_key: Optional[str] = None,
                          verbose: bool = False) -> Iterator:
        """Scan data blob hashes, skipping row groups that can be pruned.

        This is the core of Vortex-style pushdown for Pond:
          1. Walk the zone-map ProllyTreeIndex
          2. For each entry, read the zone map blob (small, cheap)
          3. Evaluate PruningPredicate.can_prune(zone_map)
          4. If pruned: skip (data blob NOT read, NOT decoded)
          5. If not pruned: yield the data blob hash for the caller to read

        Args:
            collection: collection name
            predicate: PruningPredicate to evaluate. If None, no pruning
                (yield all data blob hashes).
            start_key: optional lower bound on row group keys (inclusive)
            end_key: optional upper bound on row group keys (inclusive)
            verbose: if True, yield (row_group_key, data_blob_hash, zm_dict)
                tuples instead of just data_blob_hash strings. The verbose
                form lets the caller do column-chunk pruning without a
                second zone-map lookup.

        Yields:
            data_blob_hash strings for row groups that MIGHT match.
            OR (if verbose=True) tuples of (row_group_key, data_blob_hash,
            zm_dict). The caller reads + decodes only these blobs.
        """
        # Use ProllyLensBase to read the current state (handles commit→snapshot)
        base = self._get_base(collection)
        state = base.read_all()
        if not state:
            return  # No zone maps — nothing to scan

        # Sort by key for deterministic order
        zm_keys = sorted(k for k in state.keys() if not k.startswith("_"))

        # Apply lower bound if specified
        if start_key is not None:
            zm_keys = [k for k in zm_keys if k >= start_key]

        # Apply upper bound if specified (was previously ignored — bug fix M3)
        if end_key is not None:
            zm_keys = [k for k in zm_keys if k <= end_key]

        for zm_key in zm_keys:
            zm_blob_hash = state[zm_key]

            # Read the zone map blob (small — just min/max/null_count)
            zm_dict = json.loads(self.kernel.read_blob(zm_blob_hash))

            # Evaluate pruning predicate if provided
            if predicate is not None:
                zm = ZoneMap.from_dict(zm_dict)
                if predicate.can_prune(zm):
                    continue  # SKIP — data blob not read, not decoded

            # Yield the DATA blob hash (not the zone-map blob hash)
            # The zone map dict includes "blob_hash" pointing to the data
            data_blob_hash = zm_dict.get("blob_hash", zm_blob_hash)
            if verbose:
                yield (zm_key, data_blob_hash, zm_dict)
            else:
                yield data_blob_hash

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def rebuild_zone_maps(self, collection: str,
                          scan_fn: Iterator[tuple[str, bytes, int]],
                          columns: Optional[list[str]] = None) -> str:
        """Rebuild all zone maps for a collection by rescanning data.

        This is the same as build_zone_maps_from_scan but overwrites
        existing zone maps (clears old ones first).
        """
        # Clear existing zone maps
        base = ProllyLensBase(self.kernel, f"{collection}__zone_maps")
        existing = base.read_all()
        for k in existing.keys():
            base.stage_delete(k)

        return self.build_zone_maps_from_scan(collection, scan_fn, columns)

    def drop_zone_maps(self, collection: str) -> bool:
        """Drop all zone maps for a collection (tombstone pattern)."""
        from maintenance import drop_name
        ref = self._zm_ref(collection)
        current = self.kernel.resolve(ref)
        if not current:
            return False
        drop_name(self.kernel, ref)
        return True

    def has_zone_maps(self, collection: str) -> bool:
        """Check if a collection has zone maps."""
        return self.kernel.resolve(self._zm_ref(collection)) is not None
