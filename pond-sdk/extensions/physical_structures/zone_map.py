"""
Zone Map — per-chunk min/max Physical Structure for range pruning.

Zone maps store min/max for each chunk of data. When querying a range,
chunks whose [min, max] doesn't overlap the query range can be skipped.

Unlike Statistics (which covers the entire collection), Zone Maps
cover individual chunks — enabling finer-grained pruning.

Use cases:
  - Range query pruning at chunk granularity
  - Time-series: skip chunks outside the time range
  - Cross-Lens pruning (any Lens builds, any Lens queries)

Storage:
  - JSON blob with per-chunk stats
  - Referenced by __zonemaps/{collection}
  - Any Lens can build or query it

Usage:
    from extensions.physical_structures import ZoneMap

    # Build from chunked data
    ZoneMap.build(kernel, "events", chunks={"chunk_0": [1,2,3], "chunk_1": [4,5,6]})

    # Find which chunks might contain values in [3, 5]
    zm = ZoneMap.load(kernel, "events")
    relevant = ZoneMap.find_relevant_chunks(zm, 3, 5)  # → ["chunk_0", "chunk_1"]
"""

from __future__ import annotations
import json
from typing import Optional, Any, List
from extensions.physical_structures.base import PhysicalStructure


class ZoneMap(PhysicalStructure):
    """Per-chunk min/max for range pruning."""

    type_name = "zonemaps"

    @staticmethod
    def build(kernel, collection: str, source_data: dict, **kwargs) -> str:
        """Build zone maps from chunked data.

        Args:
            kernel: PondMinimal instance
            collection: collection name
            source_data: dict of {chunk_id: [values]} or {chunk_id: {"min": x, "max": y}}

        Returns:
            Blob hash of the stored zone maps.
        """
        zone_maps = []
        for chunk_id, values in source_data.items():
            if isinstance(values, dict) and "min" in values:
                zone_maps.append({
                    "chunk_id": chunk_id,
                    "min": values["min"],
                    "max": values["max"],
                })
            else:
                non_null = [v for v in values if v is not None]
                if non_null:
                    zone_maps.append({
                        "chunk_id": chunk_id,
                        "min": min(non_null),
                        "max": max(non_null),
                    })
                else:
                    zone_maps.append({
                        "chunk_id": chunk_id,
                        "min": None,
                        "max": None,
                    })

        blob_hash = kernel.write(json.dumps(zone_maps, sort_keys=True).encode())
        kernel.reference(ZoneMap.ref_name(collection), blob_hash)
        return blob_hash

    @staticmethod
    def find_relevant_chunks(zone_maps: list, target_min: Any, target_max: Any) -> list:
        """Find chunks whose [min, max] overlaps [target_min, target_max].

        Returns list of chunk_ids that MIGHT contain values in the target range.
        Chunks whose range doesn't overlap are skipped (pruned).
        """
        relevant = []
        for zm in zone_maps:
            if zm["min"] is None or zm["max"] is None:
                relevant.append(zm["chunk_id"])
                continue
            # Skip if chunk range doesn't overlap target range
            if zm["max"] < target_min or zm["min"] > target_max:
                continue
            relevant.append(zm["chunk_id"])
        return relevant

    @staticmethod
    def query(kernel, collection: str, target_min: Any, target_max: Any, **kwargs) -> list:
        """Query: find chunks that might contain values in [target_min, target_max]."""
        data = ZoneMap.load(kernel, collection)
        if data is None:
            return []  # No zone maps = can't prune
        return ZoneMap.find_relevant_chunks(data, target_min, target_max)
