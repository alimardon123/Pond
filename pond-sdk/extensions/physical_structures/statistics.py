"""
Statistics — column-level min/max/null_count Physical Structure.

Statistics enable chunk pruning: if the query value is outside the
[min, max] range of a column, skip the chunk entirely.

Use cases:
  - Range query pruning (skip chunks where value can't exist)
  - Query optimization (estimate selectivity)
  - Cross-Lens pruning (Track 2: built by Lakehouse, used by FeatureStore)

Storage:
  - JSON blob with per-column stats
  - Referenced by __stats/{collection}
  - Any Lens can build or query it

Usage:
    from extensions.physical_structures import Statistics

    # Build from a PyArrow table
    Statistics.build(kernel, "users", table_data)

    # Use for pruning
    stats = Statistics.load(kernel, "users")
    Statistics.can_prune(stats, "age", 999)  # → True (can skip; 999 > max)
    Statistics.can_prune(stats, "age", 25)   # → False (can't skip; 25 in range)
"""

from __future__ import annotations
import json
from typing import Optional, Any
from extensions.physical_structures.base import PhysicalStructure


class Statistics(PhysicalStructure):
    """Column-level statistics for pruning and optimization."""

    type_name = "stats"

    @staticmethod
    def build(kernel, collection: str, source_data: Any, **kwargs) -> str:
        """Build column statistics from source data.

        Args:
            kernel: PondMinimal instance
            collection: collection name
            source_data: PyArrow Table or dict of {column: [values]}

        Returns:
            Blob hash of the stored statistics.
        """
        # Handle PyArrow Table
        if hasattr(source_data, "column_names"):
            columns = source_data.column_names
            data = {}
            for col_name in columns:
                col = source_data.column(col_name)
                try:
                    values = col.to_pylist()
                    non_null = [v for v in values if v is not None]
                    if non_null:
                        data[col_name] = {
                            "min": str(min(non_null)),
                            "max": str(max(non_null)),
                            "null_count": col.null_count,
                            "count": len(col),
                        }
                    else:
                        data[col_name] = {
                            "min": None, "max": None,
                            "null_count": col.null_count, "count": len(col),
                        }
                except Exception:
                    data[col_name] = {
                        "min": None, "max": None,
                        "null_count": col.null_count, "count": len(col),
                    }
        else:
            # Handle dict of {column: [values]}
            data = {}
            for col_name, values in source_data.items():
                non_null = [v for v in values if v is not None]
                if non_null:
                    data[col_name] = {
                        "min": str(min(non_null)),
                        "max": str(max(non_null)),
                        "null_count": sum(1 for v in values if v is None),
                        "count": len(values),
                    }
                else:
                    data[col_name] = {
                        "min": None, "max": None,
                        "null_count": len(values), "count": len(values),
                    }

        blob_hash = kernel.write(json.dumps(data, sort_keys=True).encode())
        kernel.reference(Statistics.ref_name(collection), blob_hash)
        return blob_hash

    @staticmethod
    def can_prune(stats: dict, column: str, value: Any) -> bool:
        """Check if a chunk can be skipped based on statistics.

        Returns True if the chunk CANNOT contain the value (can be pruned).
        Returns False if the chunk MIGHT contain the value (must scan).
        """
        if column not in stats:
            return False
        col_stats = stats[column]
        if col_stats["min"] is None or col_stats["max"] is None:
            return False
        try:
            # Try numeric comparison first; fall back to string
            try:
                val = float(value)
                min_val = float(col_stats["min"])
                max_val = float(col_stats["max"])
            except (ValueError, TypeError):
                val = str(value)
                min_val = str(col_stats["min"])
                max_val = str(col_stats["max"])
            if val < min_val or val > max_val:
                return True
        except Exception:
            pass
        return False
