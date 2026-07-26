"""
PruningPredicate — Vortex-style predicate pushdown for Pond.

Inspired by the Vortex file format (https://vortex.dev), this module
implements predicate pushdown using zone maps (per-row-group min/max
statistics) to skip row groups that cannot match a filter — WITHOUT
decoding the row group data.

HOW IT WORKS (Vortex pattern):
  1. Zone maps: For each row group, store {min, max, null_count, row_count}
     for each column. This is metadata only — stored in the ProllyTreeIndex
     alongside the row group entries.
  2. PruningPredicate: Take the user's filter expression, simplify it to
     a conservative superset that can be evaluated against min/max stats.
     Drop non-monotonic functions, handle OR carefully.
  3. Pruning: For each row group, evaluate the pruning predicate against
     its zone map. If the predicate returns False, the row group CANNOT
     contain matching rows — skip it entirely (no data read, no decode).
     If True, the row group MIGHT contain matches — read and decode it.

This is Parquet-compatible (Parquet already stores row-group statistics).
The Vortex innovation is making this first-class in the index layer and
combining it with encoding-aware compute (future work).

Usage:
    from pruning import ZoneMap, PruningPredicate, ColumnPredicate

    # Build zone maps for a row group
    zm = ZoneMap.build(table, key_col="event_id")
    # zm.min = "e0001", zm.max = "e0100", zm.null_count = 0, zm.row_count = 100

    # Create a pruning predicate from a user filter
    pred = PruningPredicate([
        ColumnPredicate(column="age", op=">", value=30),
    ])

    # Check if a row group can be pruned
    if pred.can_prune(zm):
        skip_row_group()  # zone map says no match possible
    else:
        read_and_filter_row_group()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Any, Callable, Union


@dataclass
class ZoneMap:
    """Per-row-group statistics for pruning.

    Stores min/max/null_count/row_count for each column in a row group.
    Used by PruningPredicate to skip row groups that cannot match a filter.

    This is the Pond equivalent of Vortex's ZoneMap + Parquet's row-group
    statistics. The key insight: if a filter asks for age > 30, and the
    zone map says max(age) = 25 for this row group, the entire row group
    can be skipped without reading or decoding any data.

    Optionally stores column_chunks: per-column-chunk zone maps for
    finer-grained pruning within a surviving row group.
    """
    min: dict[str, Any] = field(default_factory=dict)      # column → min value
    max: dict[str, Any] = field(default_factory=dict)       # column → max value
    null_count: dict[str, int] = field(default_factory=dict)  # column → null count
    row_count: int = 0
    column_chunks: Optional[dict] = None  # ColumnChunkZoneMap.to_dict() for finer pruning

    @classmethod
    def build(cls, table, columns: Optional[list[str]] = None) -> "ZoneMap":
        """Build a ZoneMap from a PyArrow Table.

        Args:
            table: PyArrow Table (a single row group)
            columns: columns to compute stats for. If None, uses all columns.

        Returns:
            ZoneMap with min/max/null_count for each column.
        """
        import pyarrow.compute as pc

        if columns is None:
            columns = table.column_names

        zm = cls(row_count=table.num_rows)
        for col in columns:
            if col not in table.column_names:
                continue
            column = table[col]
            # Min/max (skip if all null)
            null_count = pc.sum(pc.is_null(column)).as_py()
            zm.null_count[col] = null_count
            if null_count < table.num_rows:
                try:
                    zm.min[col] = pc.min(column).as_py()
                    zm.max[col] = pc.max(column).as_py()
                except Exception:
                    # Some types (lists, structs) don't support min/max
                    pass
        return zm

    def to_dict(self) -> dict:
        """Serialize to a dict for storage in ProllyTreeIndex."""
        d = {
            "min": self.min,
            "max": self.max,
            "null_count": self.null_count,
            "row_count": self.row_count,
        }
        if self.column_chunks is not None:
            d["column_chunks"] = self.column_chunks
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "ZoneMap":
        """Deserialize from a dict."""
        return cls(
            min=d.get("min", {}),
            max=d.get("max", {}),
            null_count=d.get("null_count", {}),
            row_count=d.get("row_count", 0),
            column_chunks=d.get("column_chunks"),
        )


@dataclass
class ColumnPredicate:
    """A single-column predicate for pruning.

    Represents a condition like: column OP value
    where OP is one of: =, !=, <, <=, >, >=, in, not_in

    The PruningPredicate uses this to evaluate against zone maps.
    """
    column: str
    op: str  # "=", "!=", "<", "<=", ">", ">=", "in", "not_in"
    value: Any  # the comparison value (or list for "in"/"not_in")

    def evaluate_against_zone(self, zm: ZoneMap) -> bool:
        """Evaluate this predicate against a zone map.

        Returns True if the row group MIGHT contain matching rows
        (cannot be pruned). Returns False if the row group CANNOT
        contain matching rows (can be pruned).

        This is a CONSERVATIVE check: it may return True for row groups
        that don't actually contain matches (false positives), but it
        will never return False for a row group that does contain matches
        (no false negatives).
        """
        if self.column not in zm.min or self.column not in zm.max:
            # No stats for this column — can't prune, might match
            return True

        zmin = zm.min[self.column]
        zmax = zm.max[self.column]

        if self.op == "=":
            # Can prune if value < zmin or value > zmax
            return not (self.value < zmin or self.value > zmax)
        elif self.op == "!=":
            # Can prune only if all values in zone equal self.value
            # (i.e., zmin == zmax == self.value and null_count == 0)
            if zmin == zmax == self.value and zm.null_count.get(self.column, 0) == 0:
                return False  # all rows are self.value, none match !=
            return True
        elif self.op == "<":
            # Can prune if zmin >= self.value (all values >= value)
            return not (zmin >= self.value)
        elif self.op == "<=":
            # Can prune if zmin > self.value (all values > value)
            return not (zmin > self.value)
        elif self.op == ">":
            # Can prune if zmax <= self.value (all values <= value)
            return not (zmax <= self.value)
        elif self.op == ">=":
            # Can prune if zmax < self.value (all values < value)
            return not (zmax < self.value)
        elif self.op == "in":
            # value is a list. Can prune if no overlap between [zmin, zmax]
            # and the value list's [min, max].
            if not self.value:
                return False  # empty list — no match possible
            v_min = min(self.value)
            v_max = max(self.value)
            return not (v_max < zmin or v_min > zmax)
        elif self.op == "not_in":
            # Can prune only if all zone values are in the not_in list
            # and there are no nulls. Conservative: rarely prune.
            return True
        else:
            # Unknown op — can't prune
            return True


class PruningPredicate:
    """A collection of ColumnPredicates combined with AND or OR.

    Used to prune row groups based on zone maps. This is the Pond
    equivalent of Vortex's PruningPredicate.

    For AND: all predicates must say "might match" for the row group
    to survive pruning. If ANY predicate says "cannot match", prune.

    For OR: any predicate saying "might match" means the row group
    survives. Prune only if ALL predicates say "cannot match".
    """

    def __init__(self, predicates: list[ColumnPredicate],
                 combine: str = "and"):
        """Create a pruning predicate.

        Args:
            predicates: list of ColumnPredicate
            combine: "and" (all must match) or "or" (any must match)
        """
        self.predicates = predicates
        self.combine = combine

    def can_prune(self, zm: ZoneMap) -> bool:
        """Check if a row group can be pruned (skipped).

        Returns True if the row group CANNOT contain matching rows
        (safe to skip). Returns False if the row group MIGHT contain
        matching rows (must read and check).
        """
        if not self.predicates:
            return False  # no predicates — can't prune

        if self.combine == "and":
            # AND: prune if ANY predicate says "cannot match"
            for pred in self.predicates:
                if not pred.evaluate_against_zone(zm):
                    return True  # this predicate can never match → prune
            return False  # all predicates might match → can't prune
        elif self.combine == "or":
            # OR: prune only if ALL predicates say "cannot match"
            for pred in self.predicates:
                if pred.evaluate_against_zone(zm):
                    return False  # this predicate might match → can't prune
            return True  # all predicates cannot match → prune
        else:
            return False  # unknown combine — can't prune
