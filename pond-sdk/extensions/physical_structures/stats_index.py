"""
StatsIndex — ONE blob, TWO round trips, ANY workload.

A single JSON blob stored at ref `collections/{name}/stats` that
aggregates min/max/null_count across ALL row groups in the collection.

Read path:
  1. Fetch stats blob (1 fetch — ~20KB for 100 row groups)
  2. Evaluate predicate → identify surviving row groups
  3. Fetch surviving data blobs (1-N fetches)
  Total: 2-N round trips (vs N without stats index)

Write path:
  1. Lens writes data blobs (existing path)
  2. Lens calls stats_index.update(name, entries) — ONE write
  3. Stats ref is updated atomically with the commit

This replaces ZoneMapIndex (460 LOC) with ~100 LOC. No separate
ProllyTreeIndex for zone maps. No add_zone_map/commit_zone_maps API.
Just one blob, one ref, one fetch.

GENERIC: works for ANY workload (tabular, KV, vector, streaming,
notebooks). The stats are per-column, and any lens can produce them.
"""

from __future__ import annotations

import json
from typing import Optional, Any, Iterator
from dataclasses import dataclass, field


@dataclass
class RowGroupStats:
    """Stats for a single row group — one entry in the stats index."""
    key: str                    # ProllyTreeIndex key (e.g., "rg/9999")
    blob_hash: str              # data blob hash
    n_rows: int = 0
    # column_name → {min, max, null_count}
    columns: dict[str, dict] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "blob_hash": self.blob_hash,
            "n_rows": self.n_rows,
            "columns": self.columns,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "RowGroupStats":
        return cls(
            key=d["key"],
            blob_hash=d["blob_hash"],
            n_rows=d.get("n_rows", 0),
            columns=d.get("columns", {}),
        )

    def can_prune(self, column: str, op: str, value: Any) -> bool:
        """Return True if this row group CANNOT match the predicate."""
        col_stats = self.columns.get(column)
        if not col_stats:
            return False  # no stats — can't prune
        mn = col_stats.get("min")
        mx = col_stats.get("max")
        if mn is None or mx is None:
            return False
        try:
            if op == ">" and mx <= value: return True
            if op == ">=" and mx < value: return True
            if op == "<" and mn >= value: return True
            if op == "<=" and mn > value: return True
            if op == "=" and (value < mn or value > mx): return True
            if op == "in":
                v_min, v_max = min(value), max(value)
                if mx < v_min or mn > v_max: return True
        except TypeError:
            return False
        return False


class StatsIndex:
    """Manages the stats index blob for a collection.

    ONE blob at ref `collections/{name}/stats`. Updated atomically
    on every commit. Fetched in ONE read per query.
    """

    def __init__(self, kernel):
        self.kernel = kernel

    @staticmethod
    def _ref(collection: str) -> str:
        return f"collections/{collection}/stats"

    def update(self, collection: str,
               entries: list[RowGroupStats]) -> str:
        """Write/update the stats index for a collection.

        Replaces the entire stats index (overwrite semantics — same
        as the data write). Called by the lens after writing data.

        Args:
            collection: collection name
            entries: list of RowGroupStats (one per row group)

        Returns:
            The stats blob hash.
        """
        data = json.dumps(
            [e.to_dict() for e in entries],
            sort_keys=True, default=str
        ).encode()
        blob_hash = self.kernel.write(data)
        self.kernel.reference(self._ref(collection), blob_hash)
        return blob_hash

    def has_stats(self, collection: str) -> bool:
        """Check if a collection has a stats index."""
        return self.kernel.resolve(self._ref(collection)) is not None

    def load(self, collection: str) -> list[RowGroupStats]:
        """Load the stats index for a collection (1 fetch).

        Returns empty list if no stats index exists.
        """
        blob_hash = self.kernel.resolve(self._ref(collection))
        if not blob_hash:
            return []
        data = json.loads(self.kernel.read_blob(blob_hash))
        return [RowGroupStats.from_dict(e) for e in data]

    def scan_with_pruning(
            self,
            collection: str,
            predicates: Optional[list[tuple[str, str, Any]]] = None,
    ) -> Iterator[tuple[str, str, dict]]:
        """Scan the stats index, yielding surviving row groups.

        Fetches ONE stats blob, evaluates predicates, yields only
        row groups that MIGHT match. The caller fetches only the
        surviving data blobs.

        Args:
            collection: collection name
            predicates: list of (column, op, value) tuples.
                None = no pruning (yield all row groups).

        Yields:
            Tuples of (row_group_key, data_blob_hash, stats_dict).
        """
        entries = self.load(collection)
        if not entries:
            return

        self.last_scan_total = len(entries)

        for entry in entries:
            if predicates:
                pruned = False
                for col, op, val in predicates:
                    if entry.can_prune(col, op, val):
                        pruned = True
                        break
                if pruned:
                    continue

            yield (entry.key, entry.blob_hash, entry.to_dict())

    @property
    def last_scan_total(self) -> int:
        """Total row groups examined in the last scan (for stats)."""
        return getattr(self, '_last_scan_total', 0)

    @last_scan_total.setter
    def last_scan_total(self, value: int):
        self._last_scan_total = value
