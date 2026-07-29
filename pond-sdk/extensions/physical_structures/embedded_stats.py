"""
Embedded stats — pruning metadata travels WITH the data blob.

DESIGN:
  Instead of storing zone maps as SEPARATE blobs (which requires extra
  fetches on S3), the stats (min/max/null_count per column) are embedded
  directly in the data blob header. The reader fetches ONE blob, reads
  the first ~100 bytes (header + stats), and decides whether to decode
  the rest. Zero extra round trips.

  This is what Parquet (row-group footer), Iceberg (manifest stats),
  and Vortex (array-level stats) do. Pond embeds stats in the blob.

FORMAT (after CompressionTag + EncodingHeader):
  StatsMagic:    4 bytes (b"STAT")
  n_columns:     2 bytes (uint16)
  For each column:
    col_name_len: 1 byte
    col_name:     col_name_len bytes (UTF-8)
    value_type:   1 byte (1=INT64, 2=FLOAT64, 3=STRING, 4=NULL)
    has_min:      1 byte (0=no min/max, 1=present)
    min:          8 bytes (INT64/FLOAT64) or 4B len + bytes (STRING)
    max:          8 bytes or variable
    null_count:   4 bytes (uint32)

  Total overhead: ~20 bytes per column. For a 3-column chunk: ~60 bytes.
  For a 1000-row INT64 chunk (8000 bytes payload): 0.75% overhead.

READ PATH (zero extra round trips):
  1. Fetch data blob (1 fetch — same as reading the data)
  2. Read StatsHeader from the first bytes
  3. If stats prove the blob can't match the predicate: SKIP (no decode)
  4. For surviving blobs: decode the payload

  No zone-map manifest fetch. No separate zone-map blobs. The stats
  travel WITH the data — they can never get out of sync.

GENERIC: works for ANY workload (tabular, KV, vector, streaming,
notebooks). The stats are per-column, and any ColumnSource can
produce them.
"""

from __future__ import annotations

import struct
import json
from typing import Optional, Any

_STATS_MAGIC = b"STAT"
VALUE_TYPE_INT64 = 1
VALUE_TYPE_FLOAT64 = 2
VALUE_TYPE_STRING = 3
VALUE_TYPE_NULL = 4


class ColumnStats:
    """Per-column statistics embedded in the blob header."""
    __slots__ = ("name", "value_type", "min", "max", "null_count")

    def __init__(self, name: str, value_type: int,
                 min: Any = None, max: Any = None, null_count: int = 0):
        self.name = name
        self.value_type = value_type
        self.min = min
        self.max = max
        self.null_count = null_count

    def can_prune(self, op: str, value: Any) -> bool:
        """Return True if this column's stats prove NO rows can match."""
        if self.min is None or self.max is None:
            return False  # no stats — can't prune
        try:
            if op == ">" and self.max <= value:
                return True
            if op == ">=" and self.max < value:
                return True
            if op == "<" and self.min >= value:
                return True
            if op == "<=" and self.min > value:
                return True
            if op == "=" and (value < self.min or value > self.max):
                return True
        except TypeError:
            return False  # type mismatch — can't prune
        return False


class StatsHeader:
    """Embedded stats header for a data blob.

    Contains per-column min/max/null_count. Travel WITH the data —
    no separate zone-map fetch needed.
    """

    @staticmethod
    def build(column_stats: list[ColumnStats]) -> bytes:
        """Build the stats header bytes from a list of ColumnStats."""
        parts = [_STATS_MAGIC, struct.pack("<H", len(column_stats))]
        for cs in column_stats:
            name_bytes = cs.name.encode("utf-8")
            parts.append(struct.pack("<B", len(name_bytes)))
            parts.append(name_bytes)
            parts.append(struct.pack("<B", cs.value_type))

            has_min = cs.min is not None and cs.max is not None
            parts.append(struct.pack("<B", 1 if has_min else 0))

            if has_min:
                if cs.value_type in (VALUE_TYPE_INT64,):
                    parts.append(struct.pack("<q", int(cs.min)))
                    parts.append(struct.pack("<q", int(cs.max)))
                elif cs.value_type == VALUE_TYPE_FLOAT64:
                    parts.append(struct.pack("<d", float(cs.min)))
                    parts.append(struct.pack("<d", float(cs.max)))
                elif cs.value_type == VALUE_TYPE_STRING:
                    min_bytes = str(cs.min).encode("utf-8")
                    max_bytes = str(cs.max).encode("utf-8")
                    parts.append(struct.pack("<I", len(min_bytes)))
                    parts.append(min_bytes)
                    parts.append(struct.pack("<I", len(max_bytes)))
                    parts.append(max_bytes)

            parts.append(struct.pack("<I", cs.null_count))
        return b"".join(parts)

    @staticmethod
    def parse(data: bytes, offset: int = 0) -> tuple[list[ColumnStats], int]:
        """Parse stats header from bytes. Returns (stats_list, new_offset).

        Returns ([], offset) if no stats header is present (legacy blob).
        """
        if len(data) - offset < 6:
            return [], offset
        if data[offset:offset + 4] != _STATS_MAGIC:
            return [], offset  # no stats — legacy blob

        offset += 4
        (n_cols,) = struct.unpack_from("<H", data, offset)
        offset += 2

        stats = []
        for _ in range(n_cols):
            (name_len,) = struct.unpack_from("<B", data, offset)
            offset += 1
            name = data[offset:offset + name_len].decode("utf-8")
            offset += name_len

            (vt,) = struct.unpack_from("<B", data, offset)
            offset += 1
            (has_min,) = struct.unpack_from("<B", data, offset)
            offset += 1

            mn = mx = None
            if has_min:
                if vt == VALUE_TYPE_INT64:
                    mn = struct.unpack_from("<q", data, offset)[0]
                    offset += 8
                    mx = struct.unpack_from("<q", data, offset)[0]
                    offset += 8
                elif vt == VALUE_TYPE_FLOAT64:
                    mn = struct.unpack_from("<d", data, offset)[0]
                    offset += 8
                    mx = struct.unpack_from("<d", data, offset)[0]
                    offset += 8
                elif vt == VALUE_TYPE_STRING:
                    (slen,) = struct.unpack_from("<I", data, offset)
                    offset += 4
                    mn = data[offset:offset + slen].decode("utf-8")
                    offset += slen
                    (slen,) = struct.unpack_from("<I", data, offset)
                    offset += 4
                    mx = data[offset:offset + slen].decode("utf-8")
                    offset += slen

            (null_count,) = struct.unpack_from("<I", data, offset)
            offset += 4

            stats.append(ColumnStats(name, vt, mn, mx, null_count))

        return stats, offset

    @staticmethod
    def can_prune_blob(stats: list[ColumnStats],
                        predicates: list[tuple[str, str, Any]]) -> bool:
        """Check if ANY predicate can prune this entire blob.

        Returns True if the blob CANNOT match (should be skipped).
        Returns False if the blob MIGHT match (should be decoded).
        """
        stat_lookup = {s.name: s for s in stats}
        for col, op, val in predicates:
            if col in stat_lookup:
                if stat_lookup[col].can_prune(op, val):
                    return True
        return False


def compute_column_stats(name: str, values: list) -> ColumnStats:
    """Compute ColumnStats from a list of values.

    Uses compute_list_stats for the min/max/null_count computation.
    """
    from column_source import compute_list_stats, _detect_value_type

    mn, mx, null_count = compute_list_stats(values)
    vt = _detect_value_type(values)
    return ColumnStats(name, vt, mn, mx, null_count)
