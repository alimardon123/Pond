"""
Hybrid Logical Clock (HLC) — clock-skew-safe versioning for CRDT.

PROBLEM (B5):
  UUIDv7-based _version uses wall clock for ordering. Under clock skew,
  writer A (clock 100ms ahead) wins over writer B even if B's update
  is logically newer. This breaks CRDT's "last-writer-wins" guarantee.

SOLUTION:
  HLC (Hybrid Logical Clock) combines physical time with a logical
  counter. It's monotonic even under clock skew:
    - If physical time > last physical: use physical, logical = 0
    - If physical time <= last physical: use last physical, logical += 1

  HLC tuples (physical_ms, logical) compare lexicographically:
    (T1, L1) < (T2, L2) iff T1 < T2 or (T1 == T2 and L1 < L2)

  This is the standard fix for LWW CRDTs under clock skew. Used by
  CockroachDB, YugabyteDB, and others.

FORMAT:
  HLC is encoded as a 16-byte string: 8 bytes physical_ms (big-endian)
  + 8 bytes logical (big-endian). This sorts lexicographically = chronologically.
"""
import time
import struct
from typing import Optional, Tuple


class HLC:
    """Hybrid Logical Clock — monotonic under clock skew.

    Each process has its own HLC instance. The clock advances on every
    tick (before each write). The logical counter increments when the
    physical clock hasn't advanced.

    Usage:
        clock = HLC()
        version1 = clock.tick()  # before write 1
        version2 = clock.tick()  # before write 2 (may have same physical, higher logical)

        # Compare versions (lexicographic = chronological)
        assert version1 < version2  # True
    """

    def __init__(self):
        self._physical: int = 0
        self._logical: int = 0

    def tick(self) -> str:
        """Advance the clock and return the current HLC value as a string.

        The string is 32 hex chars (16 bytes): 8 bytes physical_ms
        big-endian + 8 bytes logical big-endian. This sorts
        lexicographically = chronologically.
        """
        now = int(time.time() * 1000)  # physical time in ms

        if now > self._physical:
            self._physical = now
            self._logical = 0
        else:
            self._logical += 1

        # Encode as 16 bytes (8 physical + 8 logical, big-endian)
        raw = struct.pack(">QQ", self._physical, self._logical)
        return raw.hex()

    def observe(self, other: str) -> None:
        """Observe another HLC value (e.g., from a remote write).

        Updates the local clock to be at least as high as the observed
        value. This ensures monotonicity across processes.
        """
        if len(other) != 32:
            return  # not a valid HLC string
        try:
            raw = bytes.fromhex(other)
            other_physical, other_logical = struct.unpack(">QQ", raw)
        except (ValueError, struct.error):
            return

        now = int(time.time() * 1000)

        if now > self._physical and now > other_physical:
            self._physical = now
            self._logical = 0
        elif other_physical > self._physical:
            self._physical = other_physical
            self._logical = other_logical + 1
        elif other_physical == self._physical:
            if other_logical > self._logical:
                self._logical = other_logical + 1
            else:
                self._logical += 1
        else:
            self._logical += 1

    @staticmethod
    def compare(a: str, b: str) -> int:
        """Compare two HLC strings. Returns -1, 0, or 1."""
        if a < b:
            return -1
        elif a > b:
            return 1
        return 0

    @staticmethod
    def max(a: str, b: str) -> str:
        """Return the later of two HLC values."""
        return a if a > b else b

    @staticmethod
    def is_valid(s: str) -> bool:
        """Check if a string is a valid HLC value."""
        return len(s) == 32 and all(c in "0123456789abcdef" for c in s)
