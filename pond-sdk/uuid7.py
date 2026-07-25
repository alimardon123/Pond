"""
UUIDv7 — time-ordered UUID for distributed row identification.

UUIDv7 format (128 bits total):
  - 48 bits: Unix epoch milliseconds (big-endian)
  - 4 bits: version (7)
  - 12 bits: random_a
  - 2 bits: variant (10)
  - 62 bits: random_b

Properties:
  - Time-ordered: earlier timestamps sort before later ones lexicographically.
    This makes UUIDv7 ideal for range scans and ProllyTreeIndex keys.
  - Distributed-friendly: no central ID allocator needed. Each node generates
    unique IDs using its clock + random bits.
  - Monotonic-ish: within the same millisecond, random bits provide uniqueness.
    For strict monotonicity within a process, see uuidv7_monotonic().
  - Backward-compatible: formatted as a standard UUID string (36 chars with dashes).

Used by:
  - AutoIndexMixin: generates _rowid for rows that don't have a natural key.
  - LakehouseLens: hidden _rowid column for row-level indexing.
  - Any lens that needs distributed row identification.

Reference: RFC 9562 (UUID Version 7)
"""

from __future__ import annotations

import os
import time
import uuid
import threading
from typing import Optional


# Monotonic counter for same-millisecond uniqueness (per-process)
_monotonic_lock = threading.Lock()
_last_ms: int = 0
_counter: int = 0


def uuidv7(timestamp_ms: Optional[int] = None) -> str:
    """Generate a UUIDv7 string (36 chars, dashed format).

    Args:
        timestamp_ms: Optional Unix epoch in milliseconds. If None, uses
            current time. Useful for testing and deterministic generation.

    Returns:
        UUIDv7 string like "0190f3e1-07c0-7abc-8def-0123456789ab".

    The first 12 hex chars encode the timestamp, so UUIDv7 values generated
    later sort after those generated earlier (lexicographic ordering).
    """
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)

    # 48 bits: timestamp_ms (big-endian)
    # 4 bits: version (7)
    # 12 bits: random_a
    # 2 bits: variant (10 → 0b10)
    # 62 bits: random_b

    rand_bytes = os.urandom(10)  # 80 random bits; we use 74

    # Build the 128-bit UUID
    # Bytes 0-5: timestamp (48 bits)
    ts_bytes = timestamp_ms.to_bytes(6, 'big')

    # Byte 6: version (4 bits) + random_a high (4 bits)
    version = 0x70  # version 7 << 4
    byte6 = version | (rand_bytes[0] & 0x0F)

    # Byte 7: random_a low (8 bits)
    byte7 = rand_bytes[1]

    # Byte 8: variant (2 bits = 10) + random_b high (6 bits)
    byte8 = 0x80 | (rand_bytes[2] & 0x3F)

    # Bytes 9-15: random_b remaining (56 bits)
    byte9_to_15 = rand_bytes[3:10]

    uuid_bytes = bytes(ts_bytes) + bytes([byte6, byte7, byte8]) + byte9_to_15

    # Format as standard UUID string
    return str(uuid.UUID(bytes=uuid_bytes))


def uuidv7_monotonic() -> str:
    """Generate a UUIDv7 with guaranteed monotonic ordering within a process.

    If called multiple times within the same millisecond, uses a counter
    to ensure strict ordering. This is slower than uuidv7() due to locking
    but guarantees no duplicates and strict monotonicity per process.

    For distributed systems, each process generates independently — cross-
    process ordering is millisecond-granular (same as uuidv7()).
    """
    global _last_ms, _counter

    with _monotonic_lock:
        now_ms = int(time.time() * 1000)
        if now_ms <= _last_ms:
            # Same or earlier millisecond — use counter for ordering
            _counter += 1
            # Embed counter in the random bits (12 bits of random_a)
            # This gives us 4096 unique IDs per millisecond per process
            if _counter > 0xFFF:
                # Counter overflow — advance to next millisecond
                _last_ms += 1
                _counter = 0
                now_ms = _last_ms
            else:
                now_ms = _last_ms
        else:
            _last_ms = now_ms
            _counter = 0

        # Generate UUIDv7 with the (possibly adjusted) timestamp
        # and counter embedded in random_a
        ts_bytes = now_ms.to_bytes(6, 'big')

        version = 0x70
        byte6 = version | ((_counter >> 8) & 0x0F)
        byte7 = _counter & 0xFF

        rand_bytes = os.urandom(8)
        byte8 = 0x80 | (rand_bytes[0] & 0x3F)
        byte9_to_15 = rand_bytes[1:8]

        uuid_bytes = bytes(ts_bytes) + bytes([byte6, byte7, byte8]) + byte9_to_15
        return str(uuid.UUID(bytes=uuid_bytes))


def uuidv7_timestamp(uuid_str: str) -> int:
    """Extract the Unix millisecond timestamp from a UUIDv7 string.

    Returns:
        Unix epoch in milliseconds.
    """
    u = uuid.UUID(uuid_str)
    # First 48 bits = timestamp
    ts_bytes = u.bytes[:6]
    return int.from_bytes(ts_bytes, 'big')


def uuidv7_datetime(uuid_str: str):
    """Extract the timestamp from a UUIDv7 as a datetime object."""
    import datetime
    ts_ms = uuidv7_timestamp(uuid_str)
    return datetime.datetime.fromtimestamp(ts_ms / 1000, tz=datetime.timezone.utc)


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== UUIDv7 self-test ===")

    # Generate 5 UUIDs
    ids = [uuidv7() for _ in range(5)]
    for i, uid in enumerate(ids):
        ts = uuidv7_timestamp(uid)
        print(f"  {i}: {uid}  (ts={ts})")

    # Verify time ordering (with small delay)
    import time as _time
    id1 = uuidv7()
    _time.sleep(0.002)  # 2ms
    id2 = uuidv7()
    assert id1 < id2, f"UUIDv7 not time-ordered: {id1} >= {id2}"
    print(f"\n  [OK] time-ordered: {id1} < {id2}")

    # Verify monotonic
    mono_ids = [uuidv7_monotonic() for _ in range(100)]
    for i in range(len(mono_ids) - 1):
        assert mono_ids[i] < mono_ids[i + 1], f"Monotonic violated at {i}"
    print(f"  [OK] monotonic: 100 IDs strictly ordered")

    # Verify timestamp extraction
    ts = int(time.time() * 1000)
    uid = uuidv7(timestamp_ms=ts)
    assert uuidv7_timestamp(uid) == ts, f"Timestamp mismatch: {uuidv7_timestamp(uid)} != {ts}"
    print(f"  [OK] timestamp extraction: {ts} from {uid}")

    # Verify uniqueness (10000 IDs)
    unique_ids = set(uuidv7() for _ in range(10000))
    assert len(unique_ids) == 10000, "Duplicate UUIDs generated!"
    print(f"  [OK] uniqueness: 10000 unique IDs")

    # Verify datetime extraction
    dt = uuidv7_datetime(uid)
    print(f"  [OK] datetime: {dt}")

    print("\nAll UUIDv7 tests pass.")
