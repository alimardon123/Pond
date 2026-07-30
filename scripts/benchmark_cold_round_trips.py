"""
Cold-read round-trip benchmark — HONEST counts, no caching, no SQLite.

This benchmark measures the REAL cost of storage interactions on an
object store:
  - Every kernel.read_blob() call is 1 S3 GET
  - Every ref resolution is 2 S3 GETs (root pointer + root ref blob)
  - NO SQLite (all state in the object store)
  - NO caching across measurements (caches invalidated before each read)
  - Simulated S3 latency (default 50ms/GET) to show real-world timing

The benchmark proves that the unified storage + object-store-native
kernel achieves the irreducible minimum round trips for content-addressed
object storage.

COLD READ PATH (no caches):
  1. Read root pointer (1 GET — well-known path, ~80 bytes)
  2. Read root ref blob (1 GET — content-addressed, ~1KB for 50 refs)
  3. Look up collections/{name}/manifest in the root ref (in-memory, free)
  4. Read manifest blob (1 GET — has all row-group stats + blob hashes)
  5. Evaluate predicates IN MEMORY against manifest stats (free)
  6. For each surviving row group: read 1 data blob (1 GET each)

Total cold: 3 + K S3 GETs (root pointer + root ref + manifest + K data blobs)
  - K=1 (point lookup or 1% selectivity): 4 GETs
  - K=10 (10% selectivity): 13 GETs
  - K=100 (full scan of 100 row groups): 103 GETs

With SDK caching (warm reads):
  - Root ref blob cached: 2 + K GETs
  - Manifest cached: 1 + K GETs
  - Both cached: K GETs (just the data blobs)
"""
from __future__ import annotations

import os
import sys
import time
import statistics

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk", "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from unified_storage import UnifiedStorage


def make_test_rows(n_rows: int, n_groups: int) -> list[dict]:
    """Build test data: n_rows in n_groups row groups."""
    ids = list(range(n_rows))
    ages = [i % 100 for i in ids]
    regions = [("ASIA", "EU", "US")[i % 3] for i in ids]
    scores = [float(i % 100) + 0.5 for i in ids]
    return [{"id": i, "age": a, "region": r, "score": s}
            for i, a, r, s in zip(ids, ages, regions, scores)]


def cold_read_benchmark():
    """Measure COLD round trips for each interaction type.

    Cold = invalidate all caches before each read.
    """
    print("\n" + "=" * 80)
    print("COLD-READ ROUND-TRIP BENCHMARK (no cache, no SQLite)")
    print("=" * 80)
    print("""
Setup:
  - ObjectStoreNativeKernel (NO SQLite — refs as content-addressed blobs)
  - InMemoryObjectStore with simulated S3 latency
  - UnifiedStorage (ONE format, ONE write, ONE read)
  - All caches invalidated before each measurement
""")

    # Test at different scales
    configs = [
        ("10 row groups", 1_000, 10, 100),       # 10 row groups, 100 rows each
        ("100 row groups", 10_000, 100, 100),    # 100 row groups, 100 rows each
        ("1,000 row groups", 100_000, 1_000, 100),  # 1000 row groups
    ]

    # Simulated S3 latency values
    latencies = [0.0, 5.0, 50.0]  # 0ms (pure in-memory), 5ms (LAN), 50ms (S3)

    for label, n_rows, n_groups, rg_size in configs:
        print(f"\n--- Scale: {label} ({n_rows:,} rows, {rg_size} rows/group) ---")
        rows = make_test_rows(n_rows, n_groups)

        for latency_ms in latencies:
            kernel, store = make_object_store_native_kernel(latency_ms=latency_ms)
            storage = UnifiedStorage(kernel)
            storage.write("test", rows, key_col="id", row_group_size=rg_size)

            print(f"\n  Latency: {latency_ms}ms/GET"
                  f"{' (S3-like)' if latency_ms == 50 else ''}"
                  f"{' (LAN-like)' if latency_ms == 5 else ''}"
                  f"{' (in-memory)' if latency_ms == 0 else ''}")

            # ---- Cold point lookup ----
            kernel.invalidate_root_cache()
            storage._manifest_cache.clear()
            kernel.reset_stats()
            store.reset_stats()

            storage.point_lookup("test", key="9")  # first row group

            data_gets = kernel.stats["reads"]
            ref_gets = kernel.stats["ref_reads"]
            total_gets = data_gets + ref_gets
            actual_latency = store.stats["latency_ms_total"]

            print(f"    Cold point lookup:        {total_gets} GETs "
                  f"({ref_gets} ref + {data_gets} data)"
                  f"  = {actual_latency:.0f}ms")

            # ---- Cold full scan ----
            kernel.invalidate_root_cache()
            storage._manifest_cache.clear()
            kernel.reset_stats()
            store.reset_stats()

            storage.read("test")

            data_gets = kernel.stats["reads"]
            ref_gets = kernel.stats["ref_reads"]
            total_gets = data_gets + ref_gets
            actual_latency = store.stats["latency_ms_total"]

            print(f"    Cold full scan:           {total_gets} GETs "
                  f"({ref_gets} ref + {data_gets} data)"
                  f"  = {actual_latency:.0f}ms")

            # ---- Cold predicate-pruned read (1% selectivity) ----
            kernel.invalidate_root_cache()
            storage._manifest_cache.clear()
            kernel.reset_stats()
            store.reset_stats()

            # id > (n_rows - n_rows//100) → 1% selectivity = 1 row group
            threshold = n_rows - n_rows // 100
            storage.read("test", predicates=[("id", ">", threshold)])

            data_gets = kernel.stats["reads"]
            ref_gets = kernel.stats["ref_reads"]
            total_gets = data_gets + ref_gets
            actual_latency = store.stats["latency_ms_total"]

            print(f"    Cold pruned read (1%):    {total_gets} GETs "
                  f"({ref_gets} ref + {data_gets} data)"
                  f"  = {actual_latency:.0f}ms")

            # ---- Warm point lookup (caches populated) ----
            # Don't invalidate — root ref + manifest cached
            kernel.reset_stats()
            store.reset_stats()

            storage.point_lookup("test", key="9")

            data_gets = kernel.stats["reads"]
            ref_gets = kernel.stats["ref_reads"]
            total_gets = data_gets + ref_gets
            actual_latency = store.stats["latency_ms_total"]

            print(f"    Warm point lookup:        {total_gets} GETs "
                  f"({ref_gets} ref + {data_gets} data)"
                  f"  = {actual_latency:.0f}ms")


def write_path_benchmark():
    """Measure COLD round trips for the write path."""
    print("\n" + "=" * 80)
    print("WRITE PATH ROUND-TRIP BENCHMARK (no cache, no SQLite)")
    print("=" * 80)
    print()

    configs = [
        ("10 row groups", 1_000, 10, 100),
        ("100 row groups", 10_000, 100, 100),
    ]

    for label, n_rows, n_groups, rg_size in configs:
        kernel, store = make_object_store_native_kernel(latency_ms=50.0)
        storage = UnifiedStorage(kernel)
        rows = make_test_rows(n_rows, n_groups)

        kernel.reset_stats()
        store.reset_stats()

        storage.write("test", rows, key_col="id", row_group_size=rg_size)

        data_puts = kernel.stats["writes"]
        ref_puts = kernel.stats["ref_writes"]
        total_puts = data_puts + ref_puts
        actual_latency = store.stats["latency_ms_total"]

        print(f"  {label}: {total_puts} PUTs "
              f"({data_puts} data blobs + {ref_puts} ref updates)"
              f"  = {actual_latency:.0f}ms at 50ms/PUT")


def honest_summary():
    """Print the honest summary of round-trip counts."""
    print("\n" + "=" * 80)
    print("HONEST ROUND-TRIP SUMMARY (cold reads, no cache, no SQLite)")
    print("=" * 80)
    print("""
EVERY object-store interaction is counted. No SQLite hidden. No cache
assumed for cold reads.

COLD READ PATH (caches invalidated):
  1. Root pointer GET        (1 GET — well-known path, ~80 bytes)
  2. Root ref blob GET       (1 GET — content-addressed, ~1KB)
  3. Manifest blob GET       (1 GET — has all row-group stats)
  4. K data blob GETs        (K GETs — one per surviving row group)

  Total cold: 3 + K S3 GETs

  Point lookup (K=1):        4 GETs
  Pruned read 1% (K=1):      4 GETs
  Pruned read 10% (K=10):    13 GETs
  Full scan (K=N):           3 + N GETs

WARM READ PATH (root ref + manifest cached by SDK):
  1. K data blob GETs

  Total warm: K S3 GETs

  Point lookup (K=1):        1 GET
  Pruned read 1% (K=1):      1 GET
  Full scan (K=N):           N GETs

WRITE PATH:
  1. N data blob PUTs        (one per row group)
  2. 1 manifest blob PUT     (one per commit)
  3. 1 root ref blob PUT     (updated with manifest hash)
  4. 1 root pointer PUT      (updated with root ref hash)

  Total write: N + 3 S3 PUTs

REAL-WORLD TIMING (50ms/GET — typical S3):
  Cold point lookup:    4 × 50ms = 200ms
  Cold pruned read 1%:  4 × 50ms = 200ms
  Cold full scan (100): 103 × 50ms = 5.15s
  Warm point lookup:    1 × 50ms = 50ms

The 200ms cold point lookup is the irreducible cost of ref resolution
on an object store. SDK caching brings it down to 50ms (warm) by
reusing the root ref + manifest across reads.

The OLD design (SQLite + zone-map path) reported "0 ref reads" because
SQLite hid them. That's not honest for object stores — S3 has no SQLite.
This kernel makes the real cost explicit and measurable.
""")


if __name__ == "__main__":
    try:
        cold_read_benchmark()
        write_path_benchmark()
        honest_summary()
        print("=" * 80)
        print("ALL BENCHMARKS COMPLETE")
        print("=" * 80)
        sys.exit(0)
    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
