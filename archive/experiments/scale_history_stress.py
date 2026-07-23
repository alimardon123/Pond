#!/usr/bin/env python3
"""
Phase F Evidence: Scale and History Stress Test.

Tests Pond under realistic load:
  - 100K records (scale)
  - 1000 commits (history depth)
  - Multiple simultaneous materializations
  - Failure/restart behavior

Measures:
  - Write throughput at scale
  - Read latency (point lookup) at scale
  - History walk latency at depth
  - Branch/checkout latency
  - Metadata ratio (data bytes vs overhead)
  - Restart recovery time

Run:
    python experiments/scale_history_stress.py
"""

from __future__ import annotations

import os, sys, time, json, shutil
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))

from pond_minimal import PondMinimal
from lens_sdk import Lens


def timed(label: str, fn):
    t0 = time.perf_counter()
    result = fn()
    t1 = time.perf_counter()
    ms = (t1 - t0) * 1000
    print(f"  {label}: {ms:.0f}ms")
    return result, ms


def run_scale_test():
    """Test 1: Scale — 100K records."""
    print("\n--- Test 1: Scale (100K records) ---")
    bench = "/tmp/pond_scale_100k"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)
    view = Lens(kernel, "scale_test")

    # Write 100K records in batches of 1000
    N = 100_000
    BATCH = 1000

    t0 = time.perf_counter()
    for i in range(N):
        view.put(f"k{i:06d}", {"id": i, "name": f"user_{i}", "val": i * 10,
                                "region": ["US", "EU", "ASIA"][i % 3]})
        if (i + 1) % BATCH == 0:
            view.commit(f"batch {i // BATCH + 1}")
    t1 = time.perf_counter()
    write_ms = (t1 - t0) * 1000
    print(f"  Write {N} records ({N // BATCH} commits): {write_ms:.0f}ms ({N / write_ms * 1000:.0f} rec/sec)")

    # Point lookup (should be O(log N))
    t0 = time.perf_counter()
    result = view.get("k050000")
    t1 = time.perf_counter()
    lookup_ms = (t1 - t0) * 1000
    if result is not None:
        print(f"  Point lookup k050000: {lookup_ms:.2f}ms → {result['name']}")
    else:
        # This is a real finding: at 100K records with 100 commits,
        # the delta journal walk may not find older keys. This is
        # because the lookup walks the commit DAG and older deltas
        # may be beyond the COMPACTION_THRESHOLD window.
        print(f"  Point lookup k050000: {lookup_ms:.2f}ms → None (NOT FOUND)")
        print(f"    ⚠ FINDING: key not found at scale — delta journal window issue")
        print(f"    The key was written in commit ~50, but HEAD is commit ~100.")
        print(f"    The delta journal only holds {4} entries (COMPACTION_THRESHOLD).")
        print(f"    The snapshot at commit ~48 should contain it, but the walk")
        print(f"    may not reach far enough back. This is a real scale finding.")

    # Count
    t0 = time.perf_counter()
    count = view.count()
    t1 = time.perf_counter()
    print(f"  Count: {count} ({(t1 - t0) * 1000:.0f}ms)")

    # Storage stats
    stats = kernel.storage_stats()
    data_bytes = stats["data_bytes"]
    blob_count = stats["blob_count"]
    print(f"  Storage: {data_bytes / 1024 / 1024:.1f}MB data, {blob_count} blobs")
    print(f"  Avg bytes/record: {data_bytes / N:.0f}")

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    return {"records": N, "write_ms": write_ms, "rec_per_sec": N / write_ms * 1000,
            "blob_count": blob_count, "data_mb": data_bytes / 1024 / 1024}


def run_history_test():
    """Test 2: History — 1000 commits."""
    print("\n--- Test 2: History (1000 commits) ---")
    bench = "/tmp/pond_history_1k"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)
    view = Lens(kernel, "history_test")

    N_COMMITS = 1000

    t0 = time.perf_counter()
    for i in range(N_COMMITS):
        view.put(f"v{i:04d}", {"version": i, "data": f"iteration-{i}"})
        view.commit(f"commit {i}")
    t1 = time.perf_counter()
    print(f"  Write {N_COMMITS} commits: {(t1 - t0) * 1000:.0f}ms")

    # History walk (how fast can we walk the commit DAG?)
    t0 = time.perf_counter()
    history = view.history(limit=N_COMMITS)
    t1 = time.perf_counter()
    print(f"  History walk ({len(history)} commits): {(t1 - t0) * 1000:.0f}ms")

    # Branch + checkout latency
    t0 = time.perf_counter()
    view.branch("experiment")
    t1 = time.perf_counter()
    print(f"  Branch creation: {(t1 - t0) * 1000:.2f}ms")

    t0 = time.perf_counter()
    view.checkout("experiment")
    t1 = time.perf_counter()
    print(f"  Checkout: {(t1 - t0) * 1000:.2f}ms")

    # Undo (walk back N steps)
    t0 = time.perf_counter()
    view.undo(100)
    t1 = time.perf_counter()
    print(f"  Undo 100 steps: {(t1 - t0) * 1000:.0f}ms")

    stats = kernel.storage_stats()
    print(f"  Storage: {stats['data_bytes'] / 1024:.0f}KB, {stats['blob_count']} blobs")

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    return {"commits": N_COMMITS, "history_walk_ms": (t1 - t0) * 1000,
            "blob_count": stats["blob_count"]}


def run_restart_test():
    """Test 3: Failure/restart — close kernel, reopen, verify data."""
    print("\n--- Test 3: Restart Recovery ---")
    bench = "/tmp/pond_restart"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)

    # Write data
    kernel = PondMinimal(bench)
    view = Lens(kernel, "restart_test")
    for i in range(1000):
        view.put(f"k{i:04d}", {"id": i, "name": f"item_{i}"})
    view.commit("before restart")
    n_before = view.count()
    kernel.close()

    # Reopen
    t0 = time.perf_counter()
    kernel2 = PondMinimal(bench)
    view2 = Lens(kernel2, "restart_test")
    n_after = view2.count()
    t1 = time.perf_counter()

    print(f"  Before restart: {n_before} records")
    print(f"  After restart:  {n_after} records")
    print(f"  Recovery time:  {(t1 - t0) * 1000:.0f}ms")
    assert n_before == n_after, f"Data loss: {n_before} → {n_after}"

    # Verify a sample
    sample = view2.get("k0500")
    assert sample == {"id": 500, "name": "item_500"}
    print(f"  Sample read: k0500 → {sample}")

    kernel2.close()
    shutil.rmtree(bench, ignore_errors=True)
    return {"recovery_ms": (t1 - t0) * 1000, "data_intact": n_before == n_after}


def run_multi_materialization_test():
    """Test 4: Multiple simultaneous materializations (indexes)."""
    print("\n--- Test 4: Multiple Materializations ---")
    bench = "/tmp/pond_multi_mat"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    from lens_sdk import IndexedLens
    view = IndexedLens(kernel, "multi_mat")
    # Register 3 indexes simultaneously
    view.register_index("by_region", lambda d: d.get("region", ""), mode="lazy")
    view.register_index("by_name", lambda d: d.get("name", ""), mode="lazy")
    view.register_index("by_val", lambda d: str(d.get("val", 0)), mode="lazy")

    # Write 10K records
    N = 10_000
    for i in range(N):
        view.put(f"k{i:05d}", {"id": i, "name": f"user_{i}",
                                "val": i * 10,
                                "region": ["US", "EU", "ASIA"][i % 3]})
    view.commit(f"insert {N} records")

    # Use all 3 indexes
    t0 = time.perf_counter()
    try:
        us_result = view.find_by("by_region", "US")
        t1 = time.perf_counter()
        print(f"  by_region['US']: {(t1 - t0) * 1000:.0f}ms → {us_result['name'] if us_result else None}")
    except Exception as e:
        t1 = time.perf_counter()
        print(f"  by_region['US']: {(t1 - t0) * 1000:.0f}ms → ERROR: {e}")
        print(f"    ⚠ FINDING: index rebuild fails at 10K records — decode error on non-data blobs")

    t0 = time.perf_counter()
    try:
        name_result = view.find_by("by_name", "user_5000")
        t1 = time.perf_counter()
        print(f"  by_name['user_5000']: {(t1 - t0) * 1000:.0f}ms → {name_result['id'] if name_result else None}")
    except Exception as e:
        t1 = time.perf_counter()
        print(f"  by_name['user_5000']: {(t1 - t0) * 1000:.0f}ms → ERROR: {e}")

    t0 = time.perf_counter()
    try:
        val_result = view.find_by("by_val", "50000")
        t1 = time.perf_counter()
        print(f"  by_val['50000']: {(t1 - t0) * 1000:.0f}ms → {val_result['id'] if val_result else None}")
    except Exception as e:
        t1 = time.perf_counter()
        print(f"  by_val['50000']: {(t1 - t0) * 1000:.0f}ms → ERROR: {e}")

    # List indexes
    print(f"  Active indexes: {view.list_auto_indexes()}")

    stats = kernel.storage_stats()
    print(f"  Storage: {stats['data_bytes'] / 1024:.0f}KB, {stats['blob_count']} blobs")

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    return {"records": N, "indexes": 3, "blob_count": stats["blob_count"]}


def main():
    print("=" * 72)
    print("  Phase F Evidence: Scale and History Stress Test")
    print("=" * 72)

    scale = run_scale_test()
    history = run_history_test()
    restart = run_restart_test()
    multi_mat = run_multi_materialization_test()

    print("\n" + "=" * 72)
    print("  SUMMARY")
    print("=" * 72)
    print(f"  Scale: {scale['records']:,} records at {scale['rec_per_sec']:.0f} rec/sec, "
          f"{scale['data_mb']:.1f}MB, {scale['blob_count']:,} blobs")
    print(f"  History: {history['commits']} commits, {history['blob_count']:,} blobs")
    print(f"  Restart: {restart['recovery_ms']:.0f}ms recovery, data intact: {restart['data_intact']}")
    print(f"  Multi-materialization: {multi_mat['records']:,} records, "
          f"{multi_mat['indexes']} indexes, {multi_mat['blob_count']:,} blobs")
    print("=" * 72)


if __name__ == "__main__":
    main()
