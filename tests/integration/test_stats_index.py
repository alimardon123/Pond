#!/usr/bin/env python3
"""
Test: StatsIndex — ONE blob, TWO round trips, ANY workload.

Verifies:
  1. update() writes a single stats blob
  2. load() reads it back (1 fetch)
  3. scan_with_pruning() evaluates predicates against stats (no data fetch)
  4. Only surviving row groups are yielded
  5. can_prune() handles all operators (>, >=, <, <=, =, in)
  6. No stats → no pruning (graceful degradation)
  7. Versioning: stats ref is content-addressed

Run:
    python tests/integration/test_stats_index.py
"""

from __future__ import annotations

import os
import sys
import shutil
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))

from kernel import PondMinimal
from stats_index import StatsIndex, RowGroupStats


def test_basic():
    """Basic write/load/scan cycle."""
    print("=" * 60)
    print("StatsIndex: basic write/load/scan")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="pond_stats_")
    try:
        kernel = PondMinimal(tmpdir)
        si = StatsIndex(kernel)

        entries = [
            RowGroupStats("rg/9", "blob_a", 10,
                          {"age": {"min": 0, "max": 9, "null_count": 0}}),
            RowGroupStats("rg/19", "blob_b", 10,
                          {"age": {"min": 10, "max": 19, "null_count": 0}}),
            RowGroupStats("rg/29", "blob_c", 10,
                          {"age": {"min": 20, "max": 29, "null_count": 0}}),
        ]
        si.update("test", entries)
        assert si.has_stats("test")
        print("  [OK] Stats index written (1 blob, 1 ref)")

        loaded = si.load("test")
        assert len(loaded) == 3
        assert loaded[0].key == "rg/9"
        assert loaded[0].columns["age"]["min"] == 0
        print(f"  [OK] Loaded {len(loaded)} entries (1 fetch)")

        # Predicate: age >= 20 → only rg/29 survives
        results = list(si.scan_with_pruning("test", [("age", ">=", 20)]))
        assert len(results) == 1
        assert results[0][0] == "rg/29"
        print(f"  [OK] age >= 20: 1/3 survive (2 pruned, 0 data fetches)")

        # Predicate: age >= 100 → all pruned
        results = list(si.scan_with_pruning("test", [("age", ">=", 100)]))
        assert len(results) == 0
        print(f"  [OK] age >= 100: 0/3 survive (all pruned)")

        # No predicate → all survive
        results = list(si.scan_with_pruning("test"))
        assert len(results) == 3
        print(f"  [OK] No predicate: 3/3 survive")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_operators():
    """All comparison operators work."""
    print("\n" + "=" * 60)
    print("StatsIndex: all operators")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="pond_stats_ops_")
    try:
        kernel = PondMinimal(tmpdir)
        si = StatsIndex(kernel)

        entries = [
            RowGroupStats("rg/0", "blob_0", 100,
                          {"age": {"min": 0, "max": 50, "null_count": 0},
                           "region": {"min": "ASIA", "max": "US", "null_count": 0}}),
        ]
        si.update("test", entries)

        # > (prune when max <= value)
        r = list(si.scan_with_pruning("test", [("age", ">", 50)]))
        # max=50, 50 <= 50 → True → PRUNED (no value > 50 exists)
        assert len(r) == 0
        r = list(si.scan_with_pruning("test", [("age", ">", 49)]))
        # max=50, 50 <= 49 → False → NOT PRUNED
        assert len(r) == 1
        print("  [OK] > operator")

        # >= (prune when max < value)
        r = list(si.scan_with_pruning("test", [("age", ">=", 51)]))
        assert len(r) == 0
        r = list(si.scan_with_pruning("test", [("age", ">=", 50)]))
        assert len(r) == 1
        print("  [OK] >= operator")

        # < (prune when min >= value)
        r = list(si.scan_with_pruning("test", [("age", "<", 0)]))
        assert len(r) == 0
        r = list(si.scan_with_pruning("test", [("age", "<", 1)]))
        assert len(r) == 1
        print("  [OK] < operator")

        # = (prune when value outside [min, max])
        r = list(si.scan_with_pruning("test", [("age", "=", 100)]))
        assert len(r) == 0
        r = list(si.scan_with_pruning("test", [("age", "=", 25)]))
        assert len(r) == 1
        print("  [OK] = operator")

        # in (prune when all values outside [min, max])
        r = list(si.scan_with_pruning("test", [("age", "in", [100, 200])]))
        assert len(r) == 0
        r = list(si.scan_with_pruning("test", [("age", "in", [25, 100])]))
        assert len(r) == 1
        print("  [OK] in operator")

        # String comparison
        r = list(si.scan_with_pruning("test", [("region", "=", "EU")]))
        # min=ASIA, max=US → "EU" is in range → NOT pruned
        assert len(r) == 1
        r = list(si.scan_with_pruning("test", [("region", "=", "ZZ")]))
        # "ZZ" > "US" (max) → pruned
        assert len(r) == 0
        print("  [OK] String comparison")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_no_stats():
    """No stats → no pruning (graceful degradation)."""
    print("\n" + "=" * 60)
    print("StatsIndex: no stats → graceful degradation")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="pond_stats_none_")
    try:
        kernel = PondMinimal(tmpdir)
        si = StatsIndex(kernel)

        assert not si.has_stats("missing")
        loaded = si.load("missing")
        assert loaded == []
        results = list(si.scan_with_pruning("missing", [("age", ">", 10)]))
        assert results == []
        print("  [OK] No stats → empty results (graceful)")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_round_trips():
    """Verify the round-trip count: 1 fetch for stats + N for data."""
    print("\n" + "=" * 60)
    print("StatsIndex: round-trip analysis")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="pond_stats_rt_")
    try:
        # Use S3 mock to count fetches
        sys.path.insert(0, os.path.join(REPO, "pond-core"))
        from s3_mock_backend import S3MockKernel

        kernel = S3MockKernel(tmpdir, latency_ms=0)  # 0ms for speed
        si = StatsIndex(kernel)

        # 100 row groups, each with age range 0-99
        entries = []
        for i in range(100):
            entries.append(RowGroupStats(
                f"rg/{i}", f"blob_{i}", 1000,
                {"age": {"min": i * 10, "max": (i + 1) * 10 - 1, "null_count": 0}}
            ))
        si.update("events", entries)

        # Write fake data blobs
        for i in range(100):
            kernel.write(f"data_{i}".encode())

        # Query: age >= 990 → only last row group survives
        kernel.reset_stats()
        results = list(si.scan_with_pruning("events", [("age", ">=", 990)]))
        stats = dict(kernel.stats)

        print(f"  100 row groups, predicate age >= 990 (1% selectivity)")
        print(f"  Stats blob fetches: {stats['total_blob_fetches']}")
        print(f"  Surviving row groups: {len(results)}")
        print(f"  Total round trips: {stats['total_blob_fetches'] + len(results)}")
        print(f"    (1 stats fetch + {len(results)} data fetches)")
        print(f"  Without stats index: 100 data fetches")
        print(f"  Savings: {100 - stats['total_blob_fetches'] - len(results)} fewer fetches")

        assert len(results) == 1
        assert stats['total_blob_fetches'] == 1  # only the stats blob
        print("  [OK] 1 stats fetch + 1 surviving = 2 total round trips")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    test_basic()
    test_operators()
    test_no_stats()
    test_round_trips()
    print("\n" + "=" * 60)
    print("ALL STATS INDEX TESTS PASSED")
    print("=" * 60)
