"""Smoke test for stats_tree.py — verify build + scan with pruning."""
import os, sys, tempfile, shutil

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/core"))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/sdk", "extensions", "physical_structures"))

from kernel import PondMinimal
from collection_manifest import (
    RowGroupEntry, ColumnStatsEntry, VALUE_TYPE_INT64, VALUE_TYPE_STRING,
)
from stats_tree import (
    build_stats_tree, StatsTreeReader, should_use_stats_tree,
    TARGET_LEAF_ENTRIES, FLAT_MANIFEST_MAX_ROW_GROUPS,
)


def make_kernel():
    tmp = tempfile.mkdtemp(prefix="pond-stats-tree-test-")
    return PondMinimal(tmp), tmp


def test_basic_tree():
    """Build a small tree, verify scan_with_pruning works."""
    kernel, tmp = make_kernel()
    try:
        # 200 row groups, ages 0-99 each (so 0..19999)
        entries = []
        for i in range(200):
            lo = i * 100
            hi = lo + 99
            rg = RowGroupEntry(
                key=f"rg/{hi}",
                blob_hash=f"{i:064d}",
                n_rows=100,
            )
            rg.columns = [
                ColumnStatsEntry("age", VALUE_TYPE_INT64, lo, hi, 0),
                ColumnStatsEntry("region", VALUE_TYPE_STRING, "ASIA", "US", 0),
            ]
            entries.append(rg)

        root = build_stats_tree(kernel, entries)
        reader = StatsTreeReader(kernel, root)

        # Predicate age > 15000: should yield row groups where max > 15000
        # i.e., groups with lo >= 15000, which is i >= 150
        # But also group 149 (14900-14999) has max=14999 < 15000 — should be pruned
        # So we expect groups 150..199 = 50 groups
        surviving = list(reader.scan_with_pruning([("age", ">", 15000)]))
        assert len(surviving) == 50, f"expected 50, got {len(surviving)}"
        assert surviving[0].key == "rg/15099"
        assert surviving[-1].key == "rg/19999"

        # Predicate age = 5000: only the group containing 5000 (i=50, 5000-5099)
        surviving = list(reader.scan_with_pruning([("age", "=", 5000)]))
        assert len(surviving) == 1
        assert surviving[0].key == "rg/5099"

        # Predicate age > 99999: nothing
        surviving = list(reader.scan_with_pruning([("age", ">", 99999)]))
        assert len(surviving) == 0

        # No predicate: all 200
        surviving = list(reader.scan_with_pruning())
        assert len(surviving) == 200

        print("PASS: test_basic_tree")
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_large_tree():
    """Build a tree large enough to need multiple levels."""
    kernel, tmp = make_kernel()
    try:
        # TARGET_LEAF_ENTRIES=64, so 64*64=4096 leaves need 64 internal nodes,
        # which fits in 1 internal node at the top. To force 3 levels we need
        # 64*64*64 = 262144 entries. That's a lot for a smoke test — instead
        # let's do 1000 entries to verify multi-leaf + 1-level internal.
        n = 1000
        entries = []
        for i in range(n):
            lo = i * 10
            hi = lo + 9
            rg = RowGroupEntry(
                key=f"rg/{hi:08d}",
                blob_hash=f"{i:064x}",
                n_rows=10,
            )
            rg.columns = [
                ColumnStatsEntry("v", VALUE_TYPE_INT64, lo, hi, 0),
            ]
            entries.append(rg)

        root = build_stats_tree(kernel, entries)
        reader = StatsTreeReader(kernel, root)

        # Predicate v > 5000: groups with max >= 5001 → i >= 500
        surviving = list(reader.scan_with_pruning([("v", ">", 5000)]))
        expected = n - 500  # 500 groups
        assert len(surviving) == expected, f"expected {expected}, got {len(surviving)}"

        # Predicate v = 7777: only group i=777 (7770-7779)
        surviving = list(reader.scan_with_pruning([("v", "=", 7777)]))
        assert len(surviving) == 1
        assert surviving[0].key == "rg/00007779"

        # Verify the cache works: second scan should not re-fetch
        # (We can't easily verify cache hits without instrumentation, but
        # we can verify the scan is correct.)
        surviving2 = list(reader.scan_with_pruning([("v", "=", 7777)]))
        assert len(surviving2) == 1

        print(f"PASS: test_large_tree ({n} entries, multi-level tree)")
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_threshold_logic():
    """Verify should_use_stats_tree returns expected values."""
    assert not should_use_stats_tree(0)
    assert not should_use_stats_tree(1000)
    assert not should_use_stats_tree(FLAT_MANIFEST_MAX_ROW_GROUPS)
    assert should_use_stats_tree(FLAT_MANIFEST_MAX_ROW_GROUPS + 1)
    print("PASS: test_threshold_logic")
    return True


def test_empty_tree():
    """An empty tree should produce zero entries."""
    kernel, tmp = make_kernel()
    try:
        root = build_stats_tree(kernel, [])
        reader = StatsTreeReader(kernel, root)
        surviving = list(reader.scan_with_pruning([("v", "=", 1)]))
        assert len(surviving) == 0
        print("PASS: test_empty_tree")
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    ok1 = test_basic_tree()
    ok2 = test_large_tree()
    ok3 = test_threshold_logic()
    ok4 = test_empty_tree()
    if all([ok1, ok2, ok3, ok4]):
        print("\n=== ALL STATS TREE TESTS PASS ===")
        sys.exit(0)
    else:
        print("\n=== SOME TESTS FAILED ===")
        sys.exit(1)
