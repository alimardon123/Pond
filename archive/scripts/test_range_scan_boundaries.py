"""Round 16 regression test: range scan across row-group boundaries.

Verifies that range_read returns ALL rows in [start, end], including
rows from row groups whose max_pk > end_key (the Round 16 bug).
"""
import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/core"))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(HERE, "..", "lenses", "lakehouse"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage
from unified_storage import _format_rg_key


def test_range_scan_across_boundaries():
    """Range scan must return ALL rows in [start, end], even when end
    falls inside a row group whose max_pk > end."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    # 100K rows, 10K per row group → 10 row groups
    # Row groups: [0-9999], [10000-19999], ..., [90000-99999]
    rows = [{"id": i, "val": i} for i in range(100000)]
    storage.write("test", rows, key_col="id", row_group_size=10000)

    # Range [15000, 25000] — spans two row groups:
    #   [10000-19999] → rows 15000-19999 (5000 rows)
    #   [20000-29999] → rows 20000-25000 (5001 rows)
    # Total: 10001 rows
    # Before fix: the [20000-29999] group was excluded (max_pk=29999 > 25000)
    # After fix: the group is included (min_pk=20000 <= 25000), row-level filter keeps 20000-25000
    result = storage.read("test",
                           start_key=_format_rg_key(15000),
                           end_key=_format_rg_key(25000))

    assert len(result) == 10001, \
        f"Expected 10001 rows in [15000, 25000], got {len(result)}"

    ids = sorted(r["id"] for r in result)
    assert ids[0] == 15000, f"Min id should be 15000, got {ids[0]}"
    assert ids[-1] == 25000, f"Max id should be 25000, got {ids[-1]}"

    print(f"PASS: test_range_scan_across_boundaries ({len(result)} rows, ids {ids[0]}..{ids[-1]})")
    return True


def test_range_scan_single_group():
    """Range within a single row group."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    rows = [{"id": i} for i in range(10000)]
    storage.write("test", rows, key_col="id", row_group_size=10000)

    result = storage.read("test",
                           start_key=_format_rg_key(3000),
                           end_key=_format_rg_key(7000))
    assert len(result) == 4001, f"Expected 4001, got {len(result)}"

    print(f"PASS: test_range_scan_single_group ({len(result)} rows)")
    return True


def test_range_scan_full():
    """Full range scan (no boundaries)."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    rows = [{"id": i} for i in range(50000)]
    storage.write("test", rows, key_col="id", row_group_size=5000)

    result = storage.read("test")
    assert len(result) == 50000, f"Expected 50000, got {len(result)}"

    print(f"PASS: test_range_scan_full ({len(result)} rows)")
    return True


def test_range_scan_start_only():
    """Range with only start_key."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)

    rows = [{"id": i} for i in range(50000)]
    storage.write("test", rows, key_col="id", row_group_size=5000)

    result = storage.read("test", start_key=_format_rg_key(45000))
    assert len(result) == 5000, f"Expected 5000, got {len(result)}"

    print(f"PASS: test_range_scan_start_only ({len(result)} rows)")
    return True


if __name__ == "__main__":
    tests = [
        test_range_scan_across_boundaries,
        test_range_scan_single_group,
        test_range_scan_full,
        test_range_scan_start_only,
    ]
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{passed}/{len(tests)} tests passed")
    sys.exit(0 if passed == len(tests) else 1)
