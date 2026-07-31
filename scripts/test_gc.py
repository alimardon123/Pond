"""GC + Vacuum tests — verifies garbage collection and space reclamation."""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage


def test_gc_finds_dead_blobs():
    """After appends + compaction, GC finds dead blobs."""
    kernel, _ = make_object_store_native_kernel()
    s = PondStorage(kernel)
    s.write("e", [{"id": i, "v": str(i)} for i in range(20)], key_col="id", row_group_size=5)
    s.append("e", [{"id": 20, "v": "new"}], key_col="id", row_group_size=5)
    s.append("e", [{"id": 21, "v": "new2"}], key_col="id", row_group_size=5)

    stats = s.gc()
    assert stats["dead"] > 0, f"Expected dead blobs, got {stats['dead']}"
    assert stats["live"] > 0
    print(f"PASS: test_gc_finds_dead_blobs — {stats['dead']} dead, {stats['live']} live")
    return True


def test_vacuum_deletes_dead():
    """Vacuum deletes dead blobs and reclaims space."""
    kernel, _ = make_object_store_native_kernel()
    s = PondStorage(kernel)
    s.write("e", [{"id": i, "v": str(i)} for i in range(20)], key_col="id", row_group_size=5)
    s.append("e", [{"id": 20, "v": "new"}], key_col="id", row_group_size=5)
    s.append_shard("e", [{"id": 100, "v": "s1"}], key_col="id", row_group_size=5)
    s.compact_shards("e")

    before = s.gc()
    result = s.vacuum()
    after = s.gc()

    assert result["deleted"] > 0, "Expected blobs to be deleted"
    assert after["dead"] == 0, f"Expected 0 dead after vacuum, got {after['dead']}"
    print(f"PASS: test_vacuum_deletes_dead — deleted {result['deleted']}, "
          f"freed {result['freed_bytes']} bytes")
    return True


def test_data_survives_vacuum():
    """Live data is still readable after vacuum."""
    kernel, _ = make_object_store_native_kernel()
    s = PondStorage(kernel)
    s.write("e", [{"id": i, "v": str(i)} for i in range(50)], key_col="id", row_group_size=10)
    s.append("e", [{"id": 50, "v": "new"}], key_col="id", row_group_size=10)
    s.append_shard("e", [{"id": 100, "v": "s1"}], key_col="id", row_group_size=10)
    s.compact_shards("e")

    s.vacuum()
    rows = s.read_with_shards("e")
    assert len(rows) == 52, f"Expected 52 rows after vacuum, got {len(rows)}"
    print(f"PASS: test_data_survives_vacuum — {len(rows)} rows readable")
    return True


def test_dry_run():
    """Dry run doesn't delete anything."""
    kernel, _ = make_object_store_native_kernel()
    s = PondStorage(kernel)
    s.write("e", [{"id": i, "v": str(i)} for i in range(10)], key_col="id", row_group_size=5)
    s.append("e", [{"id": 10, "v": "new"}], key_col="id", row_group_size=5)

    before = s.gc()
    dry = s.vacuum(dry_run=True)
    after = s.gc()

    assert dry["dry_run"] is True
    assert after["dead"] == before["dead"], "Dry run should not delete anything"
    print(f"PASS: test_dry_run — {after['dead']} dead preserved")
    return True


def test_targeted_gc():
    """GC can target a single collection (for analysis only).

    Note: targeted GC may over-report dead blobs because it doesn't
    account for blobs reachable from OTHER collections. For safe
    vacuum, always use vacuum(collection=None) or vacuum_all().
    """
    kernel, _ = make_object_store_native_kernel()
    s = PondStorage(kernel)
    s.write("a", [{"id": i, "v": str(i)} for i in range(5)], key_col="id", row_group_size=5)
    s.write("b", [{"id": i, "v": str(i)} for i in range(5)], key_col="id", row_group_size=5)
    s.append("a", [{"id": 5, "v": "new"}], key_col="id", row_group_size=5)

    stats_a = s.gc(collection="a")
    stats_all = s.gc()
    # Both should return valid results
    assert stats_a["live"] > 0
    assert stats_all["live"] > 0
    print(f"PASS: test_targeted_gc — collection 'a': {stats_a['live']} live, "
          f"all: {stats_all['live']} live")
    return True


def main():
    tests = [
        test_gc_finds_dead_blobs,
        test_vacuum_deletes_dead,
        test_data_survives_vacuum,
        test_dry_run,
        test_targeted_gc,
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
    if passed == len(tests):
        print("=== ALL GC/VACUUM TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
