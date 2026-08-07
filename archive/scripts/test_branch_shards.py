"""Branch-aware shard tests — verifies git-like concurrent branch workflows.

Tests:
1. Concurrent writers on feature1 + concurrent writers on main (isolated)
2. Writer switches branches mid-work (feature1 → main → feature1)
3. Merge feature1 into main (shards from both branches merge correctly)
4. feature1 shards cleared after merge
5. New connection on a branch sees the right shards
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bindings/python/core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bindings/python/sdk"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bindings/python/sdk",
                                  "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage


def test_concurrent_writers_different_branches():
    """2 writers on feature1 + 3 writers on main — isolated, no cross-contamination."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    storage.write("events", [{"id": 0, "v": "init"}], key_col="id", row_group_size=10)
    storage.branch("events", "main")
    storage.branch("events", "feature1")

    def writer(wid, branch, n):
        local = PondStorage(kernel)
        local.checkout("events", branch)
        for i in range(n):
            local.append_shard("events",
                [{"id": wid * 1000 + i + 1, "v": f"{branch}_{wid}_{i}"}],
                key_col="id", row_group_size=10)

    threads = [
        threading.Thread(target=writer, args=(1, "feature1", 5)),
        threading.Thread(target=writer, args=(2, "feature1", 5)),
        threading.Thread(target=writer, args=(3, "main", 5)),
        threading.Thread(target=writer, args=(4, "main", 5)),
        threading.Thread(target=writer, args=(5, "main", 5)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # feature1: 1 init + 10 shards = 11 rows
    s_f1 = PondStorage(kernel)
    s_f1.checkout("events", "feature1")
    f1_rows = s_f1.read_with_shards("events")
    assert len(f1_rows) == 11, f"feature1: expected 11 rows, got {len(f1_rows)}"

    # main: 1 init + 15 shards = 16 rows
    s_main = PondStorage(kernel)
    s_main.checkout("events", "main")
    main_rows = s_main.read_with_shards("events")
    assert len(main_rows) == 16, f"main: expected 16 rows, got {len(main_rows)}"

    print(f"PASS: test_concurrent_writers_different_branches — "
          f"feature1={len(f1_rows)} rows, main={len(main_rows)} rows (isolated)")
    return True


def test_switch_branches_mid_work():
    """Writer switches from feature1 to main and back — shards follow the branch."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    storage.write("events", [{"id": 0, "v": "init"}], key_col="id", row_group_size=10)
    storage.branch("events", "main")
    storage.branch("events", "feature1")

    # Start on feature1
    storage.checkout("events", "feature1")
    storage.append_shard("events", [{"id": 1, "v": "f1_work"}], key_col="id", row_group_size=10)

    # Switch to main
    storage.checkout("events", "main")
    storage.append_shard("events", [{"id": 100, "v": "main_work"}], key_col="id", row_group_size=10)

    # Switch back to feature1
    storage.checkout("events", "feature1")
    storage.append_shard("events", [{"id": 2, "v": "f1_more"}], key_col="id", row_group_size=10)

    # Verify isolation
    s_f1 = PondStorage(kernel)
    s_f1.checkout("events", "feature1")
    f1_ids = sorted(r["id"] for r in s_f1.read_with_shards("events"))
    assert f1_ids == [0, 1, 2], f"feature1: expected [0,1,2], got {f1_ids}"

    s_main = PondStorage(kernel)
    s_main.checkout("events", "main")
    main_ids = sorted(r["id"] for r in s_main.read_with_shards("events"))
    assert main_ids == [0, 100], f"main: expected [0,100], got {main_ids}"

    print(f"PASS: test_switch_branches_mid_work — "
          f"feature1={f1_ids}, main={main_ids} (isolated)")
    return True


def test_merge_branch_with_shards():
    """Merge feature1 (with shards) into main — CRDT union of shards from both."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    storage.write("events", [{"id": 0, "v": "init"}], key_col="id", row_group_size=10)
    storage.branch("events", "main")
    storage.branch("events", "feature1")

    # Add shards to both branches
    s_f1 = PondStorage(kernel)
    s_f1.checkout("events", "feature1")
    s_f1.append_shard("events", [{"id": 1, "v": "f1"}], key_col="id", row_group_size=10)
    s_f1.append_shard("events", [{"id": 2, "v": "f2"}], key_col="id", row_group_size=10)

    s_main = PondStorage(kernel)
    s_main.checkout("events", "main")
    s_main.append_shard("events", [{"id": 100, "v": "m1"}], key_col="id", row_group_size=10)

    # Merge feature1 into main
    s_main.merge("events", "feature1")
    # Wait for async tombstoning to complete (merge returns immediately,
    # shard refs are deleted in background)
    s_main.wait_for_background_tasks()

    # main should now have all rows: init + f1 + f2 + m1 = 4 rows
    merged = s_main.read_with_shards("events")
    assert len(merged) == 4, f"After merge: expected 4 rows, got {len(merged)}"
    merged_ids = sorted(r["id"] for r in merged)
    assert merged_ids == [0, 1, 2, 100], f"Expected [0,1,2,100], got {merged_ids}"

    print(f"PASS: test_merge_branch_with_shards — merged {len(merged)} rows {merged_ids}")
    return True


def test_shards_cleared_after_merge():
    """After merge, the source branch's shards are cleared (now in HEAD)."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    storage.write("events", [{"id": 0, "v": "init"}], key_col="id", row_group_size=10)
    storage.branch("events", "main")
    storage.branch("events", "feature1")

    s_f1 = PondStorage(kernel)
    s_f1.checkout("events", "feature1")
    s_f1.append_shard("events", [{"id": 1, "v": "f1"}], key_col="id", row_group_size=10)
    assert s_f1.shard_count("events") == 1

    s_main = PondStorage(kernel)
    s_main.checkout("events", "main")
    s_main.merge("events", "feature1")
    # Wait for async tombstoning to complete
    s_main.wait_for_background_tasks()

    # feature1 shards should be cleared
    s_f1_new = PondStorage(kernel)
    s_f1_new.checkout("events", "feature1")
    assert s_f1_new.shard_count("events") == 0, \
        f"feature1 shards after merge: expected 0, got {s_f1_new.shard_count('events')}"

    print("PASS: test_shards_cleared_after_merge — source branch shards cleared")
    return True


def test_new_connection_on_branch():
    """A new connection checking out a branch sees that branch's shards."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    storage.write("events", [{"id": 0, "v": "init"}], key_col="id", row_group_size=10)
    storage.branch("events", "main")
    storage.branch("events", "feature1")

    # Writer 1 adds to feature1
    s1 = PondStorage(kernel)
    s1.checkout("events", "feature1")
    s1.append_shard("events", [{"id": 1, "v": "f1"}], key_col="id", row_group_size=10)

    # New connection checks out feature1
    s2 = PondStorage(kernel)
    s2.checkout("events", "feature1")
    rows = s2.read_with_shards("events")
    assert len(rows) == 2, f"New connection: expected 2 rows, got {len(rows)}"
    assert rows[0]["v"] in ("init", "f1")

    # New connection checks out main — should NOT see feature1's shard
    s3 = PondStorage(kernel)
    s3.checkout("events", "main")
    rows = s3.read_with_shards("events")
    assert len(rows) == 1, f"main: expected 1 row, got {len(rows)}"

    print(f"PASS: test_new_connection_on_branch — "
          f"new connection sees correct branch state")
    return True


def main():
    tests = [
        test_concurrent_writers_different_branches,
        test_switch_branches_mid_work,
        test_merge_branch_with_shards,
        test_shards_cleared_after_merge,
        test_new_connection_on_branch,
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
        print("=== ALL BRANCH-AWARE SHARD TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
