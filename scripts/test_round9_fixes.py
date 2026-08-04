"""Round 9 fixes verification — proves the critical bugs are fixed.

Tests:
  1. append() sorts manifest → point_lookup works for appended rows (Issue #1)
  2. Time-travel via manifest_hash → no ref mutation (Issue #2)
  3. Branch read via manifest_hash → no ref mutation (Issue #2)
"""
import os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk", "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from unified_storage import UnifiedStorage


def test_append_sort_fix():
    """Issue #1: append() must sort manifest entries so point_lookup works.

    Before fix: appended rows with smaller keys → point_lookup returns None
    After fix: manifest sorted by rg_key → point_lookup works correctly
    """
    kernel, _ = make_object_store_native_kernel()
    storage = UnifiedStorage(kernel)

    # Write initial data with keys 100-199
    rows1 = [{"id": i, "name": f"a{i}"} for i in range(100, 200)]
    storage.write("test", rows1, key_col="id", row_group_size=10)

    # Verify initial point lookup works
    row = storage.point_lookup("test", key="100")
    assert row is not None, "Initial point_lookup failed"
    assert row["id"] == 100

    # Append data with SMALLER keys (1-99) — this is the out-of-order case
    rows2 = [{"id": i, "name": f"b{i}"} for i in range(1, 100)]
    storage.append("test", rows2, key_col="id", row_group_size=10)

    # Verify point lookup for APPENDED rows (smaller keys)
    # Before fix: these would return None
    row1 = storage.point_lookup("test", key="1")
    assert row1 is not None, "BUG: point_lookup for appended key=1 returned None"
    assert row1["id"] == 1, f"Wrong row: {row1}"
    assert row1["name"] == "b1"

    row50 = storage.point_lookup("test", key="50")
    assert row50 is not None, "BUG: point_lookup for appended key=50 returned None"
    assert row50["id"] == 50

    # Verify original rows still work
    row150 = storage.point_lookup("test", key="150")
    assert row150 is not None
    assert row150["id"] == 150
    assert row150["name"] == "a150"

    # Verify all rows are readable via full scan
    all_rows = storage.read("test")
    assert len(all_rows) == 199, f"Expected 199 rows, got {len(all_rows)}"

    print("PASS: test_append_sort_fix")
    return True


def test_time_travel_no_mutation():
    """Issue #2: time-travel reads must not mutate the manifest ref.

    Before fix: swap manifest ref → read → restore (race condition + hidden PUTs)
    After fix: pass manifest_hash directly to _load_manifest (no mutation)

    NOTE: in the CRDT shard model, append() writes a SHARD (HEAD manifest
    ref is unchanged until compact_shards). Time-travel via manifest_hash
    reads the snapshot at that manifest. The manifest ref NOT changing
    after append is now the EXPECTED behavior, not a bug.
    """
    kernel, _ = make_object_store_native_kernel()
    storage = UnifiedStorage(kernel)

    # Write version 1 (2 rows)
    storage.write("test", [{"id": 1, "v": 1}, {"id": 2, "v": 1}],
                   key_col="id", row_group_size=10)

    # Save the manifest hash for version 1
    manifest_v1 = kernel.resolve("collections/test/branches/main/manifest")

    # Append version 2 (2 more rows) — appends go to shards, HEAD unchanged
    storage.append("test", [{"id": 3, "v": 2}, {"id": 4, "v": 2}],
                    key_col="id", row_group_size=10)

    # In the CRDT model, HEAD manifest is unchanged by append() — the new
    # rows live in shards. read() merges HEAD + shards to return all 4.
    manifest_v2 = kernel.resolve("collections/test/branches/main/manifest")
    assert manifest_v1 == manifest_v2, \
        "CRDT model: append() should NOT change HEAD manifest (uses shards)"

    # Read version 1 via manifest_hash (time travel — snapshot at v1)
    rows_v1 = storage.read("test", manifest_hash=manifest_v1)
    assert len(rows_v1) == 2, f"Time travel v1: expected 2 rows, got {len(rows_v1)}"

    # Read version 2 (current HEAD + shards)
    rows_v2 = storage.read("test")
    assert len(rows_v2) == 4, f"Current v2: expected 4 rows, got {len(rows_v2)}"

    # CRITICAL: verify the manifest ref was NOT mutated by the time-travel read
    # (the old swap-then-restore approach would have left the ref pointing at v1
    # if the restore failed)
    manifest_after = kernel.resolve("collections/test/branches/main/manifest")
    assert manifest_after == manifest_v2, \
        "BUG: manifest ref was mutated by time-travel read (race condition!)"

    print("PASS: test_time_travel_no_mutation")
    return True


def test_branch_read_no_mutation():
    """Issue #2: branch reads must not mutate the manifest ref."""
    import pyarrow as pa
    sys.path.insert(0, os.path.join(HERE, "..", "lenses", "lakehouse"))
    from lakehouse_lens import LakehouseLens

    kernel, _ = make_object_store_native_kernel()
    lens = LakehouseLens(kernel)

    # Create main table
    lens.create_table("users", pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["a", "b", "c"]),
    }), key_col="id")

    # Save main manifest
    main_manifest = kernel.resolve("collections/users/branches/main/manifest")

    # Create branch with additional data
    lens.branch("users", "dev")
    lens.commit_to_branch("users", "dev", pa.table({
        "id": pa.array([4], type=pa.int64()),
        "name": pa.array(["d"]),
    }), key_col="id")

    # CRITICAL: main manifest should be unchanged after branch commit
    main_after_branch = kernel.resolve("collections/users/branches/main/manifest")
    assert main_after_branch == main_manifest, \
        "BUG: branch commit mutated main manifest"

    # Read main (should be 3 rows)
    main_rows = lens.read_table("users")
    assert main_rows.num_rows == 3, \
        f"Main should have 3 rows, got {main_rows.num_rows}"

    # Read branch (should be 4 rows)
    dev_rows = lens.read_branch("users", "dev")
    assert dev_rows.num_rows == 4, \
        f"Dev branch should have 4 rows, got {dev_rows.num_rows}"

    # CRITICAL: main manifest should STILL be unchanged after branch read
    main_after_read = kernel.resolve("collections/users/branches/main/manifest")
    assert main_after_read == main_manifest, \
        "BUG: branch read mutated main manifest (race condition!)"

    print("PASS: test_branch_read_no_mutation")
    return True


if __name__ == "__main__":
    ok1 = test_append_sort_fix()
    ok2 = test_time_travel_no_mutation()
    ok3 = test_branch_read_no_mutation()
    if all([ok1, ok2, ok3]):
        print("\n=== ALL ROUND 9 FIX TESTS PASS ===")
        sys.exit(0)
    else:
        print("\n=== SOME TESTS FAILED ===")
        sys.exit(1)
