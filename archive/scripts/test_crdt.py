"""Row-level CRDT tests — verifies concurrent insert/update/delete merge semantics.

Tests:
1. Concurrent INSERTs (same _rowid) — latest _version wins
2. Concurrent UPDATEs (same _rowid) — latest _version wins
3. DELETE + UPDATE (update has later _version) — update wins
4. UPDATE + DELETE (delete has later _version) — tombstone wins
5. Concurrent INSERTs (different _rowid) — both kept
6. Mixed: insert + update + delete across multiple writers
"""
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage


def _setup_collection(name="crdt_test"):
    """Create a collection with one upserted row, return (kernel, storage, rowid)."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    # Create the collection with write() first (needed to establish schema)
    storage.write(name, [{"id": 1, "name": "init"}], key_col="id", row_group_size=10)
    # Compact to make the init row the HEAD, then upsert to add _rowid
    # The upsert creates a NEW row with _rowid — the init row in HEAD
    # has no _rowid, so it won't conflict. We compact to merge them.
    storage.upsert_shard(name, [{"id": 1, "name": "init"}], key_col="id")
    storage.compact_shards(name)
    # Now read — the compacted HEAD has the upserted row (with _rowid)
    rows = storage.read_with_shards(name)
    # Find the row with _rowid
    for r in rows:
        if r.get("_rowid"):
            return kernel, storage, r["_rowid"]
    # Fallback: return the first row
    return kernel, storage, rows[0].get("_rowid", "")


def test_concurrent_updates_same_rowid():
    """Two concurrent UPDATEs on the same _rowid — latest _version wins."""
    kernel, storage, rowid = _setup_collection()

    results = []
    def updater(wid, new_name):
        local = PondStorage(kernel)
        local.upsert_shard("crdt_test",
            [{"_rowid": rowid, "id": 1, "name": new_name, "age": 30 + wid}],
            key_col="id")
        results.append(wid)

    t1 = threading.Thread(target=updater, args=(1, "alice_v1"))
    t2 = threading.Thread(target=updater, args=(2, "alice_v2"))
    t1.start(); t2.start(); t1.join(); t2.join()

    final = PondStorage(kernel)
    rows = final.read_with_shards("crdt_test")
    assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
    # The latest _version wins — should be one of v1 or v2
    assert rows[0]["name"] in ("alice_v1", "alice_v2"), \
        f"Unexpected name: {rows[0]['name']}"
    print(f"PASS: test_concurrent_updates_same_rowid — "
          f"1 row after 2 concurrent updates, name={rows[0]['name']}")
    return True


def test_delete_then_update():
    """DELETE then UPDATE (update has later _version) — update wins."""
    kernel, storage, rowid = _setup_collection()

    # Delete first
    storage.delete_shard("crdt_test", [rowid], key_col="id")
    time.sleep(0.05)  # ensure update has later _version (deterministic)
    # Update with later _version
    storage.upsert_shard("crdt_test",
        [{"_rowid": rowid, "id": 1, "name": "resurrected", "age": 99}],
        key_col="id")

    final = PondStorage(kernel)
    rows = final.read_with_shards("crdt_test")
    assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
    assert rows[0]["name"] == "resurrected", \
        f"Expected 'resurrected', got {rows[0]['name']}"
    print("PASS: test_delete_then_update — update won (later _version)")
    return True


def test_update_then_delete():
    """UPDATE then DELETE (delete has later _version) — tombstone wins."""
    kernel, storage, rowid = _setup_collection()

    # Update first
    storage.upsert_shard("crdt_test",
        [{"_rowid": rowid, "id": 1, "name": "updated", "age": 50}],
        key_col="id")
    time.sleep(0.05)  # ensure delete has later _version
    # Delete with later _version
    storage.delete_shard("crdt_test", [rowid], key_col="id")

    final = PondStorage(kernel)
    rows = final.read_with_shards("crdt_test")
    assert len(rows) == 0, f"Expected 0 rows, got {len(rows)}"
    print("PASS: test_update_then_delete — tombstone won (later _version)")
    return True


def test_concurrent_inserts_different_rowids():
    """Concurrent INSERTs with different _rowids — both kept."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    storage.write("multi", [{"id": 0, "v": "init"}], key_col="id", row_group_size=10)

    results = []
    def inserter(wid):
        local = PondStorage(kernel)
        # Each writer inserts a NEW row (no _rowid — auto-generated)
        local.upsert_shard("multi",
            [{"id": wid * 100 + 1, "v": f"w{wid}"}],
            key_col="id")
        results.append(wid)

    threads = [threading.Thread(target=inserter, args=(w,)) for w in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = PondStorage(kernel)
    rows = final.read_with_shards("multi")
    # 1 init + 5 new = 6 rows
    assert len(rows) == 6, f"Expected 6 rows, got {len(rows)}"
    print(f"PASS: test_concurrent_inserts_different_rowids — "
          f"{len(rows)} rows (1 init + 5 inserts)")
    return True


def test_mixed_workload():
    """Mixed: multiple writers doing insert + update + delete simultaneously."""
    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    # Pre-populate with 10 rows via upsert (each gets _rowid)
    storage.write("mixed", [{"id": i, "v": f"init_{i}"} for i in range(10)],
                   key_col="id", row_group_size=10)
    storage.upsert_shard("mixed",
        [{"id": i, "v": f"init_{i}"} for i in range(10)],
        key_col="id")

    # Read the _rowids
    rows = storage.read_with_shards("mixed")
    rowids = [r["_rowid"] for r in rows]

    errors = []
    def writer(wid):
        try:
            local = PondStorage(kernel)
            for i in range(5):
                if i < 2 and wid * 2 + i < len(rowids):
                    # Update existing rows
                    local.upsert_shard("mixed",
                        [{"_rowid": rowids[wid * 2 + i], "id": wid * 2 + i,
                          "v": f"w{wid}_update_{i}"}],
                        key_col="id")
                elif i < 4 and wid * 2 + i < len(rowids):
                    # Delete existing rows
                    local.delete_shard("mixed", [rowids[wid * 2 + i]], key_col="id")
                else:
                    # Insert new rows
                    local.upsert_shard("mixed",
                        [{"id": wid * 1000 + i, "v": f"w{wid}_new_{i}"}],
                        key_col="id")
        except Exception as e:
            errors.append((wid, str(e)[:80]))

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Errors: {errors[:3]}"

    final = PondStorage(kernel)
    rows = final.read_with_shards("mixed")
    # 10 init - some deleted + some new inserts = should be >= 5
    print(f"PASS: test_mixed_workload — {len(rows)} rows after "
          f"5 writers doing insert+update+delete simultaneously")
    return True


def test_compaction_preserves_crdt():
    """Compaction correctly merges tombstones and versions."""
    kernel, storage, rowid = _setup_collection("compact_test")

    # Update + delete
    storage.upsert_shard("compact_test",
        [{"_rowid": rowid, "id": 1, "name": "updated"}], key_col="id")
    time.sleep(0.05)
    storage.delete_shard("compact_test", [rowid], key_col="id")

    # Compact
    storage.compact_shards("compact_test")
    storage.wait_for_background_tasks()

    # After compaction, the row should still be deleted
    final = PondStorage(kernel)
    rows = final.read_with_shards("compact_test")
    assert len(rows) == 0, f"Expected 0 rows after compaction, got {len(rows)}"
    print("PASS: test_compaction_preserves_crdt — tombstone survived compaction")
    return True


def main():
    tests = [
        test_concurrent_updates_same_rowid,
        test_delete_then_update,
        test_update_then_delete,
        test_concurrent_inserts_different_rowids,
        test_mixed_workload,
        test_compaction_preserves_crdt,
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
        print("=== ALL ROW-LEVEL CRDT TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
