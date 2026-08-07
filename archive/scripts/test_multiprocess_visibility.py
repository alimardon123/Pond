"""Multi-process visibility test — verifies that:
  1. Process B sees Process A's writes within cache_ttl_seconds (default 5s)
  2. Process B sees Process A's writes IMMEDIATELY if it calls invalidate_all_caches()
  3. Process B sees Process A's writes IMMEDIATELY if cache_ttl_seconds=0

This is critical for Pond's multi-process use case: multiple apps reading
and writing concurrently to the same object storage.
"""
import os, sys, tempfile, time, threading

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))

from local_fs_object_store import LocalFSObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
from pond_storage import PondStorage


def _make_storage(tmp_dir, cache_ttl=5.0):
    """Create a fresh PondStorage instance pointing at the SAME tmp_dir.
    Simulates a separate process — independent caches, same underlying store.
    """
    store = LocalFSObjectStore(tmp_dir)
    kernel = ObjectStoreNativeKernel(store, cache_ttl_seconds=cache_ttl)
    return PondStorage(kernel)


def test_process_b_sees_process_a_write_with_ttl():
    """Process A writes, Process B (with TTL=5s) sees it within 5s."""
    tmp = tempfile.mkdtemp(prefix="pond-mp-ttl-")
    try:
        # Process A: writes 100 rows
        storage_a = _make_storage(tmp, cache_ttl=5.0)
        storage_a.write("users", [{"id": i, "name": f"u{i}"} for i in range(100)],
                         key_col="id", row_group_size=10)

        # Process B: fresh instance, cold caches — should see all 100 rows
        storage_b = _make_storage(tmp, cache_ttl=5.0)
        rows = storage_b.read("users")
        assert len(rows) == 100, f"Process B saw {len(rows)} rows, expected 100"

        # Process A appends a shard (101st row)
        storage_a.append_shard("users", [{"id": 100, "name": "new"}], key_col="id")

        # Process B's shard_list_cache is now stale (it cached the empty list).
        # With TTL=5s, B should see the new shard within 5s.
        time.sleep(5.5)  # wait for TTL to expire

        rows = storage_b.read("users")
        assert len(rows) == 101, f"Process B saw {len(rows)} rows after TTL, expected 101"
        print("PASS: test_process_b_sees_process_a_write_with_ttl — TTL worked")
        return True
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_process_b_sees_write_immediately_with_invalidate():
    """Process A writes, Process B sees it IMMEDIATELY via invalidate_all_caches()."""
    tmp = tempfile.mkdtemp(prefix="pond-mp-inv-")
    try:
        storage_a = _make_storage(tmp, cache_ttl=5.0)
        storage_a.write("users", [{"id": i, "name": f"u{i}"} for i in range(100)],
                         key_col="id", row_group_size=10)

        storage_b = _make_storage(tmp, cache_ttl=5.0)
        rows = storage_b.read("users")
        assert len(rows) == 100

        # Process A appends
        storage_a.append_shard("users", [{"id": 100, "name": "new"}], key_col="id")

        # Process B calls invalidate_all_caches() — should see 101 immediately
        storage_b.invalidate_all_caches()
        rows = storage_b.read("users")
        assert len(rows) == 101, f"Process B saw {len(rows)} rows after invalidate, expected 101"
        print("PASS: test_process_b_sees_write_immediately_with_invalidate — strong consistency worked")
        return True
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_process_b_sees_write_immediately_with_ttl_zero():
    """With cache_ttl_seconds=0, Process B always sees the latest state."""
    tmp = tempfile.mkdtemp(prefix="pond-mp-zero-")
    try:
        storage_a = _make_storage(tmp, cache_ttl=0.0)  # 0 = no caching
        storage_a.write("users", [{"id": i, "name": f"u{i}"} for i in range(100)],
                         key_col="id", row_group_size=10)

        storage_b = _make_storage(tmp, cache_ttl=0.0)  # 0 = no caching
        rows = storage_b.read("users")
        assert len(rows) == 100

        # Process A appends
        storage_a.append_shard("users", [{"id": 100, "name": "new"}], key_col="id")

        # Process B should see 101 immediately (no cache, every read is live)
        rows = storage_b.read("users")
        assert len(rows) == 101, f"Process B saw {len(rows)} rows with TTL=0, expected 101"
        print("PASS: test_process_b_sees_write_immediately_with_ttl_zero — TTL=0 worked")
        return True
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_concurrent_writers_different_processes():
    """5 processes each write 20 rows. Final reader sees all 100 rows."""
    tmp = tempfile.mkdtemp(prefix="pond-mp-concurrent-")
    try:
        # Initialize the collection with 1 row (so appends work)
        storage_init = _make_storage(tmp, cache_ttl=0.0)
        storage_init.write("users", [{"id": 0, "name": "init"}], key_col="id")

        # 5 threads, each simulating a separate process
        def writer_process(writer_id):
            storage = _make_storage(tmp, cache_ttl=0.0)  # no cache
            for i in range(20):
                row_id = writer_id * 20 + i + 1
                storage.append_shard("users",
                                      [{"id": row_id, "name": f"w{writer_id}_{i}"}],
                                      key_col="id")

        threads = []
        for w in range(5):
            t = threading.Thread(target=writer_process, args=(w,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        # Final reader (fresh process, no cache)
        storage_final = _make_storage(tmp, cache_ttl=0.0)
        rows = storage_final.read("users")
        # 1 init + 100 appends = 101
        assert len(rows) == 101, f"Final reader saw {len(rows)} rows, expected 101"
        print(f"PASS: test_concurrent_writers_different_processes — 5 processes wrote 100 rows, final reader saw all 101 (1 init + 100 appends)")
        return True
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_blob_cache_is_safe_across_processes():
    """Blob cache is content-addressed — always safe across processes.

    Process A reads row group RG1 (caches it). Process B writes a NEW row
    group RG2 (different hash). Process A reads again — should see RG2 too,
    because the manifest re-validation picks up the new blob_hash.
    """
    tmp = tempfile.mkdtemp(prefix="pond-mp-blob-")
    try:
        storage_a = _make_storage(tmp, cache_ttl=1.0)  # short TTL
        storage_a.write("users", [{"id": i, "v": f"v{i}"} for i in range(50)],
                         key_col="id", row_group_size=10)

        # Process B appends 10 more rows (new row group)
        storage_b = _make_storage(tmp, cache_ttl=0.0)
        storage_b.append_shard("users", [{"id": 50 + i, "v": f"new{i}"} for i in range(10)],
                                key_col="id")

        # Process A reads — should see 60 rows (manifest re-validation picks up new shard)
        # Wait for TTL to expire (1s) so A re-validates the manifest ref
        time.sleep(1.5)
        rows = storage_a.read("users")
        assert len(rows) == 60, f"Process A saw {len(rows)} rows, expected 60"
        print("PASS: test_blob_cache_is_safe_across_processes — blob cache is content-addressed, manifest re-validation picks up new shards")
        return True
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    tests = [
        test_process_b_sees_process_a_write_with_ttl,
        test_process_b_sees_write_immediately_with_invalidate,
        test_process_b_sees_write_immediately_with_ttl_zero,
        test_concurrent_writers_different_processes,
        test_blob_cache_is_safe_across_processes,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            if t():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"FAIL: {t.__name__}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    print(f"\n{passed}/{passed + failed} tests passed")
    if failed == 0:
        print("=== ALL MULTI-PROCESS VISIBILITY TESTS PASS ===")
    else:
        print(f"=== {failed} TESTS FAILED ===")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
