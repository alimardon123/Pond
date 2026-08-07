"""Local FS integration test — pure filesystem, no SQLite.

Same 9 tests as test_s3_integration.py, but against a real tempdir.
Proves that local FS and S3 are interchangeable: swap the store object
and everything else is identical.

Tests:
1. Basic write/read/point_lookup via local FS
2. Branch/merge via local FS
3. ACID transactions via local FS
4. Config stored as a blob (no local FS config file)
5. list_paths / list_all_blob_hashes (for GC)
6. Concurrent writers (shards) — no coordination, correct merge
7. Restart persistence (close kernel, reopen, data survives)
8. base_dir detection
"""
import os, sys, json, threading, tempfile, shutil

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))


def _make_store():
    """Create a fresh LocalFSObjectStore on a tempdir."""
    from local_fs_object_store import LocalFSObjectStore
    tmpdir = tempfile.mkdtemp(prefix="pond_local_test_")
    return LocalFSObjectStore(tmpdir), tmpdir


def _make_kernel():
    """Create an ObjectStoreNativeKernel backed by local FS."""
    from object_store_native_kernel import ObjectStoreNativeKernel
    store, tmpdir = _make_store()
    return ObjectStoreNativeKernel(store), store, tmpdir


def test_base_dir_detection():
    """base_dir returns the local FS path."""
    kernel, store, tmpdir = _make_kernel()
    bd = kernel.base_dir
    assert bd == tmpdir, f"Expected {tmpdir}, got {bd}"
    assert not bd.startswith("s3://"), "Local FS should not return s3:// URL"
    shutil.rmtree(tmpdir, ignore_errors=True)
    print(f"PASS: test_base_dir_detection — base_dir = {bd}")
    return True


def test_basic_write_read():
    """Basic write/read/point_lookup via local FS."""
    kernel, store, tmpdir = _make_kernel()
    try:
        from pond_storage import PondStorage
        s = PondStorage(kernel)

        s.write("users", [{"id": i, "name": f"u{i}"} for i in range(20)],
                key_col="id", row_group_size=5)

        rows = s.read("users")
        assert len(rows) == 20, f"Expected 20 rows, got {len(rows)}"

        row = s.point_lookup("users", key="5")
        assert row is not None
        assert row["id"] == 5
        assert row["name"] == "u5"

        rows = s.read("users", predicates=[("id", ">", 15)])
        assert len(rows) == 4  # 16, 17, 18, 19

        print(f"PASS: test_basic_write_read — 20 rows, point_lookup + predicate via local FS")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_branch_merge():
    """Branch/merge via local FS."""
    kernel, store, tmpdir = _make_kernel()
    try:
        from pond_storage import PondStorage
        s = PondStorage(kernel)

        s.write("events", [{"id": i, "v": f"main_{i}"} for i in range(10)],
                key_col="id", row_group_size=5)

        s.branch("events", "dev")
        s.checkout("events", "dev")
        s.append("events", [{"id": 100, "v": "dev_100"}], key_col="id")

        s_main = PondStorage(kernel)
        assert len(s_main.read("events")) == 10, "Main changed after branch append"

        s_main.merge("events", "dev")
        rows = s_main.read("events")
        assert len(rows) == 11, f"Expected 11 rows after merge, got {len(rows)}"

        print(f"PASS: test_branch_merge — branch/checkout/append/merge via local FS")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_acid_transactions():
    """ACID transactions via local FS."""
    kernel, store, tmpdir = _make_kernel()
    try:
        from pond_storage import PondStorage
        s = PondStorage(kernel)

        s.write("users", [{"id": 1, "name": "alice"}], key_col="id")
        s.write("orders", [{"id": 1, "amount": 99.9}], key_col="id")

        tx = s.begin_tx()
        s.append_shard("users", [{"id": 2, "name": "bob"}], key_col="id", tx_id=tx)
        s.append_shard("orders", [{"id": 2, "amount": 50.0}], key_col="id", tx_id=tx)

        assert len(s.read_with_shards("users")) == 1
        assert len(s.read_with_shards("orders")) == 1

        s.commit_tx(tx)

        assert len(s.read_with_shards("users")) == 2
        assert len(s.read_with_shards("orders")) == 2

        tx2 = s.begin_tx()
        s.append_shard("users", [{"id": 3, "name": "carol"}], key_col="id", tx_id=tx2)
        s.abort_tx(tx2)
        assert len(s.read_with_shards("users")) == 2, "Aborted tx visible!"

        print(f"PASS: test_acid_transactions — atomic commit + abort via local FS")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_config_as_blob():
    """Config is stored as a blob (no .pond/config file) on object-store-backed kernels."""
    kernel, store, tmpdir = _make_kernel()
    try:
        from pond_config import PondConfig

        config = PondConfig()
        config.pruning_enabled = "true"
        config.chunk_size = 500
        config.save_to_kernel(kernel)

        loaded = PondConfig.load_from_kernel(kernel)
        assert loaded.pruning_enabled == "true", f"Expected 'true', got {loaded.pruning_enabled}"
        assert loaded.chunk_size == 500, f"Expected 500, got {loaded.chunk_size}"

        loaded2 = PondConfig.load_for_kernel(kernel)
        assert loaded2.pruning_enabled == "true"

        # Verify NO .pond/config file was created (config is a blob)
        config_file = os.path.join(tmpdir, ".pond", "config")
        assert not os.path.exists(config_file), \
            f"Config should be a blob, not a local file: {config_file}"

        print(f"PASS: test_config_as_blob — config stored as blob (no local FS file)")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_list_paths_and_blobs():
    """list_paths / list_all_blob_hashes (for GC) via local FS."""
    store, tmpdir = _make_store()
    try:
        h1 = store.put_blob(b"blob1")
        h2 = store.put_blob(b"blob2")
        h3 = store.put_blob(b"blob3")

        store.put_path("collections/users/_branches/main/commit", h1)
        store.put_path("collections/orders/_branches/main/commit", h2)
        store.put_path("collections/users/_branches/main/manifest", h3)

        all_blobs = set(store.list_all_blob_hashes())
        assert h1 in all_blobs, f"h1 not in blobs"
        assert h2 in all_blobs, f"h2 not in blobs"
        assert h3 in all_blobs, f"h3 not in blobs"

        users_paths = store.list_paths("collections/users/")
        assert "collections/users/_branches/main/commit" in users_paths
        assert "collections/users/_branches/main/manifest" in users_paths
        assert "collections/orders/_branches/main/commit" not in users_paths

        print(f"PASS: test_list_paths_and_blobs — list_paths + list_all_blob_hashes via local FS")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_concurrent_writers():
    """Concurrent writers (shards) — no coordination, correct merge via local FS."""
    kernel, store, tmpdir = _make_kernel()
    try:
        from pond_storage import PondStorage
        s = PondStorage(kernel)

        s.write("events", [{"id": 0, "v": "init"}], key_col="id")

        N_WRITERS = 5
        ROWS_PER_WRITER = 20
        errors = []

        def writer(writer_id):
            try:
                writer_s = PondStorage(kernel)
                for i in range(ROWS_PER_WRITER):
                    writer_s.append_shard("events",
                        [{"id": writer_id * 100 + i + 1, "v": f"w{writer_id}_{i}"}],
                        key_col="id")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(w,)) for w in range(N_WRITERS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Writer errors: {errors}"

        rows = s.read_with_shards("events")
        assert len(rows) == 1 + N_WRITERS * ROWS_PER_WRITER, \
            f"Expected {1 + N_WRITERS * ROWS_PER_WRITER} rows, got {len(rows)}"

        print(f"PASS: test_concurrent_writers — {N_WRITERS} concurrent writers, "
              f"{len(rows)} rows merged correctly via local FS")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_restart_persistence():
    """Restart persistence — close kernel, reopen with same store, data survives."""
    store, tmpdir = _make_store()
    try:
        from object_store_native_kernel import ObjectStoreNativeKernel
        from pond_storage import PondStorage

        # First kernel: write data
        kernel1 = ObjectStoreNativeKernel(store)
        s1 = PondStorage(kernel1)
        s1.write("persist_test", [{"id": i, "v": f"v{i}"} for i in range(10)],
                  key_col="id", row_group_size=5)
        s1.append("persist_test", [{"id": 100, "v": "appended"}], key_col="id")

        rows_before = s1.read("persist_test")
        assert len(rows_before) == 11

        kernel1.close()  # no-op for object store kernel

        # Second kernel: SAME store (simulates restart)
        kernel2 = ObjectStoreNativeKernel(store)
        s2 = PondStorage(kernel2)

        rows_after = s2.read("persist_test")
        assert len(rows_after) == 11, \
            f"Data lost on restart: {len(rows_before)} → {len(rows_after)} rows"

        # Point lookup survives
        row = s2.point_lookup("persist_test", key="5")
        assert row is not None
        assert row["id"] == 5

        print(f"PASS: test_restart_persistence — {len(rows_after)} rows survived restart")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_make_kernel_url():
    """Test the unified make_kernel() factory with file:// URL."""
    from make_kernel import make_kernel
    tmpdir = tempfile.mkdtemp(prefix="pond_url_test_")
    try:
        # file:// URL
        kernel = make_kernel(f"file://{tmpdir}")
        from pond_storage import PondStorage
        s = PondStorage(kernel)
        s.write("test", [{"id": 1, "v": "a"}], key_col="id")
        assert len(s.read("test")) == 1

        # Verify it's a LocalFSObjectStore
        assert kernel.base_dir == tmpdir

        print(f"PASS: test_make_kernel_url — make_kernel('file://...') works")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    print("=" * 70)
    print("  Local FS Integration Test (pure files, no SQLite)")
    print("=" * 70)

    tests = [
        test_base_dir_detection,
        test_basic_write_read,
        test_branch_merge,
        test_acid_transactions,
        test_config_as_blob,
        test_list_paths_and_blobs,
        test_concurrent_writers,
        test_restart_persistence,
        test_make_kernel_url,
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
        print("=== ALL LOCAL FS INTEGRATION TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
