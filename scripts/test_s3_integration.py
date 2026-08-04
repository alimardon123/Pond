"""Real S3 integration test — uses moto to mock S3 in-process.

This test exercises the full Pond stack against an S3-compatible backend:
  S3ObjectStore → ObjectStoreNativeKernel → UnifiedStorage → PondStorage

Tests:
1. Basic write/read/point_lookup via S3
2. Branch/merge via S3
3. ACID transactions via S3
4. Config stored as a blob (not local FS)
5. list_paths / list_all_blob_hashes (for GC)
6. Concurrent writers (shards) — no coordination, correct merge

This test does NOT touch real AWS. It uses moto's mock_s3 decorator to
provide an in-process S3-compatible API. To run against real S3, set
AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY + S3_BUCKET env vars and
run without moto (gated by the S3_BUCKET env var).
"""
import os, sys, json, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))

# Check if we're running against real S3 (env var set) or moto (default)
USE_REAL_S3 = bool(os.environ.get("S3_BUCKET"))
BUCKET = os.environ.get("S3_BUCKET", "pond-test-bucket")
PREFIX = os.environ.get("S3_PREFIX", "test")
REGION = os.environ.get("AWS_REGION", "us-east-1")

# Global moto mock (applies to the whole test run when not using real S3)
_MOTO_MOCK = None

if USE_REAL_S3:
    import boto3
    _real_client = boto3.client("s3", region_name=REGION)
    # Ensure bucket exists
    try:
        _real_client.create_bucket(Bucket=BUCKET)
    except Exception:
        pass  # already exists
else:
    import moto
    _MOTO_MOCK = moto.mock_aws()
    _MOTO_MOCK.start()
    import boto3

def _make_store():
    """Create a fresh S3ObjectStore. Bucket is created if needed."""
    from s3_object_store import S3ObjectStore
    if USE_REAL_S3:
        client = _real_client
    else:
        client = boto3.client("s3", region_name=REGION)
    # Ensure bucket exists (idempotent)
    try:
        client.create_bucket(Bucket=BUCKET)
    except Exception:
        pass  # already exists
    return S3ObjectStore(client, bucket=BUCKET, prefix=PREFIX)


def _make_kernel():
    """Create an ObjectStoreNativeKernel backed by S3."""
    store = _make_store()
    from object_store_native_kernel import ObjectStoreNativeKernel
    return ObjectStoreNativeKernel(store), store


def test_basic_write_read():
    """Basic write/read/point_lookup via S3."""
    kernel, store = _make_kernel()
    from pond_storage import PondStorage
    s = PondStorage(kernel)

    s.write("users", [{"id": i, "name": f"u{i}"} for i in range(20)],
            key_col="id", row_group_size=5)

    # Read all
    rows = s.read("users")
    assert len(rows) == 20, f"Expected 20 rows, got {len(rows)}"

    # Point lookup
    row = s.point_lookup("users", key="5")
    assert row is not None
    assert row["id"] == 5
    assert row["name"] == "u5"

    # Predicate read
    rows = s.read("users", predicates=[("id", ">", 15)])
    assert len(rows) == 4  # 16, 17, 18, 19

    print(f"PASS: test_basic_write_read — 20 rows written, point_lookup + predicate read via S3")
    return True


def test_branch_merge():
    """Branch/merge via S3."""
    kernel, store = _make_kernel()
    from pond_storage import PondStorage
    s = PondStorage(kernel)

    s.write("events", [{"id": i, "v": f"main_{i}"} for i in range(10)],
            key_col="id", row_group_size=5)

    # Branch
    s.branch("events", "dev")
    s.checkout("events", "dev")
    s.append("events", [{"id": 100, "v": "dev_100"}], key_col="id")

    # Main should be unchanged (10 rows), dev should have 11
    s_main = PondStorage(kernel)
    assert len(s_main.read("events")) == 10, "Main changed after branch append"

    # Merge dev into main
    s_main.merge("events", "dev")
    rows = s_main.read("events")
    assert len(rows) == 11, f"Expected 11 rows after merge, got {len(rows)}"

    print(f"PASS: test_branch_merge — branch/checkout/append/merge via S3")
    return True


def test_acid_transactions():
    """ACID transactions via S3."""
    kernel, store = _make_kernel()
    from pond_storage import PondStorage
    s = PondStorage(kernel)

    s.write("users", [{"id": 1, "name": "alice"}], key_col="id")
    s.write("orders", [{"id": 1, "amount": 99.9}], key_col="id")

    # Transaction: append to both collections atomically
    tx = s.begin_tx()
    s.append_shard("users", [{"id": 2, "name": "bob"}], key_col="id", tx_id=tx)
    s.append_shard("orders", [{"id": 2, "amount": 50.0}], key_col="id", tx_id=tx)

    # Before commit: neither visible
    assert len(s.read_with_shards("users")) == 1
    assert len(s.read_with_shards("orders")) == 1

    # Commit
    s.commit_tx(tx)

    # After commit: both visible
    assert len(s.read_with_shards("users")) == 2
    assert len(s.read_with_shards("orders")) == 2

    # Abort test
    tx2 = s.begin_tx()
    s.append_shard("users", [{"id": 3, "name": "carol"}], key_col="id", tx_id=tx2)
    s.abort_tx(tx2)  # don't commit
    assert len(s.read_with_shards("users")) == 2, "Aborted tx visible!"

    print(f"PASS: test_acid_transactions — atomic commit + abort via S3")
    return True


def test_config_as_blob():
    """Config is stored as a blob (no local FS) on object-store-backed kernels."""
    kernel, store = _make_kernel()
    from pond_config import PondConfig

    # Save config to kernel
    config = PondConfig()
    config.pruning_enabled = "true"
    config.chunk_size = 500
    config.save_to_kernel(kernel)

    # Load it back
    loaded = PondConfig.load_from_kernel(kernel)
    assert loaded.pruning_enabled == "true", f"Expected 'true', got {loaded.pruning_enabled}"
    assert loaded.chunk_size == 500, f"Expected 500, got {loaded.chunk_size}"

    # load_for_kernel should auto-detect the kernel and use blob storage
    loaded2 = PondConfig.load_for_kernel(kernel)
    assert loaded2.pruning_enabled == "true"

    print(f"PASS: test_config_as_blob — config stored/loaded from S3 (no local FS)")
    return True


def test_list_paths_and_blobs():
    """list_paths / list_all_blob_hashes (for GC) via S3."""
    store = _make_store()

    # Write some blobs
    h1 = store.put_blob(b"blob1")
    h2 = store.put_blob(b"blob2")
    h3 = store.put_blob(b"blob3")

    # Write some paths
    store.put_path("collections/users/branch-refs/main", h1)
    store.put_path("collections/orders/branch-refs/main", h2)
    store.put_path("collections/users/manifest", h3)

    # List all blobs
    all_blobs = set(store.list_all_blob_hashes())
    assert h1 in all_blobs, f"h1 {h1} not in blobs"
    assert h2 in all_blobs, f"h2 {h2} not in blobs"
    assert h3 in all_blobs, f"h3 {h3} not in blobs"

    # List paths with prefix
    users_paths = store.list_paths("collections/users/")
    assert "collections/users/branch-refs/main" in users_paths
    assert "collections/users/manifest" in users_paths
    assert "collections/orders/branch-refs/main" not in users_paths

    print(f"PASS: test_list_paths_and_blobs — list_paths + list_all_blob_hashes via S3")
    return True


def test_concurrent_writers():
    """Concurrent writers (shards) — no coordination, correct merge via S3."""
    kernel, store = _make_kernel()
    from pond_storage import PondStorage
    s = PondStorage(kernel)

    s.write("events", [{"id": 0, "v": "init"}], key_col="id")

    # 5 concurrent writers, each writing 20 rows
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

    # Read: should have 1 (init) + 5*20 = 101 rows
    rows = s.read_with_shards("events")
    assert len(rows) == 1 + N_WRITERS * ROWS_PER_WRITER, \
        f"Expected {1 + N_WRITERS * ROWS_PER_WRITER} rows, got {len(rows)}"

    print(f"PASS: test_concurrent_writers — {N_WRITERS} concurrent writers, "
          f"{len(rows)} rows merged correctly via S3")
    return True


def test_delete_and_gc():
    """Delete + GC (vacuum) via S3.

    GC cleans up UNREACHABLE blobs (old shard manifests, tombstoned refs,
    etc.). The collection's LIVE data blobs (HEAD manifest + data row groups)
    must survive. We verify by reading after GC.
    """
    # Use a unique prefix for this test so previous test blobs don't interfere
    global PREFIX
    old_prefix = PREFIX
    PREFIX = f"{old_prefix}/gc_test"

    try:
        kernel, store = _make_kernel()
        from pond_storage import PondStorage
        s = PondStorage(kernel)

        s.write("temp", [{"id": i, "v": f"v{i}"} for i in range(10)],
                key_col="id", row_group_size=5)

        # Append + compact to create some unreachable old shard blobs
        s.append("temp", [{"id": 100 + i, "v": f"new_{i}"} for i in range(5)],
                  key_col="id")
        s.compact_shards("temp")  # old shards become unreachable

        rows_before = s.read("temp")
        assert len(rows_before) == 15, f"Expected 15 rows, got {len(rows_before)}"

        # GC + vacuum
        stats = s.gc()
        s.vacuum()

        # LIVE data must survive — read should still return all 15 rows
        rows_after = s.read("temp")
        assert len(rows_after) == 15, \
            f"GC deleted live data: {len(rows_before)} → {len(rows_after)} rows"

        print(f"PASS: test_delete_and_gc — GC + vacuum via S3 "
              f"(live data preserved: {len(rows_after)} rows)")
        return True
    finally:
        PREFIX = old_prefix


def test_base_dir_detection():
    """base_dir returns s3:// URL for S3-backed kernels."""
    kernel, store = _make_kernel()
    bd = kernel.base_dir
    assert bd.startswith("s3://"), f"Expected s3:// URL, got {bd}"
    assert BUCKET in bd, f"Expected bucket {BUCKET} in base_dir, got {bd}"

    print(f"PASS: test_base_dir_detection — base_dir = {bd}")
    return True


def main():
    print("=" * 70)
    print(f"  S3 Integration Test ({'real S3' if USE_REAL_S3 else 'moto mock'})")
    print(f"  Bucket: {BUCKET}, Prefix: {PREFIX}")
    print("=" * 70)

    tests = [
        test_base_dir_detection,
        test_basic_write_read,
        test_branch_merge,
        test_acid_transactions,
        test_config_as_blob,
        test_list_paths_and_blobs,
        test_concurrent_writers,
        test_delete_and_gc,
    ]

    passed = 0
    for test in tests:
        try:
            # Each test needs a fresh bucket (moto state is per-mock)
            if not USE_REAL_S3:
                # moto mock is applied at function level via _make_store
                # but we need it to wrap each test
                import moto
                # Re-decorate — moto's mock_aws is per call site
                pass
            if test():
                passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{passed}/{len(tests)} tests passed")
    if passed == len(tests):
        print("=== ALL S3 INTEGRATION TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
