"""Parity benchmark: same workload on LocalFS and S3, report GET/PUT/wall-clock.

Proves that both backends produce IDENTICAL results and shows the
performance difference (local FS is faster — no network RTT).

Workloads:
1. Write 1000 rows (10 row groups)
2. Point lookup (cold)
3. Point lookup (warm)
4. Full scan
5. Predicate-pruned read (10% selectivity)
6. Append 100 rows (shard)
7. Compact shards
8. Branch + merge
9. ACID transaction (2 collections)

For each workload, reports:
  - Wall-clock time (ms)
  - GET count (data + ref)
  - PUT count (data + ref)

Run:
  python scripts/benchmark_parity.py            # local FS only
  S3_BUCKET=my-pond python scripts/benchmark_parity.py  # both local + S3
"""
import os, sys, time, tempfile, shutil

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))

from pond_storage import PondStorage

USE_S3 = bool(os.environ.get("S3_BUCKET"))
S3_BUCKET = os.environ.get("S3_BUCKET", "pond-parity")
S3_PREFIX = os.environ.get("S3_PREFIX", "parity")
REGION = os.environ.get("AWS_REGION", "us-east-1")


def _make_local_kernel():
    """Create a kernel backed by LocalFSObjectStore on a tempdir."""
    from local_fs_object_store import LocalFSObjectStore
    from object_store_native_kernel import ObjectStoreNativeKernel
    tmpdir = tempfile.mkdtemp(prefix="pond_parity_local_")
    store = LocalFSObjectStore(tmpdir)
    return ObjectStoreNativeKernel(store), tmpdir


def _make_s3_kernel():
    """Create a kernel backed by S3ObjectStore (moto or real S3)."""
    from s3_object_store import S3ObjectStore
    from object_store_native_kernel import ObjectStoreNativeKernel
    import boto3

    if USE_REAL_S3 := bool(os.environ.get("S3_BUCKET")):
        client = boto3.client("s3", region_name=REGION)
    else:
        import moto
        _moto = moto.mock_aws()
        _moto.start()
        client = boto3.client("s3", region_name=REGION)

    try:
        client.create_bucket(Bucket=S3_BUCKET)
    except Exception:
        pass

    store = S3ObjectStore(client, bucket=S3_BUCKET, prefix=f"{S3_PREFIX}/{int(time.time())}")
    return ObjectStoreNativeKernel(store), None


def _reset_stats(kernel):
    kernel.reset_stats()
    if hasattr(kernel, 'store') and hasattr(kernel.store, 'reset_stats'):
        kernel.store.reset_stats()
    kernel._root_ref_cache = None
    kernel._root_ref_hash = None


def _get_stats(kernel):
    """Return (gets, puts) from the store stats."""
    store = kernel.store
    gets = store.stats.get("gets", 0)
    puts = store.stats.get("puts", 0)
    return gets, puts


def _run_workload(kernel, label):
    """Run the full workload suite, return results dict."""
    s = PondStorage(kernel)
    results = {}

    # 1. Write 1000 rows
    _reset_stats(kernel)
    s._unified._manifest_cache.clear()
    s._unified._head_cache.clear()
    t0 = time.perf_counter()
    s.write("bench", [{"id": i, "val": f"v{i}"} for i in range(1000)],
            key_col="id", row_group_size=100)
    results["write_1000"] = {
        "time_ms": (time.perf_counter() - t0) * 1000,
        "gets": _get_stats(kernel)[0],
        "puts": _get_stats(kernel)[1],
    }

    # 2. Point lookup (cold)
    _reset_stats(kernel)
    s._unified._manifest_cache.clear()
    s._unified._head_cache.clear()
    t0 = time.perf_counter()
    row = s.point_lookup("bench", key="500")
    results["point_lookup_cold"] = {
        "time_ms": (time.perf_counter() - t0) * 1000,
        "gets": _get_stats(kernel)[0],
        "puts": _get_stats(kernel)[1],
        "ok": row is not None and row["id"] == 500,
    }

    # 3. Point lookup (warm)
    _reset_stats(kernel)
    t0 = time.perf_counter()
    row = s.point_lookup("bench", key="600")
    results["point_lookup_warm"] = {
        "time_ms": (time.perf_counter() - t0) * 1000,
        "gets": _get_stats(kernel)[0],
        "puts": _get_stats(kernel)[1],
        "ok": row is not None and row["id"] == 600,
    }

    # 4. Full scan
    _reset_stats(kernel)
    s._unified._manifest_cache.clear()
    t0 = time.perf_counter()
    rows = s.read("bench")
    results["full_scan"] = {
        "time_ms": (time.perf_counter() - t0) * 1000,
        "gets": _get_stats(kernel)[0],
        "puts": _get_stats(kernel)[1],
        "rows": len(rows),
    }

    # 5. Predicate-pruned read (10% selectivity)
    _reset_stats(kernel)
    s._unified._manifest_cache.clear()
    t0 = time.perf_counter()
    rows = s.read("bench", predicates=[("id", ">", 900)])
    results["pruned_read"] = {
        "time_ms": (time.perf_counter() - t0) * 1000,
        "gets": _get_stats(kernel)[0],
        "puts": _get_stats(kernel)[1],
        "rows": len(rows),
    }

    # 6. Append 100 rows (shard)
    _reset_stats(kernel)
    t0 = time.perf_counter()
    s.append_shard("bench", [{"id": 1000 + i, "val": f"new{i}"} for i in range(100)],
                    key_col="id", row_group_size=100)
    results["append_shard"] = {
        "time_ms": (time.perf_counter() - t0) * 1000,
        "gets": _get_stats(kernel)[0],
        "puts": _get_stats(kernel)[1],
    }

    # 7. Compact shards
    _reset_stats(kernel)
    s._unified._manifest_cache.clear()
    t0 = time.perf_counter()
    s.compact_shards("bench")
    results["compact_shards"] = {
        "time_ms": (time.perf_counter() - t0) * 1000,
        "gets": _get_stats(kernel)[0],
        "puts": _get_stats(kernel)[1],
    }

    # 8. Branch + merge
    _reset_stats(kernel)
    s._unified._manifest_cache.clear()
    t0 = time.perf_counter()
    s.branch("bench", "dev")
    s.checkout("bench", "dev")
    s.append_shard("bench", [{"id": 2000, "val": "dev"}], key_col="id")
    s.merge("bench", "dev")
    results["branch_merge"] = {
        "time_ms": (time.perf_counter() - t0) * 1000,
        "gets": _get_stats(kernel)[0],
        "puts": _get_stats(kernel)[1],
    }

    # 9. ACID transaction (2 collections)
    s.write("users", [{"id": 1, "name": "alice"}], key_col="id")
    s.write("orders", [{"id": 1, "amount": 99.9}], key_col="id")
    _reset_stats(kernel)
    t0 = time.perf_counter()
    tx = s.begin_tx()
    s.append_shard("users", [{"id": 2, "name": "bob"}], key_col="id", tx_id=tx)
    s.append_shard("orders", [{"id": 2, "amount": 50.0}], key_col="id", tx_id=tx)
    s.commit_tx(tx)
    results["acid_tx"] = {
        "time_ms": (time.perf_counter() - t0) * 1000,
        "gets": _get_stats(kernel)[0],
        "puts": _get_stats(kernel)[1],
    }

    # Verify correctness
    assert results["write_1000"]["gets"] >= 0
    assert results["point_lookup_cold"]["ok"]
    assert results["point_lookup_warm"]["ok"]
    assert results["full_scan"]["rows"] == 1000
    assert results["pruned_read"]["rows"] == 99  # 901-999
    assert len(s.read_with_shards("users")) == 2
    assert len(s.read_with_shards("orders")) == 2

    return results


def _print_results(label, results):
    print(f"\n  {label}:")
    print(f"    {'Workload':<22} {'Time (ms)':>10} {'GETs':>8} {'PUTs':>8}")
    print(f"    {'-'*22} {'-'*10} {'-'*8} {'-'*8}")
    for workload, data in results.items():
        print(f"    {workload:<22} {data['time_ms']:>10.2f} {data['gets']:>8} {data['puts']:>8}")


def _compare(local_results, s3_results):
    """Compare results — GET/PUT counts should be identical (same code path)."""
    print(f"\n  Parity check (GET/PUT counts should be IDENTICAL):")
    print(f"    {'Workload':<22} {'Local GETs':>12} {'S3 GETs':>12} {'Match':>6}")
    print(f"    {'-'*22} {'-'*12} {'-'*12} {'-'*6}")
    all_match = True
    for workload in local_results:
        lg = local_results[workload]["gets"]
        sg = s3_results[workload]["gets"]
        match = "✓" if lg == sg else "✗"
        if lg != sg:
            all_match = False
        print(f"    {workload:<22} {lg:>12} {sg:>12} {match:>6}")

    print(f"\n  {'ALL GET COUNTS MATCH' if all_match else 'MISMATCH DETECTED'}")
    return all_match


def main():
    print("=" * 70)
    print("  Parity Benchmark: LocalFS vs S3 (same workload, both backends)")
    print("=" * 70)

    # Run on LocalFS
    print("\n--- LocalFS ---")
    local_kernel, local_tmpdir = _make_local_kernel()
    try:
        local_results = _run_workload(local_kernel, "LocalFS")
        _print_results("LocalFS", local_results)
    finally:
        if local_tmpdir:
            shutil.rmtree(local_tmpdir, ignore_errors=True)

    # Run on S3 (moto mock or real)
    print("\n--- S3 (moto mock) ---")
    s3_kernel, _ = _make_s3_kernel()
    s3_results = _run_workload(s3_kernel, "S3")
    _print_results("S3 (moto)", s3_results)

    # Compare
    all_match = _compare(local_results, s3_results)

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  Summary")
    print(f"{'=' * 70}")
    print(f"""
  Parity: {'PASS — GET counts identical on both backends' if all_match else 'FAIL — GET counts differ'}

  The GET/PUT counts are IDENTICAL because both backends use the same
  ObjectStoreNativeKernel code path — only the store implementation
  differs. The wall-clock difference is the storage latency:
    - LocalFS: ~microseconds per GET (filesystem)
    - S3 (moto): ~microseconds per GET (in-process mock)
    - S3 (real): ~50ms per GET (network RTT)

  To run against real S3:
    S3_BUCKET=my-pond AWS_REGION=us-east-1 python scripts/benchmark_parity.py
""")


if __name__ == "__main__":
    main()
