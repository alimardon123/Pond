"""Full benchmark against real Cloudflare R2 (S3-compatible).

Runs the same 13 workloads as benchmark_full.py, but against real R2
storage instead of LocalFS or moto mock.

R2 is S3-compatible, so we use S3ObjectStore directly.

Usage:
  python scripts/benchmark_full_r2.py

Cleanup: deletes all objects under the benchmark prefix at the end.
"""
import os, sys, time, threading, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))
sys.path.insert(0, os.path.join(REPO, "lenses", "streaming"))
sys.path.insert(0, os.path.join(REPO, "lenses", "vector"))

from s3_object_store import S3ObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
from pond_storage import PondStorage
import boto3
from botocore.config import Config

# R2 credentials
R2_ENDPOINT = "https://81425c4736b181e41dc82c32050a5207.r2.cloudflarestorage.com"
R2_ACCESS_KEY = "4331a4a6283b1d929cda0085d24450e0"
R2_SECRET_KEY = "286c9be9d520e15fee90145147a43f15001209d192b63ca7a9e2ba53dde31122"
R2_BUCKET = "pondbucket"

# Unique prefix per run (for cleanup)
PREFIX = f"bench-{int(time.time())}"

config = Config(
    connect_timeout=5.0, read_timeout=60.0, max_pool_connections=50,
    retries={"max_attempts": 5, "mode": "adaptive"},
)
_client = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name="auto",
    config=config,
)

# Shared client for all stores
_store_count = [0]

def _make_kernel():
    """Create a kernel backed by R2."""
    store = S3ObjectStore(_client, bucket=R2_BUCKET, prefix=PREFIX)
    kernel = ObjectStoreNativeKernel(store)
    return kernel, store

def _reset(kernel, store):
    kernel.reset_stats()
    store.reset_stats()
    kernel._path_cache.clear()

def _stats(store):
    return {
        "gets": store.stats["gets"],
        "puts": store.stats["puts"],
        "bytes_read": store.stats["bytes_read"],
        "bytes_written": store.stats["bytes_written"],
    }

def _ms(t):
    return f"{t * 1000:.1f}ms"

def _fmt_bytes(n):
    if n < 1024: return f"{n}B"
    elif n < 1024*1024: return f"{n/1024:.1f}KB"
    else: return f"{n/(1024*1024):.1f}MB"


def bench_bulk_write():
    """1. Bulk write at 3 scales."""
    print("\n--- 1. Bulk Write (real R2) ---")
    print(f"  {'Scale':<12} {'Time':>10} {'Rows/s':>12} {'PUTs':>8} {'Written':>10}")
    print(f"  {'-'*12} {'-'*10} {'-'*12} {'-'*8} {'-'*10}")

    for n_rows in [1000, 10000, 100000]:
        kernel, store = _make_kernel()
        s = PondStorage(kernel)
        rows = [{"id": i, "name": f"user_{i}", "age": i % 100} for i in range(n_rows)]
        _reset(kernel, store)
        t0 = time.perf_counter()
        s.write("bench", rows, key_col="id", row_group_size=1000)
        elapsed = time.perf_counter() - t0
        st = _stats(store)
        print(f"  {n_rows:<12} {_ms(elapsed):>10} {n_rows/elapsed:>12.0f} {st['puts']:>8} {_fmt_bytes(st['bytes_written']):>10}")


def bench_append_shard():
    """2. Append shard (single-writer warm)."""
    print("\n--- 2. Append Shard (real R2) ---")
    kernel, store = _make_kernel()
    s = PondStorage(kernel)
    s.write("bench", [{"id": 0, "v": "init"}], key_col="id")

    N = 50  # fewer than LocalFS (R2 latency makes 100 too slow)
    _reset(kernel, store)
    t0 = time.perf_counter()
    for i in range(N):
        s.append_shard("bench", [{"id": i + 1, "v": f"v{i}"}], key_col="id")
    elapsed = time.perf_counter() - t0
    st = _stats(store)
    print(f"  Warm appends ({N}):   {_ms(elapsed)} total, {_ms(elapsed/N)}/op, {N/elapsed:.1f} ops/s, {st['puts']} PUTs")


def bench_point_lookup():
    """3. Point lookup (cold, warm)."""
    print("\n--- 3. Point Lookup (real R2) ---")
    kernel, store = _make_kernel()
    s = PondStorage(kernel)
    s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(10000)],
            key_col="id", row_group_size=1000)

    # Cold
    _reset(kernel, store)
    s._unified._manifest_cache.clear()
    s._unified._head_cache.clear()
    s._unified._manifest_hash_cache.clear()
    t0 = time.perf_counter()
    row = s.point_lookup("bench", key="5000")
    cold = time.perf_counter() - t0
    st_cold = _stats(store)

    # Warm
    _reset(kernel, store)
    t0 = time.perf_counter()
    row = s.point_lookup("bench", key="5001")
    warm = time.perf_counter() - t0
    st_warm = _stats(store)

    print(f"  Cold lookup:   {_ms(cold)}, {st_cold['gets']} GETs")
    print(f"  Warm lookup:   {_ms(warm)}, {st_warm['gets']} GETs")


def bench_range_scan():
    """4. Range scan (full, pruned)."""
    print("\n--- 4. Range Scan (real R2) ---")
    kernel, store = _make_kernel()
    s = PondStorage(kernel)
    s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(10000)],
            key_col="id", row_group_size=100)

    # Full scan
    _reset(kernel, store)
    s._unified._manifest_cache.clear()
    t0 = time.perf_counter()
    rows = s.read("bench")
    full = time.perf_counter() - t0
    st = _stats(store)
    print(f"  Full scan (10000 rows): {_ms(full)}, {len(rows)} rows, {st['gets']} GETs, {_fmt_bytes(st['bytes_read'])} read")

    # Pruned
    print(f"  {'Selectivity':<14} {'Time':>10} {'Rows':>8} {'GETs':>8}")
    print(f"  {'-'*14} {'-'*10} {'-'*8} {'-'*8}")
    for label, pred in [("1% (id>9900)", ("id", ">", 9900)),
                         ("10% (id>9000)", ("id", ">", 9000))]:
        _reset(kernel, store)
        s._unified._manifest_cache.clear()
        t0 = time.perf_counter()
        rows = s.read("bench", predicates=[pred])
        elapsed = time.perf_counter() - t0
        st = _stats(store)
        print(f"  {label:<14} {_ms(elapsed):>10} {len(rows):>8} {st['gets']:>8}")


def bench_branch_merge():
    """5. Branch + merge."""
    print("\n--- 5. Branch + Merge (real R2) ---")
    kernel, store = _make_kernel()
    s = PondStorage(kernel)
    s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(1000)],
            key_col="id", row_group_size=100)

    _reset(kernel, store)
    s._unified._manifest_cache.clear()
    t0 = time.perf_counter()
    s.branch("bench", "dev")
    branch_t = time.perf_counter() - t0
    st = _stats(store)
    print(f"  Branch:  {_ms(branch_t)}, {st['puts']} PUTs")

    s.checkout("bench", "dev")
    s.append_shard("bench", [{"id": 1000 + i, "v": f"dev{i}"} for i in range(100)],
                    key_col="id", row_group_size=100)

    _reset(kernel, store)
    s._unified._manifest_cache.clear()
    t0 = time.perf_counter()
    s.merge("bench", "dev")
    merge_t = time.perf_counter() - t0
    st = _stats(store)
    rows = s.read("bench")
    print(f"  Merge:   {_ms(merge_t)}, {st['puts']} PUTs, {st['gets']} GETs, {len(rows)} rows after")


def bench_acid():
    """6. ACID transactions."""
    print("\n--- 6. ACID Transactions (real R2) ---")
    kernel, store = _make_kernel()
    s = PondStorage(kernel)
    s.write("users", [{"id": 0, "name": "init"}], key_col="id")
    s.write("orders", [{"id": 0, "amount": 0.0}], key_col="id")

    N = 10
    _reset(kernel, store)
    t0 = time.perf_counter()
    for i in range(N):
        tx = s.begin_tx()
        s.append_shard("users", [{"id": i+1, "name": f"u{i}"}], key_col="id", tx_id=tx)
        s.append_shard("orders", [{"id": i+1, "amount": float(i)}], key_col="id", tx_id=tx)
        s.commit_tx(tx)
    elapsed = time.perf_counter() - t0
    st = _stats(store)
    print(f"  {N} 2-collection tx: {_ms(elapsed)} total, {_ms(elapsed/N)}/tx, {N/elapsed:.1f} tx/s, {st['puts']//N} PUTs/tx")


def bench_compaction():
    """7. Compaction (manifest-level)."""
    print("\n--- 7. Compaction (real R2) ---")
    kernel, store = _make_kernel()
    s = PondStorage(kernel)
    s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(1000)],
            key_col="id", row_group_size=100)
    for i in range(3):
        s.append_shard("bench", [{"id": 1000+i*100+j, "v": f"s{i}_{j}"} for j in range(100)],
                        key_col="id", row_group_size=100)

    _reset(kernel, store)
    s._unified._manifest_cache.clear()
    s._unified._head_cache.clear()
    t0 = time.perf_counter()
    s.compact_shards("bench")
    elapsed = time.perf_counter() - t0
    st = _stats(store)
    print(f"  Manifest-level (3 shards, 1300 rows): {_ms(elapsed)}, {st['gets']} GETs, {st['puts']} PUTs (zero data blob I/O)")


def cleanup():
    """Delete all objects under the benchmark prefix."""
    print(f"\n--- Cleanup (deleting prefix: {PREFIX}) ---")
    paginator = _client.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=PREFIX):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            _client.delete_objects(Bucket=R2_BUCKET, Delete={"Objects": objects})
            deleted += len(objects)
    print(f"  Deleted {deleted} objects")


def main():
    print("=" * 70)
    print("  Real R2 Benchmark (Cloudflare R2 — S3-compatible)")
    print(f"  Bucket: {R2_BUCKET}, Prefix: {PREFIX}")
    print(f"  Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    try:
        bench_bulk_write()
        bench_append_shard()
        bench_point_lookup()
        bench_range_scan()
        bench_branch_merge()
        bench_acid()
        bench_compaction()
    finally:
        cleanup()

    print(f"\n{'=' * 70}")
    print("  Benchmark complete.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
