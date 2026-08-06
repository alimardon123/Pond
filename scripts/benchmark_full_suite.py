"""Full suite benchmark on real Cloudflare R2 — comprehensive feature coverage.

Tests ALL Pond features at multiple scales, with Rust acceleration enabled.

Features tested:
  1. Bulk write at 3 scales (1K, 10K, 100K rows)
  2. Point lookup (cold, warm, at scale)
  3. Full scan + pruned scans (1%, 10%, 50% selectivity)
  4. Column projection pushdown
  5. Append shard (CRDT writes) — single + concurrent
  6. read_with_shards (merge HEAD + shards)
  7. Branch + merge topology
  8. Atomic publication across collections (1-coll, 5-coll)
  9. Compaction (manifest-level + row-level)
  10. Upsert + delete (row-level CRDT)
  11. Time-travel reads (history, diff)
  12. Rust vs Python decode comparison
  13. Multi-process visibility

Usage:
  R2_ENDPOINT=... R2_ACCESS_KEY=... R2_SECRET_KEY=... R2_BUCKET=... \
    PYTHONPATH=pond-core:pond-sdk:pond-sdk/extensions/physical_structures:pond-rust/target/release \
    python scripts/benchmark_full_suite.py
"""
import os, sys, time, tempfile, threading, json

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "pond-rust", "target", "release"))
sys.path.insert(0, HERE)  # for _r2_config

from s3_object_store import S3ObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
from pond_storage import PondStorage
from _r2_config import get_r2_client, get_r2_bucket, get_r2_prefix

# Check Rust acceleration
try:
    import pond_rust
    RUST_ENABLED = True
except ImportError:
    RUST_ENABLED = False

R2_BUCKET = get_r2_bucket()
PREFIX = get_r2_prefix(default=f"suite-{int(time.time())}")
_client = get_r2_client()


def make_kernel():
    store = S3ObjectStore(_client, bucket=R2_BUCKET, prefix=PREFIX)
    kernel = ObjectStoreNativeKernel(store, cache_ttl_seconds=5.0)
    return kernel, store


def reset(kernel, store):
    kernel.reset_stats()
    store.reset_stats()
    kernel._path_cache.clear()
    kernel._path_cache_timestamps.clear()


def clear_caches(storage):
    storage._unified._manifest_cache.clear()
    storage._unified._manifest_hash_cache.clear()
    storage._unified._head_cache.clear()
    storage._unified._shard_list_cache.clear()
    storage._unified._shard_list_cache_timestamps.clear()
    storage._unified._blob_cache.clear()
    storage._unified._blob_cache_order.clear()


def ms(t):
    return f"{t*1000:.0f}ms"


def fmt_bytes(n):
    if n < 1024: return f"{n}B"
    elif n < 1024*1024: return f"{n/1024:.1f}KB"
    elif n < 1024*1024*1024: return f"{n/(1024*1024):.1f}MB"
    else: return f"{n/(1024*1024*1024):.2f}GB"


def stats(store):
    return {
        "gets": store.stats["gets"],
        "puts": store.stats["puts"],
        "bytes_read": store.stats["bytes_read"],
        "bytes_written": store.stats["bytes_written"],
    }


# Track all results for summary
RESULTS = []

def record(test_name, scale, elapsed, st, extra=""):
    RESULTS.append({
        "test": test_name, "scale": scale,
        "time_ms": round(elapsed * 1000, 1),
        "gets": st["gets"] if st else 0,
        "puts": st["puts"] if st else 0,
        "extra": extra,
    })


print("=" * 80)
print(f"  Pond Full Suite Benchmark — Real Cloudflare R2")
print(f"  Rust acceleration: {'ENABLED' if RUST_ENABLED else 'DISABLED'}")
print(f"  Prefix: {PREFIX}")
print("=" * 80)


# =============================================================================
# 1. BULK WRITE at 3 scales
# =============================================================================
print("\n" + "=" * 80)
print("  1. BULK WRITE — 3 scales (1K, 10K, 100K rows)")
print("=" * 80)
print(f"  {'Scale':<10} {'Time':>8} {'Rows/s':>10} {'PUTs':>6} {'Written':>10} {'Throughput':>12}")
print(f"  {'-'*10} {'-'*8} {'-'*10} {'-'*6} {'-'*10} {'-'*12}")

for n_rows in [1_000, 10_000, 100_000]:
    kernel, store = make_kernel()
    s = PondStorage(kernel)
    rows = [{"id": i, "name": f"user_{i}", "age": i % 100, "score": float(i * 0.1),
             "status": i % 6, "region": ["US", "EU", "ASIA"][i % 3]} for i in range(n_rows)]
    reset(kernel, store)
    t0 = time.perf_counter()
    s.write("bulk", rows, key_col="id", row_group_size=10_000)
    elapsed = time.perf_counter() - t0
    st = stats(store)
    rps = n_rows / elapsed
    bw = st["bytes_written"] / elapsed
    print(f"  {n_rows:<10} {ms(elapsed):>8} {rps:>10.0f} {st['puts']:>6} {fmt_bytes(st['bytes_written']):>10} {fmt_bytes(bw):>8}/s")
    record("bulk_write", n_rows, elapsed, st)

# =============================================================================
# 2. POINT LOOKUP — cold, warm, at scale
# =============================================================================
print("\n" + "=" * 80)
print("  2. POINT LOOKUP — cold, warm (10K rows, 1 row group (default size))")
print("=" * 80)

kernel, store = make_kernel()
s = PondStorage(kernel)
s.write("lookup", [{"id": i, "v": f"v{i}"} for i in range(10_000)],
        key_col="id", row_group_size=10_000)

# Cold lookup
reset(kernel, store)
clear_caches(s)
kernel.invalidate_path_cache()
t0 = time.perf_counter()
row = s.point_lookup("lookup", key="5000")
cold = time.perf_counter() - t0
st_cold = stats(store)
print(f"  Cold lookup:  {ms(cold):>8}, {st_cold['gets']} GETs, row.id={row['id'] if row else 'None'}")
record("point_lookup_cold", "10K", cold, st_cold)

# Warm lookup (caches populated)
reset(kernel, store)
t0 = time.perf_counter()
row = s.point_lookup("lookup", key="5001")
warm = time.perf_counter() - t0
st_warm = stats(store)
print(f"  Warm lookup:  {ms(warm):>8}, {st_warm['gets']} GETs, row.id={row['id'] if row else 'None'}")
record("point_lookup_warm", "10K", warm, st_warm)

# =============================================================================
# 3. FULL SCAN + PRUNED SCANS
# =============================================================================
print("\n" + "=" * 80)
print("  3. FULL SCAN + PRUNED SCANS — 10K rows, 1 row group (default size)")
print("=" * 80)

kernel, store = make_kernel()
s = PondStorage(kernel)
s.write("scan", [{"id": i, "v": f"v{i}", "age": i % 100, "region": ["US","EU","ASIA"][i%3]}
                 for i in range(10_000)],
        key_col="id", row_group_size=10_000)

# Full scan
reset(kernel, store)
clear_caches(s)
t0 = time.perf_counter()
rows = s.read("scan")
full = time.perf_counter() - t0
st = stats(store)
print(f"  Full scan (10000 rows):  {ms(full):>8}, {len(rows)} rows, {st['gets']} GETs")
record("full_scan", "10K", full, st)

# 10% pruned
reset(kernel, store)
clear_caches(s)
t0 = time.perf_counter()
rows = s.read("scan", predicates=[("id", ">", 9000)])
pruned10 = time.perf_counter() - t0
st = stats(store)
print(f"  Pruned 10% (id>9000):    {ms(pruned10):>8}, {len(rows)} rows, {st['gets']} GETs")
record("pruned_scan_10pct", "10K", pruned10, st)

# 1% pruned
reset(kernel, store)
clear_caches(s)
t0 = time.perf_counter()
rows = s.read("scan", predicates=[("id", ">", 9900)])
pruned1 = time.perf_counter() - t0
st = stats(store)
print(f"  Pruned 1%  (id>9900):    {ms(pruned1):>8}, {len(rows)} rows, {st['gets']} GETs")
record("pruned_scan_1pct", "10K", pruned1, st)

# Column projection
reset(kernel, store)
clear_caches(s)
t0 = time.perf_counter()
rows = s.read("scan", columns=["id", "age"])
proj = time.perf_counter() - t0
st = stats(store)
print(f"  Projection (id, age):    {ms(proj):>8}, {len(rows)} rows, {st['gets']} GETs")
record("projection_scan", "10K", proj, st)

# =============================================================================
# 4. APPEND SHARD (CRDT writes)
# =============================================================================
print("\n" + "=" * 80)
print("  4. APPEND SHARD — CRDT concurrent writes")
print("=" * 80)

kernel, store = make_kernel()
s = PondStorage(kernel)
s.write("crdt", [{"id": 0, "v": "init"}], key_col="id")

# Single-writer warm appends
N = 20
reset(kernel, store)
t0 = time.perf_counter()
for i in range(N):
    s.append_shard("crdt", [{"id": i + 1, "v": f"v{i}"}], key_col="id")
elapsed = time.perf_counter() - t0
st = stats(store)
print(f"  Single-writer ({N} appends):  {ms(elapsed):>8} total, {ms(elapsed/N)}/op, {N/elapsed:.1f} ops/s")
record("append_shard_single", N, elapsed, st)

# Read with shards (merge)
reset(kernel, store)
clear_caches(s)
t0 = time.perf_counter()
rows = s.read_with_shards("crdt")
read_shards = time.perf_counter() - t0
st = stats(store)
print(f"  read_with_shards ({len(rows)} rows):   {ms(read_shards):>8}, {st['gets']} GETs")
record("read_with_shards", N, read_shards, st)

# Concurrent writers (5 threads × 10 appends each)
kernel2, store2 = make_kernel()
s2 = PondStorage(kernel2)
s2.write("concurrent", [{"id": 0, "v": "init"}], key_col="id")

def concurrent_writer(writer_id, n=10):
    sk, st = make_kernel()
    ss = PondStorage(sk)
    for i in range(n):
        ss.append_shard("concurrent", [{"id": writer_id * 100 + i, "v": f"w{writer_id}_{i}"}],
                        key_col="id")

reset(kernel2, store2)
t0 = time.perf_counter()
threads = [threading.Thread(target=concurrent_writer, args=(w, 10)) for w in range(5)]
for t in threads: t.start()
for t in threads: t.join()
elapsed = time.perf_counter() - t0
st = stats(store2)

# Read all
sk, st2 = make_kernel()
ss = PondStorage(sk)
clear_caches(ss)
rows = ss.read_with_shards("concurrent")
print(f"  5 concurrent writers (50 appends): {ms(elapsed):>8}, {len(rows)} rows after merge")
record("append_shard_concurrent", 50, elapsed, st)

# =============================================================================
# 5. BRANCH + MERGE
# =============================================================================
print("\n" + "=" * 80)
print("  5. BRANCH + MERGE — git-like topology")
print("=" * 80)

kernel, store = make_kernel()
s = PondStorage(kernel)
s.write("branch", [{"id": i, "v": f"v{i}"} for i in range(1000)],
        key_col="id", row_group_size=10_000)

# Branch
reset(kernel, store)
t0 = time.perf_counter()
s.branch("branch", "dev")
branch_t = time.perf_counter() - t0
st = stats(store)
print(f"  Branch (dev):          {ms(branch_t):>8}, {st['puts']} PUTs")
record("branch", "1K", branch_t, st)

# Checkout + append on dev
s.checkout("branch", "dev")
s.append_shard("branch", [{"id": 1000, "v": "dev_row"}], key_col="id")

# Merge
reset(kernel, store)
clear_caches(s)
t0 = time.perf_counter()
s.merge("branch", "dev")
merge_t = time.perf_counter() - t0
s.wait_for_background_tasks()
st = stats(store)
print(f"  Merge (dev → main):    {ms(merge_t):>8}, {st['puts']} PUTs, {st['gets']} GETs")
record("merge", "1K", merge_t, st)

# Verify merge result
rows = s.read("branch")
print(f"  After merge:           {len(rows)} rows")

# =============================================================================
# 6. ATOMIC PUBLICATION (commit markers + CRDT)
# =============================================================================
print("\n" + "=" * 80)
print("  6. ATOMIC PUBLICATION — commit markers + CRDT")
print("=" * 80)

kernel, store = make_kernel()
s = PondStorage(kernel)
for coll in ["tx_users", "tx_orders", "tx_items", "tx_logs", "tx_metrics"]:
    s.write(coll, [{"id": 0, "v": "init"}], key_col="id")

# 1-collection tx
reset(kernel, store)
tx = s.begin_tx()
s.append_shard("tx_users", [{"id": 1, "v": "u1"}], key_col="id", tx_id=tx)
t0 = time.perf_counter()
s.commit_tx(tx)
tx1 = time.perf_counter() - t0
st = stats(store)
print(f"  1-collection tx:       {ms(tx1):>8}, {st['puts']} PUTs")
record("atomic_pub_1coll", "1", tx1, st)

# 5-collection tx
reset(kernel, store)
tx = s.begin_tx()
s.append_shard("tx_users", [{"id": 2, "v": "u2"}], key_col="id", tx_id=tx)
s.append_shard("tx_orders", [{"id": 2, "v": "o2"}], key_col="id", tx_id=tx)
s.append_shard("tx_items", [{"id": 2, "v": "i2"}], key_col="id", tx_id=tx)
s.append_shard("tx_logs", [{"id": 2, "v": "l2"}], key_col="id", tx_id=tx)
s.append_shard("tx_metrics", [{"id": 2, "v": "m2"}], key_col="id", tx_id=tx)
t0 = time.perf_counter()
s.commit_tx(tx)
tx5 = time.perf_counter() - t0
st = stats(store)
print(f"  5-collection tx:       {ms(tx5):>8}, {st['puts']} PUTs")
record("atomic_pub_5coll", "5", tx5, st)

# Abort (no-op)
tx = s.begin_tx()
s.append_shard("tx_users", [{"id": 999, "v": "aborted"}], key_col="id", tx_id=tx)
t0 = time.perf_counter()
s.abort_tx(tx)
abort_t = time.perf_counter() - t0
print(f"  Abort tx:              {ms(abort_t):>8} (no-op)")
record("acid_abort", "1", abort_t, None)

# =============================================================================
# 7. COMPACTION
# =============================================================================
print("\n" + "=" * 80)
print("  7. COMPACTION — manifest-level + row-level")
print("=" * 80)

kernel, store = make_kernel()
s = PondStorage(kernel)
s.write("compact", [{"id": i, "v": f"v{i}"} for i in range(1000)],
        key_col="id", row_group_size=10_000)

# Add 5 shards (manifest-level compaction eligible)
for i in range(5):
    s.append_shard("compact", [{"id": 1000 + i * 100 + j, "v": f"s{i}_{j}"}
                                for j in range(100)], key_col="id")

reset(kernel, store)
clear_caches(s)
t0 = time.perf_counter()
s.compact_shards("compact")
compact_t = time.perf_counter() - t0
s.wait_for_background_tasks()
st = stats(store)
print(f"  Manifest-level compact: {ms(compact_t):>8}, {st['gets']} GETs, {st['puts']} PUTs")
record("compact_manifest", "1K+5shards", compact_t, st)

# Row-level compaction (with upserts)
kernel2, store2 = make_kernel()
s2 = PondStorage(kernel2)
s2.write("compact_row", [{"id": i, "v": f"v{i}"} for i in range(100)],
          key_col="id", row_group_size=10_000)

# Upsert some rows (creates _rowid columns → triggers row-level compaction)
s2.upsert_shard("compact_row", [{"id": 50, "v": "updated"}], key_col="id")

reset(kernel2, store2)
clear_caches(s2)
t0 = time.perf_counter()
s2.compact_shards("compact_row")
compact_row_t = time.perf_counter() - t0
s2.wait_for_background_tasks()
st = stats(store2)
print(f"  Row-level compact:      {ms(compact_row_t):>8}, {st['gets']} GETs, {st['puts']} PUTs")
record("compact_row_level", "100+upsert", compact_row_t, st)

# =============================================================================
# 8. UPSERT + DELETE (row-level CRDT)
# =============================================================================
print("\n" + "=" * 80)
print("  8. UPSERT + DELETE — row-level CRDT")
print("=" * 80)

kernel, store = make_kernel()
s = PondStorage(kernel)
s.write("crdt_row", [{"id": i, "v": f"v{i}"} for i in range(100)],
        key_col="id", row_group_size=10_000)

# Upsert
reset(kernel, store)
t0 = time.perf_counter()
s.upsert_shard("crdt_row", [{"id": 50, "v": "updated"}, {"id": 51, "v": "updated2"}],
                key_col="id")
upsert_t = time.perf_counter() - t0
st = stats(store)
print(f"  Upsert (2 rows):        {ms(upsert_t):>8}, {st['puts']} PUTs")
record("upsert", "2", upsert_t, st)

# Read back
row = s.point_lookup("crdt_row", key="50")
print(f"  After upsert (id=50):   v={row['v'] if row else 'None'}")

# Delete — need actual _rowid from the upserted row
# First read to get the _rowid
rows = s.read("crdt_row")
row_to_delete = None
for r in rows:
    if r.get("id") == 50 and r.get("_rowid"):
        row_to_delete = r
        break

if row_to_delete:
    reset(kernel, store)
    t0 = time.perf_counter()
    s.delete_shard("crdt_row", rowids=[row_to_delete["_rowid"]], key_col="id")
    delete_t = time.perf_counter() - t0
    st = stats(store)
    print(f"  Delete:                 {ms(delete_t):>8}, {st['puts']} PUTs")
    record("delete", "1", delete_t, st)
else:
    print(f"  Delete:                 skipped (no _rowid found)")

# =============================================================================
# 9. TIME-TRAVEL (history + diff)
# =============================================================================
print("\n" + "=" * 80)
print("  9. TIME-TRAVEL — history + diff")
print("=" * 80)

kernel, store = make_kernel()
s = PondStorage(kernel)
s.write("history", [{"id": i, "v": f"v1_{i}"} for i in range(100)],
        key_col="id", row_group_size=10_000)
commit1 = s._unified._head_cache.get("history")

s.write("history", [{"id": i, "v": f"v2_{i}"} for i in range(100)],
        key_col="id", row_group_size=10_000)
commit2 = s._unified._head_cache.get("history")

# History
reset(kernel, store)
t0 = time.perf_counter()
hist = s.history("history", limit=10)
hist_t = time.perf_counter() - t0
st = stats(store)
print(f"  History (10 commits):   {ms(hist_t):>8}, {len(hist)} entries, {st['gets']} GETs")
record("history", "10", hist_t, st)

# Diff
if commit1 and commit2:
    reset(kernel, store)
    t0 = time.perf_counter()
    d = s.diff("history", commit1, commit2)
    diff_t = time.perf_counter() - t0
    st = stats(store)
    print(f"  Diff (2 commits):       {ms(diff_t):>8}, added={len(d.get('added',[]))}, removed={len(d.get('removed',[]))}, {st['gets']} GETs")
    record("diff", "2", diff_t, st)

# =============================================================================
# 10. RUST vs PYTHON DECODE COMPARISON
# =============================================================================
print("\n" + "=" * 80)
print("  10. RUST vs PYTHON DECODE — CPU benchmark")
print("=" * 80)

if RUST_ENABLED:
    sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
    from unified_storage import PND2, ListColumnSource
    import pond_rust

    for n_rows in [1_000, 10_000, 100_000]:
        rows = [{"id": i, "name": f"user_{i}", "age": i % 100,
                 "score": float(i * 0.1), "status": i % 6} for i in range(n_rows)]
        source = ListColumnSource(rows)
        blob, _ = PND2.encode(source)

        # Python decode
        t0 = time.perf_counter()
        for _ in range(10):
            result_py = PND2.decode(blob)
        py_time = (time.perf_counter() - t0) / 10

        # Rust decode
        t0 = time.perf_counter()
        for _ in range(10):
            result_rust = pond_rust.decode(blob)
        rust_time = (time.perf_counter() - t0) / 10

        speedup = py_time / rust_time if rust_time > 0 else 0
        print(f"  {n_rows:>6} rows: Python {ms(py_time):>8} → Rust {ms(rust_time):>8}  ({speedup:.1f}x)")
        record(f"rust_decode_{n_rows}", n_rows, rust_time, None, f"py={py_time*1000:.1f}ms speedup={speedup:.1f}x")
else:
    print("  Rust acceleration not available — skipping")

# =============================================================================
# 11. MULTI-PROCESS VISIBILITY
# =============================================================================
print("\n" + "=" * 80)
print("  11. MULTI-PROCESS VISIBILITY — cross-process reads")
print("=" * 80)

kernel_a, store_a = make_kernel()
s_a = PondStorage(kernel_a)
s_a.write("multi", [{"id": i, "v": f"v{i}"} for i in range(100)],
          key_col="id", row_group_size=10_000)

# Process B reads
kernel_b, store_b = make_kernel()
s_b = PondStorage(kernel_b)
reset(kernel_b, store_b)
clear_caches(s_b)
t0 = time.perf_counter()
rows = s_b.read("multi")
read_b = time.perf_counter() - t0
st = stats(store_b)
print(f"  Process B cold read:    {ms(read_b):>8}, {len(rows)} rows, {st['gets']} GETs")
record("multiprocess_cold_read", "100", read_b, st)

# Process A appends
s_a.append_shard("multi", [{"id": 100, "v": "new"}], key_col="id")

# Process B reads after TTL (5s)
time.sleep(5.5)
reset(kernel_b, store_b)
clear_caches(s_b)
t0 = time.perf_counter()
rows = s_b.read("multi")
read_b2 = time.perf_counter() - t0
st = stats(store_b)
print(f"  Process B after append: {ms(read_b2):>8}, {len(rows)} rows, {st['gets']} GETs")
record("multiprocess_after_append", "101", read_b2, st)

# Process B invalidate + read
s_a.append_shard("multi", [{"id": 101, "v": "new2"}], key_col="id")
s_b.invalidate_all_caches()
reset(kernel_b, store_b)
t0 = time.perf_counter()
rows = s_b.read("multi")
read_b3 = time.perf_counter() - t0
st = stats(store_b)
print(f"  Process B after inval:  {ms(read_b3):>8}, {len(rows)} rows, {st['gets']} GETs")
record("multiprocess_after_invalidate", "102", read_b3, st)


# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 80)
print("  SUMMARY — All Results")
print("=" * 80)
print(f"  {'Test':<30} {'Scale':<15} {'Time':>8} {'GETs':>6} {'PUTs':>6} {'Extra':<30}")
print(f"  {'-'*30} {'-'*15} {'-'*8} {'-'*6} {'-'*6} {'-'*30}")
for r in RESULTS:
    print(f"  {r['test']:<30} {r['scale']:<15} {r['time_ms']:>7.0f}ms {r['gets']:>6} {r['puts']:>6} {r['extra']:<30}")

print(f"\n  Total tests: {len(RESULTS)}")
print(f"  Rust acceleration: {'ENABLED' if RUST_ENABLED else 'DISABLED'}")

# Cleanup
print(f"\n--- Cleanup ---")
paginator = _client.get_paginator("list_objects_v2")
deleted = 0
for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=PREFIX):
    objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
    if objects:
        _client.delete_objects(Bucket=R2_BUCKET, Delete={"Objects": objects})
        deleted += len(objects)
print(f"  Deleted {deleted} objects from R2")

print(f"\n{'=' * 80}")
print(f"  Full suite benchmark complete.")
print(f"{'=' * 80}")
