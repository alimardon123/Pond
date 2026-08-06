"""Quick R2 benchmark — scaled down for real network latency."""
import os, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, HERE)  # for _r2_config

from s3_object_store import S3ObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
from pond_storage import PondStorage
from _r2_config import get_r2_client, get_r2_bucket, get_r2_prefix

R2_BUCKET = get_r2_bucket()
PREFIX = get_r2_prefix()
client = get_r2_client()

def make_kernel():
    store = S3ObjectStore(client, bucket=R2_BUCKET, prefix=PREFIX)
    kernel = ObjectStoreNativeKernel(store)
    return kernel, store

def reset(kernel, store):
    kernel.reset_stats()
    store.reset_stats()
    kernel._path_cache.clear()

def ms(t): return f"{t*1000:.0f}ms"

print("=" * 60)
print(f"  Real R2 Benchmark (prefix: {PREFIX})")
print("=" * 60)

# 1. Bulk write (1000 rows)
print("\n--- 1. Bulk Write ---")
kernel, store = make_kernel()
s = PondStorage(kernel)
rows = [{"id": i, "name": f"u{i}", "age": i % 100} for i in range(1000)]
reset(kernel, store)
t0 = time.perf_counter()
s.write("bench", rows, key_col="id", row_group_size=100)
t = time.perf_counter() - t0
print(f"  1000 rows: {ms(t)}, {store.stats['puts']} PUTs, {store.stats['bytes_written']} bytes written")

# 2. Cold point lookup
print("\n--- 2. Point Lookup ---")
reset(kernel, store)
s._unified._manifest_cache.clear()
s._unified._head_cache.clear()
s._unified._manifest_hash_cache.clear()
t0 = time.perf_counter()
row = s.point_lookup("bench", key="500")
cold = time.perf_counter() - t0
print(f"  Cold: {ms(cold)}, {store.stats['gets']} GETs")

reset(kernel, store)
t0 = time.perf_counter()
row = s.point_lookup("bench", key="501")
warm = time.perf_counter() - t0
print(f"  Warm: {ms(warm)}, {store.stats['gets']} GETs")

# 3. Full scan
print("\n--- 3. Full Scan ---")
reset(kernel, store)
s._unified._manifest_cache.clear()
t0 = time.perf_counter()
result = s.read("bench")
full = time.perf_counter() - t0
print(f"  1000 rows: {ms(full)}, {len(result)} rows, {store.stats['gets']} GETs")

# 4. Pruned read (10%)
reset(kernel, store)
s._unified._manifest_cache.clear()
t0 = time.perf_counter()
result = s.read("bench", predicates=[("id", ">", 900)])
pruned = time.perf_counter() - t0
print(f"  10% (id>900): {ms(pruned)}, {len(result)} rows, {store.stats['gets']} GETs")

# 5. Append shard
print("\n--- 4. Append Shard ---")
reset(kernel, store)
t0 = time.perf_counter()
s.append_shard("bench", [{"id": 1000, "v": "new"}], key_col="id")
t = time.perf_counter() - t0
print(f"  1 shard append: {ms(t)}, {store.stats['puts']} PUTs")

# 6. Branch
print("\n--- 5. Branch + Merge ---")
reset(kernel, store)
s._unified._manifest_cache.clear()
t0 = time.perf_counter()
s.branch("bench", "dev")
t = time.perf_counter() - t0
print(f"  Branch: {ms(t)}, {store.stats['puts']} PUTs")

s.checkout("bench", "dev")
s.append_shard("bench", [{"id": 2000, "v": "dev"}], key_col="id")

reset(kernel, store)
s._unified._manifest_cache.clear()
t0 = time.perf_counter()
s.merge("bench", "dev")
t = time.perf_counter() - t0
print(f"  Merge: {ms(t)}, {store.stats['puts']} PUTs, {store.stats['gets']} GETs")

# 7. ACID tx
print("\n--- 6. ACID Transaction ---")
s.write("tx_users", [{"id": 0, "v": "init"}], key_col="id")
s.write("tx_orders", [{"id": 0, "v": "init"}], key_col="id")
reset(kernel, store)
t0 = time.perf_counter()
tx = s.begin_tx()
s.append_shard("tx_users", [{"id": 1, "v": "u1"}], key_col="id", tx_id=tx)
s.append_shard("tx_orders", [{"id": 1, "v": "o1"}], key_col="id", tx_id=tx)
s.commit_tx(tx)
t = time.perf_counter() - t0
print(f"  2-coll tx: {ms(t)}, {store.stats['puts']} PUTs")

# 8. Compaction
print("\n--- 7. Compaction ---")
s.write("compact_test", [{"id": i, "v": f"v{i}"} for i in range(100)], key_col="id", row_group_size=100)
s.append_shard("compact_test", [{"id": 100+i, "v": f"s{i}"} for i in range(100)], key_col="id", row_group_size=100)
reset(kernel, store)
s._unified._manifest_cache.clear()
s._unified._head_cache.clear()
t0 = time.perf_counter()
s.compact_shards("compact_test")
t = time.perf_counter() - t0
print(f"  Manifest-level: {ms(t)}, {store.stats['gets']} GETs, {store.stats['puts']} PUTs")

# Cleanup
print(f"\n--- Cleanup ---")
paginator = client.get_paginator("list_objects_v2")
deleted = 0
for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=PREFIX):
    objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
    if objects:
        client.delete_objects(Bucket=R2_BUCKET, Delete={"Objects": objects})
        deleted += len(objects)
print(f"  Deleted {deleted} objects from R2")

print(f"\n{'=' * 60}")
print("  R2 Benchmark complete.")
print(f"{'=' * 60}")
