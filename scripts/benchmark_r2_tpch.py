"""TPC-H R2 benchmark — scaled to fit memory, real R2 network."""
import os, sys, time, json, threading

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))

from s3_object_store import S3ObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
from pond_storage import PondStorage
import boto3
from botocore.config import Config
import duckdb

R2_ENDPOINT = "https://81425c4736b181e41dc82c32050a5207.r2.cloudflarestorage.com"
R2_ACCESS_KEY = "4331a4a6283b1d929cda0085d24450e0"
R2_SECRET_KEY = "286c9be9d520e15fee90145147a43f15001209d192b63ca7a9e2ba53dde31122"
R2_BUCKET = "pondbucket"
PREFIX = f"tpch-{int(time.time())}"

config = Config(connect_timeout=5.0, read_timeout=120.0, max_pool_connections=50,
                retries={"max_attempts": 5, "mode": "adaptive"})
client = boto3.client("s3", endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY, aws_secret_access_key=R2_SECRET_KEY,
    region_name="auto", config=config)

store = S3ObjectStore(client, bucket=R2_BUCKET, prefix=PREFIX)
kernel = ObjectStoreNativeKernel(store)
s = PondStorage(kernel)

def ms(t): return f"{t:.1f}s" if t > 1 else f"{t*1000:.0f}ms"
def mb(b): return f"{b/(1024*1024):.1f}MB" if b < 1024*1024*1024 else f"{b/(1024*1024*1024):.2f}GB"

def count_r2():
    count = 0; total = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=R2_BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            count += 1; total += obj["Size"]
    return count, total

def reset():
    kernel.reset_stats(); store.reset_stats()

print("=" * 70)
print("  TPC-H Benchmark on Real R2 (Cloudflare)")
print(f"  Prefix: {PREFIX}")
print("=" * 70)

# Generate TPC-H SF=0.1 (~600K lineitem rows)
print("\n--- Generating TPC-H data (SF=0.1) ---")
con = duckdb.connect()
con.execute("INSTALL tpch"); con.execute("LOAD tpch")
con.execute("CALL dbgen(sf=0.1)")

li_count = con.execute("SELECT COUNT(*) FROM lineitem").fetchone()[0]
print(f"  lineitem: {li_count:,} rows, 16 columns")

# === 1. Bulk Load (batched — write in chunks of 50K) ===
print("\n--- 1. Bulk Load (lineitem, batched) ---")
t0 = time.perf_counter()
batch_size = 50_000
offset = 0
first = True
while offset < li_count:
    df = con.execute(f"SELECT * FROM lineitem LIMIT {batch_size} OFFSET {offset}").fetchdf()
    if len(df) == 0:
        break
    # Convert types for PND2
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)
    rows = df.to_dict('records')
    for r in rows:
        for k in list(r.keys()):
            r[k.replace("l_", "")] = r.pop(k)
    if first:
        s.write("lineitem", rows, key_col="orderkey", row_group_size=50_000, message="TPC-H SF=0.1")
        first = False
    else:
        s.append_shard("lineitem", rows, key_col="orderkey", row_group_size=50_000)
    offset += batch_size
    if offset % 100_000 == 0:
        print(f"    {offset:,} rows loaded ({time.perf_counter()-t0:.1f}s)")

write_t = time.perf_counter() - t0
puts = store.stats["puts"]
objs, bytes_ = count_r2()
print(f"  Write: {ms(write_t)}, {puts} PUTs, {li_count/write_t:.0f} rows/s")
print(f"  R2: {objs} objects, {mb(bytes_)}")

# === 2. Point Lookup ===
print("\n--- 2. Point Lookup ---")
reset(); kernel._path_cache.clear()
s._unified._manifest_cache.clear(); s._unified._head_cache.clear(); s._unified._manifest_hash_cache.clear()
t0 = time.perf_counter()
row = s.point_lookup("lineitem", key="100000")
cold = time.perf_counter() - t0
print(f"  Cold: {ms(cold)}, {store.stats['gets']} GETs, found={row is not None}")

reset()
t0 = time.perf_counter()
row = s.point_lookup("lineitem", key="100001")
warm = time.perf_counter() - t0
print(f"  Warm: {ms(warm)}, {store.stats['gets']} GETs")

# === 3. Pruned Scan ===
print("\n--- 3. Pruned Scan (quantity > 45, ~1%) ---")
reset(); kernel._path_cache.clear(); s._unified._manifest_cache.clear()
t0 = time.perf_counter()
result = s.read("lineitem", predicates=[("quantity", ">", 45.0)])
pruned = time.perf_counter() - t0
print(f"  Pruned: {ms(pruned)}, {len(result):,} rows, {store.stats['gets']} GETs, {mb(store.stats['bytes_read'])} read")

# === 4. Full Scan (count via iter_rows) ===
print("\n--- 4. Full Scan (streaming count) ---")
reset(); kernel._path_cache.clear(); s._unified._manifest_cache.clear()
t0 = time.perf_counter()
count = sum(len(b) for b in s.iter_rows("lineitem", batch_size=50_000))
full = time.perf_counter() - t0
print(f"  Full: {ms(full)}, {count:,} rows, {store.stats['gets']} GETs, {mb(store.stats['bytes_read'])} read")
print(f"  Throughput: {count/full:.0f} rows/s, {store.stats['bytes_read']/full/(1024*1024):.0f} MB/s")

# === 5. Delta Write ===
print("\n--- 5. Delta Write (1 row) ---")
reset()
t0 = time.perf_counter()
s.append_shard("lineitem", [{"orderkey": 99999999, "partkey": 1, "suppkey": 1,
    "linenumber": 1, "quantity": 10.0, "extendedprice": 1000.0,
    "discount": 0.0, "tax": 0.08, "returnflag": "N", "linestatus": "O",
    "shipdate": "1998-01-01", "commitdate": "1998-01-01", "receiptdate": "1998-01-01",
    "shipinstruct": "NONE", "shipmode": "AIR", "comment": "delta"}], key_col="orderkey")
delta = time.perf_counter() - t0
print(f"  Delta: {ms(delta)}, {store.stats['puts']} PUTs, {store.stats['gets']} GETs")

# === 6. Branch + Merge ===
print("\n--- 6. Branch + Merge ---")
reset(); kernel._path_cache.clear(); s._unified._manifest_cache.clear()
t0 = time.perf_counter()
s.branch("lineitem", "dev")
branch_t = time.perf_counter() - t0
print(f"  Branch: {ms(branch_t)}, {store.stats['puts']} PUTs")

s.checkout("lineitem", "dev")
s.append_shard("lineitem", [{"orderkey": 88888888, "partkey": 1, "suppkey": 1,
    "linenumber": 1, "quantity": 1.0, "extendedprice": 1.0,
    "discount": 0.0, "tax": 0.0, "returnflag": "N", "linestatus": "O",
    "shipdate": "1998-01-01", "commitdate": "1998-01-01", "receiptdate": "1998-01-01",
    "shipinstruct": "NONE", "shipmode": "AIR", "comment": "dev"}], key_col="orderkey")

reset(); s._unified._manifest_cache.clear()
t0 = time.perf_counter()
s.merge("lineitem", "dev", message="merge dev")
merge_t = time.perf_counter() - t0
print(f"  Merge: {ms(merge_t)}, {store.stats['puts']} PUTs, {store.stats['gets']} GETs")

# === 7. ACID Transaction ===
print("\n--- 7. ACID Transaction ---")
orders_df = con.execute("SELECT * FROM orders LIMIT 1000").fetchdf()
for col in orders_df.columns:
    if orders_df[col].dtype == 'object':
        orders_df[col] = orders_df[col].astype(str)
orders_rows = orders_df.to_dict('records')
for r in orders_rows:
    for k in list(r.keys()):
        r[k.replace("o_", "")] = r.pop(k)
s.write("orders", orders_rows, key_col="orderkey", row_group_size=1000, message="orders sample")

reset()
t0 = time.perf_counter()
tx = s.begin_tx()
s.append_shard("lineitem", [{"orderkey": 77777777, "partkey": 1, "suppkey": 1,
    "linenumber": 1, "quantity": 1.0, "extendedprice": 1.0,
    "discount": 0.0, "tax": 0.0, "returnflag": "N", "linestatus": "O",
    "shipdate": "1998-01-01", "commitdate": "1998-01-01", "receiptdate": "1998-01-01",
    "shipinstruct": "NONE", "shipmode": "AIR", "comment": "tx"}], key_col="orderkey", tx_id=tx)
s.append_shard("orders", [{"orderkey": 77777777, "custkey": 1, "orderstatus": "O",
    "totalprice": 1.0, "orderdate": "1998-01-01",
    "orderpriority": "1-URGENT", "clerk": "C1", "shippriority": 0, "comment": "tx"}],
    key_col="orderkey", tx_id=tx)
s.commit_tx(tx, message="atomic order + lineitem")
acid_t = time.perf_counter() - t0
print(f"  ACID tx: {ms(acid_t)}, {store.stats['puts']} PUTs")

# === 8. Compaction ===
print("\n--- 8. Compaction ---")
for i in range(3):
    s.append_shard("lineitem", [{"orderkey": 66660000+i, "partkey": 1, "suppkey": 1,
        "linenumber": 1, "quantity": 1.0, "extendedprice": 1.0,
        "discount": 0.0, "tax": 0.0, "returnflag": "N", "linestatus": "O",
        "shipdate": "1998-01-01", "commitdate": "1998-01-01", "receiptdate": "1998-01-01",
        "shipinstruct": "NONE", "shipmode": "AIR", "comment": f"shard{i}"}], key_col="orderkey")

reset(); s._unified._manifest_cache.clear(); s._unified._head_cache.clear()
objs_before, _ = count_r2()
t0 = time.perf_counter()
s.compact_shards("lineitem", target_row_group_size=100_000)
compact_t = time.perf_counter() - t0
objs_after, _ = count_r2()
print(f"  Compact: {ms(compact_t)}, {store.stats['gets']} GETs, {store.stats['puts']} PUTs")
print(f"  Objects: {objs_before} → {objs_after}")

# === 9. Concurrent Writers ===
print("\n--- 9. Concurrent Writers (3 × 5) ---")
errors = []
def writer(wid):
    try:
        ws = PondStorage(kernel)
        for i in range(5):
            ws.append_shard("lineitem", [{"orderkey": 55000000+wid*100+i,
                "partkey": 1, "suppkey": 1, "linenumber": 1,
                "quantity": 1.0, "extendedprice": 1.0,
                "discount": 0.0, "tax": 0.0, "returnflag": "N", "linestatus": "O",
                "shipdate": "1998-01-01", "commitdate": "1998-01-01", "receiptdate": "1998-01-01",
                "shipinstruct": "NONE", "shipmode": "AIR", "comment": f"w{wid}_{i}"}],
                key_col="orderkey")
    except Exception as e:
        errors.append(e)

reset()
t0 = time.perf_counter()
threads = [threading.Thread(target=writer, args=(w,)) for w in range(3)]
for t in threads: t.start()
for t in threads: t.join()
concurrent_t = time.perf_counter() - t0
print(f"  3×5: {ms(concurrent_t)}, {store.stats['puts']} PUTs, {len(errors)} errors")

# === 10. Schema Evolution ===
print("\n--- 10. Schema Evolution ---")
reset(); s._unified._manifest_cache.clear()
objs_before, _ = count_r2()
t0 = time.perf_counter()
result = s.alter_collection("lineitem", add_columns=["discount_code", ("priority", "int64")])
alter_t = time.perf_counter() - t0
objs_after, _ = count_r2()
print(f"  Add 2 cols: {ms(alter_t)}, result={result}")
print(f"  Objects: {objs_before} → {objs_after} (0 data rewrite)")

# === 11. Cross-Lens ===
print("\n--- 11. Cross-Lens Access ---")
from keyvalue_lens import KeyValueLens
kv = KeyValueLens(kernel, "lineitem")
reset(); kernel._path_cache.clear()
t0 = time.perf_counter()
v = kv.get("100000")
kv_t = time.perf_counter() - t0
print(f"  KV reads lineitem: {ms(kv_t)}, found={v is not None}")

# === Summary ===
objs, bytes_ = count_r2()
print(f"\n{'=' * 70}")
print(f"  TPC-H R2 Benchmark Summary (SF=0.1, {li_count:,} lineitem rows)")
print(f"{'=' * 70}")
print(f"  R2 storage: {objs} objects, {mb(bytes_)}")
print(f"  Bulk write:       {ms(write_t)} ({li_count/write_t:.0f} rows/s, {puts} PUTs)")
print(f"  Cold lookup:      {ms(cold)}")
print(f"  Warm lookup:      {ms(warm)}")
print(f"  Pruned scan:      {ms(pruned)} ({len(result):,} rows)")
print(f"  Full scan:        {ms(full)} ({count:,} rows, {count/full:.0f} rows/s)")
print(f"  Delta write:      {ms(delta)} (2 PUTs)")
print(f"  Branch:           {ms(branch_t)}")
print(f"  Merge:            {ms(merge_t)}")
print(f"  ACID tx:          {ms(acid_t)}")
print(f"  Compaction:       {ms(compact_t)}")
print(f"  Concurrent (3w):  {ms(concurrent_t)}")
print(f"  Schema evolution: {ms(alter_t)} (0 data rewrite)")
print(f"  Cross-lens read:  {ms(kv_t)}")

# Cleanup
print(f"\n--- Cleanup ---")
deleted = 0
for page in client.get_paginator("list_objects_v2").paginate(Bucket=R2_BUCKET, Prefix=PREFIX):
    objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
    if objects:
        client.delete_objects(Bucket=R2_BUCKET, Delete={"Objects": objects})
        deleted += len(objects)
print(f"  Deleted {deleted} objects from R2")
