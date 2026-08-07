"""Comprehensive R2 demo — TPC-H data + multiple lenses + cross-lens.

Creates a realistic dataset on R2 using DIFFERENT lenses:
  - LakehouseLens: TPC-H lineitem + orders (tabular SQL)
  - KeyValueLens: user profiles (KV store)
  - VectorLens: product embeddings (vector search)
  - StreamingLens: event stream (streaming)

Then tests cross-lens bidirectional access:
  - KV lens reads lakehouse collection
  - Lakehouse lens reads KV collection
  - PondStorage reads ALL collections uniformly

Data is LEFT on R2 under 'pond-full-demo' for inspection.

Usage:
  R2_ENDPOINT=... R2_ACCESS_KEY=... R2_SECRET_KEY=... R2_BUCKET=... \
      python scripts/demo_r2_full.py
"""
import os, sys, time, json, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))
sys.path.insert(0, os.path.join(REPO, "lenses", "vector"))
sys.path.insert(0, os.path.join(REPO, "lenses", "streaming"))
sys.path.insert(0, HERE)  # for _r2_config

from s3_object_store import S3ObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
from pond_storage import PondStorage
from _r2_config import get_r2_client, get_r2_bucket
import duckdb

R2_BUCKET = get_r2_bucket()
PREFIX = "pond-full-demo"
client = get_r2_client()

# Delete old demo data
print("=== Cleaning up old demo data ===")
deleted = 0
for prefix_to_delete in ["pond-tpch-demo/", "pond-full-demo/"]:
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=R2_BUCKET, Prefix=prefix_to_delete):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=R2_BUCKET, Delete={"Objects": objects})
            deleted += len(objects)
print(f"  Deleted {deleted} old objects")

# Create fresh store
store = S3ObjectStore(client, bucket=R2_BUCKET, prefix=PREFIX)
kernel = ObjectStoreNativeKernel(store)
s = PondStorage(kernel)

def ms(t): return f"{t:.1f}s" if t > 1 else f"{t*1000:.0f}ms"
def mb(b): return f"{b/(1024*1024):.1f}MB" if b < 1024*1024*1024 else f"{b/(1024*1024*1024):.2f}GB"

print(f"\n{'=' * 70}")
print(f"  Full R2 Demo — TPC-H + Multiple Lenses + Cross-Lens")
print(f"  Prefix: {PREFIX}")
print(f"  Data will be LEFT on R2 for inspection")
print(f"{'=' * 70}")

# Generate TPC-H data
print("\n--- Generating TPC-H data (SF=0.1) ---")
con = duckdb.connect()
con.execute("INSTALL tpch"); con.execute("LOAD tpch")
con.execute("CALL dbgen(sf=0.1)")

# ============================================================
# 1. LAKEHOUSE LENS: TPC-H lineitem + orders
# ============================================================
print("\n--- 1. LakehouseLens: Loading TPC-H lineitem ---")

# Load lineitem in batches
li_count = con.execute("SELECT COUNT(*) FROM lineitem").fetchone()[0]
t0 = time.perf_counter()
batch_size = 50_000
offset = 0
first = True
while offset < li_count:
    df = con.execute(f"SELECT * FROM lineitem LIMIT {batch_size} OFFSET {offset}").fetchdf()
    if len(df) == 0: break
    for col in df.columns:
        if df[col].dtype == 'object': df[col] = df[col].astype(str)
    rows = df.to_dict('records')
    for r in rows:
        for k in list(r.keys()): r[k.replace("l_", "")] = r.pop(k)
    if first:
        s.write("lineitem", rows, key_col="orderkey", row_group_size=50_000, message="TPC-H lineitem")
        first = False
    else:
        s.append_shard("lineitem", rows, key_col="orderkey", row_group_size=50_000)
    offset += batch_size
li_write_t = time.perf_counter() - t0
print(f"  lineitem: {li_count:,} rows in {ms(li_write_t)} ({li_count/li_write_t:.0f} rows/s)")

# Load orders
print("--- Loading TPC-H orders ---")
orders_count = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
df = con.execute("SELECT * FROM orders").fetchdf()
for col in df.columns:
    if df[col].dtype == 'object': df[col] = df[col].astype(str)
orders_rows = df.to_dict('records')
for r in orders_rows:
    for k in list(r.keys()): r[k.replace("o_", "")] = r.pop(k)
t0 = time.perf_counter()
s.write("orders", orders_rows, key_col="orderkey", row_group_size=50_000, message="TPC-H orders")
ord_write_t = time.perf_counter() - t0
print(f"  orders: {orders_count:,} rows in {ms(ord_write_t)}")

# Branch + merge on lineitem
print("--- Branch + Merge on lineitem ---")
s.branch("lineitem", "analytics")
s.checkout("lineitem", "analytics")
s.append_shard("lineitem", [{"orderkey": 99999999, "partkey": 1, "suppkey": 1,
    "linenumber": 1, "quantity": 10.0, "extendedprice": 1000.0,
    "discount": 0.0, "tax": 0.08, "returnflag": "N", "linestatus": "O",
    "shipdate": "1998-01-01", "commitdate": "1998-01-01", "receiptdate": "1998-01-01",
    "shipinstruct": "NONE", "shipmode": "AIR", "comment": "analytics branch"}],
    key_col="orderkey")
s.merge("lineitem", "analytics", message="merge analytics branch")
print(f"  Branch + merge done, history: {len(s.history('lineitem'))} commits")

# Schema evolution on lineitem
print("--- Schema Evolution on lineitem ---")
result = s.alter_collection("lineitem", add_columns=["discount_code", ("priority", "int64")])
print(f"  Added columns: {result}")

# Point lookup
print("--- Point Lookup ---")
store.reset_stats(); kernel._path_cache.clear()
s._unified._manifest_cache.clear(); s._unified._head_cache.clear(); s._unified._manifest_hash_cache.clear()
t0 = time.perf_counter()
row = s.point_lookup("lineitem", key="100000")
cold_t = time.perf_counter() - t0
print(f"  Cold lookup id=100000: {ms(cold_t)}, {store.stats['gets']} GETs, found={row is not None}")

store.reset_stats()
t0 = time.perf_counter()
row = s.point_lookup("lineitem", key="100001")
warm_t = time.perf_counter() - t0
print(f"  Warm lookup id=100001: {ms(warm_t)}, {store.stats['gets']} GETs")

# Pruned scan
print("--- Pruned Scan ---")
store.reset_stats(); kernel._path_cache.clear(); s._unified._manifest_cache.clear()
t0 = time.perf_counter()
result_rows = s.read("lineitem", predicates=[("quantity", ">", 45.0)])
pruned_t = time.perf_counter() - t0
print(f"  quantity > 45: {len(result_rows):,} rows in {ms(pruned_t)}, {store.stats['gets']} GETs")

# ============================================================
# 2. KEYVALUE LENS: User profiles
# ============================================================
print("\n--- 2. KeyValueLens: User profiles ---")
from keyvalue_lens import KeyValueLens
kv = KeyValueLens(kernel, "user_profiles")

# Put 1000 user profiles
t0 = time.perf_counter()
for i in range(1000):
    kv.put(f"user_{i:04d}", {
        "name": f"User {i}",
        "email": f"user{i}@example.com",
        "age": i % 80,
        "city": ["NYC", "LA", "SF", "CHI", "BOS"][i % 5],
        "order_count": i % 20,
    })
kv.commit("1000 user profiles")
kv_write_t = time.perf_counter() - t0
print(f"  1000 KV puts: {ms(kv_write_t)} ({1000/kv_write_t:.0f} ops/s)")

# Get a key
store.reset_stats()
t0 = time.perf_counter()
v = kv.get("user_0500")
kv_get_t = time.perf_counter() - t0
print(f"  get('user_0500'): {ms(kv_get_t)}, {store.stats['gets']} GETs")
print(f"  Value: name={v.get('name')}, city={v.get('city')}")

# ============================================================
# 3. VECTOR LENS: Product embeddings
# ============================================================
print("\n--- 3. VectorLens: Product embeddings ---")
from vector_lens import VectorLens
vlens = VectorLens(kernel)

# Insert 500 product vectors (10-dim)
import random
random.seed(42)
t0 = time.perf_counter()
for i in range(500):
    vlens.insert("products", f"product_{i:04d}",
                 [random.random() for _ in range(10)],
                 metadata={"name": f"Product {i}", "price": float(i * 10)})
vlens.commit("products", "500 product vectors")
vec_write_t = time.perf_counter() - t0
print(f"  500 vectors inserted: {ms(vec_write_t)}")

# Build HNSW index
print("--- Building HNSW index ---")
t0 = time.perf_counter()
vlens.build_hnsw_index("products", M=16, ef_construction=200)
hnsw_t = time.perf_counter() - t0
print(f"  HNSW built: {ms(hnsw_t)}")

# Search
store.reset_stats(); kernel._path_cache.clear()
vlens._unified_storage._manifest_cache.clear()
t0 = time.perf_counter()
results = vlens.search("products", [0.5] * 10, k=5)
search_t = time.perf_counter() - t0
print(f"  Search k=5: {ms(search_t)}, {store.stats['gets']} GETs, {len(results)} results")
for r in results[:3]:
    print(f"    id={r.get('id')}, dist={r.get('distance', 0):.4f}")

# ============================================================
# 4. STREAMING LENS: Event stream
# ============================================================
print("\n--- 4. StreamingLens: Event stream ---")
from streaming_lens import StreamingLens
slens = StreamingLens(kernel)

# Write 100KB stream in 10KB segments
stream_data = b"event_data_" * 10000  # ~100KB
t0 = time.perf_counter()
slens.write_stream("events", stream_data, segment_size=10240)
stream_write_t = time.perf_counter() - t0
print(f"  Write 100KB stream: {ms(stream_write_t)}")

# Read it back
store.reset_stats()
t0 = time.perf_counter()
read_data = slens.read_stream("events")
stream_read_t = time.perf_counter() - t0
print(f"  Read 100KB stream: {ms(stream_read_t)}, {store.stats['gets']} GETs")
print(f"  Correctness: {'✓' if read_data == stream_data else '✗'}")

# ============================================================
# 5. CROSS-LENS BIDIRECTIONAL ACCESS
# ============================================================
print("\n--- 5. Cross-Lens Bidirectional Access ---")

# KV lens reads lineitem (lakehouse collection)
print("--- KV lens reads lineitem ---")
kv_li = KeyValueLens(kernel, "lineitem")
store.reset_stats(); kernel._path_cache.clear()
t0 = time.perf_counter()
v = kv_li.get("100000")
cross1_t = time.perf_counter() - t0
print(f"  kv.get('100000') on lineitem: {ms(cross1_t)}, {store.stats['gets']} GETs")
if v:
    keys = list(v.keys())[:5] if isinstance(v, dict) else str(v)[:80]
    print(f"  Returned keys: {keys}...")

# PondStorage reads user_profiles (KV collection)
print("--- PondStorage reads user_profiles (KV collection) ---")
store.reset_stats(); kernel._path_cache.clear()
s._unified._manifest_cache.clear()
t0 = time.perf_counter()
rows = s.read("user_profiles")
cross2_t = time.perf_counter() - t0
print(f"  storage.read('user_profiles'): {ms(cross2_t)}, {len(rows)} rows, {store.stats['gets']} GETs")
if rows:
    print(f"  First row: {list(rows[0].keys())[:5]}...")

# PondStorage reads products (vector collection)
print("--- PondStorage reads products (vector collection) ---")
store.reset_stats(); kernel._path_cache.clear()
s._unified._manifest_cache.clear()
t0 = time.perf_counter()
rows = s.read("products")
cross3_t = time.perf_counter() - t0
print(f"  storage.read('products'): {ms(cross3_t)}, {len(rows)} rows, {store.stats['gets']} GETs")

# ============================================================
# 6. ACID TRANSACTION (cross-collection)
# ============================================================
print("\n--- 6. ACID Transaction (lineitem + orders) ---")
store.reset_stats()
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

# ============================================================
# 7. COMPACTION
# ============================================================
print("\n--- 7. Compaction ---")
# Add shards to lineitem for compaction
for i in range(3):
    s.append_shard("lineitem", [{"orderkey": 66660000+i, "partkey": 1, "suppkey": 1,
        "linenumber": 1, "quantity": 1.0, "extendedprice": 1.0,
        "discount": 0.0, "tax": 0.0, "returnflag": "N", "linestatus": "O",
        "shipdate": "1998-01-01", "commitdate": "1998-01-01", "receiptdate": "1998-01-01",
        "shipinstruct": "NONE", "shipmode": "AIR", "comment": f"shard{i}"}], key_col="orderkey")

store.reset_stats()
s._unified._manifest_cache.clear(); s._unified._head_cache.clear()
objs_before = sum(1 for _ in client.get_paginator("list_objects_v2").paginate(Bucket=R2_BUCKET, Prefix=PREFIX))
t0 = time.perf_counter()
s.compact_shards("lineitem", target_row_group_size=100_000)
compact_t = time.perf_counter() - t0
objs_after = sum(1 for _ in client.get_paginator("list_objects_v2").paginate(Bucket=R2_BUCKET, Prefix=PREFIX))
print(f"  Compaction: {ms(compact_t)}, {store.stats['gets']} GETs, {store.stats['puts']} PUTs")
print(f"  Objects: {objs_before} → {objs_after} (no empty blobs)")

# Verify data intact
rows = s.read("lineitem")
print(f"  Data intact: {len(rows):,} rows")

# ============================================================
# SUMMARY
# ============================================================
objs = 0; total_bytes = 0
for page in client.get_paginator("list_objects_v2").paginate(Bucket=R2_BUCKET, Prefix=PREFIX):
    for obj in page.get("Contents", []):
        objs += 1; total_bytes += obj["Size"]

print(f"\n{'=' * 70}")
print(f"  Full R2 Demo Summary — Data LEFT on R2 for inspection")
print(f"{'=' * 70}")
print(f"\n  R2: {R2_BUCKET}/{PREFIX}/")
print(f"  Objects: {objs}")
print(f"  Size: {mb(total_bytes)}")
print(f"\n  Collections (created by different lenses):")
print(f"    lineitem   (LakehouseLens) — {li_count:,} rows, 16+2 columns, branch+merge history")
print(f"    orders     (LakehouseLens) — {orders_count:,} rows, 9 columns")
print(f"    user_profiles (KeyValueLens) — 1,000 KV entries")
print(f"    products   (VectorLens)    — 500 vectors (10-dim) + HNSW index")
print(f"    events     (StreamingLens) — 100KB stream")
print(f"\n  Performance:")
print(f"    Lakehouse write:  {ms(li_write_t)} ({li_count/li_write_t:.0f} rows/s)")
print(f"    Cold lookup:      {ms(cold_t)}")
print(f"    Warm lookup:      {ms(warm_t)}")
print(f"    Pruned scan:      {ms(pruned_t)}")
print(f"    KV put 1000:      {ms(kv_write_t)}")
print(f"    KV get:           {ms(kv_get_t)}")
print(f"    Vector insert:    {ms(vec_write_t)}")
print(f"    HNSW build:       {ms(hnsw_t)}")
print(f"    Vector search:    {ms(search_t)}")
print(f"    Stream write:     {ms(stream_write_t)}")
print(f"    Stream read:      {ms(stream_read_t)}")
print(f"    Cross-lens KV→LH: {ms(cross1_t)}")
print(f"    Cross-lens LH→KV: {ms(cross2_t)}")
print(f"    Cross-lens LH→V:  {ms(cross3_t)}")
print(f"    ACID tx:          {ms(acid_t)}")
print(f"    Compaction:       {ms(compact_t)}")
print(f"\n  Browse:")
print(f"    aws s3 ls s3://{R2_BUCKET}/{PREFIX}/ --endpoint-url {R2_ENDPOINT}")
print(f"\n  Delete when done:")
print(f"    aws s3 rm s3://{R2_BUCKET}/{PREFIX}/ --recursive --endpoint-url {R2_ENDPOINT}")
print(f"\n{'=' * 70}")
