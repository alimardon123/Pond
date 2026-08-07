"""Query the R2 demo data from local machine.

Reads the dataset created by demo_r2_with_history.py directly from R2.
No local data — everything is fetched from Cloudflare R2.

Usage:
  R2_ENDPOINT=... R2_ACCESS_KEY=... R2_SECRET_KEY=... R2_BUCKET=... \
      python scripts/query_r2_demo.py
"""
import os, sys, time, json

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, HERE)  # for _r2_config

from s3_object_store import S3ObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
from pond_storage import PondStorage
from _r2_config import get_r2_access_key, get_r2_secret_key

# Load saved history (endpoint/bucket/prefix come from the history file,
# but credentials MUST come from env vars — never hardcoded)
history_file = os.path.join(HERE, "r2_demo_history.json")
with open(history_file) as f:
    demo = json.load(f)

R2_ENDPOINT = demo["endpoint"]
R2_BUCKET = demo["bucket"]
PREFIX = demo["prefix"]
SAVED_HISTORY = demo["history"]

# Credentials always come from env vars (the history file is shareable;
# credentials are not). Endpoint/bucket/prefix come from the history file
# so the query script reads from the same place the demo wrote to.
import boto3
from botocore.config import Config
config = Config(
    connect_timeout=5.0, read_timeout=60.0, max_pool_connections=50,
    retries={"max_attempts": 5, "mode": "adaptive"},
)
client = boto3.client("s3", endpoint_url=R2_ENDPOINT,
    aws_access_key_id=get_r2_access_key(),
    aws_secret_access_key=get_r2_secret_key(),
    region_name="auto", config=config)

store = S3ObjectStore(client, bucket=R2_BUCKET, prefix=PREFIX)
kernel = ObjectStoreNativeKernel(store)
s = PondStorage(kernel)

def ms(t): return f"{t*1000:.0f}ms"

print("=" * 70)
print(f"  Querying R2 Demo Data")
print(f"  Bucket: {R2_BUCKET}, Prefix: {PREFIX}")
print("=" * 70)

# 1. List collections
print("\n--- Collections ---")
collections = s.list_collections()
for name in collections:
    rows = s.read(name)
    branches = s.list_branches(name)
    hist = s.history(name)
    print(f"  {name}: {len(rows)} rows, branches={branches}, commits={len(hist)}")

# 2. Point lookups on users
print("\n--- Point Lookups (users) ---")
for uid in [0, 5000, 9999, 10000]:
    t0 = time.perf_counter()
    row = s.point_lookup("users", key=str(uid))
    t = time.perf_counter() - t0
    if row:
        print(f"  id={uid}: name={row.get('name')}, city={row.get('city')} ({ms(t)})")
    else:
        print(f"  id={uid}: NOT FOUND ({ms(t)})")

# 3. Range scan on orders
print("\n--- Range Scan (orders, id 100-104) ---")
t0 = time.perf_counter()
rows = s.read("orders", predicates=[("id", ">=", 100), ("id", "<=", 104)])
t = time.perf_counter() - t0
for r in rows:
    print(f"  order {r['id']}: user={r['user_id']}, amount=${r['amount']:.2f}, status={r['status']}")
print(f"  ({ms(t)}, {len(rows)} rows)")

# 4. Predicate pruning on events
print("\n--- Predicate Pruning (events, type=purchase) ---")
t0 = time.perf_counter()
rows = s.read("events", predicates=[("event_type", "=", "purchase")])
t = time.perf_counter() - t0
print(f"  Found {len(rows)} purchase events ({ms(t)})")
if rows:
    print(f"  Example: {rows[0]}")

# 5. Time travel on users
print("\n--- Time Travel (users) ---")
print(f"  Current state:")
rows = s.read("users")
print(f"    {len(rows)} users")

if "users_v1" in SAVED_HISTORY:
    v1_manifest = SAVED_HISTORY["users_v1"]
    print(f"  Time travel to v1 (manifest: {v1_manifest[:16]}...):")
    t0 = time.perf_counter()
    rows_v1 = s._unified.read("users", manifest_hash=v1_manifest)
    t = time.perf_counter() - t0
    print(f"    {len(rows_v1)} users ({ms(t)})")

# 6. Branches
print("\n--- Branches ---")
for name in collections:
    branches = s.list_branches(name)
    if branches:
        for b in branches:
            print(f"  {name}/{b}")
            if b != "main":
                rows = s._unified.read_branch_with_shards(name, b)
                print(f"    {len(rows)} rows on branch '{b}'")

# 7. History
print("\n--- Commit History ---")
for name in collections:
    hist = s.history(name)
    if hist:
        print(f"  {name} ({len(hist)} commits):")
        for h in hist[:5]:
            msg = h.get("message", "?")[:50]
            ts = h.get("timestamp", 0)
            print(f"    {msg:<50}")

# 8. Storage stats
print("\n--- R2 Storage Stats ---")
blob_count = 0
total_bytes = 0
paginator = client.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=f"{PREFIX}/"):
    for obj in page.get("Contents", []):
        blob_count += 1
        total_bytes += obj["Size"]
print(f"  Total objects: {blob_count}")
print(f"  Total size:    {total_bytes / (1024*1024):.2f} MB")

# Show directory structure (first 20 objects)
print(f"\n  First 20 objects on R2:")
count = 0
for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=f"{PREFIX}/"):
    for obj in page.get("Contents", []):
        key = obj["Key"][len(PREFIX)+1:]  # strip prefix
        size = obj["Size"]
        print(f"    {key:<60} {size:>6}B")
        count += 1
        if count >= 20:
            break
    if count >= 20:
        break

print(f"\n{'=' * 70}")
print("  Query complete. Data is still on R2 — inspect anytime.")
print(f"{'=' * 70}")
