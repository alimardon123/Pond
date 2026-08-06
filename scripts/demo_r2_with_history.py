"""Demo with history — creates a realistic dataset on R2 and LEAVES it.

Creates:
  - 3 collections (users, orders, events)
  - Multiple commits per collection (history for time travel)
  - Branch + merge on users
  - Atomic publication across collections

Data is LEFT on R2 under prefix 'pond-demo' for inspection.

To browse via AWS CLI (set env vars first):
  export R2_ENDPOINT=... R2_ACCESS_KEY=... R2_SECRET_KEY=... R2_BUCKET=...
  aws s3 ls s3://$R2_BUCKET/pond-demo/ --endpoint-url $R2_ENDPOINT

To query from local:
  R2_ENDPOINT=... R2_ACCESS_KEY=... R2_SECRET_KEY=... R2_BUCKET=... \
      python scripts/query_r2_demo.py
"""
import os, sys, time, json

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, HERE)  # for _r2_config

from s3_object_store import S3ObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
from pond_storage import PondStorage
from _r2_config import get_r2_client, get_r2_bucket

R2_BUCKET = get_r2_bucket()
PREFIX = "pond-demo"  # Fixed prefix — data stays for inspection
client = get_r2_client()

store = S3ObjectStore(client, bucket=R2_BUCKET, prefix=PREFIX)
kernel = ObjectStoreNativeKernel(store)
s = PondStorage(kernel)

print("=" * 70)
print(f"  Pond R2 Demo — creating dataset with history")
print(f"  R2 bucket: {R2_BUCKET}, prefix: {PREFIX}")
print(f"  Data will be LEFT on R2 for inspection (NOT cleaned up)")
print("=" * 70)

# Save commit hashes for time travel
history = {}

# === Collection 1: users (with 3 commits + branch + merge) ===
print("\n--- Creating 'users' collection with 3 commits ---")

# Commit 1: initial 5000 users
print("  Commit 1: writing 5000 users...")
t0 = time.perf_counter()
s.write("users", [{"id": i, "name": f"user_{i}", "age": i % 100, "city": ["NYC", "LA", "SF"][i % 3]} for i in range(5000)],
        key_col="id", row_group_size=1000, message="initial 5000 users")
history["users_v1"] = kernel.resolve("collections/users/_branches/main/manifest")
print(f"    Done in {time.perf_counter()-t0:.1f}s, manifest: {history['users_v1'][:16]}...")

# Commit 2: append 3000 more users
print("  Commit 2: appending 3000 users...")
t0 = time.perf_counter()
s.append("users", [{"id": 5000+i, "name": f"new_user_{i}", "age": i % 50, "city": ["NYC", "LA", "SF"][i % 3]} for i in range(3000)],
         key_col="id", row_group_size=1000, message="append 3000 users")
history["users_v2"] = kernel.resolve("collections/users/_branches/main/manifest")
print(f"    Done in {time.perf_counter()-t0:.1f}s, manifest: {history['users_v2'][:16]}...")

# Commit 3: append 2000 more users
print("  Commit 3: appending 2000 users...")
t0 = time.perf_counter()
s.append("users", [{"id": 8000+i, "name": f"late_user_{i}", "age": i % 30, "city": "CHI"} for i in range(2000)],
         key_col="id", row_group_size=1000, message="append 2000 users")
history["users_v3"] = kernel.resolve("collections/users/_branches/main/manifest")
print(f"    Done in {time.perf_counter()-t0:.1f}s, manifest: {history['users_v3'][:16]}...")

# Branch + merge
print("  Branching 'dev' and adding 500 users...")
s.branch("users", "dev")
s.checkout("users", "dev")
s.append("users", [{"id": 10000+i, "name": f"dev_user_{i}", "age": 25, "city": "BOS"} for i in range(500)],
         key_col="id", row_group_size=1000, message="dev branch: 500 users")
print("  Merging dev back to main...")
s.merge("users", "dev", message="merge dev branch")

# Verify current state
rows = s.read("users")
print(f"  Current: {len(rows)} users (expected 10500)")

# Time travel to v1
print("  Time travel to v1 (5000 users)...")
rows_v1 = s._unified.read("users", manifest_hash=history["users_v1"])
print(f"  v1: {len(rows_v1)} users (expected 5000)")

# === Collection 2: orders (2 commits) ===
print("\n--- Creating 'orders' collection with 2 commits ---")
print("  Commit 1: writing 8000 orders...")
t0 = time.perf_counter()
s.write("orders", [{"id": i, "user_id": i % 10500, "amount": float(i * 9.99), "status": "shipped" if i % 3 == 0 else "pending"} for i in range(8000)],
        key_col="id", row_group_size=1000, message="initial 8000 orders")
history["orders_v1"] = kernel.resolve("collections/orders/_branches/main/manifest")
print(f"    Done in {time.perf_counter()-t0:.1f}s")

print("  Commit 2: appending 2000 orders...")
t0 = time.perf_counter()
s.append("orders", [{"id": 8000+i, "user_id": i % 10500, "amount": float(i * 19.99), "status": "pending"} for i in range(2000)],
         key_col="id", row_group_size=1000, message="append 2000 orders")
print(f"    Done in {time.perf_counter()-t0:.1f}s")

# === Collection 3: events (streaming-like, 2 commits) ===
print("\n--- Creating 'events' collection with 2 commits ---")
print("  Commit 1: writing 5000 events...")
t0 = time.perf_counter()
s.write("events", [{"id": i, "event_type": ["click", "view", "purchase"][i % 3], "user_id": i % 10500, "ts": 1700000000 + i} for i in range(5000)],
        key_col="id", row_group_size=1000, message="initial 5000 events")
print(f"    Done in {time.perf_counter()-t0:.1f}s")

print("  Commit 2: appending 5000 events...")
t0 = time.perf_counter()
s.append("events", [{"id": 5000+i, "event_type": ["click", "view", "purchase"][i % 3], "user_id": i % 10500, "ts": 1700005000 + i} for i in range(5000)],
         key_col="id", row_group_size=1000, message="append 5000 events")
print(f"    Done in {time.perf_counter()-t0:.1f}s")

# === Atomic Publication (cross-collection) ===
print("\n--- Atomic Publication (2 collections) ---")
t0 = time.perf_counter()
tx = s.begin_tx()
s.append_shard("users", [{"id": 99999, "name": "tx_user", "age": 30, "city": "SEA"}], key_col="id", tx_id=tx)
s.append_shard("orders", [{"id": 99999, "user_id": 99999, "amount": 99.99, "status": "pending"}], key_col="id", tx_id=tx)
s.commit_tx(tx, message="atomic user + order creation")
print(f"  Done in {time.perf_counter()-t0:.1f}s")

# === Summary ===
print("\n" + "=" * 70)
print("  DATASET CREATED — LEFT ON R2 FOR INSPECTION")
print("=" * 70)

# Count blobs on R2
blob_count = 0
total_bytes = 0
paginator = client.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=f"{PREFIX}/"):
    for obj in page.get("Contents", []):
        blob_count += 1
        total_bytes += obj["Size"]

print(f"\n  R2 bucket: {R2_BUCKET}")
print(f"  Prefix:    {PREFIX}/")
print(f"  Objects:   {blob_count}")
print(f"  Size:      {total_bytes / (1024*1024):.1f} MB")

print(f"\n  Collections:")
for name in ["users", "orders", "events"]:
    rows = s.read(name)
    branches = s.list_branches(name)
    hist = s.history(name)
    print(f"    {name}: {len(rows)} rows, {len(branches)} branches ({branches}), {len(hist)} commits")

print(f"\n  History (users):")
for h in s.history("users")[:5]:
    print(f"    {h.get('message', '?')[:50]:<50} → {str(h.get('manifest', '?'))[:16]}...")

print(f"\n  Time travel manifests (saved for querying):")
for label, h in history.items():
    print(f"    {label}: {h[:16]}...")

# Save history for the query script
history_file = os.path.join(HERE, "r2_demo_history.json")
with open(history_file, "w") as f:
    json.dump({
        "prefix": PREFIX,
        "bucket": R2_BUCKET,
        "endpoint": R2_ENDPOINT,
        "history": history,
        "collections": ["users", "orders", "events"],
    }, f, indent=2)
print(f"\n  History saved to: {history_file}")

print(f"\n  To browse via AWS CLI:")
print(f"    aws s3 ls s3://{R2_BUCKET}/{PREFIX}/ \\")
print(f"      --endpoint-url {R2_ENDPOINT}")

print(f"\n  To query from local:")
print(f"    python scripts/query_r2_demo.py")

print(f"\n  To DELETE everything when done:")
print(f"    aws s3 rm s3://{R2_BUCKET}/{PREFIX}/ --recursive \\")
print(f"      --endpoint-url {R2_ENDPOINT}")

print(f"\n{'=' * 70}")
