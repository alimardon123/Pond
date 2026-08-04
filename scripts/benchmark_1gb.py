"""1 GB benchmark — tests at real data volume.

Writes 10M rows (~1 GB raw, ~300MB compressed) and measures:
1. Write throughput
2. Object count at 1 GB scale
3. Point lookup performance
4. Full scan performance
5. Pruned read performance
6. Delta write (append) at scale
7. Compaction at scale
8. Storage efficiency (data vs metadata ratio)

Run on LocalFS (fast — no network latency):
  python scripts/benchmark_1gb.py
"""
import os, sys, time, tempfile, shutil, json

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))

from local_fs_object_store import LocalFSObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
from pond_storage import PondStorage

def count_objects(tmpdir):
    blobs = 0
    blobs_dir = os.path.join(tmpdir, 'blobs')
    if os.path.isdir(blobs_dir):
        for shard in os.listdir(blobs_dir):
            shard_dir = os.path.join(blobs_dir, shard)
            if os.path.isdir(shard_dir):
                blobs += len(os.listdir(shard_dir))
    paths = 0
    for root, dirs, files in os.walk(os.path.join(tmpdir, 'paths')):
        paths += len(files)
    return blobs, paths, blobs + paths

def total_bytes(tmpdir):
    return sum(os.path.getsize(os.path.join(root, f))
               for root, _, files in os.walk(tmpdir) for f in files)

def ms(t): return f"{t*1000:.0f}ms"
def mb(b): return f"{b/(1024*1024):.1f}MB"

N_ROWS = 10_000_000  # 10M rows
RG_SIZE = 100_000     # 100K rows per group = 100 row groups
ROW_TEMPLATE = {"id": 0, "name": "user_000000", "age": 0, "city": "NYC", "email": "user@example.com", "score": 0.0}

print("=" * 70)
print(f"  1 GB Benchmark (LocalFS)")
print(f"  {N_ROWS:,} rows × ~100 bytes/row = ~1 GB raw data")
print(f"  Row group size: {RG_SIZE:,} rows = {N_ROWS//RG_SIZE} row groups")
print("=" * 70)

tmpdir = tempfile.mkdtemp(prefix="pond_1gb_")
try:
    kernel_store = LocalFSObjectStore(tmpdir)
    kernel = ObjectStoreNativeKernel(kernel_store)
    s = PondStorage(kernel)

    # === 1. Write 10M rows ===
    print(f"\n--- 1. Bulk Write ({N_ROWS:,} rows, {RG_SIZE:,} per group) ---")
    # Generate rows in batches to avoid 10M-element list in memory
    t0 = time.perf_counter()
    batch = []
    for i in range(N_ROWS):
        batch.append({
            "id": i,
            "name": f"user_{i:07d}",
            "age": i % 100,
            "city": ["NYC", "LA", "SF", "CHI", "BOS"][i % 5],
            "email": f"user_{i:07d}@example.com",
            "score": float(i % 1000) / 10.0,
        })
        if len(batch) >= RG_SIZE:
            if i < RG_SIZE:
                # First batch: write
                s.write("big", batch, key_col="id", row_group_size=RG_SIZE, message=f"bulk load {N_ROWS} rows")
            else:
                # Subsequent batches: append
                s.append_shard("big", batch, key_col="id", row_group_size=RG_SIZE)
            batch = []
            if (i + 1) % 1_000_000 == 0:
                elapsed = time.perf_counter() - t0
                print(f"    {i+1:,} rows written ({elapsed:.1f}s, {(i+1)/elapsed:.0f} rows/s)")
    # Write any remaining
    if batch:
        s.append_shard("big", batch, key_col="id", row_group_size=RG_SIZE)

    write_t = time.perf_counter() - t0
    blobs, paths, total = count_objects(tmpdir)
    bytes_ = total_bytes(tmpdir)
    print(f"  Write complete: {ms(write_t)} ({N_ROWS/write_t:.0f} rows/s)")
    print(f"  Objects: {blobs} blobs + {paths} paths = {total} total")
    print(f"  Storage: {mb(bytes_)} ({bytes_} bytes)")
    print(f"  Objects per 1M rows: {total / (N_ROWS / 1_000_000):.1f}")

    # === 2. Compact shards (merge appends into HEAD) ===
    print(f"\n--- 2. Compaction ---")
    t0 = time.perf_counter()
    s.compact_shards("big")
    compact_t = time.perf_counter() - t0
    blobs_after, paths_after, total_after = count_objects(tmpdir)
    print(f"  Compaction: {ms(compact_t)}")
    print(f"  Objects before: {total}")
    print(f"  Objects after:  {total_after}")
    print(f"  Reclaimed:      {total - total_after}")

    # === 3. Point lookup (cold) ===
    print(f"\n--- 3. Point Lookup ---")
    kernel_store.reset_stats()
    kernel.reset_stats()
    kernel._path_cache.clear()
    s._unified._manifest_cache.clear()
    s._unified._head_cache.clear()
    s._unified._manifest_hash_cache.clear()

    t0 = time.perf_counter()
    row = s.point_lookup("big", key="5000000")  # middle of 10M
    cold_t = time.perf_counter() - t0
    gets = kernel_store.stats["gets"]
    print(f"  Cold lookup (id=5000000): {ms(cold_t)}, {gets} GETs")
    print(f"    Found: {row.get('name') if row else 'NOT FOUND'}")

    # Warm lookup
    kernel_store.reset_stats()
    kernel.reset_stats()
    t0 = time.perf_counter()
    row = s.point_lookup("big", key="5000001")
    warm_t = time.perf_counter() - t0
    gets = kernel_store.stats["gets"]
    print(f"  Warm lookup (id=5000001): {ms(warm_t)}, {gets} GETs")

    # === 4. Full scan ===
    print(f"\n--- 4. Full Scan ({N_ROWS:,} rows) ---")
    kernel_store.reset_stats()
    kernel.reset_stats()
    kernel._path_cache.clear()
    s._unified._manifest_cache.clear()
    t0 = time.perf_counter()
    rows = s.read("big")
    scan_t = time.perf_counter() - t0
    gets = kernel_store.stats["gets"]
    bytes_read = kernel_store.stats["bytes_read"]
    print(f"  Full scan: {ms(scan_t)}, {len(rows):,} rows, {gets} GETs, {mb(bytes_read)} read")
    print(f"  Throughput: {len(rows)/scan_t:.0f} rows/s, {bytes_read/scan_t/(1024*1024):.0f} MB/s")

    # === 5. Pruned read (1% selectivity) ===
    print(f"\n--- 5. Pruned Read (1% — id > 9900000) ---")
    kernel_store.reset_stats()
    kernel.reset_stats()
    kernel._path_cache.clear()
    s._unified._manifest_cache.clear()
    t0 = time.perf_counter()
    rows = s.read("big", predicates=[("id", ">", 9_900_000)])
    pruned_t = time.perf_counter() - t0
    gets = kernel_store.stats["gets"]
    bytes_read = kernel_store.stats["bytes_read"]
    print(f"  Pruned read: {ms(pruned_t)}, {len(rows):,} rows, {gets} GETs, {mb(bytes_read)} read")
    print(f"  vs full scan: {scan_t/pruned_t:.1f}x faster, {bytes_read:.0f}B vs full {mb(bytes_read)} read")

    # === 6. Delta write at scale ===
    print(f"\n--- 6. Delta Write (append 1 row to 10M collection) ---")
    kernel_store.reset_stats()
    kernel.reset_stats()
    t0 = time.perf_counter()
    s.append_shard("big", [{"id": N_ROWS, "name": "delta", "age": 99, "city": "SEA", "email": "d@x.com", "score": 99.9}], key_col="id")
    delta_t = time.perf_counter() - t0
    puts = kernel_store.stats["puts"]
    gets = kernel_store.stats["gets"]
    print(f"  Delta write: {ms(delta_t)}, {puts} PUTs, {gets} GETs")

    # === 7. Storage efficiency ===
    print(f"\n--- 7. Storage Efficiency ---")
    blobs, paths, total = count_objects(tmpdir)
    bytes_ = total_bytes(tmpdir)
    # Count PND2 data blobs vs metadata
    pnd2_bytes = 0
    meta_bytes = 0
    blobs_dir = os.path.join(tmpdir, 'blobs')
    for shard in os.listdir(blobs_dir):
        shard_dir = os.path.join(blobs_dir, shard)
        if not os.path.isdir(shard_dir):
            continue
        for f in os.listdir(shard_dir):
            path = os.path.join(shard_dir, f)
            size = os.path.getsize(path)
            with open(path, 'rb') as pf:
                magic = pf.read(4)
            if magic == b'PND2':
                pnd2_bytes += size
            else:
                meta_bytes += size

    path_bytes = sum(os.path.getsize(os.path.join(root, f))
                     for root, _, files in os.walk(os.path.join(tmpdir, 'paths')) for f in files)

    print(f"  PND2 data blobs:  {pnd2_bytes/(1024*1024):.1f} MB ({pnd2_bytes/bytes_*100:.1f}% of total)")
    print(f"  Metadata blobs:   {meta_bytes/(1024*1024):.1f} MB ({meta_bytes/bytes_*100:.1f}%)")
    print(f"  Path files:       {path_bytes/(1024*1024):.1f} MB ({path_bytes/bytes_*100:.1f}%)")
    print(f"  Total storage:    {bytes_/(1024*1024):.1f} MB")
    print(f"  Compression:      {N_ROWS * 100 / bytes_:.1f}x (raw ~{N_ROWS*100/(1024*1024*1024):.1f} GB → stored {bytes_/(1024*1024*1024):.2f} GB)")

    # === Summary ===
    print(f"\n{'=' * 70}")
    print(f"  1 GB Benchmark Summary")
    print(f"{'=' * 70}")
    print(f"  Data: {N_ROWS:,} rows, {RG_SIZE:,} per row group")
    print(f"  Storage: {bytes_/(1024*1024):.0f} MB ({bytes_/(1024*1024*1024):.2f} GB)")
    print(f"  Objects: {total} ({blobs} blobs + {paths} paths)")
    print(f"  Write: {ms(write_t)} ({N_ROWS/write_t:.0f} rows/s)")
    print(f"  Cold lookup: {ms(cold_t)} ({gets} GETs)")
    print(f"  Warm lookup: {ms(warm_t)}")
    print(f"  Full scan: {ms(scan_t)} ({len(rows)/scan_t:.0f} rows/s)")
    print(f"  Pruned 1%: {ms(pruned_t)} ({scan_t/pruned_t:.1f}x faster than full)")
    print(f"  Delta write: {ms(delta_t)} ({puts} PUTs)")
    print(f"  Compaction: {ms(compact_t)} (reclaimed {total - total_after if total > total_after else 0} objects)")

finally:
    shutil.rmtree(tmpdir, ignore_errors=True)
