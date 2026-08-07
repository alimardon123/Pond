"""Comprehensive architecture benchmark — tests all concerns at scale.

Tests:
1. Object amplification at different scales (1K, 10K, 100K, 1M rows)
2. O(1) delta writes (append_shard at various scales)
3. OLTP lens (memtable + batch flush)
4. Streaming lens (segment writes)
5. CRDT concurrency (multiple writers)
6. GC correctness + efficiency
7. Branching/History operations
8. Cross-lens bidirectional access
9. Compaction effectiveness (before/after object count)

Run on LocalFS (fast — no network latency):
  python scripts/benchmark_architecture.py
"""
import os, sys, time, tempfile, shutil, threading, json

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))
sys.path.insert(0, os.path.join(REPO, "lenses", "streaming"))
sys.path.insert(0, os.path.join(REPO, "lenses", "vector"))
sys.path.insert(0, os.path.join(REPO, "lenses", "oltp"))

from local_fs_object_store import LocalFSObjectStore
from object_store_native_kernel import ObjectStoreNativeKernel
from pond_storage import PondStorage

def make_kernel(tmpdir):
    store = LocalFSObjectStore(tmpdir)
    return ObjectStoreNativeKernel(store), store

def count_objects(tmpdir):
    blobs = len(os.listdir(os.path.join(tmpdir, 'blobs')))
    paths = 0
    for root, dirs, files in os.walk(os.path.join(tmpdir, 'paths')):
        paths += len(files)
    return blobs, paths, blobs + paths

def total_bytes(tmpdir):
    return sum(os.path.getsize(os.path.join(root, filename))
               for root, _, files in os.walk(tmpdir) for filename in files)

def ms(t): return f"{t*1000:.1f}ms"


print("=" * 70)
print("  Comprehensive Architecture Benchmark (LocalFS)")
print("=" * 70)

# === 1. Object Amplification at Scale ===
print("\n--- 1. Object Amplification at Different Scales ---")
print(f"  {'Rows':<10} {'RG Size':<8} {'Time':>8} {'Blobs':>6} {'Paths':>6} {'Total':>6} {'Bytes':>10} {'Obj/1K rows':>12}")
print(f"  {'-'*10} {'-'*8} {'-'*8} {'-'*6} {'-'*6} {'-'*6} {'-'*10} {'-'*12}")

for n_rows, rg_size in [(1000, 1000), (10000, 1000), (100000, 10000), (1000000, 100000)]:
    tmpdir = tempfile.mkdtemp(prefix=f"pond_scale_{n_rows}_")
    try:
        kernel, store = make_kernel(tmpdir)
        s = PondStorage(kernel)
        rows = [{"id": i, "name": f"user_{i}", "age": i % 100} for i in range(n_rows)]
        t0 = time.perf_counter()
        s.write("bench", rows, key_col="id", row_group_size=rg_size)
        t = time.perf_counter() - t0
        blobs, paths, total = count_objects(tmpdir)
        bytes_ = total_bytes(tmpdir)
        obj_per_1k = total / (n_rows / 1000)
        print(f"  {n_rows:<10} {rg_size:<8} {ms(t):>8} {blobs:>6} {paths:>6} {total:>6} {bytes_:>10} {obj_per_1k:>12.2f}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

# === 2. O(1) Delta Writes (append_shard) ===
print("\n--- 2. O(1) Delta Writes (append_shard at scale) ---")
tmpdir = tempfile.mkdtemp(prefix="pond_delta_")
try:
    kernel, store = make_kernel(tmpdir)
    s = PondStorage(kernel)
    # Start with 100K rows
    s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(100000)],
            key_col="id", row_group_size=10000, message="initial 100K")

    # Append 1 row at a time (worst case for amplification)
    print("  Appending 1 row at a time (100 appends):")
    times = []
    for i in range(100):
        t0 = time.perf_counter()
        s.append_shard("bench", [{"id": 100000+i, "v": f"new{i}"}], key_col="id")
        times.append(time.perf_counter() - t0)

    blobs, paths, total = count_objects(tmpdir)
    print(f"    First append:  {ms(times[0])}")
    print(f"    Last append:   {ms(times[-1])}")
    print(f"    Median append: {ms(sorted(times)[50])}")
    print(f"    Total objects: {total} (100K base + 100 appends)")
    print(f"    Objects per append: ~{(total - 30) / 100:.1f} new objects per single-row append")

    # Now compact
    print("\n  After compaction:")
    t0 = time.perf_counter()
    s.compact_shards("bench")
    t = time.perf_counter() - t0
    blobs_after, paths_after, total_after = count_objects(tmpdir)
    print(f"    Compaction time: {ms(t)}")
    print(f"    Objects before: {total}")
    print(f"    Objects after:  {total_after}")
    print(f"    Objects reclaimed: {total - total_after}")
    rows = s.read("bench")
    print(f"    Rows: {len(rows)} (expected 100100)")
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

# === 3. OLTP Lens (memtable + batch flush) ===
print("\n--- 3. OLTP Lens (in-memory memtable + batch flush) ---")
tmpdir = tempfile.mkdtemp(prefix="pond_oltp_")
try:
    from oltp_lens import OLTPLens
    kernel, store = make_kernel(tmpdir)
    s = PondStorage(kernel)
    lens = OLTPLens(s, "kv")

    # Write 1000 single rows (should be sub-µs in memtable)
    t0 = time.perf_counter()
    for i in range(1000):
        lens.put(f"key{i}", {"value": f"val{i}", "ts": i})
    t = time.perf_counter() - t0
    print(f"  1000 single-row puts (memtable): {ms(t)}, {1000/t:.0f} ops/s")

    # Flush to storage
    t0 = time.perf_counter()
    lens.flush()
    t = time.perf_counter() - t0
    blobs, paths, total = count_objects(tmpdir)
    print(f"  Flush to storage: {ms(t)}, {total} objects")

    # Read back
    t0 = time.perf_counter()
    v = lens.get("key500")
    t = time.perf_counter() - t0
    print(f"  Point read: {ms(t)}, value={v}")
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

# === 4. Streaming Lens ===
print("\n--- 4. Streaming Lens (segment writes) ---")
tmpdir = tempfile.mkdtemp(prefix="pond_stream_")
try:
    from streaming_lens import StreamingLens
    kernel, store = make_kernel(tmpdir)
    lens = StreamingLens(kernel)

    # Write 10 segments of 10KB each
    data = b"x" * (10 * 1024)  # 10KB
    t0 = time.perf_counter()
    for i in range(10):
        lens.write_stream("stream1", data, segment_size=10240)
    t = time.perf_counter() - t0
    blobs, paths, total = count_objects(tmpdir)
    print(f"  10 × 10KB segments: {ms(t)}, {total} objects")

    # Read back
    t0 = time.perf_counter()
    read_data = lens.read_stream("stream1")
    t = time.perf_counter() - t0
    print(f"  Read 100KB stream: {ms(t)}, {len(read_data)} bytes")
    print(f"  Correctness: {'✓' if read_data == data * 10 else '✗'}")
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

# === 5. CRDT Concurrency (multiple writers) ===
print("\n--- 5. CRDT Concurrency (5 concurrent writers) ---")
tmpdir = tempfile.mkdtemp(prefix="pond_crdt_")
try:
    kernel, store = make_kernel(tmpdir)
    s = PondStorage(kernel)
    s.write("events", [{"id": 0, "v": "init"}], key_col="id")

    N_WRITERS = 5
    ROWS_PER = 200
    errors = []

    def writer(wid):
        try:
            ws = PondStorage(kernel)
            for i in range(ROWS_PER):
                ws.append_shard("events",
                    [{"id": wid*1000+i+1, "v": f"w{wid}_{i}"}], key_col="id")
        except Exception as e:
            errors.append(e)

    t0 = time.perf_counter()
    threads = [threading.Thread(target=writer, args=(w,)) for w in range(N_WRITERS)]
    for t in threads: t.start()
    for t in threads: t.join()
    t = time.perf_counter() - t0

    rows = s.read_with_shards("events")
    total_appends = N_WRITERS * ROWS_PER
    print(f"  {N_WRITERS} writers × {ROWS_PER} appends = {total_appends} total")
    print(f"  Time: {ms(t)} ({total_appends/t:.0f} ops/s)")
    print(f"  Rows merged: {len(rows)} (expected {1 + total_appends})")
    print(f"  Errors: {len(errors)}")
    print(f"  Correctness: {'✓' if len(rows) == 1 + total_appends and not errors else '✗'}")
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

# === 6. GC Correctness + Efficiency ===
print("\n--- 6. GC + Vacuum (correctness + efficiency) ---")
tmpdir = tempfile.mkdtemp(prefix="pond_gc_")
try:
    kernel, store = make_kernel(tmpdir)
    s = PondStorage(kernel)

    # Create data + garbage (via append + compact)
    s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(10000)],
            key_col="id", row_group_size=1000)
    for i in range(5):
        s.append_shard("bench", [{"id": 10000+i*100+j, "v": f"s{i}_{j}"} for j in range(100)],
                        key_col="id", row_group_size=100)
    s.compact_shards("bench")

    blobs_before, paths_before, total_before = count_objects(tmpdir)

    # GC
    t0 = time.perf_counter()
    gc_stats = s.gc()
    gc_t = time.perf_counter() - t0

    # Vacuum
    t0 = time.perf_counter()
    s.vacuum()
    vacuum_t = time.perf_counter() - t0

    blobs_after, paths_after, total_after = count_objects(tmpdir)

    rows = s.read("bench")
    print(f"  Before GC:    {total_before} objects ({blobs_before} blobs + {paths_before} paths)")
    print(f"  GC analysis:  {ms(gc_t)}, found {gc_stats.get('dead', 0)} dead blobs")
    print(f"  Vacuum:       {ms(vacuum_t)}")
    print(f"  After vacuum: {total_after} objects ({blobs_after} blobs + {paths_after} paths)")
    print(f"  Reclaimed:    {total_before - total_after} objects")
    print(f"  Data intact:  {'✓' if len(rows) == 10500 else '✗'} ({len(rows)} rows)")
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

# === 7. Branching + History ===
print("\n--- 7. Branching + History ---")
tmpdir = tempfile.mkdtemp(prefix="pond_branch_")
try:
    kernel, store = make_kernel(tmpdir)
    s = PondStorage(kernel)

    s.write("bench", [{"id": i, "v": 1} for i in range(1000)], key_col="id", row_group_size=100, message="v1")
    v1_manifest = kernel.resolve("collections/bench/_branches/main/manifest")

    s.append("bench", [{"id": 1000+i, "v": 2} for i in range(1000)], key_col="id", row_group_size=100, message="v2")
    v2_manifest = kernel.resolve("collections/bench/_branches/main/manifest")

    s.branch("bench", "dev")
    s.checkout("bench", "dev")
    s.append("bench", [{"id": 2000+i, "v": 3} for i in range(500)], key_col="id", row_group_size=100, message="dev")
    s.merge("bench", "dev", message="merge dev")

    # History
    t0 = time.perf_counter()
    hist = s.history("bench")
    t = time.perf_counter() - t0
    print(f"  History ({len(hist)} commits): {ms(t)}")
    for h in hist[:5]:
        print(f"    {h.get('message', '?')[:40]}")

    # Time travel
    t0 = time.perf_counter()
    rows_v1 = s._unified.read("bench", manifest_hash=v1_manifest)
    t = time.perf_counter() - t0
    print(f"  Time travel to v1: {ms(t)}, {len(rows_v1)} rows (expected 1000)")

    # Current
    rows = s.read("bench")
    print(f"  Current: {len(rows)} rows (expected 2500)")
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

# === 8. Cross-Lens Bidirectional Access ===
print("\n--- 8. Cross-Lens Bidirectional Access ---")
tmpdir = tempfile.mkdtemp(prefix="pond_cross_")
try:
    from keyvalue_lens import KeyValueLens
    kernel, store = make_kernel(tmpdir)

    # Write via PondStorage (tabular)
    s = PondStorage(kernel)
    s.write("shared", [{"id": 1, "name": "alice", "age": 30},
                        {"id": 2, "name": "bob", "age": 25}],
            key_col="id", message="tabular write")

    # Read via KeyValueLens
    kv = KeyValueLens(kernel, "shared")
    t0 = time.perf_counter()
    v = kv.get("1")  # uses id as key
    t = time.perf_counter() - t0
    print(f"  KV lens reads tabular collection: {ms(t)}")
    print(f"    get('1') = {v}")

    # Write via KV lens
    kv.put("3", {"name": "carol", "age": 35})
    kv.commit("KV append to tabular collection")

    # Read back via PondStorage
    t0 = time.perf_counter()
    rows = s.read("shared")
    t = time.perf_counter() - t0
    print(f"  Tabular lens reads KV append: {ms(t)}, {len(rows)} rows")
    print(f"  Cross-lens bidirectional: {'✓' if len(rows) >= 3 else '✗'}")
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

# === 9. Compaction Effectiveness ===
print("\n--- 9. Compaction Effectiveness (object count reduction) ---")
tmpdir = tempfile.mkdtemp(prefix="pond_compact_")
try:
    kernel, store = make_kernel(tmpdir)
    s = PondStorage(kernel)

    # Write base
    s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(10000)],
            key_col="id", row_group_size=1000)

    # Append 20 shards (simulates 20 concurrent writers or 20 batches)
    for i in range(20):
        s.append_shard("bench", [{"id": 10000+i*10+j, "v": f"s{i}_{j}"} for j in range(10)],
                        key_col="id", row_group_size=10)

    blobs_before, paths_before, total_before = count_objects(tmpdir)
    rows_before = len(s.read_with_shards("bench"))

    # Compact
    t0 = time.perf_counter()
    s.compact_shards("bench")
    t = time.perf_counter() - t0

    blobs_after, paths_after, total_after = count_objects(tmpdir)
    rows_after = len(s.read("bench"))

    print(f"  Before compaction: {total_before} objects, {rows_before} rows")
    print(f"  After compaction:  {total_after} objects, {rows_after} rows")
    print(f"  Compaction time:   {ms(t)}")
    print(f"  Objects reclaimed: {total_before - total_after}")
    print(f"  Reduction:         {(1 - total_after/total_before)*100:.1f}%")
    print(f"  Data intact:       {'✓' if rows_before == rows_after else '✗'}")
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

print(f"\n{'=' * 70}")
print("  Architecture benchmark complete.")
print(f"{'=' * 70}")
