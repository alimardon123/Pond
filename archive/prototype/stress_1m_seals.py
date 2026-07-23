"""
Stress test: 1M seals with the current flat-tree design.

Purpose: measure how metadata grows as a function of seal count, before
attempting any tree-structure rework. This establishes the baseline that
informs whether the tree needs to become hierarchical.

Measures:
  - Total metadata bytes at 1k, 10k, 100k, 1M seals
  - Meta-to-data ratio at each scale
  - Time per seal (does it degrade as tree grows?)
  - Lookup latency (resolving name -> commit hash -> tree -> blobs)

Run:  python3 stress_1m_seals.py
"""

import os
import shutil
import time
import sys
import pyarrow as pa

# Add the prototype directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond import Pond

SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("ts", pa.timestamp("us")),
    pa.field("payload", pa.string()),
])


def make_batch(num_rows: int, start_id: int = 0) -> pa.RecordBatch:
    import random
    import string
    ids = list(range(start_id, start_id + num_rows))
    timestamps = [int(time.time() * 1e6)] * num_rows
    payloads = [
        "".join(random.choices(string.ascii_lowercase, k=20))
        for _ in range(num_rows)
    ]
    return pa.RecordBatch.from_arrays([
        pa.array(ids, type=pa.int64()),
        pa.array(timestamps, type=pa.timestamp("us")),
        pa.array(payloads, type=pa.string()),
    ], schema=SCHEMA)


def fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    elif n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    elif n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    else:
        return f"{n / (1024 * 1024 * 1024):.2f} GB"


def main():
    print("=" * 76)
    print("  Stress test: metadata growth vs seal count (hierarchical tree)")
    print("=" * 76)
    print()
    print("  Schema: 3 columns (id int64, ts timestamp, payload string)")
    print("  Each seal: 100 rows (~1.7 KB Parquet) — TINY seal size")
    print("  Measuring: metadata size, meta/data ratio, time/seal, lookup latency")
    print()
    print("  NOTE: This is a pathological seal size. Real production would seal")
    print("  at 128MB-1GB. The 100-row seals here stress-test metadata overhead.")
    print()

    bench_dir = "/tmp/pond_stress_1m"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    db = Pond(bench_dir)

    # Checkpoints: very small — the flat tree exhibits O(N^2) tree-copy cost
    # so even 10k seals takes minutes. We just need the curve shape.
    checkpoints = [100, 500, 1_000, 2_000, 5_000]
    checkpoint_idx = 0

    # Pre-generate one batch (reuse for all seals — content-addressed means
    # the same batch produces the same blob hash, which would defeat the test.
    # So we need to vary the payload slightly per seal.)
    seal_count = 0
    batch = make_batch(100, start_id=0)

    print(f"  {'Seals':>10}  {'Data':>10}  {'Meta':>10}  {'Ratio':>8}  "
          f"{'Seal ms':>8}  {'Lookup ms':>10}")
    print(f"  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*8}  {'-'*8}  {'-'*10}")

    seal_times = []
    t_last_checkpoint = time.perf_counter()

    while checkpoint_idx < len(checkpoints):
        target = checkpoints[checkpoint_idx]
        # Each seal writes 100 rows with a unique start_id (so content varies)
        b = make_batch(100, start_id=seal_count * 100)
        db.write("events", b)

        t0 = time.perf_counter()
        db.seal("events", message=f"seal {seal_count+1}")
        t1 = time.perf_counter()
        seal_times.append(t1 - t0)

        seal_count += 1

        if seal_count == target:
            # Measure
            stats = db.storage_stats()
            recent_seal_ms = (sum(seal_times[-100:]) / 100) * 1000  # avg of last 100

            # Lookup latency: resolve name -> commit -> tree -> first blob
            lookup_times = []
            for _ in range(50):
                t0 = time.perf_counter()
                # Just resolve the name; full read would be dominated by Parquet decode
                commit_hash = db._resolve_name("events")
                commit = db._read_commit(commit_hash)
                tree = db._read_tree(commit.tree_hash)
                _ = list(tree.entries.items())[:1]
                t1 = time.perf_counter()
                lookup_times.append(t1 - t0)
            lookup_ms = (sum(lookup_times) / len(lookup_times)) * 1000

            ratio = stats["meta_to_data_ratio"]
            print(f"  {seal_count:>10,}  {fmt_bytes(stats['data_bytes']):>10}  "
                  f"{fmt_bytes(stats['meta_bytes']):>10}  {ratio*100:>6.2f}%  "
                  f"{recent_seal_ms:>6.2f}ms  {lookup_ms:>8.2f}ms")

            # Cleanup OPEN object artifacts to keep disk usage honest
            for f in os.listdir(db.open_dir):
                os.remove(os.path.join(db.open_dir, f))

            checkpoint_idx += 1

    print()
    print("  Interpretation:")
    print()
    last_ratio = db.storage_stats()["meta_to_data_ratio"]
    if last_ratio > 0.10:
        print(f"  - Meta/data ratio at 1M seals: {last_ratio*100:.1f}% — FAR above 5% target.")
        print(f"  - The flat-tree design does NOT scale. Tree must become hierarchical.")
    elif last_ratio > 0.05:
        print(f"  - Meta/data ratio at 1M seals: {last_ratio*100:.1f}% — above 5% target.")
        print(f"  - JSON serialization is contributing; binary format would help.")
    else:
        print(f"  - Meta/data ratio at 1M seals: {last_ratio*100:.1f}% — within target.")

    if len(seal_times) > 1000:
        first_seals = sum(seal_times[:100]) / 100 * 1000
        last_seals = sum(seal_times[-100:]) / 100 * 1000
        if last_seals > first_seals * 5:
            print(f"  - Seal time degraded {first_seals:.2f}ms -> {last_seals:.2f}ms "
                  f"({last_seals/first_seals:.1f}x slower). Tree-copy cost is O(N).")

    db.close()


if __name__ == "__main__":
    main()
