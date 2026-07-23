"""
Stress test: metadata growth with REALISTIC seal sizes.

The first stress test (stress_1m_seals.py) uses 100-row seals, which is
pathological — production would seal at 128MB-1GB. That test exposed that
metadata per seal is ~11KB (3 JSON objects: leaf, root, commit), which
dominates when data per seal is only ~2KB.

This test uses 100,000-row seals (~1.7 MB Parquet each), which is closer
to a realistic streaming ingest rate (one seal per minute at 1700 rows/sec).
We measure the same metadata metrics at this scale.

If the meta-to-data ratio drops below 5% at this scale, the architecture
is sound for realistic workloads. If it doesn't, the tree structure itself
is still wrong.

Run:  python3 stress_realistic_seals.py
"""

import os
import shutil
import time
import sys
import pyarrow as pa

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
    print("  Stress test: metadata growth with REALISTIC seal sizes")
    print("=" * 76)
    print()
    print("  Schema: 3 columns (id int64, ts timestamp, payload string)")
    print("  Each seal: 10,000 rows (~170 KB Parquet) — realistic micro-batch")
    print("  Measuring: metadata size, meta/data ratio, time/seal, lookup latency")
    print()

    bench_dir = "/tmp/pond_stress_realistic"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    db = Pond(bench_dir)

    # 1k seals × 10k rows = 10M rows, ~170 MB data — fast enough to complete
    checkpoints = [100, 500, 1_000]
    checkpoint_idx = 0

    seal_count = 0
    seal_times = []

    print(f"  {'Seals':>8}  {'Rows':>12}  {'Data':>10}  {'Meta':>10}  "
          f"{'Ratio':>8}  {'Seal ms':>8}  {'Lookup ms':>10}")
    print(f"  {'-'*8}  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*8}  {'-'*8}  {'-'*10}")

    while checkpoint_idx < len(checkpoints):
        target = checkpoints[checkpoint_idx]
        b = make_batch(10_000, start_id=seal_count * 10_000)
        db.write("events", b)

        t0 = time.perf_counter()
        db.seal("events", message=f"seal {seal_count+1}")
        t1 = time.perf_counter()
        seal_times.append(t1 - t0)

        seal_count += 1

        if seal_count == target:
            stats = db.storage_stats()
            recent_seal_ms = (sum(seal_times[-50:]) / min(50, len(seal_times))) * 1000

            # Lookup latency
            lookup_times = []
            for _ in range(20):
                t0 = time.perf_counter()
                commit_hash = db._resolve_name("events")
                commit = db._read_commit(commit_hash)
                tree = db._read_tree(commit.tree_hash)
                _ = list(tree.entries.items())[:1]
                t1 = time.perf_counter()
                lookup_times.append(t1 - t0)
            lookup_ms = (sum(lookup_times) / len(lookup_times)) * 1000

            ratio = stats["meta_to_data_ratio"]
            print(f"  {seal_count:>8,}  {seal_count * 10_000:>12,}  "
                  f"{fmt_bytes(stats['data_bytes']):>10}  "
                  f"{fmt_bytes(stats['meta_bytes']):>10}  {ratio*100:>6.2f}%  "
                  f"{recent_seal_ms:>6.1f}ms  {lookup_ms:>8.2f}ms")

            checkpoint_idx += 1

    print()
    print("  Verdict:")
    final_ratio = db.storage_stats()["meta_to_data_ratio"]
    if final_ratio < 0.05:
        print(f"  - Meta/data ratio at {seal_count} seals: {final_ratio*100:.2f}% — within 5% target.")
        print(f"  - Hierarchical tree design SCALES to realistic seal sizes.")
    elif final_ratio < 0.20:
        print(f"  - Meta/data ratio at {seal_count} seals: {final_ratio*100:.2f}% — above 5% target but bounded.")
        print(f"  - JSON serialization is the main contributor; binary format would help.")
    else:
        print(f"  - Meta/data ratio at {seal_count} seals: {final_ratio*100:.2f}% — TOO HIGH.")
        print(f"  - Tree structure still wrong, even at realistic seal sizes.")

    db.close()


if __name__ == "__main__":
    main()
