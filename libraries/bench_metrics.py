"""
Performance, scale, and metadata metrics for ProllyViewBase.

Measures:
  1. Write throughput (rows/sec, MB/sec)
  2. Point lookup latency (p50, p99)
  3. Full scan latency
  4. Commit latency (delta vs compaction)
  5. Metadata ratio (metadata bytes / data bytes)
  6. Scale: 100, 1K, 10K, 100K entries
"""

import sys, os, shutil, json, time, statistics

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "prototype"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "libraries"))
from pond_minimal import PondMinimal
from prolly_view import ProllyViewBase, ProllyTree


def fmt_bytes(n):
    if n < 1024: return f"{n} B"
    if n < 1024**2: return f"{n/1024:.1f} KB"
    if n < 1024**3: return f"{n/1024**2:.1f} MB"
    return f"{n/1024**3:.2f} GB"


def fmt_us(s):
    if s < 1e-3: return f"{s*1e6:.1f} us"
    if s < 1: return f"{s*1e3:.2f} ms"
    return f"{s:.2f} s"


def bench_scale(n_entries):
    """Benchmark at a given scale."""
    bench_dir = f"/tmp/pond_bench_{n_entries}"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)
    base = ProllyViewBase(kernel, "bench")

    # Write N entries
    data_size = 0
    t0 = time.perf_counter()
    for i in range(n_entries):
        val = f'value-{i:010d}-padding-to-100-bytes-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'.encode()
        h = kernel.write(val)
        base.stage(f'key-{i:010d}', h)
        data_size += len(val)
    commit_h = base.commit(f'insert {n_entries}')
    t1 = time.perf_counter()
    write_time = t1 - t0

    # Point lookups
    lookup_times = []
    for i in range(min(100, n_entries)):
        idx = (i * 37) % n_entries  # pseudo-random
        t0 = time.perf_counter()
        h = base.lookup(f'key-{idx:010d}')
        t1 = time.perf_counter()
        lookup_times.append(t1 - t0)

    # Full scan
    t0 = time.perf_counter()
    state = base.read_all()
    t1 = time.perf_counter()
    scan_time = t1 - t0

    # Metadata ratio
    stats = kernel.storage_stats()
    data_bytes = stats['data_bytes']  # all .bin files (data + metadata blobs)
    meta_bytes = 0
    # Count JSON metadata blobs (trees, commits, snapshots)
    for shard in os.listdir(kernel.objects_dir):
        shard_path = os.path.join(kernel.objects_dir, shard)
        if not os.path.isdir(shard_path): continue
        for f in os.listdir(shard_path):
            fpath = os.path.join(shard_path, f)
            size = os.path.getsize(fpath)
            if f.endswith('.json'):
                meta_bytes += size

    # The .bin files include both data blobs AND Prolly tree nodes
    # Prolly tree nodes are metadata (they store key→hash mappings)
    # Data blobs are the actual values
    # We need to separate them
    # Tree nodes have "type":"leaf" or "type":"internal" in their content
    # Commit blobs have "type":"commit"
    # Snapshot blobs have "type":"snapshot"
    # Data blobs are raw bytes (not JSON)

    actual_data_bytes = 0
    actual_meta_bytes = 0
    for shard in os.listdir(kernel.objects_dir):
        shard_path = os.path.join(kernel.objects_dir, shard)
        if not os.path.isdir(shard_path): continue
        for f in os.listdir(shard_path):
            fpath = os.path.join(shard_path, f)
            size = os.path.getsize(fpath)
            if f.endswith('.json'):
                actual_meta_bytes += size
            elif f.endswith('.bin'):
                # With binary encoding, tree nodes and commits are .bin files
                # Try to detect: tree nodes start with type byte (1=leaf, 2=internal, 3=commit)
                # Data blobs are raw bytes that don't start with these type bytes
                try:
                    with open(fpath, 'rb') as fh:
                        first_byte = fh.read(1)
                        if first_byte and first_byte[0] in (1, 2, 3):
                            actual_meta_bytes += size  # tree node or commit
                        else:
                            actual_data_bytes += size  # data blob
                except:
                    actual_data_bytes += size

    meta_ratio = actual_meta_bytes / actual_data_bytes if actual_data_bytes > 0 else 0

    # History
    history = base.history(limit=5)

    kernel.close()

    return {
        'n': n_entries,
        'write_time': write_time,
        'write_throughput': n_entries / write_time,
        'write_mb_s': (data_size / 1024**2) / write_time,
        'lookup_p50': statistics.median(lookup_times),
        'lookup_p99': sorted(lookup_times)[int(len(lookup_times) * 0.99)],
        'scan_time': scan_time,
        'scan_throughput': n_entries / scan_time,
        'data_bytes': actual_data_bytes,
        'meta_bytes': actual_meta_bytes,
        'meta_ratio': meta_ratio,
        'total_blobs': stats['blob_count'],
        'history_types': [h['type'] for h in history],
    }


def main():
    print("=" * 76)
    print("  Performance, Scale, and Metadata Metrics for ProllyViewBase")
    print("=" * 76)

    scales = [100, 1000, 10000]

    print(f"\n  {'Scale':<10} {'Write r/s':<12} {'Write MB/s':<12} {'Lookup p50':<12} {'Lookup p99':<12} {'Scan time':<12} {'Meta ratio':<12} {'Blobs':<8}")
    print(f"  {'-'*10} {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*8}")

    for n in scales:
        r = bench_scale(n)
        print(f"  {n:<10} {r['write_throughput']:>10,.0f} {r['write_mb_s']:>10.2f} "
              f"{fmt_us(r['lookup_p50']):>12} {fmt_us(r['lookup_p99']):>12} "
              f"{fmt_us(r['scan_time']):>12} {r['meta_ratio']*100:>10.2f}% {r['total_blobs']:>8}")

    # Detailed breakdown at 10K
    print("\n  Detailed breakdown at 10,000 entries:")
    r = bench_scale(10000)
    print(f"    Data bytes:     {fmt_bytes(r['data_bytes'])}")
    print(f"    Metadata bytes: {fmt_bytes(r['meta_bytes'])}")
    print(f"    Meta/data ratio: {r['meta_ratio']*100:.2f}%")
    print(f"    Total blobs:    {r['total_blobs']}")
    print(f"    Write throughput: {r['write_throughput']:,.0f} rows/sec ({r['write_mb_s']:.2f} MB/s)")
    print(f"    Lookup p50: {fmt_us(r['lookup_p50'])}, p99: {fmt_us(r['lookup_p99'])}")
    print(f"    Full scan: {fmt_us(r['scan_time'])} ({r['scan_throughput']:,.0f} rows/sec)")
    print(f"    History types: {r['history_types']}")

    # Cleanup
    for n in scales:
        shutil.rmtree(f"/tmp/pond_bench_{n}", ignore_errors=True)


if __name__ == "__main__":
    main()
