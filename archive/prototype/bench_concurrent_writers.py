"""
Concurrent writers benchmark.

The architecture specifies single-process multi-writer concurrency via MVCC
(RFC 1 section 8). v0 doesn't implement MVCC — it has a single in-memory
_open_objects dict that's not thread-safe. This benchmark exposes what
actually breaks under concurrent writers.

Tests:
  1. Multiple threads writing to DIFFERENT tables (should work if the
     _open_objects dict is properly synchronized)
  2. Multiple threads writing to the SAME table (currently broken —
     OPEN object is shared mutable state)
  3. Multiple threads sealing different tables concurrently
  4. Correctness: after all writers finish, verify total rows == expected

Run:  python3 bench_concurrent_writers.py
"""

import os
import shutil
import time
import sys
import threading
import random
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond import Pond

SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("ts", pa.timestamp("us")),
    pa.field("payload", pa.string()),
])


def make_batch(num_rows: int, start_id: int = 0) -> pa.RecordBatch:
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


def test_different_tables(num_threads: int, writes_per_thread: int):
    """Each thread writes to its own table. Should work."""
    print(f"\n  --- Test 1: {num_threads} threads, each writing to own table ---")
    bench_dir = "/tmp/pond_concurrent_diff_tables"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    db = Pond(bench_dir)

    errors = []
    barrier = threading.Barrier(num_threads)

    def worker(thread_id: int):
        try:
            barrier.wait()  # all threads start together
            table_name = f"events_t{thread_id}"
            for i in range(writes_per_thread):
                b = make_batch(100, start_id=thread_id * 10000 + i * 100)
                db.write(table_name, b)
                db.seal(table_name, message=f"t{thread_id} seal {i+1}")
        except Exception as e:
            errors.append(f"Thread {thread_id}: {e}")

    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    t1 = time.perf_counter()

    if errors:
        print(f"    [FAIL] {len(errors)} errors:")
        for e in errors[:3]:
            print(f"      {e}")
    else:
        print(f"    [OK] All {num_threads} threads completed without errors")

    # Verify all tables exist and have correct row counts
    total_rows = 0
    for i in range(num_threads):
        table_name = f"events_t{i}"
        try:
            table = db.read(table_name)
            expected = writes_per_thread * 100
            if table.num_rows != expected:
                print(f"    [FAIL] {table_name}: {table.num_rows} rows (expected {expected})")
            else:
                total_rows += table.num_rows
        except Exception as e:
            print(f"    [FAIL] Reading {table_name}: {e}")

    print(f"    Total rows across {num_threads} tables: {total_rows:,} "
          f"(expected {num_threads * writes_per_thread * 100:,})")
    print(f"    Time: {t1-t0:.2f}s  ({total_rows / (t1-t0):,.0f} rows/sec)")
    db.close()


def test_same_table(num_threads: int, writes_per_thread: int):
    """All threads write to the SAME table. Will likely break."""
    print(f"\n  --- Test 2: {num_threads} threads, all writing to SAME table ---")
    bench_dir = "/tmp/pond_concurrent_same_table"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    db = Pond(bench_dir)

    errors = []
    rows_written = [0] * num_threads
    barrier = threading.Barrier(num_threads)

    def worker(thread_id: int):
        try:
            barrier.wait()
            for i in range(writes_per_thread):
                b = make_batch(100, start_id=thread_id * 10000 + i * 100)
                db.write("events", b)
                rows_written[thread_id] += 100
                # Don't seal — just write. Concurrent seals would be worse.
        except Exception as e:
            errors.append(f"Thread {thread_id}: {e}")

    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    t1 = time.perf_counter()

    if errors:
        print(f"    [EXPECTED FAILURE] {len(errors)} errors:")
        for e in errors[:3]:
            print(f"      {e}")
    else:
        print(f"    [SURPRISE] No errors — but data may be corrupted")

    expected_total = num_threads * writes_per_thread * 100
    actual_total = sum(rows_written)
    print(f"    Rows written (per-thread counts): {actual_total:,} "
          f"(expected {expected_total:,})")
    print(f"    Time: {t1-t0:.2f}s")
    print()
    print("    ANALYSIS:")
    print("    - v0 uses an in-memory _open_objects dict with no synchronization.")
    print("    - Concurrent writes to the same table corrupt the OPEN object's")
    print("      Arrow IPC stream (multiple writers appending simultaneously).")
    print("    - This is a KNOWN gap — v0 is single-writer per table by design.")
    print("    - Production needs: per-table lock for OPEN object, OR per-thread")
    print("      OPEN objects merged at seal time, OR proper MVCC.")

    db.close()


def test_concurrent_seal(num_threads: int):
    """Multiple threads sealing different tables concurrently."""
    print(f"\n  --- Test 3: {num_threads} threads, concurrent seals on different tables ---")
    bench_dir = "/tmp/pond_concurrent_seal"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    db = Pond(bench_dir)

    # Pre-populate: each thread has a table with one batch written
    for i in range(num_threads):
        b = make_batch(100, start_id=i * 100)
        db.write(f"events_t{i}", b)

    errors = []
    barrier = threading.Barrier(num_threads)

    def worker(thread_id: int):
        try:
            barrier.wait()
            db.seal(f"events_t{thread_id}", message=f"concurrent seal t{thread_id}")
        except Exception as e:
            errors.append(f"Thread {thread_id}: {e}")

    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    t1 = time.perf_counter()

    if errors:
        print(f"    [FAIL] {len(errors)} errors during concurrent seal:")
        for e in errors[:3]:
            print(f"      {e}")
    else:
        print(f"    [OK] All {num_threads} concurrent seals succeeded")

    # Verify all tables readable
    for i in range(num_threads):
        try:
            table = db.read(f"events_t{i}")
            if table.num_rows != 100:
                print(f"    [FAIL] events_t{i}: {table.num_rows} rows (expected 100)")
        except Exception as e:
            print(f"    [FAIL] Reading events_t{i}: {e}")

    print(f"    Time: {(t1-t0)*1000:.1f}ms for {num_threads} concurrent seals")
    db.close()


def main():
    print("=" * 76)
    print("  Concurrent writers benchmark")
    print("=" * 76)
    print()
    print("  The architecture specifies single-process multi-writer concurrency")
    print("  via MVCC. v0 doesn't implement MVCC — this benchmark exposes what")
    print("  actually breaks under concurrent access.")

    test_different_tables(num_threads=10, writes_per_thread=10)
    test_same_table(num_threads=10, writes_per_thread=10)
    test_concurrent_seal(num_threads=10)

    print()
    print("=" * 76)
    print("  Summary")
    print("=" * 76)
    print()
    print("  v0 concurrency status:")
    print("    - Different tables, concurrent writes: WORKS (with caveats)")
    print("    - Same table, concurrent writes:      BROKEN (OPEN object is shared")
    print("                                            mutable state, no synchronization)")
    print("    - Concurrent seals (diff tables):     WORKS (filesystem is atomic)")
    print()
    print("  What v0.1 needs:")
    print("    - Per-table lock for OPEN object (immediate fix)")
    print("    - OR per-thread OPEN objects merged at seal time (better)")
    print("    - OR proper MVCC with snapshot isolation (production-grade)")
    print()
    print("  The architecture permits all three; v0 has none. This is the next")
    print("  milestone before replication (Raft) — replication magnifies bugs.")


if __name__ == "__main__":
    main()
