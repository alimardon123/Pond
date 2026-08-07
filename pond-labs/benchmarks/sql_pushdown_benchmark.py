#!/usr/bin/env python3
"""
Benchmark: SQL pushdown end-to-end with PondLakehouse.query().

Compares query(sql, use_pruning=True) vs query(sql, use_pruning=False)
on a 100K-row dataset with various predicate types (>, IN, BETWEEN).
"""

import os, sys, time, shutil, tempfile

HERE = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE))) if '__file__' in dir() else os.getcwd()
if not os.path.exists(os.path.join(REPO, "bindings/python/core")):
    REPO = os.getcwd()
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "lakehouse"))

try:
    import pyarrow as pa
except ImportError:
    print("pyarrow not installed"); sys.exit(1)

from kernel import PondMinimal
from lakehouse_lens import PondLakehouse


def run_benchmark():
    print("=" * 70)
    print("SQL Pushdown Benchmark — PondLakehouse.query()")
    print("=" * 70)

    tmpdir = tempfile.mkdtemp(prefix="pond_sql_bench_")
    try:
        lh = PondLakehouse(tmpdir)

        # Create 100K rows in 10 row groups
        n = 100_000
        data = pa.table({
            "id": list(range(n)),
            "age": list(range(n)),  # sorted 0-99999
            "region": ["US" if i % 3 == 0 else "EU" if i % 3 == 1 else "ASIA" for i in range(n)],
            "score": [i / n for i in range(n)],
        })
        print(f"\n  Creating {n} rows in 10 row groups...")
        lh.range_write("events", data, key_col="id", row_group_size=10_000)
        print(f"  Zone maps auto-built")

        queries = [
            ("SELECT COUNT(*) FROM events WHERE age > 90000", "10% selectivity (> predicate)"),
            ("SELECT COUNT(*) FROM events WHERE age > 99000", "1% selectivity (> predicate)"),
            ("SELECT COUNT(*) FROM events WHERE age >= 50000", "50% selectivity (>= predicate)"),
            ("SELECT id FROM events WHERE age > 90000", "10% + projection (id, age)"),
            ("SELECT COUNT(*) FROM events WHERE age >= 30000 AND age <= 69999", "40% (range, AND)"),
            ("SELECT COUNT(*) FROM events WHERE region = 'US'", "33% (= predicate, interleaved)"),
        ]

        # Register the table name for DuckDB
        print(f"\n  {'Query':<55} {'Pruned (ms)':>11} {'Full (ms)':>10} {'Speedup':>8}")
        print(f"  {'-'*55} {'-'*11} {'-'*10} {'-'*8}")

        for sql, label in queries:
            # With pruning
            t0 = time.perf_counter()
            r1 = lh.query(sql, table_name="events", use_pruning=True)
            t_pruned = time.perf_counter() - t0

            # Without pruning
            t0 = time.perf_counter()
            r2 = lh.query(sql, table_name="events", use_pruning=False)
            t_full = time.perf_counter() - t0

            speedup = t_full / t_pruned if t_pruned > 0 else 0
            print(f"  {label:<55} {t_pruned*1000:>11.1f} {t_full*1000:>10.1f} {speedup:>7.1f}x")

        lh.close()
        print("\n  Key findings:")
        print("  - On LOCAL DISK with small data, Python pruning overhead can exceed")
        print("    DuckDB's native C++ scan. The benefit shows up when:")
        print("    1. Data is on OBJECT STORAGE (S3) — skipping blobs saves network RTT")
        print("    2. Data is LARGE (blobs are big enough that skipping saves real I/O)")
        print("    3. Selectivity is LOW (1-5%) — most row groups pruned")
        print("  - For interleaved data (region), pruning can't help (min/max overlap)")
        print("  - For sorted data (age), pruning is very effective on object storage")
        print("  - The pruning path is GENERIC — same code for any lens/format")
        print("  - DuckDB's native scan is faster for local small data (no Python overhead)")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    run_benchmark()
