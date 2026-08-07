#!/usr/bin/env python3
"""
Benchmark: PND1+DuckDB vs Parquet+DuckDB — head-to-head comparison.

Tests the same dataset with two storage formats:
  A. Parquet (DuckDB native): write as .parquet, query with DuckDB
  B. PND1+DuckDB adapter: write with range_write_encoded, read via adapter

Measures:
  - Write time
  - Full scan time (SELECT COUNT(*))
  - Selective query time (WHERE age >= 990 — 1% selectivity)
  - Storage size (bytes on disk)

This is the external benchmark the architect requested (issue #5b):
"Run PND1+DuckDB vs Parquet+DuckDB on the same data."

Run:
    python pond-labs/benchmarks/pnd1_vs_parquet_benchmark.py
"""

from __future__ import annotations

import os
import sys
import time
import shutil
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "bindings/python/core"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk"))
sys.path.insert(0, os.path.join(REPO, "bindings/python/sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "lakehouse"))

try:
    import pyarrow as pa
    import duckdb
except ImportError:
    print("pyarrow/duckdb not installed — skipping"); sys.exit(0)

from kernel import PondMinimal
from lakehouse_lens import LakehouseLens
from duckdb_pond_adapter import PondDuckDBAdapter


def main():
    print("=" * 76)
    print("PND1+DuckDB vs Parquet+DuckDB — Head-to-Head Benchmark")
    print("=" * 76)

    tmpdir = tempfile.mkdtemp(prefix="pond_vs_parquet_")
    try:
        N = 100_000

        # Generate data: id (sequential), age (0-99 cycling), region (3 values)
        data = pa.table({
            "id": list(range(N)),
            "age": [i % 100 for i in range(N)],
            "region": (["US", "EU", "ASIA"] * (N // 3 + 1))[:N],
        })

        print(f"\n  Dataset: {N:,} rows, 3 columns (id, age, region)")
        print(f"  Query: SELECT COUNT(*) FROM t WHERE age >= 90 (10% selectivity)")

        # ================================================================
        # A. Parquet + DuckDB (native)
        # ================================================================
        print(f"\n  A. Parquet + DuckDB (native)")

        # Write Parquet file
        parquet_path = os.path.join(tmpdir, "data.parquet")
        t0 = time.perf_counter()
        import pyarrow.parquet as pq
        pq.write_table(data, parquet_path)
        ms_write_a = (time.perf_counter() - t0) * 1000
        parquet_size = os.path.getsize(parquet_path)

        # Open with DuckDB
        conn_a = duckdb.connect()
        conn_a.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{parquet_path}')")

        # Warmup
        conn_a.execute("SELECT COUNT(*) FROM data").fetchone()

        # Full scan
        t0 = time.perf_counter()
        for _ in range(5):
            result = conn_a.execute("SELECT COUNT(*) FROM data").fetchone()
        ms_full_a = (time.perf_counter() - t0) * 1000 / 5

        # Selective query
        t0 = time.perf_counter()
        for _ in range(5):
            result = conn_a.execute(
                "SELECT COUNT(*) FROM data WHERE age >= 90"
            ).fetchone()
        ms_selective_a = (time.perf_counter() - t0) * 1000 / 5
        count_a = result[0]

        conn_a.close()
        print(f"     Write:        {ms_write_a:7.1f} ms")
        print(f"     Storage:      {parquet_size:>10,} bytes")
        print(f"     Full scan:    {ms_full_a:7.1f} ms")
        print(f"     WHERE age>=90:{ms_selective_a:7.1f} ms → {count_a:,} rows")

        # ================================================================
        # B. PND1 + DuckDB adapter
        # ================================================================
        print(f"\n  B. PND1 + DuckDB adapter")

        kernel = PondMinimal(os.path.join(tmpdir, "pond"))
        lens = LakehouseLens(kernel)

        # Write with encoded storage (bitpack for id/age, dict for region)
        t0 = time.perf_counter()
        lens.range_write_encoded("data", data, key_col="id",
                                   row_group_size=N,
                                   chunk_size=1000,
                                   encoding_hints={"id": "bitpack",
                                                    "age": "bitpack",
                                                    "region": "dict"})
        ms_write_b = (time.perf_counter() - t0) * 1000

        # Measure PND1 storage size (sum of all blob files)
        pond_base = os.path.join(tmpdir, "pond")
        pnd1_size = 0
        for root, dirs, files in os.walk(pond_base):
            for f in files:
                pnd1_size += os.path.getsize(os.path.join(root, f))

        # Read via adapter
        adapter = PondDuckDBAdapter(kernel)

        # Warmup
        table = adapter.read_encoded_collection("data")

        # Full scan
        t0 = time.perf_counter()
        for _ in range(5):
            table = adapter.read_encoded_collection("data")
            conn_b = duckdb.connect()
            conn_b.register("data", table)
            result = conn_b.execute("SELECT COUNT(*) FROM data").fetchone()
            conn_b.close()
        ms_full_b = (time.perf_counter() - t0) * 1000 / 5

        # Selective query — with predicate pushdown via adapter
        t0 = time.perf_counter()
        for _ in range(5):
            table = adapter.read_encoded_collection_with_predicate(
                "data", predicates=[("age", ">=", 90)], columns=["id", "age"])
            conn_b = duckdb.connect()
            conn_b.register("data", table)
            result = conn_b.execute(
                "SELECT COUNT(*) FROM data WHERE age >= 90"
            ).fetchone()
            conn_b.close()
        ms_selective_b = (time.perf_counter() - t0) * 1000 / 5
        count_b = result[0]

        kernel.close()
        print(f"     Write:        {ms_write_b:7.1f} ms")
        print(f"     Storage:      {pnd1_size:>10,} bytes")
        print(f"     Full scan:    {ms_full_b:7.1f} ms")
        print(f"     WHERE age>=90:{ms_selective_b:7.1f} ms → {count_b:,} rows")

        # ================================================================
        # Comparison
        # ================================================================
        print(f"\n  {'=' * 60}")
        print(f"  {'Metric':<25} {'Parquet':>12} {'PND1':>12} {'Ratio':>8}")
        print(f"  {'-'*25} {'-'*12} {'-'*12} {'-'*8}")
        print(f"  {'Write time':<25} {ms_write_a:>11.1f}ms {ms_write_b:>11.1f}ms "
              f"{ms_write_b/ms_write_a:>7.2f}x")
        print(f"  {'Storage size':<25} {parquet_size:>12,} {pnd1_size:>12,} "
              f"{pnd1_size/parquet_size:>7.2f}x")
        print(f"  {'Full scan':<25} {ms_full_a:>11.1f}ms {ms_full_b:>11.1f}ms "
              f"{ms_full_b/ms_full_a:>7.2f}x")
        print(f"  {'WHERE age>=90':<25} {ms_selective_a:>11.1f}ms {ms_selective_b:>11.1f}ms "
              f"{ms_selective_b/ms_selective_a:>7.2f}x")

        # Verify correctness
        assert count_a == count_b, f"Row count mismatch: Parquet={count_a}, PND1={count_b}"
        print(f"\n  [OK] Both return {count_a:,} rows for WHERE age >= 90")

        print(f"\n  Notes:")
        print(f"  - PND1 storage includes zone maps + manifests + chunk blobs")
        print(f"  - PND1 write includes encoding + compression + zone map build")
        print(f"  - PND1 read uses predicate pushdown (zone-map prune + encoded scan)")
        print(f"  - Parquet uses DuckDB's native SIMD scanner (C++ optimized)")
        print(f"  - On local disk, Parquet+DuckDB will be faster (C++ vs Python)")
        print(f"  - On S3, PND1's predicate pushdown skips blob fetches entirely")
        print(f"    (zone maps are separate small blobs — no data blob read)")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
