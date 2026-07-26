#!/usr/bin/env python3
"""
Benchmark: Column-chunk pruning effectiveness.

Demonstrates the third level of Pond's three-level pruning hierarchy:
  1. Row-group pruning  (skip entire row groups via ZoneMap)
  2. Column-chunk pruning (skip individual chunks within surviving row groups)
  3. Row-level filtering (exact match on decoded rows)

Setup: 1 row group with 50,000 rows (ages 0..49999), chunk_size=1000 →
50 column chunks. The single row group's zone map covers ages [0, 49999],
so row-group pruning cannot skip anything. Column-chunk pruning then
takes over: with predicate age >= 49000, only chunks 49 (49000-49999)
survives → 49/50 chunks pruned.

Measures:
  - Full scan (no pruning, no row filter)
  - Row-group + row_filter only (no column-chunk pruning)
  - Row-group + column-chunk + row_filter (full three-level pruning)
  - PruningReader stats: column_chunks_pruned, rows_yielded
  - Wall-clock speedup ratio

Run:
    python pond-labs/benchmarks/column_chunk_pruning_benchmark.py
"""

from __future__ import annotations

import os
import sys
import time
import shutil
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "lakehouse"))

try:
    import pyarrow as pa
except ImportError:
    print("pyarrow not installed — skipping")
    sys.exit(0)

from kernel import PondMinimal
from lakehouse_lens import LakehouseLens
from pruning_reader import PruningReader
from pruning import PruningPredicate, ColumnPredicate
from collection_metadata import CollectionMetadata


def _time_read(lens, name, **kwargs):
    """Time a single read_with_pruning call. Returns (rows, ms)."""
    t0 = time.perf_counter()
    table = lens.read_with_pruning(name, **kwargs)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    return table.num_rows, elapsed_ms


def main():
    print("=" * 72)
    print("Benchmark: Column-Chunk Pruning (third level of pruning hierarchy)")
    print("=" * 72)

    tmpdir = tempfile.mkdtemp(prefix="pond_cc_bench_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = LakehouseLens(kernel)

        # 50,000 rows in ONE row group, ages 0..49999
        n = 50_000
        print(f"\n  Setup: {n:,} rows in 1 row group, "
              f"chunk_size=1000 → {n // 1000} column chunks per column")

        data = pa.table({
            "id": list(range(n)),
            "age": list(range(n)),
            "score": [float(i) / n for i in range(n)],
        })
        lens.range_write("events", data, key_col="id",
                         row_group_size=n)  # single row group

        # Sanity: verify column-chunk stats exist
        meta = CollectionMetadata(kernel)
        zm = meta.zm_index.get_zone_map("events", f"rg/{n - 1}")
        assert zm is not None and "column_chunks" in zm, (
            "column-chunk stats missing")
        n_chunks = len(zm["column_chunks"]["column_chunks"]["age"])
        assert n_chunks == n // 1000, (
            f"Expected {n // 1000} chunks, got {n_chunks}")
        print(f"  [OK] Column-chunk stats: {n_chunks} chunks for 'age'")

        # Warmup: read everything once (populate OS page cache)
        lens.read_with_pruning("events")
        print("  [OK] Warmup complete\n")

        # Three scenarios — predicate: age >= 49000
        # Row-group zone map: age [0, 49999] → can't prune row group
        # Column-chunk zone maps: 49 chunks [0..48999] pruned, 1 survives
        # Expected: 1000 rows (ages 49000..49999)

        predicate = [("age", ">=", 49000)]
        row_filter = lambda r: r.get("age", 0) >= 49000

        # Scenario A: Full scan, no pruning
        rows_a, ms_a = _time_read(lens, "events")
        print(f"  A. Full scan (no pruning):           "
              f"{rows_a:,} rows in {ms_a:7.2f} ms")

        # Scenario B: Row-group + row_filter only (no column-chunk pruning)
        # Reads the whole 50K-row blob, then filters down to 1000
        rows_b, ms_b = _time_read(
            lens, "events",
            predicates=predicate,
            row_filter=row_filter,
            columns=None,  # disable column-chunk pruning
        )
        print(f"  B. Row-group + row_filter only:      "
              f"{rows_b:,} rows in {ms_b:7.2f} ms")

        # Scenario C: Full three-level pruning (row-group + column-chunk + filter)
        # Reads the blob, but only yields rows from chunk 49 (1000 rows)
        # → row_filter runs on 1000 rows instead of 50,000
        rows_c, ms_c = _time_read(
            lens, "events",
            predicates=predicate,
            row_filter=row_filter,
            columns=["age"],  # enable column-chunk pruning
            chunk_size=1000,
        )
        print(f"  C. + Column-chunk pruning:           "
              f"{rows_c:,} rows in {ms_c:7.2f} ms")

        # Verify correctness
        assert rows_a == n, f"Scenario A: expected {n}, got {rows_a}"
        assert rows_b == 1000, f"Scenario B: expected 1000, got {rows_b}"
        assert rows_c == 1000, f"Scenario C: expected 1000, got {rows_c}"

        # Inspect PruningReader stats for scenario C
        pred = PruningPredicate([ColumnPredicate(column="age", op=">=", value=49000)])
        reader = PruningReader(kernel, meta.zm_index, "events", pred)
        list(reader.scan(
            decode_fn=lambda b: lens._decode_table(b).to_pylist(),
            row_filter=row_filter,
            columns=["age"],
            chunk_size=1000,
        ))
        stats = reader.get_stats()
        print(f"\n  PruningReader stats (scenario C):")
        print(f"    data_blobs_read:       {stats['data_blobs_read']}")
        print(f"    rows_yielded:          {stats['rows_yielded']}")
        print(f"    column_chunks_pruned:  {stats['column_chunks_pruned']} "
              f"(of {n_chunks} total)")

        assert stats["column_chunks_pruned"] == n_chunks - 1, (
            f"Expected {n_chunks - 1} chunks pruned, "
            f"got {stats['column_chunks_pruned']}")

        # Speedup
        if ms_c > 0 and ms_b > 0:
            speedup = ms_b / ms_c
            print(f"\n  Speedup (B → C): {speedup:.2f}x")
            print(f"  (Column-chunk pruning saved "
                  f"{(ms_b - ms_c):.2f} ms of decode/filter work)")

        # Why the speedup?
        print(f"\n  Why the speedup?")
        print(f"  - Scenario B: row_filter runs on ALL {n:,} decoded rows")
        print(f"  - Scenario C: row_filter runs on only "
              f"{stats['rows_yielded']:,} rows (from surviving chunks)")
        print(f"  - The decode itself is unchanged (whole blob is read in both)")
        print(f"  - On object storage, separate column-chunk blobs would also")
        print(f"    save I/O — that's the next level of structural sharing.")

        kernel.close()

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
