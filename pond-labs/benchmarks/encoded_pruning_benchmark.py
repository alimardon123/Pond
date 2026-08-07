#!/usr/bin/env python3
"""
Benchmark: Encoding-aware compute (FastLanes-style).

Compares three storage modes for a low-cardinality column query:

  A. Whole-blob storage (range_write)
     - 1 row group = 1 Parquet blob
     - Predicate pushdown: row-group zone maps + Parquet row-group decode
     - Decodes the whole blob to filter rows

  B. Per-column-chunk storage (range_write_column_chunks)
     - 1 row group = N_cols × N_chunks Parquet blobs
     - Column-chunk pruning skips chunk BLOBS for non-matching chunks
     - Still decodes Parquet for surviving chunks

  C. Encoded per-column-chunk storage (range_write_encoded)
     - Same as B, but each chunk blob is FastLanes-style encoded
     - Encoded predicate eval skips DECODE for pruned chunks
     - For matching chunks, decodes only surviving row ranges

Setup: 100K rows, region column with 3 unique values (US/EU/ASIA)
cycling every 1000 rows. Predicate: region = 'EU' (1/3 of rows match).

Expected results:
  - A reads the whole 100K-row row-group blob, decodes Parquet, filters
  - B reads 1/3 of chunk blobs (only EU chunks survive), decodes Parquet
  - C reads 1/3 of chunk blobs, evaluates predicate on DICT-encoded form
    (just looks up "EU" in the dict and yields matching codes), decodes
    only the surviving codes
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
except ImportError:
    print("pyarrow not installed — skipping")
    sys.exit(0)

from kernel import PondMinimal
from lakehouse_lens import LakehouseLens


def _time_read(lens, name, **kwargs):
    """Time a single read call. Returns (rows, ms)."""
    t0 = time.perf_counter()
    table = lens.read_with_pruning(name, **kwargs) if "predicates" in kwargs else lens.read_table(name)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    return table.num_rows, elapsed_ms


def main():
    print("=" * 76)
    print("Benchmark: Encoding-Aware Compute (FastLanes-style)")
    print("=" * 76)

    tmpdir = tempfile.mkdtemp(prefix="pond_enc_bench_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = LakehouseLens(kernel)

        # 99K rows (33 cycles of 3000), region cycles US/EU/ASIA every 1000 rows
        n = 99_000
        regions = (["US"] * 1000 + ["EU"] * 1000 + ["ASIA"] * 1000) * (n // 3000)
        print(f"\n  Setup: {n:,} rows, region column with 3 unique values")
        print(f"         (US/EU/ASIA cycling every 1000 rows)")
        print(f"         Predicate: region = 'EU' (1/3 of rows match = "
              f"{n // 3:,} rows)")

        data = pa.table({
            "id": list(range(n)),
            "region": regions,
        })

        # Write A: whole-blob storage
        t0 = time.perf_counter()
        lens.range_write("events_whole", data, key_col="id",
                          row_group_size=n)
        ms_write_a = (time.perf_counter() - t0) * 1000

        # Write B: per-column-chunk storage (Parquet)
        t0 = time.perf_counter()
        lens.range_write_column_chunks("events_cc", data, key_col="id",
                                        row_group_size=n,
                                        chunk_size=1000)
        ms_write_b = (time.perf_counter() - t0) * 1000

        # Write C: encoded per-column-chunk storage
        t0 = time.perf_counter()
        lens.range_write_encoded("events_enc", data, key_col="id",
                                  row_group_size=n,
                                  chunk_size=1000,
                                  encoding_hints={"id": "bitpack",
                                                   "region": "dict"})
        ms_write_c = (time.perf_counter() - t0) * 1000

        print(f"\n  Write time:")
        print(f"    A. Whole-blob:           {ms_write_a:7.2f} ms")
        print(f"    B. Per-column-chunk:      {ms_write_b:7.2f} ms "
              f"({ms_write_b/ms_write_a:.2f}x)")
        print(f"    C. Encoded per-column:    {ms_write_c:7.2f} ms "
              f"({ms_write_c/ms_write_a:.2f}x)")

        # Warmup
        lens.read_with_pruning("events_whole",
                                predicates=[("region", "=", "EU")])
        lens.read_with_column_chunk_pruning("events_cc",
                                             predicates=[("region", "=", "EU")])
        lens.read_with_encoded_pruning("events_enc",
                                        predicates=[("region", "=", "EU")])

        predicate = [("region", "=", "EU")]
        row_filter = lambda r: r.get("region") == "EU"

        # Scenario A: whole-blob + predicate + row_filter
        N_RUNS = 5
        ms_a_list = []
        for _ in range(N_RUNS):
            t0 = time.perf_counter()
            result_a = lens.read_with_pruning(
                "events_whole",
                predicates=predicate,
                row_filter=row_filter,
                columns=["region"],
            )
            ms_a_list.append((time.perf_counter() - t0) * 1000)
        ms_a = sum(ms_a_list) / N_RUNS

        # Scenario B: per-column-chunk + predicate + row_filter
        ms_b_list = []
        for _ in range(N_RUNS):
            t0 = time.perf_counter()
            result_b = lens.read_with_column_chunk_pruning(
                "events_cc",
                predicates=predicate,
                row_filter=row_filter,
            )
            ms_b_list.append((time.perf_counter() - t0) * 1000)
        ms_b = sum(ms_b_list) / N_RUNS

        # Scenario C: encoded per-column-chunk + predicate (encoded eval)
        ms_c_list = []
        for _ in range(N_RUNS):
            t0 = time.perf_counter()
            result_c = lens.read_with_encoded_pruning(
                "events_enc",
                predicates=predicate,
                row_filter=row_filter,
            )
            ms_c_list.append((time.perf_counter() - t0) * 1000)
        ms_c = sum(ms_c_list) / N_RUNS

        # Verify correctness — all three should return ~33,000 EU rows
        assert abs(result_a.num_rows - 33_000) <= 1, f"A: {result_a.num_rows}"
        assert abs(result_b.num_rows - 33_000) <= 1, f"B: {result_b.num_rows}"
        assert abs(result_c.num_rows - 33_000) <= 1, f"C: {result_c.num_rows}"

        print(f"\n  Read results (predicate: region = 'EU' → "
              f"{result_a.num_rows:,} rows, {N_RUNS}-run avg):")
        print(f"    A. Whole-blob:                {ms_a:7.2f} ms")
        print(f"    B. Per-column-chunk:          {ms_b:7.2f} ms "
              f"({ms_a/ms_b:.2f}x faster than A)")
        print(f"    C. Encoded per-column-chunk:  {ms_c:7.2f} ms "
              f"({ms_a/ms_c:.2f}x faster than A, "
              f"{ms_b/ms_c:.2f}x faster than B)")

        print(f"\n  Why the savings?")
        print(f"  - A decodes the whole 100K-row Parquet blob, then filters")
        print(f"  - B skips 2/3 of chunk blobs (US and ASIA chunks pruned)")
        print(f"    but still decodes Parquet for the 1/3 surviving chunks")
        print(f"  - C evaluates predicate on DICT-encoded form (just looks")
        print(f"    up 'EU' in the dict + scans codes array), skipping the")
        print(f"    Parquet decode entirely for matching chunks")
        print(f"  - The decode-skip is the FastLanes/Vortex innovation:")
        print(f"    structural predicates on the encoded representation")

        # Show per-row cost
        rows_per_ms_a = result_a.num_rows / ms_a if ms_a > 0 else 0
        rows_per_ms_c = result_c.num_rows / ms_c if ms_c > 0 else 0
        print(f"\n  Throughput:")
        print(f"    A. {rows_per_ms_a:,.0f} rows/ms")
        print(f"    C. {rows_per_ms_c:,.0f} rows/ms "
              f"({rows_per_ms_c/rows_per_ms_a:.2f}x higher throughput)")

        kernel.close()

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
