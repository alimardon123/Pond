#!/usr/bin/env python3
"""
Benchmark: Per-column-chunk storage — real I/O savings on object storage.

Compares three storage modes for the same data and same selective predicate:

  A. Whole-blob storage (range_write)
     - 1 row group = 1 Parquet blob containing all columns × all rows
     - Predicate pushdown: row-group zone maps skip non-matching groups
     - Column-chunk pruning: skips row_filter work but NOT I/O
     - The whole blob is read even if only 1/5 chunks match

  B. Per-column-chunk storage (range_write_column_chunks)
     - 1 row group = N_columns × N_chunks separate Parquet blobs
     - Each blob is a single-column, single-chunk file
     - Predicate pushdown: row-group zone maps + per-column-chunk zone maps
     - Column-chunk pruning skips ACTUAL kernel.read_blob() calls
     - Skip 4/5 chunks = skip 4/5 of bytes per column

  C. Per-column-chunk storage + projection pushdown
     - Same as B, but only the requested columns are read
     - Skip non-projected columns entirely (no I/O)

Setup: 50,000 rows in 1 row group (so row-group pruning can't skip anything),
10 column chunks of 5000 rows each, 3 columns. Predicate: age >= 45000
(very selective — only the last chunk survives).

Expected results:
  - A reads the whole 50K-row blob (~600 KB)
  - B reads 3 surviving chunk blobs (~60 KB total — 1/10th of A)
  - C reads 1 surviving chunk blob for the projected column (~20 KB)
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
from collection_metadata import CollectionMetadata
from column_chunk_storage import ColumnChunkStorage


def _instrument_read(kernel):
    """Wrap kernel.read_blob to count bytes read."""
    original = kernel.read_blob
    counter = {"bytes": 0, "calls": 0}

    def counting_read(blob_hash):
        data = original(blob_hash)
        counter["bytes"] += len(data)
        counter["calls"] += 1
        return data

    return counting_read, counter


def main():
    print("=" * 76)
    print("Benchmark: Per-Column-Chunk Storage (real I/O savings)")
    print("=" * 76)

    tmpdir = tempfile.mkdtemp(prefix="pond_ccs_bench_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = LakehouseLens(kernel)

        # 50,000 rows in 1 row group, 10 chunks of 5000 rows each
        n = 50_000
        chunk_size = 5_000
        n_chunks = n // chunk_size
        print(f"\n  Setup: {n:,} rows in 1 row group, chunk_size={chunk_size} "
              f"→ {n_chunks} column chunks per column")
        print(f"         3 columns: id, age, score")

        data = pa.table({
            "id": list(range(n)),
            "age": list(range(n)),
            "score": [float(i) / n for i in range(n)],
        })

        # Write A: whole-blob storage
        t0 = time.perf_counter()
        lens.range_write("events_whole", data, key_col="id",
                          row_group_size=n)
        ms_write_a = (time.perf_counter() - t0) * 1000

        # Write B: per-column-chunk storage
        t0 = time.perf_counter()
        lens.range_write_column_chunks("events_cc", data, key_col="id",
                                        row_group_size=n,
                                        chunk_size=chunk_size)
        ms_write_b = (time.perf_counter() - t0) * 1000

        print(f"\n  Write time:")
        print(f"    A. Whole-blob:           {ms_write_a:7.2f} ms")
        print(f"    B. Per-column-chunk:      {ms_write_b:7.2f} ms "
              f"({ms_write_b/ms_write_a:.2f}x — extra cost: chunk splitting + N blobs)")

        # Warmup
        lens.read_with_pruning("events_whole")
        lens.read_with_column_chunk_pruning("events_cc")

        predicate = [("age", ">=", 45_000)]  # last chunk survives
        row_filter = lambda r: r.get("age", 0) >= 45_000

        # Scenario A: Whole-blob + predicate + row_filter
        # (column-chunk pruning inside scan skips row_filter work, not I/O)
        counting_read, counter = _instrument_read(kernel)
        original_read = kernel.read_blob
        kernel.read_blob = counting_read
        t0 = time.perf_counter()
        result_a = lens.read_with_pruning(
            "events_whole",
            predicates=predicate,
            row_filter=row_filter,
            columns=["age"],
        )
        ms_a = (time.perf_counter() - t0) * 1000
        bytes_a = counter["bytes"]
        calls_a = counter["calls"]
        kernel.read_blob = original_read

        # Scenario B: Per-column-chunk + predicate + row_filter
        counting_read, counter = _instrument_read(kernel)
        kernel.read_blob = counting_read
        t0 = time.perf_counter()
        result_b = lens.read_with_column_chunk_pruning(
            "events_cc",
            predicates=predicate,
            row_filter=row_filter,
        )
        ms_b = (time.perf_counter() - t0) * 1000
        bytes_b = counter["bytes"]
        calls_b = counter["calls"]
        kernel.read_blob = original_read

        # Scenario C: Per-column-chunk + predicate + row_filter + projection
        counting_read, counter = _instrument_read(kernel)
        kernel.read_blob = counting_read
        t0 = time.perf_counter()
        result_c = lens.read_with_column_chunk_pruning(
            "events_cc",
            predicates=predicate,
            row_filter=row_filter,
            columns=["age"],  # projection: only 'age' column
        )
        ms_c = (time.perf_counter() - t0) * 1000
        bytes_c = counter["bytes"]
        calls_c = counter["calls"]
        kernel.read_blob = original_read

        # Verify correctness — all three should return the same rows
        assert result_a.num_rows == 5_000, f"A: {result_a.num_rows}"
        assert result_b.num_rows == 5_000, f"B: {result_b.num_rows}"
        assert result_c.num_rows == 5_000, f"C: {result_c.num_rows}"

        print(f"\n  Read results (predicate: age >= 45,000 → 5,000 rows):")
        print(f"    A. Whole-blob:              {ms_a:7.2f} ms, "
              f"{bytes_a:>10,} bytes, {calls_a:3d} reads")
        print(f"    B. Per-column-chunk:        {ms_b:7.2f} ms, "
              f"{bytes_b:>10,} bytes, {calls_b:3d} reads")
        print(f"    C. Per-column-chunk+proj:   {ms_c:7.2f} ms, "
              f"{bytes_c:>10,} bytes, {calls_c:3d} reads")

        # Compute savings
        if bytes_b > 0:
            ratio_ba = bytes_a / bytes_b
            print(f"\n  I/O savings (B vs A):")
            print(f"    Bytes reduction: {ratio_ba:.2f}x "
                  f"({bytes_a:,} → {bytes_b:,})")
            print(f"    Bytes saved:     {bytes_a - bytes_b:,}")

        if bytes_c > 0:
            ratio_ca = bytes_a / bytes_c
            print(f"\n  I/O savings (C vs A):")
            print(f"    Bytes reduction: {ratio_ca:.2f}x "
                  f"({bytes_a:,} → {bytes_c:,})")
            print(f"    Bytes saved:     {bytes_a - bytes_c:,}")

        print(f"\n  Why the savings?")
        print(f"  - Scenario A reads the whole 50K-row row-group blob "
              f"(all cols × all rows)")
        print(f"  - Scenario B reads only 1 surviving chunk per column "
              f"(3 small chunk blobs, ~1/10th of row group)")
        print(f"  - Scenario C reads only 1 surviving chunk for 'age' "
              f"(1 tiny chunk blob, ~1/30th of row group)")
        print(f"  - On object storage (S3/GCS), this directly reduces:")
        print(f"      * Bytes transferred (network I/O)")
        print(f"      * Per-request latency (fewer GET calls)")
        print(f"      * Cost (GB-seconds pricing)")

        # Show write tradeoff
        print(f"\n  Tradeoff:")
        print(f"  - Per-column-chunk write is {ms_write_b/ms_write_a:.1f}x slower")
        print(f"    (extra cost: N_cols × N_chunks blobs vs 1 blob)")
        print(f"  - Worth it for read-heavy workloads with selective predicates")

        kernel.close()

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
