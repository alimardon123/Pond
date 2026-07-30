"""
Round-trip benchmark — proves the CollectionManifest path reduces
S3 round trips for ALL storage interactions.

This benchmark:
  1. Writes a 100-row-group table via each write mode (whole-blob,
     column-chunks, encoded)
  2. Performs point lookups, range reads, predicate-pruned reads via
     BOTH the old zone-map path AND the new manifest path
  3. Counts kernel.read_blob() calls (= S3 GETs) for each
  4. Prints a comparison table

Expected result: manifest path is consistently faster (fewer GETs)
than the zone-map path, especially for column-chunk and encoded
storage where the manifest eliminates the per-row-group chunk-manifest
fetch.
"""
from __future__ import annotations

import os
import sys
import tempfile
import shutil
import time
from typing import Optional

# Make pond-core + pond-sdk importable
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(HERE, "..", "lenses", "lakehouse"))

import pyarrow as pa
from kernel import PondMinimal
from lakehouse_lens import LakehouseLens


def make_test_table(n_rows: int = 10_000, n_groups: int = 100) -> pa.Table:
    """Build a test table with n_groups row groups of n_rows/n_groups rows each.

    Columns:
      - id (int): primary key, 0..n_rows-1
      - age (int): 0..99 (low cardinality — good for RLE/Dict)
      - region (str): "ASIA"/"EU"/"US" (very low cardinality — Dict)
      - score (float): 0..100
      - status (int): 0..5 (small range — Bitpack)
    """
    n_rows_per_group = n_rows // n_groups
    ids = []
    ages = []
    regions = []
    scores = []
    statuses = []
    region_choices = ["ASIA", "EU", "US"]
    for i in range(n_rows):
        ids.append(i)
        ages.append(i % 100)
        regions.append(region_choices[i % 3])
        scores.append(float(i % 100) + 0.5)
        statuses.append(i % 6)
    return pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "age": pa.array(ages, type=pa.int64()),
        "region": pa.array(regions, type=pa.string()),
        "score": pa.array(scores, type=pa.float64()),
        "status": pa.array(statuses, type=pa.int64()),
    })


def count_reads(kernel: PondMinimal) -> int:
    """Get the current kernel read count."""
    return kernel.stats.get("reads", 0)


def reset_reads(kernel: PondMinimal) -> None:
    """Reset the kernel read counter."""
    kernel.stats["reads"] = 0


def benchmark_write_and_read():
    """Run the full benchmark: write + read with both paths."""
    tmp = tempfile.mkdtemp(prefix="pond-rt-bench-")
    kernel = PondMinimal(tmp)
    lens = LakehouseLens(kernel)

    table = make_test_table(n_rows=10_000, n_groups=100)
    n_groups = 100

    print("\n" + "=" * 78)
    print("ROUND-TRIP BENCHMARK: CollectionManifest vs ZoneMapIndex")
    print("=" * 78)
    print(f"\nWorkload: 10,000 rows in {n_groups} row groups (100 rows/group)")
    print(f"Columns: id (int), age (int 0-99), region (str ASIA/EU/US),")
    print(f"         score (float), status (int 0-5)")
    print(f"\nKernel: {kernel.base_dir}")
    print()

    # ----- Write modes -----
    write_modes = [
        ("range_write", "Whole-blob Parquet"),
        ("range_write_column_chunks", "Column-chunk Parquet"),
        ("range_write_encoded", "Encoded (PND1)"),
    ]

    results: dict[str, dict] = {}

    for write_method, write_label in write_modes:
        print(f"\n--- Write mode: {write_label} ---")
        collection = f"bench_{write_method}"

        # Write
        reset_reads(kernel)
        t0 = time.perf_counter()
        getattr(lens, write_method)(
            collection, table, key_col="id", row_group_size=100)
        write_time = time.perf_counter() - t0
        write_reads = count_reads(kernel)

        # Verify manifest was built
        manifest_hash = kernel.resolve(f"collections/{collection}/manifest")
        has_manifest = manifest_hash is not None
        print(f"  Write: {write_time*1000:.1f}ms, {write_reads} reads, "
              f"manifest={'yes' if has_manifest else 'NO'}")

        # Clear lens cache before each read benchmark
        lens._cached_tables.clear()

        # ----- Read benchmarks -----

        # 1. Point lookup
        # NOTE: range_point_lookup (old path) only works on whole-blob mode
        # because it tries to decode the blob as Parquet. For column-chunk
        # and encoded modes, the blob is a JSON manifest, not Parquet.
        # We skip the old-path point lookup for those modes.
        if write_method == "range_write":
            reset_reads(kernel)
            t0 = time.perf_counter()
            result_old = lens.range_point_lookup(collection, key=5555)
            old_time = time.perf_counter() - t0
            old_reads = count_reads(kernel)
        else:
            old_reads, old_time, result_old = None, None, None

        reset_reads(kernel)
        t0 = time.perf_counter()
        result_new = lens.range_point_lookup_via_manifest(collection, key=5555)
        new_time = time.perf_counter() - t0
        new_reads = count_reads(kernel)

        print(f"\n  Point lookup (key=5555):")
        if old_reads is not None:
            print(f"    ZoneMap path:  {old_reads} reads, {old_time*1000:.2f}ms")
            print(f"    Manifest path: {new_reads} reads, {new_time*1000:.2f}ms")
            print(f"    Savings:       {old_reads - new_reads} reads "
                  f"({(old_reads - new_reads) / max(old_reads, 1) * 100:.0f}%)")
        else:
            print(f"    Manifest path: {new_reads} reads, {new_time*1000:.2f}ms")
            print(f"    (ZoneMap path N/A for {write_label} — pre-existing limitation)")

        # 2. Full scan
        # Clear cache between reads
        lens._cached_tables.clear()
        reset_reads(kernel)
        t0 = time.perf_counter()
        result_old = lens.read_table(collection)
        old_time = time.perf_counter() - t0
        old_reads = count_reads(kernel)

        lens._cached_tables.clear()
        reset_reads(kernel)
        t0 = time.perf_counter()
        result_new = lens.read_table_via_manifest(collection)
        new_time = time.perf_counter() - t0
        new_reads = count_reads(kernel)

        print(f"\n  Full scan (read all {n_groups} row groups):")
        print(f"    ZoneMap path:  {old_reads} reads, {old_time*1000:.2f}ms, "
              f"{result_old.num_rows} rows")
        print(f"    Manifest path: {new_reads} reads, {new_time*1000:.2f}ms, "
              f"{result_new.num_rows} rows")
        print(f"    Savings:       {old_reads - new_reads} reads "
              f"({(old_reads - new_reads) / max(old_reads, 1) * 100:.0f}%)")

        # 3. Predicate-pruned read (1/100 selectivity)
        # Predicate: id > 9900 → only the last group survives
        predicates = [("id", ">", 9900)]

        # Estimate via manifest (no actual read)
        rt_estimate = lens.get_manifest_round_trip_count(collection, predicates)
        print(f"\n  Pruned read (id > 9900 — 1/{n_groups} selectivity):")
        print(f"    Manifest estimate: {rt_estimate}")

        lens._cached_tables.clear()
        reset_reads(kernel)
        t0 = time.perf_counter()
        try:
            result_old = lens.read_with_encoded_pruning(
                collection, predicates=predicates, columns=["id", "age"])
            old_time = time.perf_counter() - t0
            old_reads = count_reads(kernel)
            old_rows = result_old.num_rows
        except Exception as exc:
            old_time = 0
            old_reads = 0
            old_rows = 0
            print(f"    ZoneMap path failed: {type(exc).__name__}: {exc}")

        lens._cached_tables.clear()
        reset_reads(kernel)
        t0 = time.perf_counter()
        result_new = lens.read_with_pruning_via_manifest(
            collection, predicates=predicates, columns=["id", "age"])
        new_time = time.perf_counter() - t0
        new_reads = count_reads(kernel)

        print(f"    ZoneMap path:  {old_reads} reads, {old_time*1000:.2f}ms, "
              f"{old_rows} rows")
        print(f"    Manifest path: {new_reads} reads, {new_time*1000:.2f}ms, "
              f"{result_new.num_rows} rows")
        print(f"    Savings:       {old_reads - new_reads} reads "
              f"({(old_reads - new_reads) / max(old_reads, 1) * 100:.0f}%)")

        # Verify both paths return the same data
        if old_rows != result_new.num_rows:
            print(f"    WARNING: row count mismatch! "
                  f"old={old_rows}, new={result_new.num_rows}")
        else:
            print(f"    ✓ Both paths return {result_new.num_rows} rows")

        results[write_method] = {
            "point_lookup": (old_reads or 0, new_reads),
            "full_scan": (old_reads, new_reads),
            "pruned_read": (old_reads, new_reads),
        }

    # ----- Summary -----
    print("\n" + "=" * 78)
    print("SUMMARY: S3 GETs per interaction (zone-map path → manifest path)")
    print("=" * 78)
    print(f"\n{'Interaction':<30} {'Whole-blob':<20} {'Column-chunk':<20} {'Encoded':<20}")
    print("-" * 90)

    for interaction in ["point_lookup", "full_scan", "pruned_read"]:
        row = f"{interaction:<30}"
        for write_method, _ in write_modes:
            old_r, new_r = results[write_method][interaction]
            row += f" {old_r:>3} → {new_r:<3} ({old_r - new_r:+d})    "
        print(row)

    print("\n" + "-" * 90)
    print("Key: 'X → Y (Z)' means X reads on zone-map path, Y reads on manifest")
    print("     path, Z = savings (negative means manifest is slower).")
    print()

    shutil.rmtree(tmp, ignore_errors=True)


def benchmark_point_lookup_scaling():
    """Show that manifest point lookup stays at 3 reads regardless of scale."""
    tmp = tempfile.mkdtemp(prefix="pond-scale-bench-")
    kernel = PondMinimal(tmp)
    lens = LakehouseLens(kernel)

    print("\n" + "=" * 78)
    print("POINT LOOKUP SCALING: manifest path stays at 3 reads (O(1))")
    print("=" * 78)
    print(f"\n{'Row groups':<15} {'Total rows':<15} {'Manifest reads':<18} {'ZoneMap reads':<18}")
    print("-" * 65)

    for n_groups in [10, 100, 1000]:
        n_rows = n_groups * 100
        table = make_test_table(n_rows=n_rows, n_groups=n_groups)
        collection = f"scale_{n_groups}"

        # Write
        lens.range_write(collection, table, key_col="id",
                          row_group_size=100)

        # Point lookup via manifest
        reset_reads(kernel)
        lens.range_point_lookup_via_manifest(collection, key=n_rows // 2)
        manifest_reads = count_reads(kernel)

        # Point lookup via zone map (old path)
        reset_reads(kernel)
        lens.range_point_lookup(collection, key=n_rows // 2)
        zm_reads = count_reads(kernel)

        print(f"{n_groups:<15} {n_rows:<15} {manifest_reads:<18} {zm_reads:<18}")

    print()
    shutil.rmtree(tmp, ignore_errors=True)


def benchmark_manifest_size():
    """Show that manifest size stays in the single-fetch sweet spot."""
    tmp = tempfile.mkdtemp(prefix="pond-size-bench-")
    kernel = PondMinimal(tmp)
    lens = LakehouseLens(kernel)

    print("\n" + "=" * 78)
    print("MANIFEST SIZE: stays under 1MB (S3 single-fetch sweet spot)")
    print("=" * 78)
    print(f"\n{'Row groups':<15} {'Manifest size':<20} {'Per row group':<20} {'Under 1MB?':<15}")
    print("-" * 70)

    for n_groups in [10, 100, 1000, 10_000]:
        n_rows = n_groups * 100
        table = make_test_table(n_rows=n_rows, n_groups=n_groups)
        collection = f"size_{n_groups}"

        lens.range_write(collection, table, key_col="id",
                          row_group_size=100)

        manifest_hash = kernel.resolve(f"collections/{collection}/manifest")
        if manifest_hash:
            blob_path = kernel._blob_path(manifest_hash)
            size = os.path.getsize(blob_path)
            print(f"{n_groups:<15} {size:>8,} bytes    {size // n_groups:>5} B/rg        "
                  f"{'✓' if size < 1_000_000 else '✗'}")

    print()
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    try:
        benchmark_write_and_read()
        benchmark_point_lookup_scaling()
        benchmark_manifest_size()
        print("=" * 78)
        print("ALL BENCHMARKS COMPLETE")
        print("=" * 78)
        sys.exit(0)
    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
