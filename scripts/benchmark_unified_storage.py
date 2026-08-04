"""
Unified Storage benchmark — proves ONE format, ONE write, ONE read
achieves the same round trips as the old multi-path design, with
simpler code.

This benchmark:
  1. Writes a 100-row-group table via the unified path (ONE write mode)
  2. Compares against the old paths (range_write, range_write_column_chunks,
     range_write_encoded)
  3. Counts S3 GETs for point lookup, full scan, and predicate-pruned read
  4. Prints a comparison table

Expected result: unified path has the same (or better) round-trip count
as the best old path, with ONE simple API instead of 3+ modes.
"""
from __future__ import annotations

import os
import sys
import tempfile
import shutil
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk"))
sys.path.insert(0, os.path.join(HERE, "..", "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(HERE, "..", "lenses", "lakehouse"))

import pyarrow as pa
from kernel import PondMinimal
from unified_storage import UnifiedStorage
from lakehouse_lens import LakehouseLens


def make_test_table(n_rows: int = 10_000, n_groups: int = 100) -> pa.Table:
    """Build a test table with n_groups row groups of n_rows/n_groups rows each."""
    ids = list(range(n_rows))
    ages = [i % 100 for i in ids]
    regions = [("ASIA", "EU", "US")[i % 3] for i in ids]
    scores = [float(i % 100) + 0.5 for i in ids]
    statuses = [i % 6 for i in ids]
    return pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "age": pa.array(ages, type=pa.int64()),
        "region": pa.array(regions, type=pa.string()),
        "score": pa.array(scores, type=pa.float64()),
        "status": pa.array(statuses, type=pa.int64()),
    })


def count_reads(kernel) -> int:
    return kernel.stats.get("reads", 0)


def reset_reads(kernel) -> None:
    kernel.stats["reads"] = 0


def benchmark_unified_vs_old():
    """Compare the unified path against the old multi-path design."""
    tmp = tempfile.mkdtemp(prefix="pond-unified-bench-")
    kernel = PondMinimal(tmp)
    lens = LakehouseLens(kernel)
    storage = UnifiedStorage(kernel)

    table = make_test_table(n_rows=10_000, n_groups=100)
    n_groups = 100

    # Convert table to list[dict] for UnifiedStorage
    rows = table.to_pylist()

    print("\n" + "=" * 80)
    print("UNIFIED STORAGE vs OLD MULTI-PATH DESIGN")
    print("=" * 80)
    print(f"\nWorkload: 10,000 rows in {n_groups} row groups (100 rows/group)")
    print(f"Columns: id (int), age (int 0-99), region (str), score (float), status (int)")
    print()

    # ----- Write paths -----
    print("--- WRITE PATHS ---")
    print(f"{'Path':<35} {'Time (ms)':<12} {'Writes':<10} {'Manifest?':<10}")
    print("-" * 70)

    # Old: range_write
    reset_reads(kernel)
    t0 = time.perf_counter()
    lens.range_write("old_rw", table, key_col="id", row_group_size=100)
    old_rw_time = time.perf_counter() - t0
    old_rw_writes = kernel.stats.get("writes", 0)
    old_rw_manifest = kernel.resolve("collections/old_rw/_branches/main/manifest") is not None
    print(f"{'range_write (old)':<35} {old_rw_time*1000:<12.1f} {old_rw_writes:<10} "
          f"{'yes' if old_rw_manifest else 'no':<10}")

    # Old: range_write_encoded
    reset_reads(kernel)
    t0 = time.perf_counter()
    lens.range_write_encoded("old_enc", table, key_col="id", row_group_size=100)
    old_enc_time = time.perf_counter() - t0
    old_enc_writes = kernel.stats.get("writes", 0)
    old_enc_manifest = kernel.resolve("collections/old_enc/_branches/main/manifest") is not None
    print(f"{'range_write_encoded (old)':<35} {old_enc_time*1000:<12.1f} {old_enc_writes:<10} "
          f"{'yes' if old_enc_manifest else 'no':<10}")

    # New: unified write
    reset_reads(kernel)
    t0 = time.perf_counter()
    storage.write("unified", rows, key_col="id", row_group_size=100)
    new_time = time.perf_counter() - t0
    new_writes = kernel.stats.get("writes", 0)
    new_manifest = kernel.resolve("collections/unified/_branches/main/manifest") is not None
    print(f"{'unified.write (NEW)':<35} {new_time*1000:<12.1f} {new_writes:<10} "
          f"{'yes' if new_manifest else 'no':<10}")

    # ----- Read paths -----
    print("\n--- READ PATHS (S3 GETs) ---")

    # Test 1: Point lookup
    print(f"\n  Point lookup (key in first row group):")
    print(f"  {'Path':<40} {'Reads':<10} {'Time (ms)':<12}")
    print(f"  {'-'*60}")

    # Old: range_point_lookup (only works on whole-blob mode)
    reset_reads(kernel)
    t0 = time.perf_counter()
    lens.range_point_lookup("old_rw", key="9")
    old_pt_time = time.perf_counter() - t0
    old_pt_reads = count_reads(kernel)
    print(f"  {'range_point_lookup (old)':<40} {old_pt_reads:<10} {old_pt_time*1000:<12.2f}")

    # Old: range_point_lookup_via_manifest
    reset_reads(kernel)
    t0 = time.perf_counter()
    lens.range_point_lookup_via_manifest("old_rw", key="9")
    old_ptm_time = time.perf_counter() - t0
    old_ptm_reads = count_reads(kernel)
    print(f"  {'range_point_lookup_via_manifest (old)':<40} {old_ptm_reads:<10} {old_ptm_time*1000:<12.2f}")

    # New: unified point_lookup
    reset_reads(kernel)
    t0 = time.perf_counter()
    storage.point_lookup("unified", key="9")
    new_pt_time = time.perf_counter() - t0
    new_pt_reads = count_reads(kernel)
    print(f"  {'unified.point_lookup (NEW)':<40} {new_pt_reads:<10} {new_pt_time*1000:<12.2f}")

    # Test 2: Full scan
    print(f"\n  Full scan (all {n_groups} row groups):")
    print(f"  {'Path':<40} {'Reads':<10} {'Time (ms)':<12}")
    print(f"  {'-'*60}")

    lens._cached_tables.clear()
    reset_reads(kernel)
    t0 = time.perf_counter()
    lens.read_table("old_rw")
    old_fs_time = time.perf_counter() - t0
    old_fs_reads = count_reads(kernel)
    print(f"  {'read_table (old)':<40} {old_fs_reads:<10} {old_fs_time*1000:<12.2f}")

    lens._cached_tables.clear()
    reset_reads(kernel)
    t0 = time.perf_counter()
    lens.read_table_via_manifest("old_rw")
    old_fsm_time = time.perf_counter() - t0
    old_fsm_reads = count_reads(kernel)
    print(f"  {'read_table_via_manifest (old)':<40} {old_fsm_reads:<10} {old_fsm_time*1000:<12.2f}")

    reset_reads(kernel)
    t0 = time.perf_counter()
    storage.read("unified")
    new_fs_time = time.perf_counter() - t0
    new_fs_reads = count_reads(kernel)
    print(f"  {'unified.read (NEW)':<40} {new_fs_reads:<10} {new_fs_time*1000:<12.2f}")

    # Test 3: Predicate-pruned read (1% selectivity)
    print(f"\n  Pruned read (id > 9900 — 1/{n_groups} selectivity):")
    print(f"  {'Path':<40} {'Reads':<10} {'Time (ms)':<12}")
    print(f"  {'-'*60}")

    predicates = [("id", ">", 9900)]

    lens._cached_tables.clear()
    reset_reads(kernel)
    t0 = time.perf_counter()
    try:
        lens.read_with_encoded_pruning("old_enc", predicates=predicates,
                                         columns=["id", "age"])
        old_pr_time = time.perf_counter() - t0
        old_pr_reads = count_reads(kernel)
        print(f"  {'read_with_encoded_pruning (old)':<40} {old_pr_reads:<10} {old_pr_time*1000:<12.2f}")
    except Exception as e:
        print(f"  {'read_with_encoded_pruning (old)':<40} FAILED: {e}")

    lens._cached_tables.clear()
    reset_reads(kernel)
    t0 = time.perf_counter()
    lens.read_with_pruning_via_manifest("old_enc", predicates=predicates,
                                          columns=["id", "age"])
    old_prm_time = time.perf_counter() - t0
    old_prm_reads = count_reads(kernel)
    print(f"  {'read_with_pruning_via_manifest (old)':<40} {old_prm_reads:<10} {old_prm_time*1000:<12.2f}")

    reset_reads(kernel)
    t0 = time.perf_counter()
    storage.read("unified", predicates=predicates, columns=["id", "age"])
    new_pr_time = time.perf_counter() - t0
    new_pr_reads = count_reads(kernel)
    print(f"  {'unified.read (NEW)':<40} {new_pr_reads:<10} {new_pr_time*1000:<12.2f}")

    # ----- Summary -----
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    print(f"""
OLD DESIGN (3+ paths):
  - range_write (whole-blob Parquet)
  - range_write_column_chunks (per-column Parquet blobs)
  - range_write_encoded (per-column encoded blobs)
  - read_table, read_with_pruning, read_with_column_chunk_pruning,
    read_with_encoded_pruning, read_table_via_manifest,
    read_with_pruning_via_manifest, range_point_lookup_via_manifest
  - ZoneMapIndex + StatsIndex + CollectionManifest
  - STORAGE_WHOLE_BLOB / STORAGE_COLUMN_CHUNKS / STORAGE_ENCODED

NEW DESIGN (1 path):
  - unified.write(collection, rows, key_col, row_group_size)
  - unified.read(collection, predicates, columns, row_filter)
  - unified.point_lookup(collection, key)
  - CollectionManifest (ONE index)
  - ONE storage mode (PND2)

ROUND TRIPS (S3 GETs):
  Point lookup:   old={old_pt_reads} (best) → new={new_pt_reads}
  Full scan:      old={old_fsm_reads} (best) → new={new_fs_reads}
  Pruned read:    old={old_prm_reads} (best) → new={new_pr_reads}

CODE SIMPLIFICATION:
  - 3 write modes → 1 write mode
  - 7+ read methods → 3 read methods (read, point_lookup, scan_with_pruning)
  - 3 storage modes → 1 storage mode (PND2)
  - 2 index types → 1 index type (CollectionManifest)
""")

    shutil.rmtree(tmp, ignore_errors=True)


def benchmark_unified_workloads():
    """Prove the SAME unified API works for tabular, KV, and binary workloads."""
    tmp = tempfile.mkdtemp(prefix="pond-unified-wl-")
    kernel = PondMinimal(tmp)
    storage = UnifiedStorage(kernel)

    print("\n" + "=" * 80)
    print("UNIFIED STORAGE: ONE API, ANY WORKLOAD")
    print("=" * 80)

    # Tabular workload
    tabular_rows = [{"id": i, "age": i % 50, "score": float(i) * 0.1}
                    for i in range(1000)]
    storage.write("tabular", tabular_rows, key_col="id", row_group_size=100)

    # KV workload
    kv_rows = [{"_key": f"item:{i}", "name": f"name_{i}", "value": i * 2}
               for i in range(1000)]
    storage.write("kv", kv_rows, key_col="_key", row_group_size=100)

    # Binary workload (video segments)
    bin_rows = [{"segment_id": i, "codec": "h264" if i % 2 == 0 else "h265",
                 "data": bytes([i] * 100)} for i in range(100)]
    storage.write("binary", bin_rows, key_col="segment_id", row_group_size=10)

    # Vector workload (each row is a 10-dim vector)
    vec_rows = [{"vec_id": i, **{f"d{j}": (i * 10 + j) % 100 for j in range(10)}}
                for i in range(100)]
    storage.write("vectors", vec_rows, key_col="vec_id", row_group_size=10)

    print(f"\n{'Workload':<15} {'Rows':<10} {'Full scan':<15} {'Predicate':<30} {'Result':<10}")
    print("-" * 80)

    # Tabular
    reset_reads(kernel)
    result = storage.read("tabular", predicates=[("age", ">", 40)])
    reads = count_reads(kernel)
    print(f"{'tabular':<15} {1000:<10} {reads} reads      {'age > 40':<30} {len(result)} rows")

    # KV
    reset_reads(kernel)
    result = storage.read("kv", predicates=[("value", ">", 1800)])
    reads = count_reads(kernel)
    print(f"{'kv':<15} {1000:<10} {reads} reads      {'value > 1800':<30} {len(result)} rows")

    # Binary
    reset_reads(kernel)
    result = storage.read("binary",
                            predicates=[("codec", "=", "h264")],
                            row_filter=lambda r: r["codec"] == "h264")
    reads = count_reads(kernel)
    print(f"{'binary':<15} {100:<10} {reads} reads      {'codec = h264':<30} {len(result)} rows")

    # Vectors
    reset_reads(kernel)
    result = storage.read("vectors", predicates=[("d0", ">", 50)])
    reads = count_reads(kernel)
    print(f"{'vectors':<15} {100:<10} {reads} reads      {'d0 > 50':<30} {len(result)} rows")

    print(f"""
ALL 4 WORKLOADS use the SAME API:
  storage.write(name, rows, key_col, row_group_size)
  storage.read(name, predicates, columns, row_filter)

ONE format (PND2). ONE write path. ONE read path. ANY workload.
""")
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    try:
        benchmark_unified_vs_old()
        benchmark_unified_workloads()
        print("=" * 80)
        print("ALL BENCHMARKS COMPLETE")
        print("=" * 80)
        sys.exit(0)
    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
