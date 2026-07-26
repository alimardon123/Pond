#!/usr/bin/env python3
"""
Benchmark: O(changed) incremental refresh vs O(N) full rebuild.

Creates a 10K-row dataset, then modifies a small number of rows.
Measures the time difference between:
  1. refresh_index_incremental (O(changed) via commit-diff)
  2. build_index (O(N) full rebuild)
  3. refresh_index (O(N) scan + compare)
"""

import os, sys, time, json, shutil, tempfile

HERE = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE))) if '__file__' in dir() else os.getcwd()
if not os.path.exists(os.path.join(REPO, "pond-core")):
    REPO = os.getcwd()
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "indexing"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))

from kernel import PondMinimal
from keyvalue_lens import KeyValueLens
from collection_metadata import CollectionMetadata


def run_benchmark():
    print("=" * 70)
    print("Benchmark: Incremental Refresh vs Full Rebuild")
    print("=" * 70)

    for N in [1_000, 10_000, 100_000]:
        run_single(N)


def run_single(N: int):
    """Run benchmark for a given dataset size."""
    print(f"\n{'─' * 70}")
    print(f"  Dataset: {N:,} rows, modify 10 (0.1% for 10K, less for larger)")
    print(f"{'─' * 70}")

    tmpdir = tempfile.mkdtemp(prefix=f"pond_inc_{N}_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = KeyValueLens(kernel)
        meta = CollectionMetadata(kernel)

        CHANGED = 10
        extractor = lambda r: str(r.get("val", 0))
        scan_fn = lambda: ((k, lens.get("users", k)) for k in lens.keys("users"))

        # Write N rows
        print(f"  Writing {N:,} rows...", end=" ")
        t0 = time.perf_counter()
        for i in range(N):
            lens.put("users", f"u{i:06d}", {"id": i, "val": i * 10})
        old_commit = lens.commit("users", f"insert {N} users")
        t_write = time.perf_counter() - t0
        print(f"{t_write*1000:.0f}ms")

        # Build initial index
        t0 = time.perf_counter()
        meta.build_index("users", "by_val", extractor=extractor, scan_fn=scan_fn)
        t_build = time.perf_counter() - t0

        # Modify 10 rows
        for i in range(CHANGED):
            lens.put("users", f"u{i:06d}", {"id": i, "val": i * 100})
        lens.put("users", "u_new", {"id": 999999, "val": 9999990})
        new_commit = lens.commit("users", f"modify {CHANGED} rows")

        # Method 1: refresh_index_incremental (O(changed))
        t0 = time.perf_counter()
        meta.refresh_index_incremental("users", "by_val",
            extractor=extractor, old_commit=old_commit,
            new_commit=new_commit, decode_fn=lambda b: json.loads(b))
        t_inc = time.perf_counter() - t0

        # Verify
        assert meta.lookup_index("users", "by_val", "0") is not None
        assert meta.lookup_index("users", "by_val", "9999990") is not None

        # Method 2: build_index (O(N) full rebuild)
        t0 = time.perf_counter()
        meta.build_index("users", "by_val", extractor=extractor, scan_fn=scan_fn)
        t_full = time.perf_counter() - t0

        speedup = t_full / t_inc if t_inc > 0 else 0
        print(f"  refresh_index_incremental: {t_inc*1000:>8.1f}ms")
        print(f"  build_index (full rebuild): {t_full*1000:>8.1f}ms")
        print(f"  Speedup: {speedup:.1f}x")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    return speedup


if __name__ == "__main__":
    run_benchmark()
