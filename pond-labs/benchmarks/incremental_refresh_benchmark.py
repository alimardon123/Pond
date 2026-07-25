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

    tmpdir = tempfile.mkdtemp(prefix="pond_inc_bench_")
    try:
        kernel = PondMinimal(tmpdir)
        lens = KeyValueLens(kernel)
        meta = CollectionMetadata(kernel)

        N = 10_000
        extractor = lambda r: str(r.get("val", 0))
        scan_fn = lambda: ((k, lens.get("users", k)) for k in lens.keys("users"))

        # Write N rows
        print(f"\n  Writing {N} rows...")
        t0 = time.perf_counter()
        for i in range(N):
            lens.put("users", f"u{i:05d}", {"id": i, "val": i * 10})
        old_commit = lens.commit("users", f"insert {N} users")
        t_write = time.perf_counter() - t0
        print(f"  Written in {t_write*1000:.0f}ms")

        # Build initial index
        print(f"  Building initial index...")
        t0 = time.perf_counter()
        meta.build_index("users", "by_val", extractor=extractor, scan_fn=scan_fn)
        t_build = time.perf_counter() - t0
        print(f"  Initial index built in {t_build*1000:.0f}ms")

        # Modify a small number of rows (10 out of 10K = 0.1%)
        CHANGED = 10
        print(f"\n  Modifying {CHANGED} rows (0.1% of {N})...")
        for i in range(CHANGED):
            lens.put("users", f"u{i:05d}", {"id": i, "val": i * 100})
        lens.delete("users", f"u{CHANGED:05d}")
        lens.put("users", f"u_new", {"id": 99999, "val": 999990})
        new_commit = lens.commit("users", f"modify {CHANGED} rows")

        # Method 1: refresh_index_incremental (O(changed))
        print(f"\n  Method 1: refresh_index_incremental (O(changed))...")
        t0 = time.perf_counter()
        meta.refresh_index_incremental("users", "by_val",
            extractor=extractor,
            old_commit=old_commit,
            new_commit=new_commit,
            decode_fn=lambda b: json.loads(b))
        t_inc = time.perf_counter() - t0
        print(f"  refresh_index_incremental: {t_inc*1000:.1f}ms")

        # Verify correctness
        rowid = meta.lookup_index("users", "by_val", "0")  # modified: val=0
        assert rowid is not None, "Incremental: val=0 not found"
        rowid_new = meta.lookup_index("users", "by_val", "999990")
        assert rowid_new is not None, "Incremental: new val=999990 not found"

        # Method 2: build_index (O(N) full rebuild)
        print(f"\n  Method 2: build_index (O(N) full rebuild)...")
        t0 = time.perf_counter()
        meta.build_index("users", "by_val", extractor=extractor, scan_fn=scan_fn)
        t_full = time.perf_counter() - t0
        print(f"  build_index: {t_full*1000:.1f}ms")

        # Method 3: refresh_index (O(N) scan + compare)
        print(f"\n  Method 3: refresh_index (O(N) scan + compare)...")
        t0 = time.perf_counter()
        meta.refresh_index("users", "by_val", extractor=extractor, scan_fn=scan_fn)
        t_refresh = time.perf_counter() - t0
        print(f"  refresh_index: {t_refresh*1000:.1f}ms")

        # Summary
        print(f"\n  {'Method':<35} {'Time (ms)':>10} {'Speedup vs full':>15}")
        print(f"  {'-'*35} {'-'*10} {'-'*15}")
        print(f"  {'refresh_index_incremental':<35} {t_inc*1000:>10.1f} {t_full/t_inc:>14.1f}x")
        print(f"  {'refresh_index (scan+compare)':<35} {t_refresh*1000:>10.1f} {t_full/t_refresh:>14.1f}x")
        print(f"  {'build_index (full rebuild)':<35} {t_full*1000:>10.1f} {'1.0x':>15}")

        print(f"\n  Key findings:")
        print(f"  - refresh_index_incremental is {t_full/t_inc:.1f}x faster than full rebuild")
        print(f"  - For 0.1% change rate ({CHANGED}/{N} rows), incremental wins")
        print(f"  - ProllyTree commit-diff identifies only changed keys")
        print(f"  - Structural sharing: unchanged index entries share tree nodes")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    run_benchmark()
