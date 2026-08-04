"""
Round 19 Benchmarks — comprehensive performance + correctness at scale.

Runs 6 benchmark suites:
  1. Cold point lookup at 10/100/1000 row groups
  2. Pruned read (1% selectivity) at 10/100/1000 row groups
  3. Full scan at 10/100/1000 row groups
  4. Append performance (1000 row groups + append 1)
  5. Multi-predicate read correctness (4 read paths)
  6. Range scan correctness at row-group boundaries

Reports GETs, PUTs, wall-clock, and correctness checks.
"""
from __future__ import annotations

import os
import sys
import time
import shutil
import tempfile
from typing import Any

# Make all paths importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "lakehouse"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))

from kernel import PondMinimal
from pond_storage import PondStorage


# ---------------------------------------------------------------------------
# Counting kernel wrapper — counts GETs and PUTs
# ---------------------------------------------------------------------------

class CountingKernel:
    """Wraps a PondMinimal kernel and counts read_blob/write/reference calls."""

    def __init__(self, inner):
        self.inner = inner
        self.get_count = 0
        self.put_count = 0
        self.ref_count = 0
        self._latency_ms = 0.0  # simulated per-GET latency

    def reset(self):
        self.get_count = 0
        self.put_count = 0
        self.ref_count = 0

    def set_latency(self, ms: float):
        self._latency_ms = ms

    def write(self, data: bytes) -> str:
        self.put_count += 1
        if self._latency_ms:
            time.sleep(self._latency_ms / 1000.0)
        return self.inner.write(data)

    def read_blob(self, h: str) -> bytes:
        self.get_count += 1
        if self._latency_ms:
            time.sleep(self._latency_ms / 1000.0)
        return self.inner.read_blob(h)

    def resolve(self, ref: str):
        # ref resolution is typically SDK-cached, but count it
        self.get_count += 1
        if self._latency_ms:
            time.sleep(self._latency_ms / 1000.0)
        return self.inner.resolve(ref)

    def reference(self, ref: str, h: str) -> None:
        self.ref_count += 1
        return self.inner.reference(ref, h)

    def __getattr__(self, name):
        return getattr(self.inner, name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_rows(n: int, start: int = 0) -> list[dict]:
    """Make n rows with id, name, age."""
    return [
        {"id": start + i, "name": f"user{start + i}", "age": 20 + (start + i) % 60}
        for i in range(n)
    ]


def fmt_ms(seconds: float) -> str:
    return f"{seconds * 1000:.1f}ms"


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

def bench_1_cold_point_lookup(counting: CountingKernel, storage: PondStorage):
    """Benchmark 1: Cold point lookup at 10/100/1000 row groups."""
    print("\n" + "=" * 78)
    print("BENCHMARK 1: Cold Point Lookup")
    print("=" * 78)
    print(f"{'n_groups':>10} | {'GETs':>6} | {'0ms wall':>10} | {'50ms wall':>11}")
    print("-" * 78)

    for n_groups in [10, 100, 1000]:
        # Fresh kernel for each scale
        tmp = tempfile.mkdtemp(prefix=f"pond-b1-{n_groups}-")
        inner = PondMinimal(tmp)
        kernel = CountingKernel(inner)
        storage = PondStorage(kernel)

        n_rows = n_groups * 100
        rows = make_rows(n_rows)
        storage.write(f"t{n_groups}", rows, key_col="id", row_group_size=100)

        # Cold lookup — invalidate caches
        target_key = str(n_rows // 2)  # midpoint
        # Manifest cache
        storage._unified._invalidate_manifest_cache(f"t{n_groups}")

        # 0ms latency
        kernel.set_latency(0.0)
        kernel.reset()
        t0 = time.perf_counter()
        row = storage.point_lookup(f"t{n_groups}", key=target_key)
        t1 = time.perf_counter()
        gets_0 = kernel.get_count
        wall_0 = t1 - t0
        assert row is not None, f"Point lookup returned None for key={target_key}"

        # 50ms latency
        kernel.set_latency(50.0)
        storage._unified._invalidate_manifest_cache(f"t{n_groups}")
        kernel.reset()
        t0 = time.perf_counter()
        row = storage.point_lookup(f"t{n_groups}", key=target_key)
        t1 = time.perf_counter()
        gets_50 = kernel.get_count
        wall_50 = t1 - t0
        kernel.set_latency(0.0)

        print(f"{n_groups:>10} | {gets_0:>6} | {fmt_ms(wall_0):>10} | {fmt_ms(wall_50):>11}")

        shutil.rmtree(tmp, ignore_errors=True)


def bench_2_pruned_read(counting: CountingKernel, storage: PondStorage):
    """Benchmark 2: Pruned read (1% selectivity) at 10/100/1000 row groups."""
    print("\n" + "=" * 78)
    print("BENCHMARK 2: Pruned Read (1% selectivity)")
    print("=" * 78)
    print(f"{'n_groups':>10} | {'GETs':>6} | {'surviving':>10} | {'wall':>10}")
    print("-" * 78)

    for n_groups in [10, 100, 1000]:
        tmp = tempfile.mkdtemp(prefix=f"pond-b2-{n_groups}-")
        inner = PondMinimal(tmp)
        kernel = CountingKernel(inner)
        storage = PondStorage(kernel)

        n_rows = n_groups * 100
        rows = make_rows(n_rows)
        storage.write(f"t{n_groups}", rows, key_col="id", row_group_size=100)

        # Use id > n_rows*0.99 for ~1% row selectivity AND row-group pruning
        # (only the last row group survives — proves pruning works)
        threshold = int(n_rows * 0.99)
        storage._unified._invalidate_manifest_cache(f"t{n_groups}")
        kernel.reset()
        t0 = time.perf_counter()
        result = storage.read(f"t{n_groups}", predicates=[("id", ">", threshold)])
        t1 = time.perf_counter()
        gets = kernel.get_count
        wall = t1 - t0

        # Count surviving row groups (via get_round_trip_count)
        rt = storage.get_round_trip_count(f"t{n_groups}", predicates=[("id", ">", threshold)])
        surviving = rt["data_blob_fetches"]

        print(f"{n_groups:>10} | {gets:>6} | {surviving:>10} | {fmt_ms(wall):>10}")

        shutil.rmtree(tmp, ignore_errors=True)


def bench_3_full_scan(counting: CountingKernel, storage: PondStorage):
    """Benchmark 3: Full scan at 10/100/1000 row groups."""
    print("\n" + "=" * 78)
    print("BENCHMARK 3: Full Scan")
    print("=" * 78)
    print(f"{'n_groups':>10} | {'GETs':>6} | {'rows':>8} | {'wall':>10}")
    print("-" * 78)

    for n_groups in [10, 100, 1000]:
        tmp = tempfile.mkdtemp(prefix=f"pond-b3-{n_groups}-")
        inner = PondMinimal(tmp)
        kernel = CountingKernel(inner)
        storage = PondStorage(kernel)

        n_rows = n_groups * 100
        rows = make_rows(n_rows)
        storage.write(f"t{n_groups}", rows, key_col="id", row_group_size=100)

        storage._unified._invalidate_manifest_cache(f"t{n_groups}")
        kernel.reset()
        t0 = time.perf_counter()
        result = storage.read(f"t{n_groups}")
        t1 = time.perf_counter()
        gets = kernel.get_count
        wall = t1 - t0

        print(f"{n_groups:>10} | {gets:>6} | {len(result):>8} | {fmt_ms(wall):>10}")

        shutil.rmtree(tmp, ignore_errors=True)


def bench_4_append_performance(counting: CountingKernel, storage: PondStorage):
    """Benchmark 4: Append performance — 1000 row groups, then append 1."""
    print("\n" + "=" * 78)
    print("BENCHMARK 4: Append Performance (1000 row groups + append 1)")
    print("=" * 78)

    tmp = tempfile.mkdtemp(prefix="pond-b4-")
    inner = PondMinimal(tmp)
    kernel = CountingKernel(inner)
    storage = PondStorage(kernel)

    # Initial write: 1000 row groups
    n_initial = 1000 * 100
    rows = make_rows(n_initial)
    kernel.reset()
    t0 = time.perf_counter()
    storage.write("big", rows, key_col="id", row_group_size=100)
    t1 = time.perf_counter()
    initial_puts = kernel.put_count
    initial_gets = kernel.get_count
    initial_refs = kernel.ref_count
    initial_wall = t1 - t0
    print(f"Initial write: {initial_puts} PUTs, {initial_gets} GETs, {initial_refs} refs, {fmt_ms(initial_wall)}")

    # Append 1 row group
    new_rows = make_rows(100, start=n_initial)
    storage._unified._invalidate_manifest_cache("big")
    kernel.reset()
    t0 = time.perf_counter()
    storage.append("big", new_rows, key_col="id", row_group_size=100)
    t1 = time.perf_counter()
    append_puts = kernel.put_count
    append_gets = kernel.get_count
    append_refs = kernel.ref_count
    append_wall = t1 - t0
    print(f"Append 1 RG:   {append_puts} PUTs, {append_gets} GETs, {append_refs} refs, {fmt_ms(append_wall)}")

    # Manifest size
    manifest = storage._unified._load_manifest("big")
    manifest_hash = inner.resolve(f"collections/big/_branches/main/manifest")
    manifest_bytes = inner.read_blob(manifest_hash)
    print(f"Manifest size: {len(manifest_bytes)} bytes ({len(manifest.row_groups)} row groups inline)")

    # Verify correctness — read all
    all_rows = storage.read("big")
    expected = n_initial + 100
    print(f"Correctness:   read {len(all_rows)} rows (expected {expected}) {'OK' if len(all_rows) == expected else 'FAIL'}")

    shutil.rmtree(tmp, ignore_errors=True)


def bench_5_multi_predicate_correctness(counting: CountingKernel, storage: PondStorage):
    """Benchmark 5: Multi-predicate read correctness across 4 read paths."""
    print("\n" + "=" * 78)
    print("BENCHMARK 5: Multi-Predicate Read Correctness")
    print("=" * 78)

    tmp = tempfile.mkdtemp(prefix="pond-b5-")
    inner = PondMinimal(tmp)
    kernel = CountingKernel(inner)
    storage = PondStorage(kernel)

    # Write 500 rows with id, age, city
    rows = []
    for i in range(500):
        city = "NYC" if i % 3 == 0 else ("LA" if i % 3 == 1 else "SF")
        rows.append({"id": i, "age": 20 + (i % 50), "city": city})
    storage.write("multi", rows, key_col="id", row_group_size=50)

    preds = [("age", ">", 30), ("city", "=", "NYC")]

    # Compute expected result manually
    expected_ids = sorted([r["id"] for r in rows if r["age"] > 30 and r["city"] == "NYC"])
    print(f"Expected: {len(expected_ids)} rows matching age>30 AND city=NYC")
    print(f"  Expected IDs (first 10): {expected_ids[:10]}")

    # Path 1: read() — list[dict]
    storage._unified._invalidate_manifest_cache("multi")
    kernel.reset()
    result_read = storage.read("multi", predicates=preds)
    ids_read = sorted([r["id"] for r in result_read])
    print(f"\nread() [list[dict]]:        {len(ids_read)} rows, {kernel.get_count} GETs")
    print(f"  IDs match expected: {ids_read == expected_ids}")

    # Path 2: read() + columns=["id"]
    storage._unified._invalidate_manifest_cache("multi")
    kernel.reset()
    result_read_proj = storage.read("multi", predicates=preds, columns=["id"])
    ids_read_proj = sorted([r["id"] for r in result_read_proj])
    # Verify projection: each row should only have "id" key
    proj_clean = all(set(r.keys()) == {"id"} for r in result_read_proj)
    print(f"\nread()+columns=['id']:      {len(ids_read_proj)} rows, {kernel.get_count} GETs")
    print(f"  IDs match expected: {ids_read_proj == expected_ids}")
    print(f"  Projection clean (only 'id' in rows): {proj_clean}")

    # Path 3: read_as_arrow()
    storage._unified._invalidate_manifest_cache("multi")
    kernel.reset()
    table = storage.read_as_arrow("multi", predicates=preds)
    ids_arrow = sorted(table.column("id").to_pylist())
    print(f"\nread_as_arrow():           {len(ids_arrow)} rows, {kernel.get_count} GETs")
    print(f"  IDs match expected: {ids_arrow == expected_ids}")

    # Path 4: iter_rows()
    storage._unified._invalidate_manifest_cache("multi")
    kernel.reset()
    ids_iter = []
    for batch in storage.iter_rows("multi", predicates=preds):
        ids_iter.extend([r["id"] for r in batch])
    ids_iter = sorted(ids_iter)
    print(f"\niter_rows():               {len(ids_iter)} rows, {kernel.get_count} GETs")
    print(f"  IDs match expected: {ids_iter == expected_ids}")

    all_match = (ids_read == expected_ids and
                 ids_read_proj == expected_ids and
                 ids_arrow == expected_ids and
                 ids_iter == expected_ids)
    print(f"\nAll 4 paths agree: {all_match}")

    shutil.rmtree(tmp, ignore_errors=True)


def bench_6_range_scan_boundaries(counting: CountingKernel, storage: PondStorage):
    """Benchmark 6: Range scan correctness at row-group boundaries."""
    print("\n" + "=" * 78)
    print("BENCHMARK 6: Range Scan at Row-Group Boundaries")
    print("=" * 78)

    tmp = tempfile.mkdtemp(prefix="pond-b6-")
    inner = PondMinimal(tmp)
    kernel = CountingKernel(inner)
    storage = PondStorage(kernel)

    # 10000 rows in 10 row groups of 1000 each
    rows = make_rows(10000)
    storage.write("ranged", rows, key_col="id", row_group_size=1000)
    print(f"Wrote 10000 rows in 10 row groups of 1000 each")

    # Range read [1500, 2500] — spans 2 row groups (groups 1 and 2: 1000-1999, 2000-2999)
    storage._unified._invalidate_manifest_cache("ranged")
    kernel.reset()

    # Format the keys
    from unified_storage import _format_rg_key
    sk = _format_rg_key(1500)
    ek = _format_rg_key(2500)

    result = storage.read("ranged", start_key=sk, end_key=ek)
    gets = kernel.get_count

    ids = sorted([r["id"] for r in result])
    expected_ids = list(range(1500, 2501))  # 1001 rows: 1500..2500 inclusive
    expected_count = 1001

    print(f"\nRange [1500, 2500]: {len(ids)} rows returned, {gets} GETs")
    print(f"  Expected: {expected_count} rows (ids 1500..2500)")
    print(f"  Count correct: {len(ids) == expected_count}")
    print(f"  IDs correct:    {ids == expected_ids}")
    if ids:
        print(f"  First ID: {ids[0]}, Last ID: {ids[-1]}")

    # Also test edge cases
    print(f"\nEdge cases:")
    # Range within a single row group
    result1 = storage.read("ranged", start_key=_format_rg_key(100), end_key=_format_rg_key(200))
    ids1 = sorted([r["id"] for r in result1])
    print(f"  [100, 200] (single RG): {len(ids1)} rows (expected 101), correct: {ids1 == list(range(100, 201))}")

    # Range spanning ALL row groups
    result2 = storage.read("ranged", start_key=_format_rg_key(0), end_key=_format_rg_key(9999))
    ids2 = sorted([r["id"] for r in result2])
    print(f"  [0, 9999] (all RGs):     {len(ids2)} rows (expected 10000), correct: {ids2 == list(range(0, 10000))}")

    # Range starting at boundary
    result3 = storage.read("ranged", start_key=_format_rg_key(1000), end_key=_format_rg_key(2000))
    ids3 = sorted([r["id"] for r in result3])
    print(f"  [1000, 2000] (boundary): {len(ids3)} rows (expected 1001), correct: {ids3 == list(range(1000, 2001))}")

    shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 78)
    print("ROUND 19 BENCHMARKS — Pond Storage (commit 3860fc4)")
    print("=" * 78)

    tmp_dummy = tempfile.mkdtemp(prefix="pond-init-")
    inner = PondMinimal(tmp_dummy)
    kernel = CountingKernel(inner)
    storage = PondStorage(kernel)
    shutil.rmtree(tmp_dummy, ignore_errors=True)

    bench_1_cold_point_lookup(kernel, storage)
    bench_2_pruned_read(kernel, storage)
    bench_3_full_scan(kernel, storage)
    bench_4_append_performance(kernel, storage)
    bench_5_multi_predicate_correctness(kernel, storage)
    bench_6_range_scan_boundaries(kernel, storage)

    print("\n" + "=" * 78)
    print("ALL BENCHMARKS COMPLETE")
    print("=" * 78)


if __name__ == "__main__":
    main()
