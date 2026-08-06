#!/usr/bin/env python3
"""
Benchmark: Rust (PyO3) vs Pure-Python vs C ABI (ctypes) PND2 decode paths.

Measures decode throughput for blobs of various sizes and column mixes to
answer three questions:

  1. How much faster is the Rust decoder than the pure-Python fallback?
     (Validates Design Goal 3.3 Performant: optimizations live above
     the core, but the core itself should not be pathologically slow.)
  2. How much overhead does PyO3 add vs direct C ABI access?
     (Quantifies the cost of Python-object conversion vs raw pointers.)
  3. How much does the batch string accessor help vs per-row access?
     (Validates the C ABI design for cross-language SDK consumers.)

Usage:
    PYTHONPATH=pond-sdk:pond-rust/target/release \
        python3 scripts/benchmark_decode_paths.py

Output: tables of (blob_size, column_mix, path) → rows/sec + MB/s.
"""
import os
import sys
import time
import ctypes
import statistics

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO_ROOT, "pond-rust", "target", "release"))

# ---------------------------------------------------------------------------
# Set up the three decode paths
# ---------------------------------------------------------------------------

# Path 1: PyO3 (pond_rust.decode — Rust + Python object conversion)
import pond_rust
PYO3_DECODE = pond_rust.decode

# Path 2: Pure Python (PND2.decode — no Rust)
from extensions.physical_structures.unified_storage import PND2, ColumnSource
PY_DECODE = PND2.decode

# Path 3: C ABI via ctypes (libpond_core.so — Rust + direct pointer access)
LIBPOND_CORE = ctypes.CDLL(os.path.join(
    REPO_ROOT, "pond-rust", "target", "release", "libpond_core.so"))

# Configure ctypes signatures
_p = LIBPOND_CORE
_p.pond_pnd2_decode.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
_p.pond_pnd2_decode.restype = ctypes.c_void_p
_p.pond_result_num_columns.argtypes = [ctypes.c_void_p]
_p.pond_result_num_columns.restype = ctypes.c_size_t
_p.pond_result_column_vtype.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
_p.pond_result_column_vtype.restype = ctypes.c_uint8
_p.pond_result_column_len.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
_p.pond_result_column_len.restype = ctypes.c_size_t
_p.pond_result_column_i64.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
_p.pond_result_column_i64.restype = ctypes.POINTER(ctypes.c_int64)
_p.pond_result_column_f64.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
_p.pond_result_column_f64.restype = ctypes.POINTER(ctypes.c_double)
_p.pond_result_column_str.argtypes = [
    ctypes.c_void_p, ctypes.c_size_t, ctypes.c_size_t]
_p.pond_result_column_str.restype = ctypes.c_char_p
_p.pond_result_column_str_array.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
_p.pond_result_column_str_array.restype = ctypes.POINTER(ctypes.c_char_p)
_p.pond_result_free.argtypes = [ctypes.c_void_p]
_p.pond_result_free.restype = None


def c_abi_decode_per_row_str(blob: bytes) -> int:
    """C ABI decode with PER-ROW string access (the slow path).

    Calls pond_result_column_str() once per string row — has O(N) FFI
    overhead per string column. This is the path that needed a batch
    accessor.
    """
    h = _p.pond_pnd2_decode(blob, len(blob))
    if not h: return 0
    try:
        nc = _p.pond_result_num_columns(h)
        total = 0
        for i in range(nc):
            n = _p.pond_result_column_len(h, i)
            total += n
            vt = _p.pond_result_column_vtype(h, i)
            if vt == 1:
                ptr = _p.pond_result_column_i64(h, i)
                if ptr and n > 0: _ = ptr[0]
            elif vt == 2:
                ptr = _p.pond_result_column_f64(h, i)
                if ptr and n > 0: _ = ptr[0]
            elif vt == 3:
                # PER-ROW string access — N FFI calls
                for r in range(n):
                    _ = _p.pond_result_column_str(h, i, r)
        return total
    finally:
        _p.pond_result_free(h)


def c_abi_decode_batch_str(blob: bytes) -> int:
    """C ABI decode with BATCH string access (the fast path).

    Calls pond_result_column_str_array() once per string column — gets
    all string pointers in one call. Then iterates the local array
    with zero FFI overhead.
    """
    h = _p.pond_pnd2_decode(blob, len(blob))
    if not h: return 0
    try:
        nc = _p.pond_result_num_columns(h)
        total = 0
        for i in range(nc):
            n = _p.pond_result_column_len(h, i)
            total += n
            vt = _p.pond_result_column_vtype(h, i)
            if vt == 1:
                ptr = _p.pond_result_column_i64(h, i)
                if ptr and n > 0: _ = ptr[0]
            elif vt == 2:
                ptr = _p.pond_result_column_f64(h, i)
                if ptr and n > 0: _ = ptr[0]
            elif vt == 3:
                # BATCH string access — 1 FFI call for the whole column
                arr = _p.pond_result_column_str_array(h, i)
                if arr:
                    for r in range(n):
                        _ = arr[r]  # local pointer dereference — no FFI
        return total
    finally:
        _p.pond_result_free(h)


# ---------------------------------------------------------------------------
# Test blob generation
# ---------------------------------------------------------------------------

class ListColumnSource(ColumnSource):
    def __init__(self, columns):
        self._columns = dict(columns)
        self._col_names = [name for name, _ in columns]
        self._n_rows = len(columns[0][1]) if columns else 0

    def num_rows(self):
        return self._n_rows

    def column_names(self):
        return list(self._col_names)

    def column_slice(self, name, start, end):
        return self._columns[name][start:end]

    def column_stats(self, name):
        from extensions.physical_structures.column_source import compute_list_stats
        return compute_list_stats(self._columns[name])


def make_blob_numeric(n_rows: int) -> bytes:
    """3-column blob: id INT64, val INT64, score FLOAT64. No strings."""
    ids = [(i * 7) % 1000000 for i in range(n_rows)]
    vals = [(i * 13) % 500000 for i in range(n_rows)]
    scores = [float(i) * 1.5 for i in range(n_rows)]
    src = ListColumnSource([("id", ids), ("val", vals), ("score", scores)])
    return PND2.encode(src, compress=False)[0]


def make_blob_mixed(n_rows: int) -> bytes:
    """3-column blob: id INT64, score FLOAT64, name STRING. Realistic mix."""
    ids = [(i * 7) % 1000000 for i in range(n_rows)]
    scores = [float(i) * 1.5 for i in range(n_rows)]
    names = [f"user_{i}" for i in range(n_rows)]
    src = ListColumnSource([("id", ids), ("score", scores), ("name", names)])
    return PND2.encode(src, compress=False)[0]


def make_blob_string_heavy(n_rows: int) -> bytes:
    """3-column blob: all STRING. Worst case for per-row FFI."""
    a = [f"name_{i}" for i in range(n_rows)]
    b = [f"email_{i}@test.com" for i in range(n_rows)]
    c = [f"tag_{i%100}" for i in range(n_rows)]
    src = ListColumnSource([("a", a), ("b", b), ("c", c)])
    return PND2.encode(src, compress=False)[0]


# ---------------------------------------------------------------------------
# Benchmark harness
# ---------------------------------------------------------------------------

def time_decode(decode_fn, blob, n_iters=5) -> float:
    """Time a decode function. Returns median seconds over n_iters runs."""
    times = []
    for _ in range(n_iters):
        t0 = time.perf_counter()
        decode_fn(blob)
        t1 = time.perf_counter()
        times.append(t1 - t0)
    return statistics.median(times)


def fmt_t(seconds: float, n_rows: int, blob_size: int) -> str:
    if seconds <= 0:
        return "n/a"
    rows_per_sec = n_rows / seconds
    mb_per_sec = (blob_size / (1024 * 1024)) / seconds
    return f"{rows_per_sec:>12,.0f} rows/s  {mb_per_sec:>7.1f} MB/s"


def run_table(title: str, blob_fn, sizes):
    print()
    print("=" * 90)
    print(f"  {title}")
    print("=" * 90)
    print(f"{'Size':>10}  {'Path':<22}  {'Time (ms)':>10}  {'Throughput'}")
    print("-" * 90)

    for n_rows in sizes:
        blob = blob_fn(n_rows)
        blob_size = len(blob)

        # PyO3
        try:
            t = time_decode(PYO3_DECODE, blob, n_iters=5)
            print(f"{n_rows:>10,}  {'PyO3':<22}  {t*1000:>10.2f}  {fmt_t(t, n_rows, blob_size)}")
        except Exception as e:
            print(f"{n_rows:>10,}  {'PyO3':<22}  ERROR: {e}")

        # Pure Python (skip for very large)
        if n_rows <= 100_000:
            try:
                t = time_decode(PY_DECODE, blob, n_iters=3)
                print(f"{'':>10}  {'Pure-Python':<22}  {t*1000:>10.2f}  {fmt_t(t, n_rows, blob_size)}")
            except Exception as e:
                print(f"{'':>10}  {'Pure-Python':<22}  ERROR: {e}")
        else:
            print(f"{'':>10}  {'Pure-Python':<22}  {'skipped':>10}")

        # C ABI per-row string (slow path — for comparison)
        try:
            t = time_decode(c_abi_decode_per_row_str, blob, n_iters=3)
            print(f"{'':>10}  {'C ABI (per-row str)':<22}  {t*1000:>10.2f}  {fmt_t(t, n_rows, blob_size)}")
        except Exception as e:
            print(f"{'':>10}  {'C ABI (per-row str)':<22}  ERROR: {e}")

        # C ABI batch string (fast path)
        try:
            t = time_decode(c_abi_decode_batch_str, blob, n_iters=5)
            print(f"{'':>10}  {'C ABI (batch str)':<22}  {t*1000:>10.2f}  {fmt_t(t, n_rows, blob_size)}")
        except Exception as e:
            print(f"{'':>10}  {'C ABI (batch str)':<22}  ERROR: {e}")

        print()


def main():
    print("=" * 90)
    print("PND2 Decode Path Benchmark")
    print("  Paths:")
    print("    PyO3         — pond_rust.decode (Rust + Python object conversion)")
    print("    Pure-Python  — PND2.decode (no Rust)")
    print("    C ABI (per-row str) — libpond_core.so via ctypes, per-row str access")
    print("    C ABI (batch str)   — libpond_core.so via ctypes, batch str_array")
    print("=" * 90)

    sizes = [1_000, 10_000, 100_000, 1_000_000]

    run_table("Test 1: Numeric-heavy (id INT64, val INT64, score FLOAT64 — no strings)",
              make_blob_numeric, sizes)
    run_table("Test 2: Mixed (id INT64, score FLOAT64, name STRING — typical workload)",
              make_blob_mixed, sizes)
    run_table("Test 3: String-heavy (3 STRING columns — worst case for per-row FFI)",
              make_blob_string_heavy, sizes)

    # Summary
    print("=" * 90)
    print("Summary: C ABI (batch) vs PyO3 speedup")
    print("=" * 90)
    print(f"{'Test':<22}  {'Size':>10}  {'C/PyO3 speedup':>16}")
    print("-" * 60)
    for name, blob_fn in [
        ("Numeric-heavy", make_blob_numeric),
        ("Mixed", make_blob_mixed),
        ("String-heavy", make_blob_string_heavy),
    ]:
        for n_rows in sizes:
            blob = blob_fn(n_rows)
            t_pyo3 = time_decode(PYO3_DECODE, blob, n_iters=5)
            t_c = time_decode(c_abi_decode_batch_str, blob, n_iters=5)
            speedup = t_pyo3 / t_c if t_c > 0 else 0
            print(f"{name:<22}  {n_rows:>10,}  {speedup:>13.1f}x")
    print()


if __name__ == "__main__":
    main()
