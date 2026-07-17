"""
Pond v0 prototype — falsification benchmark suite.

Per RFC 1 section 11, the architecture makes performance claims that must be
validated by benchmark, not assumed. This suite tests the most important
claims of the storage layer:

  1. Append throughput (rows/sec)        — does Write sustain > 100k rows/sec?
  2. Seal latency at various sizes       — is OPEN->SEALED < 10s at 512MB?
  3. Snapshot read latency               — is fresh snapshot read fast?
  4. Metadata scaling                    — does meta-to-data ratio stay < 1%?
  5. Object count vs data size           — does file count stay bounded?
  6. Comparison vs vanilla DuckDB+Parquet — what's the overhead of the
                                            Pond abstraction?

How Pond will fail (falsification criteria):
  - If append throughput < 10k rows/sec, the OPEN object format is wrong.
  - If seal latency > 30s at 512MB, the Arrow->Parquet conversion is wrong.
  - If snapshot read > 10x slower than vanilla DuckDB, the read path is wrong.
  - If meta-to-data ratio > 5%, the DAG metadata is too verbose.
  - If file count grows linearly with rows (not batches), sealing is too granular.

Run:  python3 benchmark.py
"""

import os
import shutil
import time
import json
import statistics
import pyarrow as pa
import pyarrow as pa
import duckdb

from pond import Pond


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    elif n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    elif n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    else:
        return f"{n / (1024 * 1024 * 1024):.2f} GB"


def fmt_duration(s: float) -> str:
    if s < 0.001:
        return f"{s * 1e6:.0f} us"
    elif s < 1:
        return f"{s * 1e3:.1f} ms"
    else:
        return f"{s:.2f} s"


def make_batch(num_rows: int, schema: pa.Schema, start_id: int = 0) -> pa.RecordBatch:
    """Generate a synthetic batch of events."""
    import random
    import string
    ids = list(range(start_id, start_id + num_rows))
    timestamps = [int(time.time() * 1e6)] * num_rows
    payloads = [
        "".join(random.choices(string.ascii_lowercase, k=20))
        for _ in range(num_rows)
    ]
    return pa.RecordBatch.from_arrays([
        pa.array(ids, type=pa.int64()),
        pa.array(timestamps, type=pa.timestamp("us")),
        pa.array(payloads, type=pa.string()),
    ], schema=schema)


SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("ts", pa.timestamp("us")),
    pa.field("payload", pa.string()),
])


def section(title: str):
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def result(label: str, value: str, note: str = ""):
    print(f"  {label:<40} {value:>20}   {note}")


# ---------------------------------------------------------------------------
# Benchmark 1: Append throughput
# ---------------------------------------------------------------------------

def bench_append_throughput():
    section("Benchmark 1: Append throughput (Write syscall)")

    sizes = [
        (1_000, 1, "1k rows x 1 batch"),
        (10_000, 1, "10k rows x 1 batch"),
        (1_000, 100, "1k rows x 100 batches (100k total)"),
        (10_000, 10, "10k rows x 10 batches (100k total)"),
    ]

    for num_rows, num_batches, label in sizes:
        db = _fresh_pond(f"/tmp/pond_bench_append_{num_rows}_{num_batches}")
        batch = make_batch(num_rows, SCHEMA)

        t0 = time.perf_counter()
        for i in range(num_batches):
            db.write("events", batch)
        t1 = time.perf_counter()

        elapsed = t1 - t0
        total_rows = num_rows * num_batches
        throughput = total_rows / elapsed

        result(label, f"{throughput:,.0f} rows/sec",
               f"({fmt_duration(elapsed)} total)")
        db.close()


# ---------------------------------------------------------------------------
# Benchmark 2: Seal latency at various sizes
# ---------------------------------------------------------------------------

def bench_seal_latency():
    section("Benchmark 2: Seal latency (OPEN -> SEALED)")

    sizes = [
        (1_000, "1k rows"),
        (10_000, "10k rows"),
        (100_000, "100k rows"),
        (1_000_000, "1M rows"),
    ]

    for num_rows, label in sizes:
        db = _fresh_pond(f"/tmp/pond_bench_seal_{num_rows}")
        batch = make_batch(num_rows, SCHEMA)
        db.write("events", batch)

        t0 = time.perf_counter()
        commit = db.seal("events", message=f"seal {label}")
        t1 = time.perf_counter()

        elapsed = t1 - t0
        stats = db.storage_stats()
        data_size = stats["data_bytes"]

        result(f"Seal {label}", fmt_duration(elapsed),
               f"({fmt_bytes(data_size)} Parquet)")
        db.close()


# ---------------------------------------------------------------------------
# Benchmark 3: Snapshot read latency
# ---------------------------------------------------------------------------

def bench_snapshot_read():
    section("Benchmark 3: Snapshot read latency (Read syscall)")

    # Set up: seal several batches
    db = _fresh_pond("/tmp/pond_bench_read")
    for i in range(5):
        batch = make_batch(10_000, SCHEMA, start_id=i * 10_000)
        db.write("events", batch)
        db.seal("events", message=f"seal {i+1}")

    # Warm up
    db.read("events")

    # Measure
    times = []
    for _ in range(20):
        t0 = time.perf_counter()
        table = db.read("events")
        t1 = time.perf_counter()
        times.append(t1 - t0)

    p50 = statistics.median(times)
    p99 = sorted(times)[int(len(times) * 0.99)]
    rows = table.num_rows
    data_size = db.storage_stats()["data_bytes"]

    result(f"Read p50 ({rows} rows)", fmt_duration(p50),
           f"({fmt_bytes(data_size)} total)")
    result(f"Read p99 ({rows} rows)", fmt_duration(p99), "")
    db.close()


# ---------------------------------------------------------------------------
# Benchmark 4: Metadata scaling
# ---------------------------------------------------------------------------

def bench_metadata_scaling():
    section("Benchmark 4: Metadata scaling (meta-to-data ratio)")

    configs = [
        (1, 1_000, "1 seal x 1k rows"),
        (10, 1_000, "10 seals x 1k rows"),
        (100, 1_000, "100 seals x 1k rows"),
        (10, 10_000, "10 seals x 10k rows"),
        (10, 100_000, "10 seals x 100k rows"),
    ]

    for num_seals, rows_per_seal, label in configs:
        db = _fresh_pond(f"/tmp/pond_bench_meta_{num_seals}_{rows_per_seal}")
        for i in range(num_seals):
            batch = make_batch(rows_per_seal, SCHEMA, start_id=i * rows_per_seal)
            db.write("events", batch)
            db.seal("events", message=f"seal {i+1}")

        stats = db.storage_stats()
        ratio = stats["meta_to_data_ratio"]
        verdict = "OK" if ratio < 0.05 else "TOO HIGH"

        result(label, f"{ratio * 100:.2f}%",
               f"data={fmt_bytes(stats['data_bytes'])}, "
               f"meta={fmt_bytes(stats['meta_bytes'])}, "
               f"blobs={stats['blob_count']}  [{verdict}]")
        db.close()


# ---------------------------------------------------------------------------
# Benchmark 5: Comparison vs vanilla DuckDB+Parquet
# ---------------------------------------------------------------------------

def bench_vs_vanilla_duckdb():
    section("Benchmark 5: Pond vs vanilla DuckDB+Parquet (overhead)")

    num_rows = 100_000
    batch = make_batch(num_rows, SCHEMA)

    # --- Vanilla DuckDB: write to a Parquet file directly ---
    vanilla_dir = "/tmp/pond_bench_vanilla"
    if os.path.exists(vanilla_dir):
        shutil.rmtree(vanilla_dir)
    os.makedirs(vanilla_dir)

    vanilla_duck = duckdb.connect(":memory:")
    vanilla_duck.register("batch", batch.to_pandas())

    t0 = time.perf_counter()
    vanilla_duck.execute(f"COPY (SELECT * FROM batch) TO '{vanilla_dir}/events.parquet' (FORMAT PARQUET)")
    t1 = time.perf_counter()
    vanilla_write_time = t1 - t0

    t0 = time.perf_counter()
    vanilla_duck.execute(f"CREATE TABLE events AS SELECT * FROM read_parquet('{vanilla_dir}/events.parquet')")
    t1 = time.perf_counter()
    vanilla_read_time = t1 - t0

    vanilla_size = os.path.getsize(f"{vanilla_dir}/events.parquet")

    # --- Pond: write + seal, then read ---
    db = _fresh_pond("/tmp/pond_bench_pond")
    t0 = time.perf_counter()
    db.write("events", batch)
    db.seal("events", message="bench")
    t1 = time.perf_counter()
    pond_write_time = t1 - t0

    t0 = time.perf_counter()
    db.read("events")
    t1 = time.perf_counter()
    pond_read_time = t1 - t0

    pond_size = db.storage_stats()["data_bytes"]

    result("Vanilla DuckDB write", fmt_duration(vanilla_write_time),
           f"({fmt_bytes(vanilla_size)})")
    result("Pond write + seal", fmt_duration(pond_write_time),
           f"({fmt_bytes(pond_size)})  "
           f"overhead={pond_write_time / vanilla_write_time:.1f}x")
    result("Vanilla DuckDB read", fmt_duration(vanilla_read_time), "")
    result("Pond read", fmt_duration(pond_read_time),
           f"overhead={pond_read_time / vanilla_read_time:.1f}x")
    db.close()


# ---------------------------------------------------------------------------
# Benchmark 6: Time travel & branching overhead
# ---------------------------------------------------------------------------

def bench_time_travel_overhead():
    section("Benchmark 6: Time travel & branching (should be ~free)")

    db = _fresh_pond("/tmp/pond_bench_tt")
    commits = []
    for i in range(10):
        batch = make_batch(1_000, SCHEMA, start_id=i * 1_000)
        db.write("events", batch)
        c = db.seal("events", message=f"seal {i+1}")
        commits.append(c)

    # Read current
    t0 = time.perf_counter()
    current = db.read("events")
    t1 = time.perf_counter()
    current_time = t1 - t0
    result("Read current (10 seals)", fmt_duration(current_time),
           f"({current.num_rows} rows)")

    # Read at oldest commit (time travel)
    t0 = time.perf_counter()
    old = db.read(commits[0])
    t1 = time.perf_counter()
    tt_time = t1 - t0
    result("Read at commit 0 (time travel)", fmt_duration(tt_time),
           f"({old.num_rows} rows)  "
           f"overhead={tt_time / current_time:.1f}x")

    # Read at middle commit
    t0 = time.perf_counter()
    mid = db.read(commits[5])
    t1 = time.perf_counter()
    mid_time = t1 - t0
    result("Read at commit 5 (time travel)", fmt_duration(mid_time),
           f"({mid.num_rows} rows)")

    # Branch read
    db.create_branch("exp", "events", at_commit=commits[3])
    t0 = time.perf_counter()
    branch = db.read("exp")
    t1 = time.perf_counter()
    branch_time = t1 - t0
    result("Read branch 'exp' (at commit 3)", fmt_duration(branch_time),
           f"({branch.num_rows} rows)  "
           f"overhead={branch_time / current_time:.1f}x")
    db.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_pond(path: str) -> Pond:
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return Pond(path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 72)
    print("  Pond v0 prototype — falsification benchmark suite")
    print("  Per RFC 1 section 11: validating architecture claims by measurement")
    print("=" * 72)

    bench_append_throughput()
    bench_seal_latency()
    bench_snapshot_read()
    bench_metadata_scaling()
    bench_vs_vanilla_duckdb()
    bench_time_travel_overhead()

    section("Verdict")
    print("""
  The benchmark results above either validate or falsify the architecture's
  performance claims. Honest interpretation:

  - If append throughput is > 100k rows/sec, the OPEN object format works.
  - If seal latency is < 10s at 1M rows, the Arrow->Parquet path works.
  - If snapshot read overhead vs vanilla DuckDB is < 5x, the read path works.
  - If meta-to-data ratio is < 5%, the DAG metadata is acceptable.
  - If time travel overhead is ~1x, the Versioned State model is correct.

  Any failure here is more valuable than another architecture iteration.
  Failures point at exactly which abstraction needs revision.
""")


if __name__ == "__main__":
    main()
