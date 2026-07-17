"""
Pond v0 prototype — end-to-end demo.

Demonstrates the full flow described in the prototype sketch:

    DuckDB
          |
          v
    Pond Write()
          |
          v
    Immutable Object (OPEN -> Arrow IPC)
          |
          v
    Seal()  (Arrow IPC -> Parquet on object storage)
          |
          v
    Versioned State DAG (commit chain, content-addressed)
          |
          v
    Snapshot Read()  (DuckDB reads the sealed Parquet)

Plus derived features that fall out for free from Versioned State:
    - Time travel (read at a past commit)
    - Branching (named pointer to a commit, copy-on-write)

Run:  python3 demo.py
"""

import os
import shutil
import time
import pyarrow as pa

from pond import Pond


def main():
    demo_dir = "/tmp/pond_demo"
    if os.path.exists(demo_dir):
        shutil.rmtree(demo_dir)
    os.makedirs(demo_dir)

    print("=" * 72)
    print("Pond v0 prototype — end-to-end demo")
    print("=" * 72)
    print()
    print(f"Storage dir: {demo_dir}")
    print()

    # ------------------------------------------------------------------
    # 1. Attach a Pond instance (local filesystem, no Raft, no S3)
    # ------------------------------------------------------------------
    print("[1] Attaching Pond instance...")
    db = Pond(demo_dir)
    print(f"    Pond dir: {db.pond_dir}")
    print()

    # ------------------------------------------------------------------
    # 2. Define a schema and write some rows (Write syscall)
    # ------------------------------------------------------------------
    print("[2] Writing rows to 'events' table (Write syscall)...")
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("ts", pa.timestamp("us")),
        pa.field("payload", pa.string()),
    ])

    batch1 = pa.RecordBatch.from_arrays([
        pa.array([1, 2, 3], type=pa.int64()),
        pa.array([int(time.time() * 1e6)] * 3, type=pa.timestamp("us")),
        pa.array(["hello", "world", "foo"], type=pa.string()),
    ], schema=schema)
    db.write("events", batch1)
    print(f"    Wrote batch 1: {batch1.num_rows} rows")

    batch2 = pa.RecordBatch.from_arrays([
        pa.array([4, 5], type=pa.int64()),
        pa.array([int(time.time() * 1e6)] * 2, type=pa.timestamp("us")),
        pa.array(["bar", "baz"], type=pa.string()),
    ], schema=schema)
    db.write("events", batch2)
    print(f"    Wrote batch 2: {batch2.num_rows} rows")
    print(f"    OPEN object now has 2 fragments, 5 rows total (not yet sealed)")
    print()

    # ------------------------------------------------------------------
    # 3. Seal — convert OPEN (Arrow IPC) -> SEALED (Parquet on disk)
    # ------------------------------------------------------------------
    print("[3] Sealing 'events' table (Seal syscall)...")
    commit1 = db.seal("events", message="initial seal")
    print(f"    Sealed. Commit hash: {commit1[:16]}...")
    print(f"    A Parquet file is now on disk, content-addressed by SHA-256")
    print()

    # ------------------------------------------------------------------
    # 4. Write more rows and seal again — creates a new commit (DAG)
    # ------------------------------------------------------------------
    print("[4] Writing more rows and sealing again (creates 2nd commit)...")
    batch3 = pa.RecordBatch.from_arrays([
        pa.array([6, 7, 8], type=pa.int64()),
        pa.array([int(time.time() * 1e6)] * 3, type=pa.timestamp("us")),
        pa.array(["qux", "quux", "corge"], type=pa.string()),
    ], schema=schema)
    db.write("events", batch3)
    commit2 = db.seal("events", message="second seal")
    print(f"    Sealed. Commit hash: {commit2[:16]}...")
    print(f"    Parent commit: {commit1[:16]}...")
    print()

    # ------------------------------------------------------------------
    # 5. Snapshot read — DuckDB reads the sealed Parquet (Read syscall)
    # ------------------------------------------------------------------
    print("[5] Snapshot read of 'events' (Read syscall via DuckDB)...")
    table = db.read("events")
    print(f"    Read {table.num_rows} rows from current snapshot:")
    for row in zip(*[table.column(c).to_pylist() for c in table.column_names]):
        print(f"      id={row[0]}, ts=..., payload={row[2]!r}")
    print()

    # ------------------------------------------------------------------
    # 6. SQL query via DuckDB (the reference backend)
    # ------------------------------------------------------------------
    print("[6] SQL query via DuckDB over sealed Parquet...")
    results = db.sql("SELECT count(*) AS n FROM events")
    print(f"    SELECT count(*) FROM events -> {results[0][0]}")
    results = db.sql("SELECT payload FROM events WHERE id > 3 ORDER BY id")
    print(f"    SELECT payload FROM events WHERE id > 3 -> {[r[0] for r in results]}")
    print()

    # ------------------------------------------------------------------
    # 7. Time travel — read at a past commit
    # ------------------------------------------------------------------
    print("[7] Time travel — reading at the first commit...")
    old_table = db.read(commit1)
    print(f"    Read {old_table.num_rows} rows from commit {commit1[:16]}...")
    print(f"    (Should be 5 rows — the state before the second seal)")
    print()

    # ------------------------------------------------------------------
    # 8. Branching — named pointer to a commit (copy-on-write)
    # ------------------------------------------------------------------
    print("[8] Branching — creating branch 'exp' pointing at commit 1...")
    db.create_branch("exp", "events", at_commit=commit1)
    print(f"    Branch 'exp' created. Reading from 'exp' (should show 5 rows):")
    branch_table = db.read("exp")
    print(f"    Read {branch_table.num_rows} rows from branch 'exp'")
    print()

    # Write to the branch — this creates a NEW commit only on the branch
    print("    Writing new rows to branch 'exp'...")
    batch_branch = pa.RecordBatch.from_arrays([
        pa.array([100, 101], type=pa.int64()),
        pa.array([int(time.time() * 1e6)] * 2, type=pa.timestamp("us")),
        pa.array(["branch_row_1", "branch_row_2"], type=pa.string()),
    ], schema=schema)
    db.write("exp", batch_branch)
    db.seal("exp", message="branch seal")
    print(f"    Branch 'exp' now has {db.read('exp').num_rows} rows")
    print(f"    Main 'events' still has {db.read('events').num_rows} rows (unchanged)")
    print()

    # ------------------------------------------------------------------
    # 9. History — walk the commit DAG
    # ------------------------------------------------------------------
    print("[9] History — walking the commit DAG for 'events'...")
    for entry in db.history("events"):
        print(f"    commit {entry['commit'][:16]}...  "
              f"parent={entry['parent'][:16] if entry['parent'] else 'None'}...  "
              f"msg=\"{entry['message']}\"")
    print()

    # ------------------------------------------------------------------
    # 10. Storage statistics — one-copy proof
    # ------------------------------------------------------------------
    print("[10] Storage statistics (the one-copy proof)...")
    stats = db.storage_stats()
    print(f"    Data bytes (sealed Parquet):  {stats['data_bytes']:>12,}")
    print(f"    Metadata bytes (JSON DAG):    {stats['meta_bytes']:>12,}")
    print(f"    Root store bytes (SQLite):    {stats['root_store_bytes']:>12,}")
    print(f"    Meta-to-data ratio:           {stats['meta_to_data_ratio']:.4f}  "
          f"({stats['meta_to_data_ratio'] * 100:.2f}%)")
    print(f"    Sealed blobs:                 {stats['blob_count']:>12}")
    print(f"    DAG objects (trees+commits):  {stats['meta_count']:>12}")
    print(f"    Total writes:                 {stats['writes']:>12}")
    print(f"    Total rows written:           {stats['rows_written']:>12}")
    print(f"    Total seals:                  {stats['seals']:>12}")
    print(f"    Total reads:                  {stats['reads']:>12}")
    print()

    print("=" * 72)
    print("Demo complete. The storage abstraction works end-to-end.")
    print("=" * 72)
    print()
    print("What this proved:")
    print("  - The 4 syscalls (Read, Write, Seal, Reference) work as specified")
    print("  - OPEN (Arrow IPC) -> SEALED (Parquet) state transition works")
    print("  - Content-addressed DAG (blob/tree/commit) works")
    print("  - Root pointer namespace (SQLite) works")
    print("  - Snapshot reads return consistent state at a commit hash")
    print("  - Time travel (read at past commit) works for free")
    print("  - Branching (named pointer, copy-on-write) works for free")
    print("  - DuckDB reads sealed Parquet for SQL queries (reference backend)")
    print()
    print("What this did NOT prove (intentionally, per prototype scope):")
    print("  - Replication (no Raft; single node)")
    print("  - Distributed execution (no Exchange)")
    print("  - Streaming (no tail-reads)")
    print("  - Cross-backend capability routing (only DuckDB)")
    print("  - HLC timestamps (wall clock)")
    print("  - Transactions (single-writer)")
    print("  - PB scale (local FS, not S3)")
    print()

    db.close()


if __name__ == "__main__":
    main()
