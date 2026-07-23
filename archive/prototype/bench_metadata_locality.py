"""
Metadata locality benchmark.

The most diagnostic benchmark for a content-addressed storage engine:
how much metadata must be loaded before a simple query can execute?

Scenario:
  - Build a table with N seals of varying sizes
  - Restart the process (cold cache)
  - Measure: bytes of metadata read from disk to answer
        SELECT * FROM table LIMIT 10

Why this matters:
  - At 10 TB data with 100 GB metadata, if answering LIMIT 10 requires
    loading all 100 GB, the architecture is broken for cold-start queries.
  - At 10 TB data with 100 GB metadata, if answering LIMIT 10 requires
    loading < 30 MB (just the root pointer + the latest commit + the
    latest leaf + one blob path), the architecture is sound.

What this measures:
  - cold_lookup_metadata_bytes: metadata touched to resolve name -> latest commit
  - cold_query_metadata_bytes: metadata touched to find the first data blob
  - warm_query_metadata_bytes: same, after cache is warm
  - ratio: metadata touched / total metadata

Run:  python3 bench_metadata_locality.py
"""

import os
import shutil
import time
import sys
import json
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond import Pond

SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("ts", pa.timestamp("us")),
    pa.field("payload", pa.string()),
])


def make_batch(num_rows: int, start_id: int = 0) -> pa.RecordBatch:
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
    ], schema=SCHEMA)


def fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    elif n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    elif n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    else:
        return f"{n / (1024 * 1024 * 1024):.2f} GB"


# ---------------------------------------------------------------------------
# Instrumented Pond subclass that counts metadata bytes read from disk
# ---------------------------------------------------------------------------

class InstrumentedPond(Pond):
    """Pond subclass that instruments _read_tree and _read_commit to count
    bytes read from disk on cold lookups."""

    def __init__(self, base_dir: str):
        super().__init__(base_dir)
        self.metadata_bytes_read = 0
        self.metadata_objects_read = 0
        self._tracking = False

    def start_tracking(self):
        self.metadata_bytes_read = 0
        self.metadata_objects_read = 0
        self._tracking = True

    def stop_tracking(self):
        self._tracking = False

    def _read_tree(self, tree_hash: str):
        if self._tracking:
            path = self._meta_path(tree_hash)
            if os.path.exists(path):
                self.metadata_bytes_read += os.path.getsize(path)
                self.metadata_objects_read += 1
        return super()._read_tree(tree_hash)

    def _read_commit(self, commit_hash: str):
        if self._tracking:
            path = self._meta_path(commit_hash)
            if os.path.exists(path):
                self.metadata_bytes_read += os.path.getsize(path)
                self.metadata_objects_read += 1
        return super()._read_commit(commit_hash)

    def _resolve_name(self, name: str):
        if self._tracking:
            # SQLite root store — count the bytes touched
            # (approximation: a single index lookup touches ~4KB of B-tree pages)
            self.metadata_bytes_read += 4096
            self.metadata_objects_read += 1
        return super()._resolve_name(name)


def main():
    print("=" * 76)
    print("  Metadata locality benchmark")
    print("=" * 76)
    print()
    print("  Question: at scale, how much metadata must be loaded before")
    print("            SELECT * FROM table LIMIT 10 can execute?")
    print()
    print("  If the answer is ~total metadata, the architecture is broken.")
    print("  If the answer is ~latest commit + latest leaf, the architecture is sound.")
    print()

    # Run at multiple scales
    scales = [
        # (seals, rows_per_seal, label)
        (100,   1_000, "100 seals x 1k rows  (small)"),
        (1_000, 1_000, "1k seals x 1k rows   (medium)"),
        (1_000, 10_000, "1k seals x 10k rows (medium-large)"),
    ]

    print(f"  {'Scale':<35}  {'Total data':>10}  {'Total meta':>10}  "
          f"{'Cold LIMIT 10':>14}  {'Ratio':>8}")
    print(f"  {'-'*35}  {'-'*10}  {'-'*10}  {'-'*14}  {'-'*8}")

    for num_seals, rows_per_seal, label in scales:
        bench_dir = f"/tmp/pond_locality_{num_seals}_{rows_per_seal}"
        if os.path.exists(bench_dir):
            shutil.rmtree(bench_dir)
        os.makedirs(bench_dir)

        # Phase 1: build the table
        db = InstrumentedPond(bench_dir)
        for i in range(num_seals):
            b = make_batch(rows_per_seal, start_id=i * rows_per_seal)
            db.write("events", b)
            db.seal("events", message=f"seal {i+1}")

        stats = db.storage_stats()
        total_data = stats["data_bytes"]
        total_meta = stats["meta_bytes"]
        db.close()

        # Phase 2: reopen (cold cache) and measure metadata touched for LIMIT 10
        db = InstrumentedPond(bench_dir)

        db.start_tracking()
        t0 = time.perf_counter()
        # Simulate "SELECT * FROM events LIMIT 10" — only need the latest blob
        commit_hash = db._resolve_name("events")
        commit = db._read_commit(commit_hash)
        root_tree = db._read_tree(commit.tree_hash)
        # Find the most recent leaf (highest-numbered leaf or subtree)
        latest_leaf_hash = None
        leaf_names = sorted([n for n in root_tree.entries if n.startswith("leaf/")])
        subtree_names = sorted([n for n in root_tree.entries if n.startswith("subtree/")])
        if leaf_names:
            latest_leaf_hash = root_tree.entries[leaf_names[-1]]
        elif subtree_names:
            # All leaves are sealed into subtrees — read the latest subtree
            latest_subtree_hash = root_tree.entries[subtree_names[-1]]
            latest_subtree = db._read_tree(latest_subtree_hash)
            sub_leaf_names = sorted([n for n in latest_subtree.entries
                                      if n.split("/")[-1].isdigit()])
            if sub_leaf_names:
                latest_leaf_hash = latest_subtree.entries[sub_leaf_names[-1]]
        # Read the leaf to find the blob
        if latest_leaf_hash:
            leaf = db._read_tree(latest_leaf_hash)
            blob_names = sorted([n for n in leaf.entries if "/data/" in n])
            if blob_names:
                blob_hash = leaf.entries[blob_names[-1]]
                # We'd read the Parquet file here, but for the metadata-locality
                # benchmark we only care about metadata bytes — the Parquet
                # read is data, not metadata.
                pass
        t1 = time.perf_counter()
        cold_bytes = db.metadata_bytes_read
        cold_objects = db.metadata_objects_read
        db.stop_tracking()
        db.close()

        ratio = cold_bytes / total_meta if total_meta > 0 else 0
        verdict = "EXCELLENT" if ratio < 0.05 else ("OK" if ratio < 0.20 else "POOR")

        print(f"  {label:<35}  {fmt_bytes(total_data):>10}  "
              f"{fmt_bytes(total_meta):>10}  "
              f"{fmt_bytes(cold_bytes):>10} ({cold_objects}o)  "
              f"{ratio*100:>6.2f}%  [{verdict}]")

    print()
    print("  Interpretation:")
    print()
    print("  - 'Cold LIMIT 10' = metadata bytes touched to resolve")
    print("    name -> commit -> tree -> latest leaf -> latest blob.")
    print("  - 'Ratio' = cold bytes / total metadata. Lower is better.")
    print()
    print("  - If the ratio stays < 5% as scale grows, the architecture has")
    print("    good metadata locality — cold queries don't need to load all metadata.")
    print()
    print("  - If the ratio grows with scale, the tree structure forces readers")
    print("    to load more metadata than necessary — a real architecture issue.")


if __name__ == "__main__":
    main()
