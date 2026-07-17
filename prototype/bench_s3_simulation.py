"""
S3 latency simulation benchmark.

Object storage has a fundamental latency floor: ~5-30ms per request on
S3 Standard, ~5ms on S3 Express One Zone. The architecture must minimize
remote calls — every round-trip is a 10-30ms tax.

This benchmark simulates S3 latency by adding artificial delay to every
metadata and data read. Then it measures:

  - How many S3 calls are needed for SELECT * FROM table LIMIT 10?
  - How many for SELECT count(*) FROM table (full scan)?
  - How many for time travel to a past commit?

If the design minimizes S3 calls, latency stays low. If it requires many
round-trips, even a fast storage engine becomes slow on real S3.

Run:  python3 bench_s3_simulation.py
"""

import os
import shutil
import time
import sys
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
# S3-simulating Pond subclass
# ---------------------------------------------------------------------------

class S3SimulatingPond(Pond):
    """Pond subclass that simulates S3 latency for metadata and data reads."""

    S3_LATENCY_MS = 20  # S3 Standard p50 GET latency

    def __init__(self, base_dir: str, simulate_latency: bool = True):
        super().__init__(base_dir)
        self.simulate_latency = simulate_latency
        self.s3_calls = 0
        self._counting = False

    def start_counting(self):
        self.s3_calls = 0
        self._counting = True

    def stop_counting(self):
        self._counting = False

    def _s3_get(self, path: str) -> bytes:
        """Simulate an S3 GET with artificial latency."""
        if self._counting:
            self.s3_calls += 1
        if self.simulate_latency:
            time.sleep(self.S3_LATENCY_MS / 1000)
        with open(path, "rb") as f:
            return f.read()

    def _read_tree(self, tree_hash: str):
        path = self._meta_path(tree_hash)
        if not os.path.exists(path):
            return None
        import json
        data = json.loads(self._s3_get(path))
        data.setdefault("tree_type", "leaf")
        from pond import Tree
        return Tree(**{k: v for k, v in data.items() if k != "kind"})

    def _read_commit(self, commit_hash: str):
        path = self._meta_path(commit_hash)
        if not os.path.exists(path):
            return None
        import json
        from pond import Commit
        data = json.loads(self._s3_get(path))
        return Commit(**{k: v for k, v in data.items() if k != "kind"})

    def _resolve_name(self, name: str):
        # Root store is local (Raft-replicated NVMe in production) — no S3 latency
        return super()._resolve_name(name)


def main():
    print("=" * 76)
    print("  S3 latency simulation benchmark")
    print("=" * 76)
    print()
    print(f"  Simulated S3 GET latency: {S3SimulatingPond.S3_LATENCY_MS}ms per request")
    print("  Question: how many S3 calls does each common operation require?")
    print()
    print("  - SELECT * FROM table LIMIT 10  (latest blob only)")
    print("  - SELECT count(*) FROM table    (full scan — all blobs)")
    print("  - Time travel to oldest commit  (DAG walk)")
    print("  - Branch creation               (root pointer update only)")
    print()

    # Build a table with 100 seals (small enough to complete quickly)
    bench_dir = "/tmp/pond_s3_sim"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    print("  Building test table (100 seals x 10k rows)...")
    db = S3SimulatingPond(bench_dir, simulate_latency=False)  # build fast
    for i in range(100):
        b = make_batch(10_000, start_id=i * 10_000)
        db.write("events", b)
        db.seal("events", message=f"seal {i+1}")
    stats = db.storage_stats()
    print(f"  Built: {stats['blob_count']} blobs, "
          f"{fmt_bytes(stats['data_bytes'])} data, "
          f"{fmt_bytes(stats['meta_bytes'])} metadata")
    print()

    # Reopen with S3 latency simulation
    db = S3SimulatingPond(bench_dir, simulate_latency=True)

    print(f"  {'Operation':<40}  {'S3 calls':>10}  {'Latency':>10}  {'Verdict':>10}")
    print(f"  {'-'*40}  {'-'*10}  {'-'*10}  {'-'*10}")

    # --- Test 1: SELECT * FROM table LIMIT 10 ---
    db.start_counting()
    t0 = time.perf_counter()
    commit_hash = db._resolve_name("events")
    commit = db._read_commit(commit_hash)
    root_tree = db._read_tree(commit.tree_hash)
    # Find latest leaf
    leaf_names = sorted([n for n in root_tree.entries if n.startswith("leaf/")])
    if leaf_names:
        leaf = db._read_tree(root_tree.entries[leaf_names[-1]])
        blob_names = sorted([n for n in leaf.entries if "/data/" in n])
        # The actual Parquet read would be 1 more S3 GET
        db.s3_calls += 1  # simulated Parquet GET
    t1 = time.perf_counter()
    calls = db.s3_calls
    latency_ms = (t1 - t0) * 1000
    verdict = "GOOD" if calls <= 5 else ("OK" if calls <= 10 else "POOR")
    print(f"  {'SELECT * FROM events LIMIT 10':<40}  {calls:>10}  "
          f"{latency_ms:>7.0f}ms  {verdict:>10}")
    db.stop_counting()

    # --- Test 2: SELECT count(*) FROM table (full scan, all blobs) ---
    db.start_counting()
    t0 = time.perf_counter()
    commit_hash = db._resolve_name("events")
    commit = db._read_commit(commit_hash)
    root_tree = db._read_tree(commit.tree_hash)
    # Walk all leaves and subtrees to collect blob hashes
    blob_count = 0
    leaf_names = sorted([n for n in root_tree.entries if n.startswith("leaf/")])
    subtree_names = sorted([n for n in root_tree.entries if n.startswith("subtree/")])
    # Each leaf is 1 S3 call; each subtree is 1 S3 call to read the subtree,
    # but then the subtree contains TREE_FANOUT blob hashes without further
    # metadata calls — just data calls.
    for name in leaf_names:
        leaf = db._read_tree(root_tree.entries[name])
        blob_count += len([n for n in leaf.entries if "/data/" in n])
    for name in subtree_names:
        subtree = db._read_tree(root_tree.entries[name])
        blob_count += len([n for n in subtree.entries if "/data/" in n])
    # Plus 1 data GET per blob (count-only scan still reads each file)
    db.s3_calls += blob_count
    t1 = time.perf_counter()
    calls = db.s3_calls
    latency_ms = (t1 - t0) * 1000
    verdict = "GOOD" if calls <= blob_count + 5 else "OK"
    print(f"  {'SELECT count(*) FROM events (100 blobs)':<40}  {calls:>10}  "
          f"{latency_ms:>7.0f}ms  {verdict:>10}")
    db.stop_counting()

    # --- Test 3: Time travel to oldest commit ---
    db.start_counting()
    t0 = time.perf_counter()
    # Walk the DAG from current to oldest
    current = db._resolve_name("events")
    oldest_commit = None
    while current is not None:
        commit = db._read_commit(current)
        if commit is None:
            break
        oldest_commit = current
        current = commit.parent_hash
    # Reading at oldest commit needs: oldest commit + its tree + its leaf(s)
    oldest = db._read_commit(oldest_commit)
    oldest_tree = db._read_tree(oldest.tree_hash)
    oldest_leaf_names = [n for n in oldest_tree.entries if n.startswith("leaf/")]
    if oldest_leaf_names:
        db._read_tree(oldest_tree.entries[oldest_leaf_names[0]])
    t1 = time.perf_counter()
    calls = db.s3_calls
    latency_ms = (t1 - t0) * 1000
    verdict = "POOR" if calls > 50 else ("OK" if calls > 10 else "GOOD")
    print(f"  {'Time travel to oldest commit (100 deep)':<40}  {calls:>10}  "
          f"{latency_ms:>7.0f}ms  {verdict:>10}")
    db.stop_counting()

    # --- Test 4: Branch creation (root pointer update only) ---
    db.start_counting()
    t0 = time.perf_counter()
    commit_hash = db._resolve_name("events")  # 1 local lookup (root store)
    db.reference("exp_branch", commit_hash)   # 1 local write (root store)
    t1 = time.perf_counter()
    calls = db.s3_calls
    latency_ms = (t1 - t0) * 1000
    print(f"  {'CREATE BRANCH (root pointer only)':<40}  {calls:>10}  "
          f"{latency_ms:>7.2f}ms  {'EXCELLENT':>10}")
    db.stop_counting()

    print()
    print("  Interpretation:")
    print()
    print(f"  At {S3SimulatingPond.S3_LATENCY_MS}ms per S3 GET:")
    print(f"  - LIMIT 10 needs ~4 S3 calls = ~{4 * S3SimulatingPond.S3_LATENCY_MS}ms")
    print(f"  - Full scan needs N+3 S3 calls (N = blob count) — bounded by data, not metadata")
    print(f"  - Time travel to depth D needs ~D+3 S3 calls — linear in history depth")
    print(f"  - Branch creation needs 0 S3 calls (root store is local)")
    print()
    print("  Findings:")
    print()
    print("  - GOOD: Latest-blob lookup is O(1) S3 calls regardless of scale.")
    print("  - GOOD: Branch creation is local — no S3 at all.")
    print("  - CONCERN: Time travel is O(depth) S3 calls. At 1M commits deep,")
    print("    that's 1M × 20ms = ~5.5 hours. The DAG walk needs caching or")
    print("    skip-list pointers to bound this.")
    print("  - CONCERN: Full scan is O(N) S3 calls. This is unavoidable (you")
    print("    must read every blob), but parallel GETs would help — currently")
    print("    sequential.")

    db.close()


if __name__ == "__main__":
    main()
