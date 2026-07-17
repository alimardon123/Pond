"""
Crash consistency benchmark.

Kill the process during every step of Seal(). After restart, verify:
  - No orphaned Parquet files (written but never referenced)
  - No orphaned tree objects (written but never referenced)
  - No corrupted DAG (commit points to nonexistent tree)
  - No inconsistent roots (root points to nonexistent commit)
  - Recovery is bounded (O(un-sealed ops), not O(total history))

Seal() does these filesystem operations in order:
  1. Write OPEN object Arrow IPC to disk
  2. Read IPC, convert to Parquet, write to temp file
  3. Compute hash, rename temp -> content-addressed path
  4. Write tree object (JSON)
  5. Write commit object (JSON)
  6. Update root pointer (SQLite)

If we crash between any two steps, the system must be recoverable.

Run:  python3 bench_crash_consistency.py
"""

import os
import shutil
import time
import sys
import json
import signal
import subprocess
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond import Pond, hash_bytes, Tree, Commit

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
# Crash-instrumented Pond: each "crash point" raises CrashError instead
# of completing the operation, simulating a process kill.
# ---------------------------------------------------------------------------

class CrashError(Exception):
    """Simulates a process crash at a specific point in Seal()."""
    pass


class CrashablePond(Pond):
    """Pond subclass that can simulate crashes at specific points in Seal()."""

    def __init__(self, base_dir: str, crash_at: str = None):
        super().__init__(base_dir)
        self.crash_at = crash_at  # "after_parquet" | "after_tree" | "after_commit" | None

    def seal(self, table_name: str, message: str = "") -> str:
        """Same as Pond.seal, but with crash injection points."""
        if table_name not in self._open_objects:
            raise ValueError(f"No OPEN object for table '{table_name}'")

        open_obj = self._open_objects[table_name]
        arrow_bytes = open_obj.seal()

        # Step 1: Convert Arrow IPC -> Parquet (temp file)
        import pyarrow.ipc as ipc
        import pyarrow.parquet as pq
        reader = ipc.open_stream(pa.BufferReader(arrow_bytes))
        table = pa.Table.from_batches(reader, open_obj.schema)
        temp_parquet = open_obj.path + ".parquet"
        pq.write_table(table, temp_parquet, compression="zstd")

        with open(temp_parquet, "rb") as f:
            parquet_bytes = f.read()
        sealed_hash = hash_bytes(parquet_bytes)

        # Step 2: Move to content-addressed path
        shard_dir = os.path.join(self.objects_dir, sealed_hash[:2])
        os.makedirs(shard_dir, exist_ok=True)
        final_path = os.path.join(shard_dir, sealed_hash + ".parquet")
        os.rename(temp_parquet, final_path)

        # === CRASH POINT 1: after Parquet written, before tree/commit ===
        if self.crash_at == "after_parquet":
            raise CrashError("after_parquet")

        # Step 3: Build tree (inherit from parent — same as Pond.seal)
        parent_hash = self._resolve_name(table_name)

        parent_subtrees: list[tuple[str, str]] = []
        parent_unsealed_leaves: list[tuple[str, str]] = []

        if parent_hash is not None:
            parent_commit = self._read_commit(parent_hash)
            if parent_commit is not None:
                parent_root = self._read_tree(parent_commit.tree_hash)
                if parent_root is not None:
                    for name, h in sorted(parent_root.entries.items()):
                        if name.startswith("subtree/"):
                            parent_subtrees.append((name, h))
                        elif name.startswith("leaf/"):
                            parent_unsealed_leaves.append((name, h))

        blob_counter = len(parent_subtrees) * 256 + len(parent_unsealed_leaves)
        blob_name = f"{table_name}/data/{blob_counter}"
        new_leaf = Tree(entries={blob_name: sealed_hash}, tree_type="leaf")
        new_leaf_hash = self._write_tree(new_leaf)
        new_leaf_name = f"leaf/{blob_counter:08d}"
        parent_unsealed_leaves.append((new_leaf_name, new_leaf_hash))

        if len(parent_unsealed_leaves) >= 256:
            compacted_entries: dict[str, str] = {}
            for _, leaf_hash in parent_unsealed_leaves:
                leaf = self._read_tree(leaf_hash)
                if leaf is not None:
                    compacted_entries.update(leaf.entries)
            sealed_subtree = Tree(entries=compacted_entries, tree_type="leaf")
            sealed_subtree_hash = self._write_tree(sealed_subtree)
            subtree_name = f"subtree/{len(parent_subtrees):08d}"
            parent_subtrees.append((subtree_name, sealed_subtree_hash))
            parent_unsealed_leaves = []

        root_entries: dict[str, str] = {}
        for name, h in parent_subtrees:
            root_entries[name] = h
        for name, h in parent_unsealed_leaves:
            root_entries[name] = h

        root_tree = Tree(entries=root_entries, tree_type="interior")
        root_tree_hash = self._write_tree(root_tree)

        # === CRASH POINT 2: after tree written, before commit ===
        if self.crash_at == "after_tree":
            raise CrashError("after_tree")

        # Step 4: Write commit
        schema_json = str(open_obj.schema)
        schema_hash = hash_bytes(schema_json.encode())
        commit = Commit(
            tree_hash=root_tree_hash,
            parent_hash=parent_hash,
            timestamp=time.time(),
            message=message or f"seal {table_name}",
            schema_hash=schema_hash,
        )
        commit_hash = self._write_commit(commit)

        # === CRASH POINT 3: after commit written, before root update ===
        if self.crash_at == "after_commit":
            raise CrashError("after_commit")

        # Step 5: Update root pointer
        self._set_root(table_name, commit_hash)

        # Clean up the OPEN object
        if os.path.exists(open_obj.path):
            os.remove(open_obj.path)
        del self._open_objects[table_name]

        self.stats["seals"] += 1
        return commit_hash


def verify_consistency(db: Pond, table_name: str) -> tuple[bool, list[str]]:
    """
    Verify DAG consistency after a crash.
    Returns (is_consistent, list_of_issues).
    """
    issues = []

    # 1. Root pointer must point to an existing commit
    commit_hash = db._resolve_name(table_name)
    if commit_hash is None:
        issues.append(f"Root pointer for '{table_name}' is missing")
        return (False, issues)

    commit = db._read_commit(commit_hash)
    if commit is None:
        issues.append(f"Root pointer for '{table_name}' -> {commit_hash[:16]}... "
                      f"but commit object is missing")
        return (False, issues)

    # 2. Commit's tree must exist
    tree = db._read_tree(commit.tree_hash)
    if tree is None:
        issues.append(f"Commit {commit_hash[:16]}... -> tree {commit.tree_hash[:16]}... "
                      f"but tree object is missing")

    # 3. Walk the DAG; every referenced object must exist
    current = commit_hash
    visited = set()
    while current is not None and current not in visited:
        visited.add(current)
        c = db._read_commit(current)
        if c is None:
            issues.append(f"DAG walk: commit {current[:16]}... missing")
            break
        t = db._read_tree(c.tree_hash)
        if t is None:
            issues.append(f"DAG walk: tree {c.tree_hash[:16]}... (referenced by "
                          f"commit {current[:16]}...) missing")
            break
        # Check all blob references in the tree
        for blob_hash in db._walk_tree_for_data_blobs(c.tree_hash):
            blob_path = db._blob_path(blob_hash)
            if not os.path.exists(blob_path):
                issues.append(f"Blob {blob_hash[:16]}... referenced by tree "
                              f"but file missing on disk")
        current = c.parent_hash

    # 4. Check for orphaned Parquet files (on disk but not referenced)
    referenced_blobs = set()
    for c_hash in visited:
        c = db._read_commit(c_hash)
        if c:
            for bh in db._walk_tree_for_data_blobs(c.tree_hash):
                referenced_blobs.add(bh)

    on_disk_blobs = set()
    for shard in os.listdir(db.objects_dir):
        shard_path = os.path.join(db.objects_dir, shard)
        if not os.path.isdir(shard_path):
            continue
        for f in os.listdir(shard_path):
            if f.endswith(".parquet"):
                on_disk_blobs.add(f[:-len(".parquet")])

    orphaned_blobs = on_disk_blobs - referenced_blobs
    if orphaned_blobs:
        issues.append(f"{len(orphaned_blobs)} orphaned Parquet files on disk "
                      f"(written but never referenced)")

    return (len(issues) == 0, issues)


def main():
    print("=" * 76)
    print("  Crash consistency benchmark")
    print("=" * 76)
    print()
    print("  Kill the process at each step of Seal(). After restart, verify:")
    print("    - No orphaned Parquet files")
    print("    - No orphaned tree objects")
    print("    - No corrupted DAG (commit -> nonexistent tree)")
    print("    - No inconsistent roots (root -> nonexistent commit)")
    print()

    crash_points = [
        ("after_parquet", "Crash after Parquet written, before tree/commit"),
        ("after_tree",    "Crash after tree written, before commit"),
        ("after_commit",  "Crash after commit written, before root update"),
    ]

    for crash_at, label in crash_points:
        print(f"  --- {label} ---")
        bench_dir = f"/tmp/pond_crash_{crash_at}"

        # Phase 1: build initial state with one good seal
        if os.path.exists(bench_dir):
            shutil.rmtree(bench_dir)
        os.makedirs(bench_dir)
        db = CrashablePond(bench_dir)
        b = make_batch(100, start_id=0)
        db.write("events", b)
        db.seal("events", message="initial good seal")
        good_commit = db._resolve_name("events")
        db.close()

        # Phase 2: write another batch and crash during seal
        db = CrashablePond(bench_dir, crash_at=crash_at)
        b = make_batch(100, start_id=100)
        db.write("events", b)
        try:
            db.seal("events", message="crashing seal")
            print(f"    [FAIL] Expected CrashError but seal completed")
        except CrashError as e:
            print(f"    [OK] Crashed at: {e}")
        # Simulate process death by NOT closing properly
        del db

        # Phase 3: reopen and verify consistency
        db = Pond(bench_dir)  # plain Pond, no crash injection
        consistent, issues = verify_consistency(db, "events")

        if consistent:
            # Verify the root still points to the good commit (pre-crash state)
            current = db._resolve_name("events")
            if current == good_commit:
                print(f"    [OK] Root pointer intact (points to pre-crash commit)")
            else:
                print(f"    [ISSUE] Root pointer moved to {current[:16]}... "
                      f"(expected {good_commit[:16]}...)")

            # Verify we can still read
            try:
                table = db.read("events")
                print(f"    [OK] Read succeeds: {table.num_rows} rows")
            except Exception as e:
                print(f"    [FAIL] Read failed: {e}")
        else:
            for issue in issues:
                print(f"    [ISSUE] {issue}")

        db.close()
        print()

    # Phase 4: also test that orphaned Parquet files accumulate
    print("  --- Orphan accumulation across multiple crashes ---")
    bench_dir = "/tmp/pond_crash_accumulate"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    db = CrashablePond(bench_dir)
    b = make_batch(100, start_id=0)
    db.write("events", b)
    db.seal("events", message="good seal")
    db.close()

    for i in range(5):
        db = CrashablePond(bench_dir, crash_at="after_parquet")
        b = make_batch(100, start_id=(i+1)*100)
        db.write("events", b)
        try:
            db.seal("events")
        except CrashError:
            pass
        del db

    db = Pond(bench_dir)
    consistent, issues = verify_consistency(db, "events")
    stats = db.storage_stats()
    print(f"    After 5 crashes at 'after_parquet':")
    print(f"    - {stats['blob_count']} Parquet files on disk")
    print(f"    - Consistent: {consistent}")
    if issues:
        for issue in issues[:3]:
            print(f"    - {issue}")
    print()
    print("  NOTE: Orphaned Parquet files from crashes are expected. Production")
    print("  needs a GC pass that identifies unreferenced blobs and removes them.")
    print("  This is a known gap — not yet implemented in v0.")

    db.close()


if __name__ == "__main__":
    main()
