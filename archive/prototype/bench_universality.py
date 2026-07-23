"""
Universality test: can one immutable object appear in multiple Views
(SQL table, vector index, graph, streaming log) without copying?

This is the most important test for Pond's core thesis. If the storage
kernel is truly substrate-agnostic, the same blob (sealed bytes) should
be referenceable from multiple View-specific Trees, and each View should
interpret the bytes according to its own conventions.

What this tests:
  - Write one piece of data (some bytes) → seal → get blob_hash
  - Create THREE different Views, each with its own Tree referencing
    the SAME blob_hash:
      * SQL view: tree entry "sql/events/data/0" -> blob_hash
      * Vector view: tree entry "vector/events/embeddings/0" -> blob_hash
      * Streaming view: tree entry "stream/events/log/0" -> blob_hash
  - Read from each View → same bytes returned, interpreted differently
  - Verify: blob is stored ONCE on disk; three views reference it

If this works, the storage kernel is universal.
If it doesn't, something leaked.

NOTE: This test uses pond.py AS-IS — no refactoring. If the existing
prototype can't do this, that's a real finding about what leaked.
"""

import os
import shutil
import time
import sys
import json
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond import Pond, Tree, Commit, hash_bytes


def main():
    print("=" * 76)
    print("  Universality test: one object, multiple Views, no copying")
    print("=" * 76)
    print()
    print("  Thesis: storage should not know about SQL, vectors, graph, streaming.")
    print("  Each is a Lens that interprets the same immutable objects differently.")
    print()
    print("  Test: write one blob, reference it from 3 different View-specific Trees.")
    print()

    bench_dir = "/tmp/pond_universal"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    db = Pond(bench_dir)

    # ------------------------------------------------------------------
    # Step 1: Write one piece of data and seal it.
    # For universality, the storage kernel should NOT care what the bytes are.
    # We'll write Parquet bytes (so the SQL view can read them), but storage
    # doesn't know that — it just sees bytes.
    # ------------------------------------------------------------------
    print("  [1] Writing one piece of data (a Parquet file of events)...")
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("embedding", pa.list_(pa.float32(), 4),),
        pa.field("payload", pa.string()),
    ])
    batch = pa.RecordBatch.from_arrays([
        pa.array([1, 2, 3], type=pa.int64()),
        pa.array([[0.1, 0.2, 0.3, 0.4],
                  [0.5, 0.6, 0.7, 0.8],
                  [0.9, 1.0, 1.1, 1.2]], type=pa.list_(pa.float32(), 4)),
        pa.array(["foo", "bar", "baz"], type=pa.string()),
    ], schema=schema)
    db.write("events", batch)
    sql_commit = db.seal("events", message="initial seal for SQL view")
    print(f"      Sealed. Commit: {sql_commit[:16]}...")

    # Get the blob hash (the actual sealed Parquet bytes on disk)
    sql_commit_obj = db._read_commit(sql_commit)
    sql_root_tree = db._read_tree(sql_commit_obj.tree_hash)
    # Walk to find the data blob hash
    blob_hash = None
    for entry_name, h in sql_root_tree.entries.items():
        if entry_name.startswith("leaf/"):
            leaf = db._read_tree(h)
            for n, bh in leaf.entries.items():
                if "/data/" in n:
                    blob_hash = bh
                    break
        elif entry_name.startswith("subtree/"):
            subtree = db._read_tree(h)
            for n, bh in subtree.entries.items():
                if "/data/" in n:
                    blob_hash = bh
                    break
        if blob_hash:
            break
    print(f"      Underlying blob hash: {blob_hash[:16]}...")
    print(f"      Blob on disk: {db._blob_path(blob_hash)}")
    blob_size = os.path.getsize(db._blob_path(blob_hash))
    print(f"      Blob size: {blob_size} bytes")
    print()

    # ------------------------------------------------------------------
    # Step 2: Create three View-specific Trees, each referencing the SAME blob.
    # The storage kernel doesn't know what "sql", "vector", "stream" mean —
    # they're just names in Tree entries.
    # ------------------------------------------------------------------
    print("  [2] Creating 3 View-specific Trees, all referencing the same blob...")

    # SQL View: tree entry "sql/events/data/0" -> blob_hash
    sql_view_tree = Tree(
        entries={"sql/events/data/0": blob_hash},
        tree_type="leaf",
    )
    sql_view_tree_hash = db._write_tree(sql_view_tree)
    sql_view_commit = Commit(
        tree_hash=sql_view_tree_hash,
        parent_hash=None,
        timestamp=time.time(),
        message="SQL view of events",
    )
    sql_view_commit_hash = db._write_commit(sql_view_commit)
    db.reference("sql_events_view", sql_view_commit_hash)
    print(f"      SQL view:        commit {sql_view_commit_hash[:16]}...  "
          f"-> tree {sql_view_tree_hash[:16]}...  -> blob {blob_hash[:16]}...")

    # Vector View: tree entry "vector/events/embeddings/0" -> blob_hash
    vector_view_tree = Tree(
        entries={"vector/events/embeddings/0": blob_hash},
        tree_type="leaf",
    )
    vector_view_tree_hash = db._write_tree(vector_view_tree)
    vector_view_commit = Commit(
        tree_hash=vector_view_tree_hash,
        parent_hash=None,
        timestamp=time.time(),
        message="Vector view of events (same blob, different interpretation)",
    )
    vector_view_commit_hash = db._write_commit(vector_view_commit)
    db.reference("vector_events_view", vector_view_commit_hash)
    print(f"      Vector view:     commit {vector_view_commit_hash[:16]}...  "
          f"-> tree {vector_view_tree_hash[:16]}...  -> blob {blob_hash[:16]}...")

    # Streaming View: tree entry "stream/events/log/0" -> blob_hash
    stream_view_tree = Tree(
        entries={"stream/events/log/0": blob_hash},
        tree_type="leaf",
    )
    stream_view_tree_hash = db._write_tree(stream_view_tree)
    stream_view_commit = Commit(
        tree_hash=stream_view_tree_hash,
        parent_hash=None,
        timestamp=time.time(),
        message="Streaming view of events (same blob, offset 0)",
    )
    stream_view_commit_hash = db._write_commit(stream_view_commit)
    db.reference("stream_events_view", stream_view_commit_hash)
    print(f"      Streaming view:  commit {stream_view_commit_hash[:16]}...  "
          f"-> tree {stream_view_tree_hash[:16]}...  -> blob {blob_hash[:16]}...")

    print()

    # ------------------------------------------------------------------
    # Step 3: Verify — read from each View, get the same bytes.
    # ------------------------------------------------------------------
    print("  [3] Reading from each View — should return the same bytes...")

    def read_view_blob(view_name: str) -> bytes:
        """Resolve view_name -> commit -> tree -> blob, return blob bytes."""
        commit_hash = db._resolve_name(view_name)
        commit = db._read_commit(commit_hash)
        tree = db._read_tree(commit.tree_hash)
        # Find the blob hash (first /data/ or /embeddings/ or /log/ entry)
        blob_h = None
        for entry_name, h in tree.entries.items():
            if any(p in entry_name for p in ["/data/", "/embeddings/", "/log/"]):
                blob_h = h
                break
        if not blob_h:
            raise ValueError(f"No blob found in view {view_name}")
        with open(db._blob_path(blob_h), "rb") as f:
            return f.read()

    sql_bytes = read_view_blob("sql_events_view")
    vector_bytes = read_view_blob("vector_events_view")
    stream_bytes = read_view_blob("stream_events_view")

    print(f"      SQL view bytes:        {len(sql_bytes)} bytes, "
          f"sha256={hash_bytes(sql_bytes)[:16]}...")
    print(f"      Vector view bytes:     {len(vector_bytes)} bytes, "
          f"sha256={hash_bytes(vector_bytes)[:16]}...")
    print(f"      Streaming view bytes:  {len(stream_bytes)} bytes, "
          f"sha256={hash_bytes(stream_bytes)[:16]}...")

    if sql_bytes == vector_bytes == stream_bytes:
        print()
        print("      ✓ ALL THREE VIEWS RETURN THE SAME BYTES")
        print("      ✓ One blob, three interpretations, zero copies")
    else:
        print()
        print("      ✗ VIEWS RETURN DIFFERENT BYTES — something leaked")
        return

    print()

    # ------------------------------------------------------------------
    # Step 4: Verify — the blob is stored ONCE on disk.
    # ------------------------------------------------------------------
    print("  [4] Verifying the blob is stored exactly once on disk...")

    blob_count = 0
    for shard in os.listdir(db.objects_dir):
        shard_path = os.path.join(db.objects_dir, shard)
        if not os.path.isdir(shard_path):
            continue
        for f in os.listdir(shard_path):
            if f == blob_hash + ".parquet":
                blob_count += 1

    if blob_count == 1:
        print(f"      ✓ Blob {blob_hash[:16]}... exists exactly once on disk")
        print(f"      ✓ Three views reference it; storage cost is 1×, not 3×")
    else:
        print(f"      ✗ Blob exists {blob_count} times (expected 1)")
        return

    print()

    # ------------------------------------------------------------------
    # Step 5: Verify — each View can interpret the bytes its own way.
    # The SQL view reads it as Parquet. The Vector view reads it as Parquet
    # too (because that's how we wrote it) but interprets the embedding
    # column. The Streaming view reads it as Parquet and treats each row
    # as a log entry. The interpretation is in the Lens, not the storage.
    # ------------------------------------------------------------------
    print("  [5] Each View interprets the same bytes differently...")

    import pyarrow.parquet as pq

    # SQL view: scan as a table
    sql_table = pq.read_table(db._blob_path(blob_hash))
    print(f"      SQL View:       SELECT * -> {sql_table.num_rows} rows, "
          f"{sql_table.num_columns} columns: {sql_table.column_names}")

    # Vector view: extract embeddings column
    vector_table = pq.read_table(db._blob_path(blob_hash))
    embeddings = vector_table.column("embedding").to_pylist()
    print(f"      Vector View:    embeddings = {embeddings}")

    # Streaming view: treat rows as log entries
    stream_table = pq.read_table(db._blob_path(blob_hash))
    print(f"      Streaming View: log entries:")
    for row in zip(*[stream_table.column(c).to_pylist() for c in stream_table.column_names]):
        print(f"        id={row[0]}, embedding={row[1]}, payload={row[2]!r}")

    print()

    # ------------------------------------------------------------------
    # Step 6: Storage statistics — prove no copying happened.
    # ------------------------------------------------------------------
    print("  [6] Storage statistics — prove zero-copy across views...")
    stats = db.storage_stats()
    print(f"      Data blobs on disk:    {stats['blob_count']}")
    print(f"      Data bytes on disk:    {stats['data_bytes']:,}")
    print(f"      Metadata objects:      {stats['meta_count']}")
    print(f"      Metadata bytes:        {stats['meta_bytes']:,}")
    print(f"      Tables (root names):   {stats['table_count']}")
    print()
    print(f"      4 root names (sql_events, sql_events_view, vector_events_view,")
    print(f"         stream_events_view) all share 1 data blob.")
    print(f"      Storage cost: 1× data + 4× metadata. NOT 4× data.")

    print()
    print("=" * 76)
    print("  VERDICT")
    print("=" * 76)
    print()
    print("  The storage kernel AS-IS can support multiple Views over the same")
    print("  blob without copying. The Views are just different Trees with")
    print("  different naming conventions, all referencing the same blob hash.")
    print()
    print("  What this proves:")
    print("    - Storage is bytes-only (content-addressed, immutable)")
    print("    - Views are interpretation layers (Trees + naming conventions)")
    print("    - One blob can serve SQL + Vector + Streaming without duplication")
    print()
    print("  What this DOES NOT prove:")
    print("    - That the pond.py API makes this ergonomic (it currently requires")
    print("      manual Tree/Commit construction — see steps 2-3 above)")
    print("    - That the .seal() method is format-agnostic (it hardcodes Parquet)")
    print("    - That the .write() method is format-agnostic (it hardcodes Arrow)")
    print()
    print("  The leak is in the CONVENIENCE API (.write/.seal), not in the kernel.")
    print("  The kernel (Read/Write/Seal/Reference + DAG) is universal.")
    print("  The convenience API is Parquet/Arrow-specific.")
    print()
    print("  Fix: split pond.py into pond_kernel.py (bytes-only, universal) +")
    print("  pond_parquet.py (Parquet/Arrow View). This is the v0.2 refactor.")

    db.close()


if __name__ == "__main__":
    main()
