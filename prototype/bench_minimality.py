"""
Minimality test: can all 8 Views run on a 3-primitive kernel?

The minimal kernel (pond_minimal.py) has ONLY:
  1. Write(bytes) -> hash
  2. Read(hash_or_name) -> bytes
  3. Reference(name, hash)

NO Tree. NO Commit. NO OPEN/SEALED. NO lifecycle.

If all 8 Views work, then Tree/Commit/OPEN-SEALED were never primitive —
they were View-level patterns. That's the finding.

Run:  python3 bench_minimality.py
"""

import os
import shutil
import sys
import time
import struct

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond_minimal import PondMinimal
from views_minimal import (
    SQLView, VectorView, StreamView, GitView,
    GraphView, MLView, TimeSeriesView, OCIView,
)


def main():
    print("=" * 76)
    print("  Minimality test: 8 Views on a 3-primitive kernel")
    print("=" * 76)
    print()
    print("  Kernel primitives (the entire kernel):")
    print("    1. Write(bytes) -> hash")
    print("    2. Read(hash_or_name) -> bytes")
    print("    3. Reference(name, hash)")
    print()
    print("  NO Tree. NO Commit. NO OPEN/SEALED. NO lifecycle.")
    print("  Tree/Commit/Tag are View-level patterns built from these 3.")
    print()
    print("  If all 8 Views work, those concepts were never primitive.")
    print()

    bench_dir = "/tmp/pond_minimal"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    kernel = PondMinimal(bench_dir)
    failures = []

    # ------------------------------------------------------------------
    # View 1: SQL
    # ------------------------------------------------------------------
    print("  [1] SQLView on minimal kernel...")
    try:
        import pyarrow as pa
        sql = SQLView(kernel, "users")
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        sql.create(schema)
        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["alice", "bob", "carol"], type=pa.string()),
        ], schema=schema)
        sql.insert(batch)
        sql.commit()
        t = sql.read()
        assert t.num_rows == 3
        assert t.column("name").to_pylist() == ["alice", "bob", "carol"]
        print(f"      ✓ SQLView works (3 rows)")
    except Exception as e:
        print(f"      ✗ SQLView failed: {e}")
        failures.append(("SQLView", str(e)))

    # ------------------------------------------------------------------
    # View 2: Vector
    # ------------------------------------------------------------------
    print("  [2] VectorView on minimal kernel...")
    try:
        vec = VectorView(kernel, "embeddings", dim=4)
        vec.insert([0.1, 0.2, 0.3, 0.4])
        vec.insert([0.5, 0.6, 0.7, 0.8])
        vec.commit()
        results = vec.search([0.1, 0.2, 0.3, 0.4], k=1)
        assert len(results) == 1
        assert results[0][1] == 0
        print(f"      ✓ VectorView works (search found nearest)")
    except Exception as e:
        print(f"      ✗ VectorView failed: {e}")
        failures.append(("VectorView", str(e)))

    # ------------------------------------------------------------------
    # View 3: Stream
    # ------------------------------------------------------------------
    print("  [3] StreamView on minimal kernel...")
    try:
        s = StreamView(kernel, "topic")
        for i in range(5):
            s.produce(f"event-{i}".encode())
        s.commit()
        records = s.consume()
        assert len(records) == 5
        assert records[0] == b"event-0"
        assert records[4] == b"event-4"
        print(f"      ✓ StreamView works (5 records)")
    except Exception as e:
        print(f"      ✗ StreamView failed: {e}")
        failures.append(("StreamView", str(e)))

    # ------------------------------------------------------------------
    # View 4: Git
    # ------------------------------------------------------------------
    print("  [4] GitView on minimal kernel...")
    try:
        g = GitView(kernel, "repo")
        g.add("README.md", b"# Hello\n")
        g.add("main.py", b"print('hi')\n")
        g.commit("initial")
        g.add("README.md", b"# Hello world\n")
        g.commit("update readme")
        assert g.read_file("README.md") == b"# Hello world\n"
        assert g.read_file("main.py") == b"print('hi')\n"
        assert len(g.log()) == 2
        print(f"      ✓ GitView works (2 commits, file inheritance works)")
    except Exception as e:
        print(f"      ✗ GitView failed: {e}")
        failures.append(("GitView", str(e)))

    # ------------------------------------------------------------------
    # View 5: Graph
    # ------------------------------------------------------------------
    print("  [5] GraphView on minimal kernel...")
    try:
        g = GraphView(kernel, "social")
        g.add_node("alice", {"age": 30})
        g.add_node("bob", {"age": 25})
        g.add_edge("alice", "bob")
        g.commit()
        alice = g.get_node("alice")
        assert alice["properties"]["age"] == 30
        neighbors = g.neighbors("alice")
        assert len(neighbors) == 1
        assert neighbors[0]["dst"] == "bob"
        print(f"      ✓ GraphView works (traversal works)")
    except Exception as e:
        print(f"      ✗ GraphView failed: {e}")
        failures.append(("GraphView", str(e)))

    # ------------------------------------------------------------------
    # View 6: ML
    # ------------------------------------------------------------------
    print("  [6] MLView on minimal kernel...")
    try:
        ml = MLView(kernel, "registry")
        ml.log_checkpoint("model", 100, b"weights-v1", {"loss": 0.5})
        ml.log_checkpoint("model", 200, b"weights-v2", {"loss": 0.3})
        assert ml.get_weights("model", 100) == b"weights-v1"
        assert ml.get_weights("model", 200) == b"weights-v2"
        assert ml.get_metadata("model", 200)["loss"] == 0.3
        print(f"      ✓ MLView works (2 checkpoints)")
    except Exception as e:
        print(f"      ✗ MLView failed: {e}")
        failures.append(("MLView", str(e)))

    # ------------------------------------------------------------------
    # View 7: TimeSeries
    # ------------------------------------------------------------------
    print("  [7] TimeSeriesView on minimal kernel...")
    try:
        ts = TimeSeriesView(kernel, "metrics")
        points = [(int(time.time()*1e6) + i, 50.0 + i) for i in range(100)]
        ts.write_points("cpu", points)
        pts = ts.read_series("cpu")
        assert len(pts) == 100
        assert pts[0][1] == 50.0
        assert pts[99][1] == 149.0
        print(f"      ✓ TimeSeriesView works (100 points)")
    except Exception as e:
        print(f"      ✗ TimeSeriesView failed: {e}")
        failures.append(("TimeSeriesView", str(e)))

    # ------------------------------------------------------------------
    # View 8: OCI
    # ------------------------------------------------------------------
    print("  [8] OCIView on minimal kernel...")
    try:
        oci = OCIView(kernel, "registry")
        layer = oci.push_layer(b"layer-bytes")
        config = oci.push_config({"Cmd": ["run"]})
        oci.push_manifest("app", "v1", config, [layer])
        m = oci.pull_manifest("app", "v1")
        assert m["schemaVersion"] == 2
        assert oci.pull_layer(layer) == b"layer-bytes"
        print(f"      ✓ OCIView works (push + pull)")
    except Exception as e:
        print(f"      ✗ OCIView failed: {e}")
        failures.append(("OCIView", str(e)))

    # ------------------------------------------------------------------
    # Storage summary
    # ------------------------------------------------------------------
    print()
    print("  [9] Storage summary — 8 Views on minimal kernel")
    stats = kernel.storage_stats()
    print(f"      Total data blobs:    {stats['blob_count']}")
    print(f"      Total data bytes:    {stats['data_bytes']:,}")
    print(f"      Names in namespace:  {stats['name_count']}")
    print(f"        -> {kernel.list_names()}")
    print(f"      Kernel writes:       {stats['writes']}")
    print(f"      Kernel reads:        {stats['reads']}")
    print(f"      Kernel references:   {stats['references']}")
    print()

    # ------------------------------------------------------------------
    # Verdict
    # ------------------------------------------------------------------
    print("=" * 76)
    print("  VERDICT")
    print("=" * 76)
    print()

    if not failures:
        print("  ✓ ALL 8 VIEWS WORK ON A 3-PRIMITIVE KERNEL")
        print()
        print("  The minimal kernel primitives are:")
        print("    1. Write(bytes) -> hash       (create immutable content-addressed blob)")
        print("    2. Read(hash_or_name) -> bytes (fetch blob by hash or name)")
        print("    3. Reference(name, hash)      (mutable name -> hash mapping)")
        print()
        print("  What was REMOVED (and the Views still work):")
        print("    - Tree          (now a View pattern: blob with serialized {name -> hash})")
        print("    - Commit        (now a View pattern: blob with serialized metadata)")
        print("    - Tag           (now just Reference(name, commit_hash))")
        print("    - Branch        (now just Reference(name, commit_hash))")
        print("    - OPEN/SEALED   (now a View-level buffer optimization)")
        print("    - Lifecycle     (OPEN/SEALED/COMPACTED/ARCHIVED/GC — all View-level)")
        print("    - write_tree    (View helper, not kernel)")
        print("    - read_tree     (View helper, not kernel)")
        print("    - write_commit  (View helper, not kernel)")
        print("    - read_commit   (View helper, not kernel)")
        print("    - walk_tree     (View helper, not kernel)")
        print()
        print("  FINDING: Tree, Commit, OPEN/SEALED, and the lifecycle were NEVER")
        print("  primitive. They were View-level patterns. The kernel only needs")
        print("  Write + Read + Reference.")
        print()
        print("  This is the minimal basis. Pond's storage algebra is 3 primitives.")
        print()
        print("  What this means:")
        print("    - Git's blob/tree/commit model is a View, not the architecture")
        print("    - Iceberg's manifest/snapshot model would be a View")
        print("    - Delta's transaction log would be a View")
        print("    - OCI's manifest/layer model is a View")
        print("    - The kernel has zero opinion about object structure")
        print()
        print("  Architectural implications:")
        print("    - The kernel is now small enough to be formally specified in 1 page")
        print("    - The kernel is small enough to be reimplemented in any language")
        print("    - The kernel is small enough to remain stable for decades")
        print("      (like Linux syscalls — Write/Read/Reference won't change)")
        print("    - Views can innovate freely without kernel changes")
        print("    - Pond is now genuinely 'a universal immutable object runtime',")
        print("      not 'a Git-shaped storage engine'")
    else:
        print("  ✗ SOME VIEWS FAILED ON THE MINIMAL KERNEL")
        print()
        for name, err in failures:
            print(f"    - {name}: {err}")
        print()
        print("  This means the removed primitive was actually necessary.")
        print("  Investigate whether to admit it to the kernel (via the 5-criterion")
        print("  Admission Rule) or fix the View.")

    kernel.close()


if __name__ == "__main__":
    main()
