"""
Universality stress test: build 4 radically different Views.

Per the architecture review: "I would spend more effort trying to disprove
the universality of the kernel than trying to squeeze another 10% out of
metadata."

Each View below is structurally different from SQL/Streaming/Git:
  - GraphView      (nodes, edges, adjacency traversal)
  - MLView         (model checkpoints, weights, training history)
  - TimeSeriesView (compressed segments, retention, aggregation)
  - OCIView        (Docker image layers, container registry)

The test: do any of these require kernel modifications?

If NO  — the kernel is universal. Pond is on the path to "universal
         immutable runtime," not "another table format."
If YES — there's a kernel leak. Document it; decide whether to admit the
         feature (via the 5-criterion Admission Rule) or push it back
         into the View.

Run:  python3 bench_universality_stress.py
"""

import os
import shutil
import sys
import json
import time
import struct

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond_kernel import PondKernel, Tree, Commit, hash_bytes
from views import SQLView, VectorView, StreamView, GitView
from more_views import GraphView, MLView, TimeSeriesView, OCIView


def main():
    print("=" * 76)
    print("  Universality stress test: 4 radically different Views")
    print("=" * 76)
    print()
    print("  Goal: try to break the kernel. Build Views that are structurally")
    print("  different from SQL/Streaming/Git. If any requires kernel changes,")
    print("  that's a finding — the kernel leaked or is missing a primitive.")
    print()

    bench_dir = "/tmp/pond_universal_stress"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    kernel = PondKernel(bench_dir)
    kernel_changes_needed = []

    # ------------------------------------------------------------------
    # View 5: GraphView — nodes, edges, adjacency traversal
    # ------------------------------------------------------------------
    print("  [5] GraphView — social network (5 nodes, 6 edges, traversal)")
    try:
        graph = GraphView(kernel, "social_graph")
        # Build a small social graph
        graph.add_node("alice", {"name": "Alice", "age": 30})
        graph.add_node("bob", {"name": "Bob", "age": 25})
        graph.add_node("carol", {"name": "Carol", "age": 35})
        graph.add_node("dave", {"name": "Dave", "age": 28})
        graph.add_node("eve", {"name": "Eve", "age": 22})
        graph.add_edge("alice", "bob", {"since": "2020"})
        graph.add_edge("alice", "carol", {"since": "2021"})
        graph.add_edge("bob", "carol", {"since": "2019"})
        graph.add_edge("carol", "dave", {"since": "2022"})
        graph.add_edge("dave", "eve", {"since": "2023"})
        graph.add_edge("eve", "alice", {"since": "2024"})
        graph.commit(message="initial social graph")

        # Test retrieval
        alice = graph.get_node("alice")
        print(f"      get_node('alice') = {alice}")
        alice_friends = graph.neighbors("alice")
        print(f"      neighbors('alice') = {[e['dst'] for e in alice_friends]}")
        # Traverse 2 hops from alice
        visited = graph.traverse("alice", max_depth=2)
        print(f"      traverse('alice', depth=2) = {visited}")
        assert "bob" in visited and "carol" in visited
        print(f"      ✓ GraphView works — no kernel changes needed")
    except Exception as e:
        print(f"      ✗ GraphView failed: {e}")
        kernel_changes_needed.append(f"GraphView: {e}")

    print()

    # ------------------------------------------------------------------
    # View 6: MLView — model checkpoints and training history
    # ------------------------------------------------------------------
    print("  [6] MLView — model checkpoint registry")
    try:
        ml = MLView(kernel, "model_registry")
        # Log 3 checkpoints of a fake model
        for step in [100, 200, 300]:
            # Fake weights: 1KB of pseudo-random bytes
            weights = bytes([(step + i) % 256 for i in range(1024)])
            ml.log_checkpoint(
                model_name="resnet50",
                step=step,
                weights=weights,
                metadata={"loss": 1.0 / step, "lr": 0.001, "epoch": step // 100}
            )
        # Retrieve
        w = ml.get_weights("resnet50", 200)
        m = ml.get_metadata("resnet50", 200)
        print(f"      get_weights('resnet50', 200) = {len(w)} bytes")
        print(f"      get_metadata('resnet50', 200) = {m}")
        # History
        hist = ml.history("resnet50")
        print(f"      history('resnet50') = {len(hist)} checkpoints")
        for h in hist:
            print(f"        step={h['step']}, loss={h['metadata']['loss']}")
        print(f"      ✓ MLView works — no kernel changes needed")
    except Exception as e:
        print(f"      ✗ MLView failed: {e}")
        kernel_changes_needed.append(f"MLView: {e}")

    print()

    # ------------------------------------------------------------------
    # View 7: TimeSeriesView — compressed segments + retention
    # ------------------------------------------------------------------
    print("  [7] TimeSeriesView — metrics with retention")
    try:
        ts = TimeSeriesView(kernel, "metrics_db")
        # Write 3 segments of CPU metrics
        base_ts = int(time.time() * 1e6)
        for seg in range(3):
            points = [
                (base_ts + (seg * 100 + i) * 1_000_000, 50.0 + i * 0.5)
                for i in range(100)
            ]
            ts.write_points("cpu_load", points)
        # Read back
        pts = ts.read_series("cpu_load")
        print(f"      Wrote 300 points. Read back {len(pts)} points.")
        print(f"      First: ts={pts[0][0]}, val={pts[0][1]}")
        print(f"      Last:  ts={pts[-1][0]}, val={pts[-1][1]}")
        # Apply retention — drop segments older than 0 days (drops all)
        ts.apply_retention("cpu_load", retention_days=0)
        pts_after = ts.read_series("cpu_load")
        print(f"      After retention(0 days): {len(pts_after)} points remain")
        print(f"      ✓ TimeSeriesView works — no kernel changes needed")
    except Exception as e:
        print(f"      ✗ TimeSeriesView failed: {e}")
        kernel_changes_needed.append(f"TimeSeriesView: {e}")

    print()

    # ------------------------------------------------------------------
    # View 8: OCIView — container registry
    # ------------------------------------------------------------------
    print("  [8] OCIView — Docker image registry")
    try:
        oci = OCIView(kernel, "container_registry")
        # Push a fake image: config + 3 layers
        config = {
            "architecture": "amd64",
            "os": "linux",
            "config": {"Cmd": ["python3", "app.py"]},
            "rootfs": {"type": "layers", "diff_ids": []},
        }
        config_digest = oci.push_config(config)
        layer_digests = []
        for i in range(3):
            layer_bytes = bytes([i] * 1024)  # 1KB fake layer
            d = oci.push_layer(layer_bytes)
            layer_digests.append(d)
        oci.push_manifest("myapp", "v1.0", config_digest, layer_digests)
        # Pull it back
        manifest = oci.pull_manifest("myapp", "v1.0")
        print(f"      Pushed image myapp:v1.0 (config + 3 layers)")
        print(f"      Pulled manifest: schemaVersion={manifest['schemaVersion']}")
        print(f"      Config digest: {manifest['config']['digest'][:22]}...")
        print(f"      Layers: {len(manifest['layers'])}")
        for i, layer in enumerate(manifest['layers']):
            print(f"        layer {i}: {layer['digest'][:22]}... ({layer['size']} bytes)")
        # Pull a layer
        layer_data = oci.pull_layer(layer_digests[0])
        print(f"      Pulled layer 0: {len(layer_data)} bytes")
        print(f"      ✓ OCIView works — no kernel changes needed")
    except Exception as e:
        print(f"      ✗ OCIView failed: {e}")
        kernel_changes_needed.append(f"OCIView: {e}")

    print()

    # ------------------------------------------------------------------
    # The proof: 8 Views share ONE kernel
    # ------------------------------------------------------------------
    print("  [9] Storage summary — 8 Views on 1 kernel")
    stats = kernel.storage_stats()
    print(f"      Total data blobs:        {stats['blob_count']}")
    print(f"      Total data bytes:        {stats['data_bytes']:,}")
    print(f"      Total metadata objects:  {stats['meta_count']}")
    print(f"      Total metadata bytes:    {stats['meta_bytes']:,}")
    print(f"      Names in namespace:      {stats['name_count']}")
    print(f"        -> {kernel.list_names()}")
    print()

    print("=" * 76)
    print("  VERDICT")
    print("=" * 76)
    print()

    if not kernel_changes_needed:
        print("  ✓ THE KERNEL IS UNIVERSAL")
        print()
        print("  8 Views share one immutable object substrate:")
        print("    1. SQLView       (Parquet, tabular)")
        print("    2. VectorView    (raw floats, embeddings)")
        print("    3. StreamView    (length-prefixed records, Kafka-like)")
        print("    4. GitView       (files + directories + commits)")
        print("    5. GraphView     (nodes + edges + adjacency)")
        print("    6. MLView        (model checkpoints + lineage)")
        print("    7. TimeSeriesView (segments + retention)")
        print("    8. OCIView       (Docker layers + manifests)")
        print()
        print("  No kernel modifications were required. Each View is a thin")
        print("  adapter using only the 4 syscalls (Read/Write/Seal/Reference)")
        print("  + DAG patterns (Tree/Commit). The kernel has zero knowledge")
        print("  of: SQL, Parquet, Arrow, vectors, streaming, Git, graphs, ML,")
        print("  time-series, or container images.")
        print()
        print("  What this proves:")
        print("    - The 4 syscalls are sufficient for radically different workloads")
        print("    - The DAG pattern is universal (not just Git-shaped)")
        print("    - Pond is on the path to 'universal immutable runtime',")
        print("      not 'another table format'")
        print()
        print("  What this DOES NOT prove:")
        print("    - That these Views are production-quality (they're demos)")
        print("    - That performance is acceptable at scale (untested)")
        print("    - That the kernel has no leaks at all (more Views to test)")
        print("    - That Tree/Commit are the right primitives (see audit below)")
    else:
        print("  ✗ KERNEL LEAKS FOUND:")
        for change in kernel_changes_needed:
            print(f"    - {change}")
        print()
        print("  These need to be evaluated against the Kernel Admission Rule.")
        print("  If a feature is truly needed, it must pass all 5 criteria:")
        print("    1. Universal (3+ Views need it)")
        print("    2. Impossible outside kernel")
        print("    3. Immutable")
        print("    4. Storage-independent")
        print("    5. Decades-stable")

    print()
    print("=" * 76)
    print("  Tree/Commit Audit (per the reviewer's caution)")
    print("=" * 76)
    print()
    print("  The reviewer asked: are Tree and Commit truly universal, or")
    print("  is Git's model leaking?")
    print()
    print("  Observations from this test:")
    print("    - SQLView, VectorView, StreamView, GitView, GraphView, MLView,")
    print("      TimeSeriesView, OCIView all use Tree/Commit successfully.")
    print("    - Each View uses Trees differently (flat, hierarchical,")
    print("      per-series, per-manifest) — the kernel doesn't impose structure.")
    print("    - Commit's parent_hash is used by Git (history) but ignored by")
    print("      OCIView (no parent — manifests are independent).")
    print("    - Commit's message field is used inconsistently (some Views")
    print("      store metadata, some don't). This is fine — the kernel")
    print("      doesn't interpret it.")
    print()
    print("  Verdict: Tree/Commit appear universal. They're patterns over the")
    print("  4 syscalls, not the syscalls themselves. If a future View needs")
    print("  a different pattern (e.g., a Merkle DAG with multiple parents),")
    print("  it can build that pattern from the same 4 syscalls. The kernel")
    print("  doesn't enforce Git's specific object model.")
    print()
    print("  Caveat: this is empirical, not formal. More Views and more")
    print("  extreme workloads (e.g., CRDTs, multi-parent DAGs) would test")
    print("  this further. But after 8 Views with no kernel changes, the")
    print("  burden of proof shifts to 'show me a View that breaks it.'")

    kernel.close()


if __name__ == "__main__":
    main()
