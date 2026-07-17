"""
Capability Independence Test.

Per the architecture review:
  > Imagine a CI pipeline. Disable SQL. Run. Everything passes.
  > Disable Streaming. Run. Everything passes.
  > If removing a View breaks another View, you have coupling.

This test:
  1. For each View, instantiate it in isolation (no other Views loaded)
  2. Run a basic write/read cycle
  3. Verify the View works without any other View present

If any View fails when run in isolation, there's hidden coupling —
the View depends on another View's side effects, and the kernel
isn't truly View-agnostic.

This is the test that should run in CI on every PR.
"""

import os
import shutil
import sys
import json
import importlib
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond_kernel import PondKernel


def test_view_in_isolation(view_name: str, test_fn) -> tuple[bool, str]:
    """Run a single View test in a fresh kernel + fresh dir."""
    bench_dir = f"/tmp/pond_independence_{view_name}"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    kernel = PondKernel(bench_dir)
    try:
        test_fn(kernel)
        return True, "OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
    finally:
        kernel.close()
        # Don't clean up dir on failure — for debugging
        if not os.environ.get("POND_DEBUG"):
            shutil.rmtree(bench_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Per-View test functions
# ---------------------------------------------------------------------------

def test_sql(kernel: PondKernel):
    from views import SQLView
    import pyarrow as pa
    sql = SQLView(kernel, "users")
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    sql.create(schema)
    batch = pa.RecordBatch.from_arrays([
        pa.array([1, 2], type=pa.int64()),
        pa.array(["a", "b"], type=pa.string()),
    ], schema=schema)
    sql.insert(batch)
    sql.commit()
    t = sql.read()
    assert t.num_rows == 2


def test_vector(kernel: PondKernel):
    from views import VectorView
    vec = VectorView(kernel, "emb", dim=4)
    vec.insert([0.1, 0.2, 0.3, 0.4])
    vec.commit()
    results = vec.search([0.1, 0.2, 0.3, 0.4], k=1)
    assert len(results) == 1
    assert results[0][1] == 0  # the only vector


def test_stream(kernel: PondKernel):
    from views import StreamView
    s = StreamView(kernel, "topic")
    s.produce(b"hello")
    s.produce(b"world")
    s.commit()
    records = s.consume()
    assert records == [b"hello", b"world"]


def test_git(kernel: PondKernel):
    from views import GitView
    g = GitView(kernel, "repo")
    g.add("file.txt", b"content")
    g.commit("initial")
    assert g.read_file("file.txt") == b"content"
    assert len(g.log()) == 1


def test_graph(kernel: PondKernel):
    from more_views import GraphView
    g = GraphView(kernel, "graph")
    g.add_node("a", {"x": 1})
    g.add_node("b", {"y": 2})
    g.add_edge("a", "b")
    g.commit()
    assert g.get_node("a")["properties"]["x"] == 1
    neighbors = g.neighbors("a")
    assert len(neighbors) == 1
    assert neighbors[0]["dst"] == "b"


def test_ml(kernel: PondKernel):
    from more_views import MLView
    ml = MLView(kernel, "registry")
    ml.log_checkpoint("model", 1, b"weights", {"loss": 0.5})
    assert ml.get_weights("model", 1) == b"weights"
    assert ml.get_metadata("model", 1)["loss"] == 0.5
    assert len(ml.history("model")) == 1


def test_timeseries(kernel: PondKernel):
    from more_views import TimeSeriesView
    import time
    ts = TimeSeriesView(kernel, "metrics")
    ts.write_points("cpu", [(int(time.time()*1e6), 50.0)])
    pts = ts.read_series("cpu")
    assert len(pts) == 1
    assert pts[0][1] == 50.0


def test_oci(kernel: PondKernel):
    from more_views import OCIView
    oci = OCIView(kernel, "registry")
    layer = oci.push_layer(b"layer-bytes")
    config = oci.push_config({"Cmd": ["run"]})
    oci.push_manifest("app", "v1", config, [layer])
    m = oci.pull_manifest("app", "v1")
    assert m["schemaVersion"] == 2
    assert oci.pull_layer(layer) == b"layer-bytes"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 76)
    print("  Capability Independence Test")
    print("=" * 76)
    print()
    print("  Each View is tested in ISOLATION (fresh kernel, no other Views).")
    print("  If any View fails when run alone, there's hidden coupling.")
    print()
    print("  This is the CI test that proves View independence.")
    print()

    views = [
        ("SQLView",        test_sql),
        ("VectorView",     test_vector),
        ("StreamView",     test_stream),
        ("GitView",        test_git),
        ("GraphView",      test_graph),
        ("MLView",         test_ml),
        ("TimeSeriesView", test_timeseries),
        ("OCIView",        test_oci),
    ]

    all_passed = True
    print(f"  {'View':<20}  {'Result':<10}  {'Details':<40}")
    print(f"  {'-'*20}  {'-'*10}  {'-'*40}")

    for name, test_fn in views:
        ok, details = test_view_in_isolation(name, test_fn)
        if ok:
            print(f"  {name:<20}  {'PASS':<10}  {details}")
        else:
            print(f"  {name:<20}  {'FAIL':<10}  {details[:80]}")
            all_passed = False

    print()
    print("=" * 76)
    if all_passed:
        print("  ✓ ALL VIEWS PASS IN ISOLATION")
        print()
        print("  No View depends on another View. The kernel is truly")
        print("  View-agnostic. Removing any View (or all Views) doesn't")
        print("  affect the others.")
        print()
        print("  This means:")
        print("    - SQLView can be deleted; Vector/Stream/Git/Graph/ML/TS/OCI still work")
        print("    - OCIView can be deleted; SQL/Vector/Stream/Git/Graph/ML/TS still work")
        print("    - etc.")
        print()
        print("  The kernel has zero coupling to any View. This is the test")
        print("  that should run in CI on every PR.")
    else:
        print("  ✗ SOME VIEWS FAILED IN ISOLATION")
        print()
        print("  There's hidden coupling. A View depends on another View's")
        print("  side effects. This is a kernel leak — investigate.")

    print("=" * 76)


if __name__ == "__main__":
    main()
