#!/usr/bin/env python3
"""
Run view_laws.py against the externally-built GraphLens.

This is the strongest possible test of RFC-0007's generality:
a Lens built from spec alone (no access to pond-sdk internals)
should still satisfy all 6 View algebra laws.

If this passes, it confirms that the Lens algebra is a real
specification, not just a description of pond-sdk's own Views.
"""

from __future__ import annotations

import os
import sys
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
POND_ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(POND_ROOT, "pond-core"))
sys.path.insert(0, os.path.join(POND_ROOT, "pond-sdk"))
sys.path.insert(0, os.path.join(POND_ROOT, "validation"))

from pond_minimal import PondMinimal
from lens_laws import LensLaws, LensContract
from graph_view_external import GraphView


def make_graph_view_contract(kernel) -> tuple:
    """Contract for the externally-built GraphLens."""
    view = GraphView(kernel, "ci_graph_external")
    return view, LensContract(
        name="ci_graph_external",
        encode=view.encode,
        decode=view.decode,
        put=view.put,
        get=view.get,
        delete=view.delete,
        commit=view.commit,
        keys=view.keys,
        get_all=view.get_all,
        list_materializations=lambda: [],  # GraphView's indexes are not exposed as materializations
        build_materialization=None,
        # GraphView sample data: use node-style payloads
        sample_data=lambda i: {"id": f"n{i}", "type": "user", "properties": {"name": f"user-{i}", "age": i}},
    )


def main() -> int:
    print("=" * 72)
    print("  View Algebra — External GraphView Compliance Test")
    print("  (graph_view_external.py built from SDK_SPEC.md alone)")
    print("=" * 72)
    print()

    bench_dir = "/tmp/pond_ci_graph_external"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    try:
        kernel = PondMinimal(bench_dir)
        view, contract = make_graph_view_contract(kernel)
        laws = LensLaws(kernel)
        report = laws.check_all(contract, num_samples=10)
        print(report)
        print()
        kernel.close()
        shutil.rmtree(bench_dir, ignore_errors=True)
        return 0 if report.all_passed else 1
    except Exception as e:
        import traceback
        traceback.print_exc()
        shutil.rmtree(bench_dir, ignore_errors=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
