#!/usr/bin/env python3
"""
CI entry point for the View Algebra property-test harness (RFC-0007).

Runs `view_laws.py` against every registered View class and exits
non-zero if any View violates any of the 6 laws.

Designed for CI: no flakiness, no random data, deterministic output.
Add to CI with:

    python pond-sdk/run_view_laws_ci.py

Exit codes:
    0 — all Views passed all 6 laws
    1 — at least one View violated at least one law
    2 — harness itself failed to run (import errors, etc.)
"""

from __future__ import annotations

import os
import sys
import shutil
import traceback

# Path setup
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "prototype"))
sys.path.insert(0, HERE)

from pond_minimal import PondMinimal
from view_laws import ViewLaws, ViewContract


# ---------------------------------------------------------------------------
# View contracts — one per registered View class
# ---------------------------------------------------------------------------

def make_default_view_contract(kernel) -> tuple:
    """Contract for the default View class."""
    from view_sdk import View
    view = View(kernel, "ci_default")
    return view, ViewContract(
        name="ci_default",
        encode=view.encode,
        decode=view.decode,
        put=view.put,
        get=view.get,
        delete=view.delete,
        commit=view.commit,
        keys=view.keys,
        get_all=view.get_all,
        list_materializations=lambda: [],
        build_materialization=None,
        sample_data=lambda i: {"id": i, "name": f"item-{i}", "value": i * 10},
    )


def make_indexed_view_contract(kernel) -> tuple:
    """Contract for IndexedView with an eager index as materialization."""
    from auto_index import IndexedView
    view = IndexedView(kernel, "ci_indexed")
    view.register_index("by_value", lambda d: str(d.get("value", 0)), mode="eager")

    def build_mat(name: str) -> bytes:
        if name == "by_value":
            idx = view._auto_indexes.get("by_value")
            if idx is None:
                return b""
            view._rebuild_index(idx)
            return (idx.tree_root or "").encode()
        return b""

    return view, ViewContract(
        name="ci_indexed",
        encode=view.encode,
        decode=view.decode,
        put=view.put,
        get=view.get,
        delete=view.delete,
        commit=view.commit,
        keys=view.keys,
        get_all=view.get_all,
        list_materializations=lambda: ["by_value"],
        build_materialization=build_mat,
        sample_data=lambda i: {"id": i, "name": f"item-{i}", "value": i * 10},
    )


def make_semantic_view_contract(kernel) -> tuple:
    """Contract for SemanticView (a subclass of View)."""
    from view_sdk import SemanticView
    view = SemanticView(kernel, "ci_semantic")
    return view, ViewContract(
        name="ci_semantic",
        encode=view.encode,
        decode=view.decode,
        put=view.put,
        get=view.get,
        delete=view.delete,
        commit=view.commit,
        keys=view.keys,
        get_all=view.get_all,
        list_materializations=lambda: [],
        build_materialization=None,
        sample_data=lambda i: {"name": f"metric_{i}", "value": i * 100},
    )


# Registry of View contracts to test
VIEW_CONTRACTS = [
    ("Default View", make_default_view_contract),
    ("IndexedView", make_indexed_view_contract),
    ("SemanticView", make_semantic_view_contract),
]


# ---------------------------------------------------------------------------
# CI runner
# ---------------------------------------------------------------------------

def run_one(name: str, contract_factory) -> tuple[bool, str]:
    """Run view_laws against one View. Returns (passed, output)."""
    bench_dir = f"/tmp/pond_ci_{name.lower().replace(' ', '_')}"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    try:
        kernel = PondMinimal(bench_dir)
        view, contract = contract_factory(kernel)
        laws = ViewLaws(kernel)
        report = laws.check_all(contract, num_samples=10)
        output = str(report)
        passed = report.all_passed

        # Cleanup
        try:
            kernel.close()
        except Exception:
            pass
        shutil.rmtree(bench_dir, ignore_errors=True)
        return passed, output
    except Exception as e:
        tb = traceback.format_exc()
        shutil.rmtree(bench_dir, ignore_errors=True)
        return False, f"EXCEPTION during {name} test:\n{tb}"


def main() -> int:
    print("=" * 72)
    print("  View Algebra CI — RFC-0007 property tests (view_laws.py)")
    print("=" * 72)
    print()

    all_passed = True
    summary_lines = []

    for name, factory in VIEW_CONTRACTS:
        print(f"--- {name} ---")
        passed, output = run_one(name, factory)
        print(output)
        print()
        summary_lines.append((name, passed))
        if not passed:
            all_passed = False

    print("=" * 72)
    print("  SUMMARY")
    print("=" * 72)
    for name, passed in summary_lines:
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {name}")
    print()

    if all_passed:
        print("  ALL VIEWS SATISFY ALL 6 VIEW ALGEBRA LAWS.")
        print("  RFC-0007 status: compliant.")
        return 0
    else:
        failed = [n for n, p in summary_lines if not p]
        print(f"  {len(failed)} VIEW(S) FAILED: {', '.join(failed)}")
        print("  RFC-0007 status: NON-COMPLIANT — fix violations before release.")
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"HARNESS FAILURE: {type(e).__name__}: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(2)
