#!/usr/bin/env python3
"""
CI entry point for the Lens Algebra property-test harness (RFC-0007).

Runs `lens_laws.py` against every registered Lens class and exits
non-zero if any Lens violates any of the 6 laws.

Designed for CI: no flakiness, no random data, deterministic output.
Add to CI with:

    python pond-sdk/run_view_laws_ci.py

Exit codes:
    0 — all Lenses passed all 6 laws
    1 — at least one Lens violated at least one law
    2 — harness itself failed to run (import errors, etc.)
"""

from __future__ import annotations

import os
import sys
import shutil
import traceback

# Path setup
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "pond-sdk"))

from kernel import PondMinimal
from lens_laws import LensLaws, LensContract


# ---------------------------------------------------------------------------
# Lens contracts — one per registered Lens class
# ---------------------------------------------------------------------------

def make_default_view_contract(kernel) -> tuple:
    """Contract for the default KeyValueLens class."""
    from keyvalue_lens import KeyValueLens
    lens = KeyValueLens(kernel, "ci_default")
    return lens, LensContract(
        name="ci_default",
        encode=lens.encode,
        decode=lens.decode,
        put=lens.put,
        get=lens.get,
        delete=lens.delete,
        commit=lens.commit,
        keys=lens.keys,
        get_all=lens.get_all,
        list_materializations=lambda: [],
        build_materialization=None,
        sample_data=lambda i: {"id": i, "name": f"item-{i}", "value": i * 10},
    )


def make_indexed_view_contract(kernel) -> tuple:
    """Contract for IndexedLens with an eager index as materialization."""
    from extensions.indexing.auto_index import IndexedLens
    lens = IndexedLens(kernel, "ci_indexed")
    lens.register_index("by_value", lambda d: str(d.get("value", 0)), mode="eager")

    def build_mat(name: str) -> bytes:
        if name == "by_value":
            idx = lens._auto_indexes.get("by_value")
            if idx is None:
                return b""
            lens._rebuild_index(idx)
            return (idx.tree_root or "").encode()
        return b""

    return lens, LensContract(
        name="ci_indexed",
        encode=lens.encode,
        decode=lens.decode,
        put=lens.put,
        get=lens.get,
        delete=lens.delete,
        commit=lens.commit,
        keys=lens.keys,
        get_all=lens.get_all,
        list_materializations=lambda: ["by_value"],
        build_materialization=build_mat,
        sample_data=lambda i: {"id": i, "name": f"item-{i}", "value": i * 10},
    )


def make_semantic_view_contract(kernel) -> tuple:
    """Contract for SemanticLens (a subclass of KeyValueLens)."""
    from extensions.semantic.ossie import SemanticLens
    lens = SemanticLens(kernel, "ci_semantic")
    return lens, LensContract(
        name="ci_semantic",
        encode=lens.encode,
        decode=lens.decode,
        put=lens.put,
        get=lens.get,
        delete=lens.delete,
        commit=lens.commit,
        keys=lens.keys,
        get_all=lens.get_all,
        list_materializations=lambda: [],
        build_materialization=None,
        sample_data=lambda i: {"name": f"metric_{i}", "value": i * 100},
    )


def make_multikey_view_contract(kernel) -> tuple:
    """Contract for IndexedLens with a multi-key (list-returning) extractor.

    Tests Phase B.3 multikey index support: extractor returns a list of
    tags, and the row is indexed under each tag.
    """
    from extensions.indexing.auto_index import IndexedLens
    lens = IndexedLens(kernel, "ci_multikey")
    lens.register_index("by_tag",
                         lambda d: d.get("tags", []),
                         mode="eager")
    lens.register_index("by_id",
                         lambda d: str(d.get("id", 0)),
                         mode="eager")

    def build_mat(name: str) -> bytes:
        idx = lens._auto_indexes.get(name)
        if idx is None:
            return b""
        lens._rebuild_index(idx)
        return (idx.tree_root or "").encode()

    return lens, LensContract(
        name="ci_multikey",
        encode=lens.encode,
        decode=lens.decode,
        put=lens.put,
        get=lens.get,
        delete=lens.delete,
        commit=lens.commit,
        keys=lens.keys,
        get_all=lens.get_all,
        list_materializations=lambda: ["by_tag", "by_id"],
        build_materialization=build_mat,
        sample_data=lambda i: {
            "id": i,
            "name": f"item-{i}",
            "tags": [f"tag-{i % 3}", f"category-{i % 2}", "common"],
        },
    )


def make_keyless_view_contract(kernel) -> tuple:
    """Contract for KeylessLens (primary-keyless, auto-generated UUID keys).

    Tests Phase B.3 KeylessLens: put(None, data) generates a UUID4 key.
    """
    from keyvalue_lens import KeylessLens
    lens = KeylessLens(kernel, "ci_keyless")

    def keyless_put(key, data):
        # KeylessLens requires key=None; the contract's put signature
        # passes a key, so we ignore it and call lens.put(None, data).
        return lens.put(None, data)

    return lens, LensContract(
        name="ci_keyless",
        encode=lens.encode,
        decode=lens.decode,
        put=keyless_put,
        get=lens.get,
        delete=lens.delete,
        commit=lens.commit,
        keys=lens.keys,
        get_all=lens.get_all,
        list_materializations=lambda: [],
        build_materialization=None,
        sample_data=lambda i: {"event": f"event-{i}", "ts": i * 1000},
    )


# Registry of Lens contracts to test
VIEW_CONTRACTS = [
    ("Default View", make_default_view_contract),
    ("IndexedLens", make_indexed_view_contract),
    ("SemanticLens", make_semantic_view_contract),
    ("Multikey IndexedLens", make_multikey_view_contract),
    ("KeylessLens", make_keyless_view_contract),
]


# ---------------------------------------------------------------------------
# CI runner
# ---------------------------------------------------------------------------

def run_one(name: str, contract_factory) -> tuple[bool, str]:
    """Run view_laws against one Lens. Returns (passed, output)."""
    bench_dir = f"/tmp/pond_ci_{name.lower().replace(' ', '_')}"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    try:
        kernel = PondMinimal(bench_dir)
        lens, contract = contract_factory(kernel)
        laws = LensLaws(kernel)
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
    print("  View Algebra CI — RFC-0007 property tests (lens_laws.py)")
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
