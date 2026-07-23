#!/usr/bin/env python3
"""
Run view_laws.py against the ArrowLens (Phase D compatibility adapter).

This verifies that a Pond Lens built for Arrow interoperability still
satisfies RFC-0007's 6 View algebra laws. If it passes, the algebra
generalizes to Views that interoperate with external ecosystems.
"""

from __future__ import annotations

import os
import sys
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
POND_ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(POND_ROOT, "pond-core"))
sys.path.insert(0, os.path.join(POND_ROOT, "pond-sdk"))
sys.path.insert(0, os.path.join(POND_ROOT, "pond-arrow"))

import pyarrow as pa
from pond_minimal import PondMinimal
from lens_laws import LensLaws, LensContract
from arrow_view import ArrowLens


def make_arrow_view_contract(kernel) -> tuple:
    """Contract for ArrowLens.

    ArrowLens's state space Sigma is pyarrow.Table, not dict. The
    encode/decode pair round-trips Tables:
        encode(Table) -> bytes (Arrow IPC)
        decode(bytes) -> Table

    The put/get API of the base View class accepts Tables. The
    sample_data generator returns Tables.
    """
    view = ArrowLens(kernel, "ci_arrow")

    def sample_table(i: int) -> pa.Table:
        return pa.table({
            "id": [i],
            "name": [f"item-{i}"],
            "value": [i * 10],
        })

    return view, LensContract(
        name="ci_arrow",
        encode=view.encode,        # encode(pa.Table) -> bytes
        decode=view.decode,        # decode(bytes) -> pa.Table
        put=view.put,              # put(key, pa.Table) -> stages
        get=view.get,              # get(key) -> pa.Table
        delete=view.delete,
        commit=view.commit,
        keys=view.keys,
        get_all=view.get_all,
        list_materializations=lambda: [],
        build_materialization=None,
        sample_data=sample_table,  # returns pa.Table, not dict
    )


def main() -> int:
    print("=" * 72)
    print("  View Algebra — ArrowLens Compliance Test")
    print("  (Phase D compatibility adapter; Pond data as Arrow IPC)")
    print("=" * 72)
    print()

    bench_dir = "/tmp/pond_ci_arrow"
    if os.path.exists(bench_dir):
        shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    try:
        kernel = PondMinimal(bench_dir)
        view, contract = make_arrow_view_contract(kernel)
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
