#!/usr/bin/env python3
"""
Phase G: Differential Testing — reference implementation vs Pond.

Builds a tiny, obviously-correct reference implementation of a key-value
store. For every random operation sequence, runs both the reference and
Pond, then compares the resulting state. Any mismatch is a bug.

This is how mature databases uncover subtle correctness bugs that
ordinary tests never hit.

Run:
    python experiments/differential_test.py
"""

from __future__ import annotations

import os, sys, shutil, json, random, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))

from kernel import PondMinimal
from keyvalue_lens import Lens


# ---------------------------------------------------------------------------
# Reference implementation — slow but obviously correct
# ---------------------------------------------------------------------------

class ReferenceStore:
    """A trivially-correct key-value store with commit history.

    No Prolly trees, no delta journals, no binary encoding. Just a dict
    per commit, with full snapshots. Obviously correct by construction.

    IMPORTANT: this reference matches Pond's semantics — staged writes
    are NOT visible to get/count/keys until commit. This matches Pond's
    behavior where base.lookup() reads from committed state.
    """

    def __init__(self):
        self._committed: dict[str, dict] = {}  # last committed state
        self._staged: dict[str, dict] = {}
        self._staged_deletes: set[str] = set()

    def put(self, key: str, data: dict) -> None:
        self._staged[key] = dict(data)
        self._staged_deletes.discard(key)

    def delete(self, key: str) -> None:
        self._staged_deletes.add(key)
        self._staged.pop(key, None)

    def commit(self) -> int:
        """Commit staged changes. Returns the commit index."""
        state = dict(self._committed)
        for k, v in self._staged.items():
            state[k] = v
        for k in self._staged_deletes:
            state.pop(k, None)
        self._committed = state
        self._staged.clear()
        self._staged_deletes.clear()
        return 0

    def get(self, key: str) -> dict | None:
        # Like Pond: reads from COMMITTED state, not staged
        return dict(self._committed[key]) if key in self._committed else None

    def keys(self) -> list[str]:
        # Like Pond: reads from COMMITTED state
        return sorted(self._committed.keys())

    def count(self) -> int:
        return len(self._committed)

    def state(self) -> dict[str, dict]:
        """Return the full committed state."""
        return {k: dict(v) for k, v in self._committed.items()}


# ---------------------------------------------------------------------------
# Pond adapter — wraps Lens to match the reference interface
# ---------------------------------------------------------------------------

class PondStore:
    """Wraps a Pond Lens to match the ReferenceStore interface."""

    def __init__(self, kernel: PondMinimal, name: str):
        self.lens = Lens(kernel, name)

    def put(self, key: str, data: dict) -> None:
        self.lens.put(key, data)

    def delete(self, key: str) -> None:
        self.lens.delete(key)

    def commit(self) -> int:
        # Pond rejects empty commits; skip if nothing staged
        if self.lens.base.has_staged():
            self.lens.commit("differential test")
        return 0

    def get(self, key: str) -> dict | None:
        return self.lens.get(key)

    def keys(self) -> list[str]:
        return sorted(self.lens.keys())

    def count(self) -> int:
        return self.lens.count()

    def state(self) -> dict[str, dict]:
        return {k: self.lens.get(k) for k in self.lens.keys()}


# ---------------------------------------------------------------------------
# Differential test runner
# ---------------------------------------------------------------------------

def generate_random_operations(n_ops: int, n_keys: int = 20,
                                seed: int = 0) -> list[tuple]:
    """Generate a random sequence of operations.

    Operations:
      ("put", key, data)
      ("delete", key)
      ("commit",)
      ("get", key)
      ("count",)
      ("keys",)
    """
    rng = random.Random(seed)
    ops = []
    for _ in range(n_ops):
        op_type = rng.choices(
            ["put", "delete", "commit", "get", "count", "keys"],
            weights=[40, 15, 15, 15, 5, 10],
        )[0]
        if op_type in ("put", "delete", "get"):
            key = f"k{rng.randint(0, n_keys - 1):03d}"
            if op_type == "put":
                data = {"id": rng.randint(0, 10000),
                        "name": f"name_{rng.randint(0, 999)}",
                        "val": rng.randint(0, 1000)}
                ops.append(("put", key, data))
            elif op_type == "delete":
                ops.append(("delete", key))
            else:
                ops.append(("get", key))
        elif op_type == "commit":
            ops.append(("commit",))
        elif op_type == "count":
            ops.append(("count",))
        elif op_type == "keys":
            ops.append(("keys",))
    return ops


def run_operations(store, ops: list[tuple]) -> list:
    """Run operations on a store. Returns a list of results for
    comparison (get results, count results, key lists)."""
    results = []
    for op in ops:
        if op[0] == "put":
            store.put(op[1], op[2])
        elif op[0] == "delete":
            store.delete(op[1])
        elif op[0] == "commit":
            store.commit()
        elif op[0] == "get":
            results.append(("get", op[1], store.get(op[1])))
        elif op[0] == "count":
            results.append(("count", store.count()))
        elif op[0] == "keys":
            results.append(("keys", store.keys()))
    # Final state comparison
    results.append(("final_state", store.state()))
    return results


def compare_results(ref_results: list, pond_results: list) -> bool:
    """Compare results from reference and Pond. Returns True if they match."""
    if len(ref_results) != len(pond_results):
        return False
    for ref, pond in zip(ref_results, pond_results):
        if ref[0] != pond[0]:
            return False
        if ref[0] == "get":
            if ref[1] != pond[1]:  # key must match
                return False
            # Values must match (or both be None)
            if ref[2] != pond[2]:
                return False
        elif ref[0] == "count":
            if ref[1] != pond[1]:
                return False
        elif ref[0] == "keys":
            if ref[1] != pond[1]:
                return False
        elif ref[0] == "final_state":
            # Compare full states
            ref_state = ref[1]
            pond_state = pond[1]
            if set(ref_state.keys()) != set(pond_state.keys()):
                return False
            for key in ref_state:
                if ref_state[key] != pond_state[key]:
                    return False
    return True


def run_differential_test(n_scenarios: int = 1000,
                           n_ops_per_scenario: int = 50,
                           n_keys: int = 20) -> dict:
    """Run n_scenarios random differential tests.

    Returns a dict with:
      - "scenarios": total number of scenarios run
      - "passed": number that matched
      - "failed": number that didn't match
      - "failures": list of (seed, description) for failures
    """
    passed = 0
    failed = 0
    failures = []

    for seed in range(n_scenarios):
        ops = generate_random_operations(n_ops_per_scenario, n_keys, seed)

        # Run on reference
        ref = ReferenceStore()
        ref_results = run_operations(ref, ops)

        # Run on Pond
        bench = f"/tmp/pond_diff_{seed}"
        if os.path.exists(bench):
            shutil.rmtree(bench)
        os.makedirs(bench)
        kernel = PondMinimal(bench)
        pond = PondStore(kernel, "diff_test")
        pond_results = run_operations(pond, ops)
        kernel.close()
        shutil.rmtree(bench, ignore_errors=True)

        # Compare
        if compare_results(ref_results, pond_results):
            passed += 1
        else:
            failed += 1
            # Find the first mismatch
            mismatch = "unknown"
            for i, (ref, pond) in enumerate(zip(ref_results, pond_results)):
                if ref != pond:
                    mismatch = f"op {i}: ref={ref} vs pond={pond}"
                    break
            failures.append((seed, mismatch))

    return {
        "scenarios": n_scenarios,
        "passed": passed,
        "failed": failed,
        "failures": failures[:5],  # first 5 failures
    }


def main():
    print("=" * 72)
    print("  Phase G: Differential Testing")
    print("  Reference implementation vs Pond")
    print("=" * 72)

    # Quick smoke test
    print("\n  Smoke test: 10 scenarios, 20 ops each...")
    result = run_differential_test(n_scenarios=10, n_ops_per_scenario=20)
    print(f"  Passed: {result['passed']}/{result['scenarios']}")
    if result["failed"] > 0:
        print(f"  FAILED: {result['failures']}")
        return

    # Full test
    print(f"\n  Full test: 1000 scenarios, 50 ops each...")
    t0 = time.perf_counter()
    result = run_differential_test(n_scenarios=1000, n_ops_per_scenario=50)
    t1 = time.perf_counter()

    print(f"\n  Results:")
    print(f"    Scenarios:  {result['scenarios']}")
    print(f"    Passed:     {result['passed']}")
    print(f"    Failed:     {result['failed']}")
    print(f"    Time:       {(t1 - t0):.1f}s")

    if result["failed"] > 0:
        print(f"\n  FAILURES (first 5):")
        for seed, desc in result["failures"]:
            print(f"    Seed {seed}: {desc}")
        print("\n  ⚠ DIFFERENTIAL TEST FOUND BUGS")
    else:
        print(f"\n  ✓ ALL {result['scenarios']} SCENARIOS MATCHED THE REFERENCE")
        print(f"    Pond's state matches the obviously-correct reference for")
        print(f"    every random operation sequence tested.")

    print("=" * 72)


if __name__ == "__main__":
    main()
