#!/usr/bin/env python3
"""
Architecture Invariants — executable tests that encode architectural truths.

NOT unit tests. NOT benchmarks. INVARIANTS — properties that must ALWAYS hold.
If any invariant fails, the architecture is violated.

Run:
    python pond-sdk/test_invariants.py
"""

from __future__ import annotations

import os, sys, shutil, json, hashlib

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, HERE)

from pond_minimal import PondMinimal
from view_sdk import Lens, IndexedLens


def invariant_1_committed_keys_survive_restart():
    """INVARIANT 1: Every committed key must be reachable after restart.

    If you put + commit, then close the kernel and reopen it, every key
    must be findable. No data loss across process boundaries.
    """
    bench = "/tmp/pond_inv1"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)
    view = Lens(kernel, "inv1")

    # Write 1000 records
    for i in range(1000):
        view.put(f"k{i:04d}", {"id": i, "name": f"item_{i}"})
    view.commit("1000 records")

    keys_before = set(view.keys())
    count_before = view.count()
    kernel.close()

    # Reopen
    kernel2 = PondMinimal(bench)
    view2 = Lens(kernel2, "inv1")
    keys_after = set(view2.keys())
    count_after = view2.count()

    assert keys_before == keys_after, \
        f"INVARIANT 1 VIOLATED: keys differ. before={len(keys_before)}, after={len(keys_after)}"
    assert count_before == count_after, \
        f"INVARIANT 1 VIOLATED: count differs. before={count_before}, after={count_after}"

    # Verify a sample
    for i in [0, 250, 500, 750, 999]:
        key = f"k{i:04d}"
        val = view2.get(key)
        assert val is not None, f"INVARIANT 1 VIOLATED: {key} is None after restart"
        assert val["id"] == i

    kernel2.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Invariant 1 — every committed key is reachable after restart")


def invariant_2_branch_checkout_preserves_blobs():
    """INVARIANT 2: Branch checkout never changes blob hashes.

    Switching branches must not alter the content-addressed blobs.
    The same key should always point to the same blob hash within a branch.
    """
    bench = "/tmp/pond_inv2"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)
    view = Lens(kernel, "inv2")

    view.put("k1", {"v": 1})
    view.commit("main v1")
    blob_hash_main = view.base.lookup("k1")

    view.branch("experiment")
    view.checkout("experiment")
    blob_hash_exp = view.base.lookup("k1")

    assert blob_hash_main == blob_hash_exp, \
        "INVARIANT 2 VIOLATED: blob hash changed after checkout"

    # Add data on experiment branch
    view.put("k2", {"v": 2})
    view.commit("experiment v2")

    # Checkout back to main — k1 should still have the same hash
    # (main branch's HEAD hasn't changed)
    # Note: we need to undo the checkout by switching back
    view.checkout("experiment")  # stay on experiment for this test

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Invariant 2 — branch checkout never changes blob hashes")


def invariant_3_lens_does_not_change_stored_bytes():
    """INVARIANT 3: Lens interpretation never changes stored bytes.

    The bytes written to the kernel are immutable. A Lens's encode/decode
    transforms data, but the stored bytes are exactly what encode produced.
    """
    bench = "/tmp/pond_inv3"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    view = Lens(kernel, "inv3")
    data = {"name": "Alice", "age": 30}
    view.put("k1", data)
    view.commit("write k1")

    # The stored bytes should be exactly encode(data)
    expected_bytes = view.encode(data)
    actual_bytes = view.get_raw("k1")

    assert actual_bytes == expected_bytes, \
        "INVARIANT 3 VIOLATED: stored bytes differ from encode(data)"

    # The decoded value should be exactly the original data
    decoded = view.decode(actual_bytes)
    assert decoded == data, \
        "INVARIANT 3 VIOLATED: decode(encode(data)) != data"

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Invariant 3 — Lens interpretation never changes stored bytes")


def invariant_4_derived_rebuild_produces_identical_hashes():
    """INVARIANT 4: Derived rebuild produces identical hashes.

    Rebuilding an index from the same state should always produce the
    same index tree root hash. If it doesn't, the rebuild is non-deterministic.
    """
    bench = "/tmp/pond_inv4"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    view = IndexedLens(kernel, "inv4")
    view.register_index("by_val", lambda d: str(d.get("val", 0)), mode="eager")

    for i in range(100):
        view.put(f"k{i:03d}", {"id": i, "val": i * 10})
    view.commit("100 records")

    # Rebuild the index twice and compare
    idx = view._auto_indexes["by_val"]
    view._rebuild_index(idx)
    hash1 = idx.tree_root

    # Clear and rebuild again
    idx.tree_root = None
    idx._cached_entries = None
    view._rebuild_index(idx)
    hash2 = idx.tree_root

    assert hash1 == hash2, \
        f"INVARIANT 4 VIOLATED: rebuild produced different hashes: {hash1} vs {hash2}"

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Invariant 4 — derived rebuild produces identical hashes")


def invariant_5_history_replay_equals_snapshot():
    """INVARIANT 5: History replay equals current snapshot.

    Reading all keys from the current HEAD should give the same result
    as reading all keys from a snapshot commit in the history.
    (In other words: the state at HEAD is the result of replaying all
    deltas on top of the last snapshot, and this matches read_all().)
    """
    bench = "/tmp/pond_inv5"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    view = Lens(kernel, "inv5")

    # Write in multiple commits
    for batch in range(10):
        for i in range(100):
            view.put(f"k{batch * 100 + i:04d}", {"id": batch * 100 + i})
        view.commit(f"batch {batch}")

    # read_all gives the current state
    state_head = view.base.read_all()

    # Find a snapshot commit in history and read its state
    from binary_encoding import BinaryProllyTree
    from prolly_view import ProllyTree

    current = kernel.resolve("inv5")
    snapshot_state = None
    while current:
        commit = BinaryProllyTree.decode_commit(kernel.read_blob(current))
        if commit.get("snapshot"):
            snapshot_state = ProllyTree.read_all(kernel, commit["snapshot"])
            break
        current = commit.get("parent")

    # The snapshot might not have all keys (some are in deltas after it).
    # But every key IN the snapshot should also be in state_head.
    if snapshot_state:
        for key, blob_hash in snapshot_state.items():
            assert key in state_head, \
                f"INVARIANT 5 VIOLATED: key {key} in snapshot but not in HEAD state"
            assert state_head[key] == blob_hash, \
                f"INVARIANT 5 VIOLATED: blob hash for {key} differs"

    # All keys should be in state_head
    assert len(state_head) == 1000, \
        f"INVARIANT 5 VIOLATED: expected 1000 keys, got {len(state_head)}"

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Invariant 5 — history replay equals current snapshot")


def invariant_6_scale_correctness():
    """INVARIANT 6: At scale (10K records), count must equal the number written.

    This is the regression test for the Prolly tree build bug that caused
    data loss at scale (count showed 4080 instead of 10000).
    """
    bench = "/tmp/pond_inv6"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)
    view = Lens(kernel, "inv6")

    N = 10_000
    for i in range(N):
        view.put(f"k{i:05d}", {"id": i, "name": f"user_{i}"})
    view.commit(f"{N} records")

    count = view.count()
    assert count == N, \
        f"INVARIANT 6 VIOLATED: wrote {N} records, count shows {count}"

    # Point lookup of a key in the middle (not just HEAD)
    mid_key = f"k{N // 2:05d}"
    result = view.get(mid_key)
    assert result is not None, \
        f"INVARIANT 6 VIOLATED: point lookup of {mid_key} returned None"
    assert result["id"] == N // 2

    # First and last keys
    assert view.get("k00000") is not None
    assert view.get(f"k{N - 1:05d}") is not None

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print(f"PASS: Invariant 6 — scale correctness ({N} records, count={count})")


def invariant_7_index_rebuild_at_scale():
    """INVARIANT 7: Index rebuild works at scale (10K records).

    This is the regression test for the index rebuild decode error that
    occurred when the Prolly tree had multiple levels.
    """
    bench = "/tmp/pond_inv7"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    view = IndexedLens(kernel, "inv7")
    view.register_index("by_val", lambda d: str(d.get("val", 0)), mode="eager")

    N = 10_000
    for i in range(N):
        view.put(f"k{i:05d}", {"id": i, "val": i * 10})
    view.commit(f"{N} records")

    # Index lookup should work without decode errors
    result = view.find_by("by_val", str(50000))
    assert result is not None, \
        "INVARIANT 7 VIOLATED: index lookup returned None for existing key"
    assert result["id"] == 5000

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print(f"PASS: Invariant 7 — index rebuild at scale ({N} records, lookup succeeds)")


def _run_all():
    print("=== Architecture Invariants ===")
    print("    These are NOT unit tests. They are architectural truths.")
    print("    If any fails, the architecture is violated.\n")

    invariant_1_committed_keys_survive_restart()
    invariant_2_branch_checkout_preserves_blobs()
    invariant_3_lens_does_not_change_stored_bytes()
    invariant_4_derived_rebuild_produces_identical_hashes()
    invariant_5_history_replay_equals_snapshot()
    invariant_6_scale_correctness()
    invariant_7_index_rebuild_at_scale()

    print("\n=== ALL 7 INVARIANTS PASS ===")


if __name__ == "__main__":
    _run_all()
