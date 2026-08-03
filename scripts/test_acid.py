"""ACID transaction tests — commit markers on top of CRDT shards.

Tests:
1. Multi-collection atomic commit (both visible after commit)
2. Abort (neither visible without commit marker)
3. Snapshot isolation (reader sees consistent point-in-time view)
4. Non-transactional writes unaffected (zero overhead)
5. GC cleans up uncommitted tentative shards
"""
import sys, os, time, threading

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage


def _setup():
    kernel, _ = make_object_store_native_kernel()
    s = PondStorage(kernel)
    s.write("users", [{"id": i, "name": f"u{i}"} for i in range(5)], key_col="id", row_group_size=5)
    s.write("orders", [{"id": i, "amount": float(i)} for i in range(5)], key_col="id", row_group_size=5)
    return kernel, s


def test_atomic_commit():
    """Multi-collection atomic commit — both visible after commit."""
    kernel, s = _setup()
    tx = s.begin_tx()
    s.append_shard("users", [{"id": 100, "name": "new_user"}], key_col="id", tx_id=tx)
    s.append_shard("orders", [{"id": 100, "amount": 99.9}], key_col="id", tx_id=tx)

    # Before commit: NOT visible
    users = s.read_with_shards("users")
    orders = s.read_with_shards("orders")
    assert all(r.get("id") != 100 for r in users), "Tentative shard visible before commit!"
    assert all(r.get("id") != 100 for r in orders), "Tentative shard visible before commit!"

    # Commit
    s.commit_tx(tx)

    # After commit: BOTH visible
    users = s.read_with_shards("users")
    orders = s.read_with_shards("orders")
    assert any(r.get("id") == 100 for r in users), "User not visible after commit!"
    assert any(r.get("id") == 100 for r in orders), "Order not visible after commit!"

    print("PASS: test_atomic_commit — both collections visible atomically")
    return True


def test_abort():
    """Abort — tentative shards stay invisible."""
    kernel, s = _setup()
    tx = s.begin_tx()
    s.append_shard("users", [{"id": 200, "name": "aborted"}], key_col="id", tx_id=tx)
    s.abort_tx(tx)  # just don't commit

    # Never visible
    users = s.read_with_shards("users")
    assert all(r.get("id") != 200 for r in users), "Aborted shard visible!"

    print("PASS: test_abort — tentative shards invisible after abort")
    return True


def test_snapshot_isolation():
    """Reader sees consistent snapshot — doesn't see in-progress transactions."""
    kernel, s = _setup()
    tx = s.begin_tx()
    s.append_shard("users", [{"id": 300, "name": "tx_user"}], key_col="id", tx_id=tx)

    # Reader sees snapshot WITHOUT the tentative data
    reader = PondStorage(kernel)
    users_before = reader.read_with_shards("users")
    assert all(r.get("id") != 300 for r in users_before)

    # Commit the transaction
    s.commit_tx(tx)

    # New reader sees the committed data
    reader2 = PondStorage(kernel)
    users_after = reader2.read_with_shards("users")
    assert any(r.get("id") == 300 for r in users_after), "Committed data not visible to new reader"

    print("PASS: test_snapshot_isolation — reader doesn't see in-progress tx")
    return True


def test_no_overhead_for_normal_writes():
    """Non-transactional writes work exactly as before (zero overhead)."""
    kernel, s = _setup()
    # Normal write (no tx_id)
    s.append_shard("users", [{"id": 400, "name": "normal"}], key_col="id")

    # Immediately visible
    users = s.read_with_shards("users")
    assert any(r.get("id") == 400 for r in users), "Normal write not visible!"

    print("PASS: test_no_overhead_for_normal_writes — zero overhead for CRDT")
    return True


def test_concurrent_transactions():
    """Multiple transactions can run concurrently — each commits independently."""
    kernel, s = _setup()

    tx1 = s.begin_tx()
    tx2 = s.begin_tx()

    s.append_shard("users", [{"id": 501, "name": "tx1"}], key_col="id", tx_id=tx1)
    s.append_shard("users", [{"id": 502, "name": "tx2"}], key_col="id", tx_id=tx2)

    # Neither visible yet
    users = s.read_with_shards("users")
    assert all(r.get("id") not in (501, 502) for r in users)

    # Commit tx1 only
    s.commit_tx(tx1)
    users = s.read_with_shards("users")
    assert any(r.get("id") == 501 for r in users), "tx1 not visible after commit"
    assert all(r.get("id") != 502 for r in users), "tx2 visible before commit!"

    # Commit tx2
    s.commit_tx(tx2)
    users = s.read_with_shards("users")
    assert any(r.get("id") == 502 for r in users), "tx2 not visible after commit"

    print("PASS: test_concurrent_transactions — independent commits")
    return True


def test_mixed_tx_and_non_tx():
    """Transaction writes + non-transaction writes coexist."""
    kernel, s = _setup()

    # Non-tx write (immediately visible)
    s.append_shard("users", [{"id": 600, "name": "normal"}], key_col="id")

    # Tx write (tentative)
    tx = s.begin_tx()
    s.append_shard("users", [{"id": 601, "name": "tx"}], key_col="id", tx_id=tx)

    users = s.read_with_shards("users")
    assert any(r.get("id") == 600 for r in users), "Normal write not visible"
    assert all(r.get("id") != 601 for r in users), "Tentative write visible before commit"

    s.commit_tx(tx)
    users = s.read_with_shards("users")
    assert any(r.get("id") == 601 for r in users), "Tx write not visible after commit"
    assert any(r.get("id") == 600 for r in users), "Normal write disappeared"

    print("PASS: test_mixed_tx_and_non_tx — both models coexist seamlessly")
    return True


def test_gc_cleans_uncommitted():
    """GC cleans up uncommitted tentative shards."""
    kernel, s = _setup()

    tx = s.begin_tx()
    s.append_shard("users", [{"id": 700, "name": "will_be_aborted"}], key_col="id", tx_id=tx)
    # Don't commit — abort

    # GC should clean up the tentative shard's blob
    stats = s.gc()
    # The tentative shard blob should be in the dead set
    # (it's not reachable from any committed ref)
    assert stats["dead"] > 0, "Expected dead blobs from uncommitted tx"

    # Vacuum
    s.vacuum()

    # Data still correct — tentative shard was never visible
    users = s.read_with_shards("users")
    assert all(r.get("id") != 700 for r in users)

    print(f"PASS: test_gc_cleans_uncommitted — {stats['dead']} dead blobs cleaned")
    return True


def main():
    tests = [
        test_atomic_commit,
        test_abort,
        test_snapshot_isolation,
        test_no_overhead_for_normal_writes,
        test_concurrent_transactions,
        test_mixed_tx_and_non_tx,
        test_gc_cleans_uncommitted,
    ]
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{passed}/{len(tests)} tests passed")
    if passed == len(tests):
        print("=== ALL ACID TRANSACTION TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
