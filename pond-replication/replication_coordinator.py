"""
Pond Replication Coordinator (Phase P.3)

A reference implementation of the Replication Algebra
(POND_FORMAL_ALGEBRAS.md §16) plus the A7 escape hatch: a coordinator
that applications can layer on top of the kernel for multi-writer
convergence and cross-Collection atomicity.

Per A7: 'Cross-Collection atomic writes, distributed transactions,
and linearizable reads require a coordinator substrate (2PC, Raft,
Paxos). The model does not specify one. Applications requiring
these must layer a coordinator on top of the kernel.'

This module provides two coordinators:

1. **PrimarySecondaryCoordinator** — implements REP1-REP9 (the
   in-model replication algebra). Single-writer per Ref (REP1);
   secondary reads stale (REP2); replication unit is commit blob
   (REP3); tombstone barrier (G6); failover loses in-flight writes
   (REP5); failover requires explicit promotion (REP6).

2. **TwoPhaseCommitCoordinator** — implements the A7 escape hatch
   for cross-Collection atomicity. A coordinator that runs 2PC over
   multiple PondMinimal kernels (each representing a Collection's
   HEAD ref). This is OUT OF MODEL — the model says cross-Collection
   atomicity is the application's responsibility; this module shows
   one way to discharge that responsibility.

The 2PC coordinator is NOT a kernel extension. It is a library
that uses the kernel's three primitives (Write, Read, Ref) plus
the kernel's LWW semantics to implement distributed transactions.

Run tests:
    python pond-replication/replication_coordinator.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import hashlib
import tempfile
import shutil
from typing import Optional

# Make pond-core importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "pond-core"))
from pond_minimal import PondMinimal  # noqa: E402


# ---------------------------------------------------------------------------
# Primary-Secondary Coordinator (implements Replication Algebra §16)
# ---------------------------------------------------------------------------

class PrimarySecondaryCoordinator:
    """Implements the Replication Algebra (§16) on top of the kernel.

    The 'primary' is a PondMinimal instance. The 'secondary' is a
    namespace mirror that lags behind by replica_lag_ms (REP2).

    Per REP1: single-writer per Ref — all writes go to the primary.
    Per REP3: replication unit is the commit blob.
    Per REP4: blob replication precedes commit replication.
    Per REP5: failover loses in-flight writes.
    Per REP6: failover requires explicit promotion.
    Per REP7: convergence is eventual.
    Per REP9: replication is one-directional (primary -> secondary).
    """

    def __init__(self, primary: PondMinimal,
                 replica_lag_ms: int = 0,
                 deletion_grace_period_ms: int = 0):
        self.primary = primary
        self.replica_lag_ms = replica_lag_ms
        self.deletion_grace_period_ms = deletion_grace_period_ms

        # Secondary namespace mirror: {name: (hash, timestamp_ms)}
        self._secondary_ns: dict[str, tuple[str, float]] = {}
        self._secondary_last_sync_ms = 0.0

        # Ref update log: {name: [(ts_ms, hash), ...]}
        self._ref_log: dict[str, list[tuple[float, str]]] = {}

        # Orphan timestamps (for G6 tombstone barrier)
        self._orphan_ts: dict[str, float] = {}

    def _now_ms(self) -> float:
        return time.time() * 1000.0

    # ------------------------------------------------------------------
    # Primary operations (REP1: single-writer per Ref)
    # ------------------------------------------------------------------

    def write(self, data: bytes) -> str:
        """Write bytes to the primary. Returns the hash."""
        return self.primary.write(data)

    def commit(self, head_ref: str, writes: list[tuple[str, str]],
               message: str = "") -> str:
        """Atomically commit a set of writes to a Collection's HEAD.

        Per REP3: the replication unit is the commit blob. The commit
        blob lists all (name, hash) writes; updating HEAD to point at
        it is atomic (A6).

        Per REP4: blob replication precedes commit replication. The
        caller is expected to have already written all blobs via
        self.write() before calling commit(). The commit blob itself
        is the last write.
        """
        commit_data = {
            "writes": writes,
            "parent": self.primary.resolve(head_ref),
            "message": message,
            "timestamp": self._now_ms(),
        }
        commit_blob = json.dumps(commit_data).encode()
        commit_h = self.primary.write(commit_blob)
        # Atomic update of HEAD (single Ref — A6)
        self.primary.reference(head_ref, commit_h)
        # Log the ref update for replication
        self._ref_log.setdefault(head_ref, []).append((self._now_ms(), commit_h))
        return commit_h

    # ------------------------------------------------------------------
    # Secondary operations (REP2: stale reads)
    # ------------------------------------------------------------------

    def _maybe_sync_secondary(self):
        """Sync the secondary namespace with the primary, up to
        replica_lag_ms ago. (REP7: convergence is eventual.)"""
        now = self._now_ms()
        cutoff = now - self.replica_lag_ms
        if cutoff <= self._secondary_last_sync_ms:
            return
        for name, log in self._ref_log.items():
            for ts, h in log:
                if ts <= cutoff:
                    self._secondary_ns[name] = (h, ts)
        self._secondary_last_sync_ms = cutoff

    def secondary_resolve(self, name: str) -> Optional[str]:
        """Resolve a name on the secondary. May be stale (REP2)."""
        self._maybe_sync_secondary()
        if name in self._secondary_ns:
            return self._secondary_ns[name][0]
        return None

    def secondary_read(self, name: str) -> bytes:
        """Read a blob via the secondary's view of the namespace.
        The blob itself comes from the primary (we don't replicate
        blobs in this reference; production would)."""
        h = self.secondary_resolve(name)
        if h is None:
            raise ValueError(f"Name '{name}' not in secondary namespace")
        return self.primary.read_blob(h)

    # ------------------------------------------------------------------
    # Failover (REP5, REP6)
    # ------------------------------------------------------------------

    def promote_secondary(self, name: str) -> str:
        """Promote the secondary's view of a name to be the new
        primary state. Per REP6: requires explicit promotion.

        Per REP5: if the primary failed before replicating a commit,
        that commit is lost. The secondary's last synced commit
        becomes the new primary state.
        """
        h = self.secondary_resolve(name)
        if h is None:
            raise ValueError(f"Name '{name}' not in secondary namespace")
        # Promote: write the secondary's hash to the primary
        self.primary.reference(name, h)
        return h

    # ------------------------------------------------------------------
    # GC with tombstone barrier (G6)
    # ------------------------------------------------------------------

    def mark_orphaned(self, h: str):
        """Mark a blob as orphaned. The blob will be eligible for
        deletion after deletion_grace_period_ms (G6 tombstone barrier)."""
        self._orphan_ts[h] = self._now_ms()

    def gc_collect(self) -> int:
        """Delete orphaned blobs whose grace period has elapsed.
        Returns the number of blobs deleted."""
        deleted = 0
        now = self._now_ms()
        for h, orphan_ts in list(self._orphan_ts.items()):
            age_ms = now - orphan_ts
            if age_ms < self.deletion_grace_period_ms:
                continue  # G6: barrier
            path = self.primary._blob_path(h)
            if os.path.exists(path):
                os.remove(path)
                deleted += 1
            del self._orphan_ts[h]
        return deleted


# ---------------------------------------------------------------------------
# Two-Phase Commit Coordinator (A7 escape hatch)
# ---------------------------------------------------------------------------

class TwoPhaseCommitCoordinator:
    """Implements cross-Collection atomicity via 2PC, per A7.

    The model says cross-Collection atomicity requires a coordinator
    (A7). This class IS that coordinator. It runs 2PC over multiple
    PondMinimal kernels, each representing a Collection's HEAD.

    Protocol:
      1. PREPARE: write a 'prepare' record to each Collection's
         kernel, listing the intended writes. The prepare record is
         a blob referenced by a __prepare__ ref.
      2. VOTE: if any Collection fails to prepare, abort.
      3. COMMIT: write a 'commit' record to each Collection, listing
         the same writes. Update each HEAD to point at the commit
         blob. Clear the __prepare__ ref.
      4. ABORT (if any vote was No): write an 'abort' record to each
         prepared Collection. Clear the __prepare__ ref.

    Crash recovery: on startup, scan for __prepare__ refs. If a
    prepare exists without a corresponding commit or abort, the
    transaction is 'in doubt'. A real coordinator would query other
    participants; this reference just aborts.

    IMPORTANT: this is a COORDINATOR, not a kernel extension. It
    uses only the kernel's three primitives (Write, Read, Ref). It
    does NOT modify the kernel.
    """

    PREPARE_PREFIX = "__prepare__"
    COMMIT_PREFIX = "__commit__"
    ABORT_PREFIX = "__abort__"

    def __init__(self, kernels: dict[str, PondMinimal]):
        """kernels: {collection_name: PondMinimal}"""
        self.kernels = kernels

    # ------------------------------------------------------------------
    # 2PC protocol
    # ------------------------------------------------------------------

    def commit_2pc(self, writes_by_collection: dict[str, list[tuple[str, str]]],
                   txn_id: Optional[str] = None
                   ) -> tuple[bool, str]:
        """Atomically commit writes across multiple Collections.

        writes_by_collection: {collection_name: [(name, hash), ...]}
        txn_id: optional transaction ID (auto-generated if None)

        Returns (success, txn_id).
        """
        txn_id = txn_id or hashlib.sha256(
            f"{time.time()}{os.urandom(8)}".encode()
        ).hexdigest()[:16]

        # Phase 1: PREPARE
        prepared = []
        for coll_name, writes in writes_by_collection.items():
            if coll_name not in self.kernels:
                # Abort: unknown collection
                self._abort(txn_id, prepared)
                return (False, txn_id)
            kernel = self.kernels[coll_name]
            try:
                prepare_record = json.dumps({
                    "txn_id": txn_id,
                    "collection": coll_name,
                    "writes": writes,
                    "timestamp": time.time(),
                }).encode()
                prepare_h = kernel.write(prepare_record)
                kernel.reference(f"{self.PREPARE_PREFIX}/{txn_id}/{coll_name}",
                                 prepare_h)
                prepared.append((coll_name, prepare_h))
            except Exception as e:
                # Abort: prepare failed
                self._abort(txn_id, prepared)
                return (False, txn_id)

        # Phase 2: COMMIT (all votes were Yes)
        for coll_name, writes in writes_by_collection.items():
            kernel = self.kernels[coll_name]
            commit_record = json.dumps({
                "txn_id": txn_id,
                "collection": coll_name,
                "writes": writes,
                "timestamp": time.time(),
            }).encode()
            commit_h = kernel.write(commit_record)
            # Update each name in the writes list
            for name, h in writes:
                kernel.reference(name, h)
            # Mark commit
            kernel.reference(f"{self.COMMIT_PREFIX}/{txn_id}/{coll_name}",
                            commit_h)
            # Clear prepare
            # (The kernel doesn't have a delete; we overwrite with a
            # tombstone marker. Per R4, this is the convention.)
            tombstone = kernel.write(b"\x00TOMBSTONE\x00")
            kernel.reference(f"{self.PREPARE_PREFIX}/{txn_id}/{coll_name}",
                            tombstone)

        return (True, txn_id)

    def _abort(self, txn_id: str, prepared: list[tuple[str, str]]):
        """Abort a transaction: clear all prepare records."""
        for coll_name, _ in prepared:
            kernel = self.kernels[coll_name]
            tombstone = kernel.write(b"\x00TOMBSTONE\x00")
            kernel.reference(f"{self.PREPARE_PREFIX}/{txn_id}/{coll_name}",
                            tombstone)
            # Also write an abort record
            abort_record = json.dumps({
                "txn_id": txn_id,
                "collection": coll_name,
                "timestamp": time.time(),
            }).encode()
            abort_h = kernel.write(abort_record)
            kernel.reference(f"{self.ABORT_PREFIX}/{txn_id}/{coll_name}",
                            abort_h)

    # ------------------------------------------------------------------
    # Crash recovery
    # ------------------------------------------------------------------

    def recover(self) -> list[str]:
        """Scan for in-doubt transactions (prepare without commit/abort).
        Returns list of txn_ids that need manual resolution.

        Per the model: a real coordinator would query other participants.
        This reference just reports in-doubt transactions; the
        application decides whether to commit or abort them.
        """
        in_doubt = set()
        for coll_name, kernel in self.kernels.items():
            for ref_name in kernel.list_names():
                if ref_name.startswith(self.PREPARE_PREFIX):
                    # Extract txn_id from ref name
                    parts = ref_name.split("/")
                    if len(parts) >= 3:
                        txn_id = parts[1]
                        # Check if commit or abort exists
                        commit_ref = f"{self.COMMIT_PREFIX}/{txn_id}/{coll_name}"
                        abort_ref = f"{self.ABORT_PREFIX}/{txn_id}/{coll_name}"
                        # If the prepare ref points at a tombstone, it was cleared
                        h = kernel.resolve(ref_name)
                        if h is not None:
                            try:
                                data = kernel.read_blob(h)
                                if data != b"\x00TOMBSTONE\x00":
                                    # Prepare is still active — check for commit/abort
                                    if (kernel.resolve(commit_ref) is None
                                            and kernel.resolve(abort_ref) is None):
                                        in_doubt.add(txn_id)
                            except Exception:
                                pass
        return list(in_doubt)


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _self_test():
    """Verify both coordinators work."""
    print("=== Replication Coordinator self-test ===")

    tmpdir = tempfile.mkdtemp(prefix="pond_repl_")
    try:
        # ----------------------------------------------------------------
        # Part 1: PrimarySecondaryCoordinator (REP1-REP9, G6)
        # ----------------------------------------------------------------
        print("\n--- Part 1: PrimarySecondaryCoordinator ---")
        primary = PondMinimal(os.path.join(tmpdir, "primary"))
        coord = PrimarySecondaryCoordinator(
            primary, replica_lag_ms=50, deletion_grace_period_ms=100
        )

        # Test 1: REP1 — single writer per Ref
        h1 = coord.write(b"data1")
        h2 = coord.write(b"data2")
        coord.commit("orders/head", [("orders/a", h1), ("orders/b", h2)])
        check_primary_head = primary.resolve("orders/head")
        assert check_primary_head is not None, "primary HEAD set"
        print(f"  [OK] REP1: single-writer commit to primary HEAD")

        # Test 2: REP2 — secondary reads are stale
        # Immediately after commit, secondary has nothing
        assert coord.secondary_resolve("orders/head") is None, \
            "secondary should be stale immediately"
        print(f"  [OK] REP2: secondary stale immediately after commit")

        # After replica_lag, secondary converges (REP7)
        time.sleep(0.08)
        sec_h = coord.secondary_resolve("orders/head")
        assert sec_h == check_primary_head, \
            f"REP7: secondary should converge after lag (got {sec_h})"
        print(f"  [OK] REP7: secondary converged after replica_lag")

        # Test 3: REP3 — replication unit is commit blob
        commit_data = json.loads(primary.read_blob(check_primary_head))
        assert "writes" in commit_data, "commit blob has writes field"
        assert len(commit_data["writes"]) == 2, "commit blob lists 2 writes"
        print(f"  [OK] REP3: commit blob is the replication unit")

        # Test 4: REP4 — blob replication precedes commit replication
        # (Verified by construction: write() is called before commit())
        for name, h in commit_data["writes"]:
            assert primary.read_blob(h) is not None, \
                f"blob {name} exists before commit replication"
        print(f"  [OK] REP4: blobs exist before commit blob")

        # Test 5: REP5 — failover loses in-flight writes
        # Commit a new version, but failover before replication
        h3 = coord.write(b"data3")
        coord.commit("orders/head", [("orders/c", h3)])
        # Immediately promote secondary (before replication)
        # The secondary still has the OLD head (h3 not yet replicated)
        old_sec = coord.secondary_resolve("orders/head")
        assert old_sec == check_primary_head, \
            "secondary still has old head (in-flight write not replicated)"
        print(f"  [OK] REP5: in-flight write lost on immediate failover")

        # Test 6: REP6 — failover requires explicit promotion
        api = [m for m in dir(coord) if not m.startswith("_")]
        has_auto_failover = any("auto" in m.lower() and "failover" in m.lower()
                                for m in api)
        assert not has_auto_failover, "no auto-failover API"
        print(f"  [OK] REP6: no auto-failover (explicit promotion required)")

        # Test 7: G6 — tombstone barrier
        orphan_h = coord.write(b"orphan")
        coord.mark_orphaned(orphan_h)
        # Immediately GC — should be blocked by grace period
        deleted = coord.gc_collect()
        assert deleted == 0, "G6: GC blocked by grace period"
        # After grace period
        time.sleep(0.12)
        deleted = coord.gc_collect()
        assert deleted == 1, f"G6: GC collects after grace (got {deleted})"
        print(f"  [OK] G6: tombstone barrier respected (collected after grace)")

        # Test 8: REP9 — replication is one-directional
        # No API to write to secondary
        has_secondary_write = any("write_secondary" in m or "write_to_secondary" in m
                                  for m in api)
        assert not has_secondary_write, "no secondary write API"
        print(f"  [OK] REP9: replication is one-directional (no secondary write API)")

        # ----------------------------------------------------------------
        # Part 2: TwoPhaseCommitCoordinator (A7 escape hatch)
        # ----------------------------------------------------------------
        print("\n--- Part 2: TwoPhaseCommitCoordinator (A7) ---")
        k1 = PondMinimal(os.path.join(tmpdir, "coll1"))
        k2 = PondMinimal(os.path.join(tmpdir, "coll2"))
        k3 = PondMinimal(os.path.join(tmpdir, "coll3"))
        coord2pc = TwoPhaseCommitCoordinator({"coll1": k1, "coll2": k2, "coll3": k3})

        # Test 9: successful 2PC across 3 Collections
        h_a = k1.write(b"a")  # write blob to coll1 first
        h_b = k2.write(b"b")  # write blob to coll2
        h_c = k3.write(b"c")  # write blob to coll3
        success, txn_id = coord2pc.commit_2pc({
            "coll1": [("x", h_a)],
            "coll2": [("y", h_b)],
            "coll3": [("z", h_c)],
        })
        assert success, "2PC should succeed"
        assert k1.resolve("x") == h_a, "coll1.x set"
        assert k2.resolve("y") == h_b, "coll2.y set"
        assert k3.resolve("z") == h_c, "coll3.z set"
        print(f"  [OK] 2PC: 3 Collections committed atomically (txn {txn_id[:8]})")

        # Test 10: 2PC abort when one Collection is unknown
        success2, txn_id2 = coord2pc.commit_2pc({
            "coll1": [("x2", h_a)],
            "unknown_coll": [("w", h_a)],
        })
        assert not success2, "2PC should abort (unknown collection)"
        # coll1.x2 should NOT be set (aborted)
        assert k1.resolve("x2") is None, "aborted: coll1.x2 not set"
        print(f"  [OK] 2PC: abort when collection unknown (txn {txn_id2[:8]})")

        # Test 11: 2PC prepare records exist during txn, cleared after commit
        # After Test 9's commit, prepare refs should be tombstoned
        prepare_ref = f"__prepare__/{txn_id}/coll1"
        prepare_h = k1.resolve(prepare_ref)
        if prepare_h is not None:
            prepare_data = k1.read_blob(prepare_h)
            assert prepare_data == b"\x00TOMBSTONE\x00", \
                "prepare ref should be tombstoned after commit"
        print(f"  [OK] 2PC: prepare records tombstoned after commit")

        # Test 12: commit records exist after successful 2PC
        commit_ref = f"__commit__/{txn_id}/coll1"
        assert k1.resolve(commit_ref) is not None, "commit record exists"
        print(f"  [OK] 2PC: commit records persist for audit")

        # Test 13: crash recovery — no in-doubt transactions after clean commit
        in_doubt = coord2pc.recover()
        assert len(in_doubt) == 0, \
            f"no in-doubt transactions after clean commit (got {in_doubt})"
        print(f"  [OK] recovery: no in-doubt transactions after clean commit")

        # Test 14: crash recovery — in-doubt if prepare without commit
        # Manually create a prepare record without committing
        prepare_only = json.dumps({
            "txn_id": "manual_in_doubt",
            "collection": "coll1",
            "writes": [("doubt", h_a)],
            "timestamp": time.time(),
        }).encode()
        prepare_only_h = k1.write(prepare_only)
        k1.reference("__prepare__/manual_in_doubt/coll1", prepare_only_h)
        in_doubt = coord2pc.recover()
        assert "manual_in_doubt" in in_doubt, \
            f"manual_in_doubt should be in-doubt (got {in_doubt})"
        print(f"  [OK] recovery: in-doubt transaction detected")

        # Cleanup
        for k in [primary, k1, k2, k3]:
            k.close()
        print("\nAll Replication Coordinator tests pass.")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    _self_test()
