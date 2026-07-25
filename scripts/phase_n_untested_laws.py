"""
Pond Phase N.4 — Tests for Untested Laws

Covers the laws Phase L did not test:
  Merge Algebra (M1-M4):
    M1  Commutativity of topology (parent/second_parent order is conventional)
    M2  Associativity of merge commits (merging A<-B then (A<-B)<-C)
    M3  Lens determines semantics (kernel doesn't decide merge content)
    M4' Merge has a well-defined result (snapshot OR delta; demoted from M4)

  Workspace Algebra (W1-W5):
    W1  Isolation (changes not visible until commit)
    W2  Atomicity (commit all-or-nothing) — within-Collection per A6
    W3  Savepoint rollback
    W4  Lens independence (Workspace not bound to a Lens) — restricted to within-Collection
    W5  Workspace is ephemeral (in memory, not in commit history until committed)

Run:
    python scripts/phase_n_untested_laws.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import tempfile
import shutil
import random

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "pond-core"))
from kernel import PondMinimal  # noqa: E402

PASS = 0
FAIL = 0


def check(cond, label, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [OK] {label}")
    else:
        FAIL += 1
        print(f"  [FAIL] {label} {detail}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_kernel():
    tmpdir = tempfile.mkdtemp(prefix="pond_n_")
    return PondMinimal(tmpdir), tmpdir


def cleanup(k, d):
    try:
        k.close()
    except Exception:
        pass
    shutil.rmtree(d, ignore_errors=True)


def make_commit(kernel, parent=None, second_parent=None, tree_hash=None,
                delta=None, msg="x"):
    """Build a commit blob. Either tree_hash (snapshot commit) or
    delta (delta commit); not both. second_parent for merge commits."""
    commit = {
        "parent": parent,
        "second_parent": second_parent,
        "tree": tree_hash,
        "delta": delta,
        "message": msg,
        "timestamp": time.time(),
    }
    return kernel.write(json.dumps(commit).encode())


# ---------------------------------------------------------------------------
# Merge Algebra (M1-M4)
# ---------------------------------------------------------------------------

def test_M1_commutativity_of_topology():
    """M1: The order of parent and second_parent is conventional.
    A merge commit with (parent=A, second_parent=B) records the same
    topology as (parent=B, second_parent=A) — both have 2 parents."""
    print("\n=== M1: Commutativity of topology ===")
    k, d = make_kernel()
    try:
        # Two base commits
        ca = make_commit(k, parent=None, tree_hash=k.write(b"tree_a"), msg="a")
        cb = make_commit(k, parent=None, tree_hash=k.write(b"tree_b"), msg="b")

        # Merge commit with (parent=A, second_parent=B)
        m1 = make_commit(k, parent=ca, second_parent=cb,
                        tree_hash=k.write(b"merged"), msg="merge1")
        # Merge commit with (parent=B, second_parent=A) — same topology
        m2 = make_commit(k, parent=cb, second_parent=ca,
                        tree_hash=k.write(b"merged"), msg="merge2")

        # Both have 2 parents
        c1 = json.loads(k.read(m1))
        c2 = json.loads(k.read(m2))
        parents1 = {c1["parent"], c1["second_parent"]}
        parents2 = {c2["parent"], c2["second_parent"]}
        check(parents1 == parents2 == {ca, cb},
              "topology same regardless of parent/second_parent order")
    finally:
        cleanup(k, d)


def test_M2_associativity_of_merge_commits():
    """M2: Merging A<-B then merging (A<-B)<-C produces a commit graph
    that records both merges. Topology is preserved."""
    print("\n=== M2: Associativity of merge commits ===")
    k, d = make_kernel()
    try:
        ca = make_commit(k, tree_hash=k.write(b"a"), msg="a")
        cb = make_commit(k, parent=ca, tree_hash=k.write(b"b"), msg="b")
        cc = make_commit(k, parent=ca, tree_hash=k.write(b"c"), msg="c")

        # First merge: A <- B (merge B into A)
        m1 = make_commit(k, parent=ca, second_parent=cb,
                        tree_hash=k.write(b"ab"), msg="merge_ab")
        # Second merge: M1 <- C
        m2 = make_commit(k, parent=m1, second_parent=cc,
                        tree_hash=k.write(b"abc"), msg="merge_abc")

        # Walk m2's ancestry: should reach m1, ca, cb, cc
        visited = set()
        to_visit = [m2]
        while to_visit:
            h = to_visit.pop()
            if h in visited or h is None:
                continue
            visited.add(h)
            c = json.loads(k.read(h))
            if c["parent"]:
                to_visit.append(c["parent"])
            if c["second_parent"]:
                to_visit.append(c["second_parent"])

        check({ca, cb, cc, m1, m2}.issubset(visited),
              "all commits reachable from m2 (associativity preserved)")
        check(m1 in visited, "first merge commit reachable from second merge")
    finally:
        cleanup(k, d)


def test_M3_lens_determines_semantics():
    """M3: The kernel does not decide what the merged state contains.
    The Lens defines merge(A, B). We verify the kernel has no merge
    method — merge is a Lens-level composition."""
    print("\n=== M3: Lens determines semantics ===")
    k, d = make_kernel()
    try:
        api = [m for m in dir(k) if not m.startswith("_")]
        has_merge = any("merge" in m.lower() for m in api)
        check(not has_merge, "kernel has no merge method (Lens's responsibility)")
        # The kernel only records topology (parent, second_parent).
        # The Lens reads both parents' trees, applies its own merge
        # function, writes the result as a new tree, then creates a
        # merge commit referencing that tree.
    finally:
        cleanup(k, d)


def test_M4_prime_merge_well_defined():
    """M4': A merge commit's snapshot field may be either a full
    snapshot OR a delta relative to parent. (Demoted from M4 which
    required always-snapshot.)"""
    print("\n=== M4': Merge has a well-defined result ===")
    k, d = make_kernel()
    try:
        # Two commits with snapshot trees
        ta = k.write(json.dumps({"x": k.write(b"a")}).encode())
        tb = k.write(json.dumps({"x": k.write(b"b"), "y": k.write(b"new")}).encode())
        ca = make_commit(k, tree_hash=ta, msg="a")
        cb = make_commit(k, parent=ca, tree_hash=tb, msg="b")

        # Option 1: merge with full snapshot (default policy)
        merged_tree = k.write(json.dumps({
            "x": k.write(b"b"),  # B wins
            "y": k.write(b"new"),
        }).encode())
        m_snap = make_commit(k, parent=ca, second_parent=cb,
                            tree_hash=merged_tree, msg="merge_snap")

        # Option 2: merge with delta (optimization)
        delta = {"add": {"y": k.write(b"new")}, "del": [], "mod": {"x": k.write(b"b")}}
        m_delta = make_commit(k, parent=ca, second_parent=cb,
                             delta=delta, msg="merge_delta")

        # Both are valid merge commits (second_parent != None)
        c_snap = json.loads(k.read(m_snap))
        c_delta = json.loads(k.read(m_delta))
        check(c_snap["second_parent"] == cb, "snapshot merge has second_parent")
        check(c_delta["second_parent"] == cb, "delta merge has second_parent")
        check(c_snap["tree"] is not None, "snapshot merge has tree")
        check(c_delta["delta"] is not None, "delta merge has delta")
        check(c_delta["tree"] is None, "delta merge has no tree (delta only)")
    finally:
        cleanup(k, d)


# ---------------------------------------------------------------------------
# Workspace Algebra (W1-W5)
# ---------------------------------------------------------------------------

class Workspace:
    """In-memory staging area. Not part of the kernel; built on top.

    W4: A Workspace is not bound to a Lens. Any Lens can stage
    changes to the same Workspace. (Restricted to within-Collection
    per A6 — cross-Collection atomicity requires a coordinator per A7.)
    """

    def __init__(self, kernel: PondMinimal, head_ref: str):
        self.kernel = kernel
        self.head_ref = head_ref
        self.staged = {}  # {name: hash} — staged writes
        self.deletions = set()  # names to delete
        self.savepoints = []  # list of (staged, deletions) snapshots

    def stage(self, name: str, h: str):
        """Stage a write."""
        self.staged[name] = h
        self.deletions.discard(name)

    def stage_delete(self, name: str):
        """Stage a deletion."""
        self.deletions.add(name)
        self.staged.pop(name, None)

    def savepoint(self) -> int:
        """Create a savepoint. Returns savepoint id."""
        sp_id = len(self.savepoints)
        self.savepoints.append((dict(self.staged), set(self.deletions)))
        return sp_id

    def rollback_to(self, sp_id: int):
        """Rollback to a savepoint."""
        if sp_id < len(self.savepoints):
            self.staged, self.deletions = self.savepoints[sp_id]
            self.savepoints = self.savepoints[:sp_id]

    def abort(self):
        """Discard all staged changes."""
        self.staged.clear()
        self.deletions.clear()
        self.savepoints.clear()

    def commit(self, msg: str = "commit") -> str:
        """Atomically commit all staged changes. Returns the new HEAD hash."""
        # Build a commit blob listing all staged writes
        commit_data = {
            "writes": list(self.staged.items()),
            "deletions": list(self.deletions),
            "parent": self.kernel.resolve(self.head_ref),
            "message": msg,
            "timestamp": time.time(),
        }
        commit_blob = json.dumps(commit_data).encode()
        commit_h = self.kernel.write(commit_blob)
        # Atomic update: single Ref to HEAD (A6)
        self.kernel.reference(self.head_ref, commit_h)
        # Clear staging
        self.staged.clear()
        self.deletions.clear()
        self.savepoints.clear()
        return commit_h


def test_W1_isolation():
    """W1: Changes in a Workspace are not visible to other readers
    until commit."""
    print("\n=== W1: Isolation ===")
    k, d = make_kernel()
    try:
        # Initial HEAD
        h0 = k.write(b"initial")
        k.reference("HEAD", h0)
        original_head = k.resolve("HEAD")

        # Open a workspace
        ws = Workspace(k, "HEAD")
        # Stage a write
        h_new = k.write(b"new")
        ws.stage("x", h_new)

        # Reader (resolving HEAD) sees the OLD state
        check(k.resolve("HEAD") == original_head,
              "reader sees OLD state before commit")

        # Commit
        new_head = ws.commit()
        # Reader now sees the NEW state
        check(k.resolve("HEAD") == new_head,
              "reader sees NEW state after commit")
    finally:
        cleanup(k, d)


def test_W2_atomicity():
    """W2: commit() either commits all staged changes or none.
    Within-Collection (per A6); cross-Collection is out-of-model (A7)."""
    print("\n=== W2: Atomicity (within-Collection) ===")
    k, d = make_kernel()
    try:
        h0 = k.write(b"init")
        k.reference("HEAD", h0)

        ws = Workspace(k, "HEAD")
        # Stage 3 writes
        h1 = k.write(b"a")
        h2 = k.write(b"b")
        h3 = k.write(b"c")
        ws.stage("a", h1)
        ws.stage("b", h2)
        ws.stage("c", h3)

        # Before commit: HEAD still points to h0
        check(k.resolve("HEAD") == h0, "before commit: HEAD unchanged")

        # Commit: all 3 writes appear atomically
        new_h = ws.commit()
        commit_data = json.loads(k.read(new_h))
        check(len(commit_data["writes"]) == 3,
              "commit blob lists all 3 writes (atomic)")
        # Reader sees all 3 or none — there is no "partial" state
        # because HEAD is a single Ref (C3)
    finally:
        cleanup(k, d)


def test_W3_savepoint_rollback():
    """W3: rollback_to(sp) discards changes staged after sp but
    keeps changes staged before sp."""
    print("\n=== W3: Savepoint rollback ===")
    k, d = make_kernel()
    try:
        h0 = k.write(b"init")
        k.reference("HEAD", h0)

        ws = Workspace(k, "HEAD")
        h1 = k.write(b"a")
        ws.stage("a", h1)

        sp = ws.savepoint()

        h2 = k.write(b"b")
        ws.stage("b", h2)
        h3 = k.write(b"c")
        ws.stage("c", h3)

        # Before rollback: 3 staged
        check(len(ws.staged) == 3, "3 staged before rollback")

        # Rollback to sp: should keep 'a' (staged before sp),
        # discard 'b' and 'c' (staged after sp)
        ws.rollback_to(sp)
        check("a" in ws.staged, "rollback kept 'a' (staged before sp)")
        check("b" not in ws.staged, "rollback discarded 'b' (staged after sp)")
        check("c" not in ws.staged, "rollback discarded 'c' (staged after sp)")
    finally:
        cleanup(k, d)


def test_W4_lens_independence():
    """W4: A Workspace is not bound to a Lens. Any Lens can stage
    to the same Workspace. (Restricted to within-Collection per A6.)"""
    print("\n=== W4: Lens independence (within-Collection) ===")
    k, d = make_kernel()
    try:
        h0 = k.write(b"init")
        k.reference("HEAD", h0)

        # Two "Lenses" — both encode bytes differently, but both
        # can stage to the same Workspace
        ws = Workspace(k, "HEAD")

        # Lens A (e.g., JSON) stages a write
        json_bytes = json.dumps({"x": 1}).encode()
        h_json = k.write(json_bytes)
        ws.stage("json/x", h_json)

        # Lens B (e.g., Arrow) stages a write
        arrow_bytes = b"ARROW1\x00\x00..."  # fake
        h_arrow = k.write(arrow_bytes)
        ws.stage("arrow/y", h_arrow)

        # Both writes are in the same Workspace; commit batches them
        new_h = ws.commit()
        commit_data = json.loads(k.read(new_h))
        names = [n for n, _ in commit_data["writes"]]
        check("json/x" in names, "Lens A's write committed")
        check("arrow/y" in names, "Lens B's write committed")
        check(len(names) == 2, "both Lenses' writes in one atomic commit")
    finally:
        cleanup(k, d)


def test_W5_workspace_ephemeral():
    """W5: A Workspace lives in memory. It is not part of the commit
    history until committed."""
    print("\n=== W5: Workspace is ephemeral ===")
    k, d = make_kernel()
    try:
        h0 = k.write(b"init")
        k.reference("HEAD", h0)

        ws = Workspace(k, "HEAD")
        h1 = k.write(b"a")
        ws.stage("a", h1)

        # The Workspace is in memory — not on disk, not in any commit
        # Verify HEAD has not changed
        check(k.resolve("HEAD") == h0, "HEAD unchanged while workspace staged")

        # Abort — discard everything
        ws.abort()
        check(len(ws.staged) == 0, "abort clears staging")
        check(k.resolve("HEAD") == h0, "HEAD still unchanged after abort")

        # No commit blob was written for the aborted workspace
        # (We can't easily count blobs, but we can verify HEAD history
        # is unchanged.)
    finally:
        cleanup(k, d)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TESTS = [
    test_M1_commutativity_of_topology,
    test_M2_associativity_of_merge_commits,
    test_M3_lens_determines_semantics,
    test_M4_prime_merge_well_defined,
    test_W1_isolation,
    test_W2_atomicity,
    test_W3_savepoint_rollback,
    test_W4_lens_independence,
    test_W5_workspace_ephemeral,
]


def main():
    print("=" * 70)
    print("Pond Phase N.4 — Tests for Untested Laws")
    print("Covers Merge Algebra (M1-M4') and Workspace Algebra (W1-W5)")
    print("=" * 70)

    for test in ALL_TESTS:
        try:
            test()
        except Exception as e:
            global FAIL
            FAIL += 1
            print(f"  [ERROR] {test.__name__} raised: {type(e).__name__}: {e}")

    print("\n" + "=" * 70)
    print(f"RESULTS: {PASS} pass, {FAIL} fail")
    print("=" * 70)
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
