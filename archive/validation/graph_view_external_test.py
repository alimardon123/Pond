"""
Test script for GraphView external validation.

Exercises every operation in the task spec, plus the SDK_SPEC.md
contracts for branching, history, diff, merge, and index drop.
"""

from __future__ import annotations

import os
import sys
import shutil
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from graph_view_external import GraphView, TOMBSTONE_HASH  # noqa: E402
from pond_minimal import PondMinimal  # noqa: E402


def fresh_kernel():
    d = tempfile.mkdtemp(prefix="pond_graph_test_")
    return PondMinimal(d), d


PASS = 0
FAIL = 0


def check(label, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS: {label}")
    else:
        FAIL += 1
        print(f"  FAIL: {label}")


# ---------------------------------------------------------------------------
# 1. Basic node + edge operations
# ---------------------------------------------------------------------------
print("\n=== 1. Basic add/get ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g1")
    g.add_node("n1", "person", {"name": "Alice", "age": 30})
    g.add_node("n2", "person", {"name": "Bob"})
    g.add_node("n3", "movie", {"title": "Inception"})
    g.add_edge("n1", "n2", "knows", {"since": 2010})
    g.add_edge("n1", "n3", "acted_in", {"role": "Cobb"})
    g.add_edge("n2", "n3", "directed", {})

    c1 = g.commit("add graph")
    check("commit returned a hash", isinstance(c1, str) and len(c1) == 64)

    n1 = g.get_node("n1")
    check("get_node n1 type", n1["type"] == "person")
    check("get_node n1 props", n1["properties"]["name"] == "Alice")

    check("count_nodes=3", g.count_nodes() == 3)
    check("count_edges=3", g.count_edges() == 3)

    nbrs = g.get_neighbors("n1")
    nbrs_to = sorted(n["to"] for n in nbrs)
    check("neighbors of n1 = [n2, n3]", nbrs_to == ["n2", "n3"])

    nbrs_knows = g.get_neighbors("n1", edge_type="knows")
    check("neighbors of n1 filtered by 'knows' = [n2]",
          [n["to"] for n in nbrs_knows] == ["n2"])
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 2. find_nodes_by_type / find_edges_by_type (uses indexes)
# ---------------------------------------------------------------------------
print("\n=== 2. Index-based lookups ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g2")
    g.add_node("u1", "user", {"name": "A"})
    g.add_node("u2", "user", {"name": "B"})
    g.add_node("u3", "admin", {"name": "C"})
    g.add_edge("u1", "u2", "follows", {})
    g.add_edge("u2", "u3", "follows", {})
    g.add_edge("u1", "u3", "mentions", {})
    g.commit("build")

    users = sorted(n["id"] for n in g.find_nodes_by_type("user"))
    check("find_nodes_by_type('user') = [u1, u2]", users == ["u1", "u2"])

    admins = g.find_nodes_by_type("admin")
    check("find_nodes_by_type('admin') count=1", len(admins) == 1)

    follows = sorted(e["from"] + "→" + e["to"]
                     for e in g.find_edges_by_type("follows"))
    check("find_edges_by_type('follows') = [u1→u2, u2→u3]",
          follows == ["u1→u2", "u2→u3"])

    mentions = g.find_edges_by_type("mentions")
    check("find_edges_by_type('mentions') count=1", len(mentions) == 1)

    check("list_indexes has by_node_type",
          "by_node_type" in g.list_indexes())
    check("list_indexes has by_edge_type",
          "by_edge_type" in g.list_indexes())
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 3. delete_node cascades to edges; delete_edge removes one edge
# ---------------------------------------------------------------------------
print("\n=== 3. Deletions ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g3")
    g.add_node("a", "x", {})
    g.add_node("b", "x", {})
    g.add_node("c", "x", {})
    g.add_edge("a", "b", "r", {})
    g.add_edge("b", "c", "r", {})
    g.add_edge("c", "a", "r", {})
    g.commit("triangle")

    check("count_edges before delete=3", g.count_edges() == 3)

    g.delete_edge("a", "b", "r")
    g.commit("delete edge a→b")
    check("count_edges after delete_edge=2", g.count_edges() == 2)
    check("edge a→b gone", g.get("edge:a:b:r") is None)
    check("edge b→c still there", g.get("edge:b:c:r") is not None)

    g.delete_node("c")
    g.commit("delete node c (cascades)")
    check("count_nodes after delete=2", g.count_nodes() == 2)
    # c had: b→c (incoming), c→a (outgoing). Both should be gone.
    check("edge b→c gone (cascaded)", g.get("edge:b:c:r") is None)
    check("edge c→a gone (cascaded)", g.get("edge:c:a:r") is None)
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 4. History shape (spec §6.2)
# ---------------------------------------------------------------------------
print("\n=== 4. History shape ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g4")
    for i in range(6):
        g.add_node(f"n{i}", "t", {"i": i})
        g.commit(f"commit {i}")

    h = g.history(limit=10)
    check("history len=6", len(h) == 6)
    check("history most-recent-first",
          h[0]["message"] == "commit 5" and h[-1]["message"] == "commit 0")
    check("history first index=0", h[-1]["index"] == 0)
    check("history last index=5", h[0]["index"] == 5)

    # Verify exact dict shape per spec §6.2.
    expected_keys = {"commit", "message", "timestamp", "index", "type"}
    check("history record has exactly 5 keys",
          set(h[0].keys()) == expected_keys)
    check("history commit is 12 chars", len(h[0]["commit"]) == 12)
    check("history timestamp is float", isinstance(h[0]["timestamp"], float))
    check("history index is int", isinstance(h[0]["index"], int))
    check("history type is str", h[0]["type"] in ("snapshot", "delta"))

    # COMPACTION_THRESHOLD=4: commit 0 = snapshot (no parent);
    # commits 1-4 = delta; commit 5 = snapshot (5 deltas since last).
    types = [r["type"] for r in reversed(h)]
    check("commit 0 is snapshot", types[0] == "snapshot")
    check("commits 1-4 are deltas",
          types[1:5] == ["delta", "delta", "delta", "delta"])
    check("commit 5 is snapshot (compaction)", types[5] == "snapshot")
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 5. Branching + merge (spec §6.1: union, merged-branch-wins)
# ---------------------------------------------------------------------------
print("\n=== 5. Branching + merge ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g5")
    g.add_node("x", "base", {"v": 1})
    g.commit("main c0")

    g.branch("feature")
    g.checkout("feature")
    g.add_node("y", "feature-only", {"v": 2})
    g.commit("feature adds y")

    g.checkout("main")
    # x exists in main; modify it on main.
    g.add_node("x", "base", {"v": 100})  # main-side update
    g.commit("main updates x")

    # Sanity before merge.
    check("main has x (v=100)", g.get_node("x")["properties"]["v"] == 100)
    check("main has no y", g.get_node("y") is None)

    # Merge feature into main. Spec §6.1: merged-branch wins on conflict.
    mc = g.merge("feature")
    check("merge returned a hash", isinstance(mc, str) and len(mc) == 64)

    # After merge: x should have feature's value (v=1, since feature didn't
    # touch x — wait, feature didn't modify x, so x is still v=1 in feature's
    # state? No — feature was branched from main c0 where x.v=1. So feature's
    # x.v=1. main's x.v=100. merged.update(branch_state) → branch wins → 1.)
    x_after = g.get_node("x")
    check("after merge, x.v = 1 (merged-branch-wins)",
          x_after["properties"]["v"] == 1)
    check("after merge, y present", g.get_node("y") is not None)

    check("branches listed", set(g.list_branches()) == {"main", "feature"})
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 6. Diff between two commits (spec §6.3)
# ---------------------------------------------------------------------------
print("\n=== 6. Diff ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g6")
    g.add_node("a", "t", {"v": 1})
    g.commit("c0: add a")
    c0 = g._head_hash()

    g.add_node("b", "t", {"v": 2})
    g.commit("c1: add b")
    c1 = g._head_hash()

    g.delete_node("a")
    g.commit("c2: delete a")

    g.add_node("a", "t", {"v": 999})  # re-add with different value
    g.commit("c3: re-add a")
    c3 = g._head_hash()

    # diff c0 → c1: should add b
    d = g.diff(c0[:8], c1[:8])
    check("diff c0→c1 added has 'node:b'", "node:b" in d["added"])
    check("diff c0→c1 removed empty", d["removed"] == {})
    check("diff c0→c1 modified empty", d["modified"] == {})

    # diff c0 → c3: b added; a modified (different blob hash).
    d2 = g.diff(c0[:8], c3[:8])
    check("diff c0→c3 added has 'node:b'", "node:b" in d2["added"])
    check("diff c0→c3 modified has 'node:a'", "node:a" in d2["modified"])
    check("diff c0→c3 modified has old/new",
          "old" in d2["modified"]["node:a"] and
          "new" in d2["modified"]["node:a"])

    # Diff with bad prefix raises ValueError (spec §6.3).
    raised = False
    try:
        g.diff("deadbeef", c0[:8])
    except ValueError:
        raised = True
    check("diff with unknown prefix raises ValueError", raised)
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 7. drop_index tombstone behavior (spec §4.5, RFC-0008)
# ---------------------------------------------------------------------------
print("\n=== 7. drop_index tombstone ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g7")
    g.add_node("n1", "t", {})
    g.commit("c0")
    check("by_node_type index present before drop",
          "by_node_type" in g.list_indexes())

    dropped = g.drop_index("by_node_type")
    check("drop_index returns True", dropped is True)

    check("by_node_type absent from list_indexes after drop",
          "by_node_type" not in g.list_indexes())

    # The Reference still exists in the kernel but points to TOMBSTONE_HASH.
    from graph_view_external import is_dropped
    ref = g._index_ref("by_node_type")
    check("tombstone bound", is_dropped(kernel, ref))

    # Drop again is idempotent (returns False; already tombstoned).
    dropped2 = g.drop_index("by_node_type")
    check("drop_index idempotent returns False", dropped2 is False)

    # find_nodes_by_type falls back to linear scan after drop.
    ns = g.find_nodes_by_type("t")
    check("find_nodes_by_type still works after drop (fallback)",
          len(ns) == 1 and ns[0]["id"] == "n1")

    # Revive: rebuild indexes via a new commit.
    g.add_node("n2", "t", {})
    g.commit("c1 (revives indexes)")
    check("by_node_type revived after new commit",
          "by_node_type" in g.list_indexes())
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 8. get() correctness across the delta/snapshot boundary
# ---------------------------------------------------------------------------
print("\n=== 8. get() across compaction boundary ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g8")
    # 5 commits — should produce snapshot at c0, deltas at c1-c4, snapshot at c5.
    g.add_node("a", "t", {"v": 0}); g.commit("c0")
    g.add_node("a", "t", {"v": 1}); g.commit("c1")
    g.add_node("a", "t", {"v": 2}); g.commit("c2")
    g.add_node("a", "t", {"v": 3}); g.commit("c3")
    g.add_node("a", "t", {"v": 4}); g.commit("c4")
    g.add_node("a", "t", {"v": 5}); g.commit("c5")  # snapshot (5 deltas since c0)
    check("get('node:a') after 6 commits", g.get_node("a")["properties"]["v"] == 5)

    # Add a node then delete it across the snapshot boundary.
    g.add_node("temp", "t", {"v": 99}); g.commit("c6: add temp")
    g.delete_node("temp");                g.commit("c7: delete temp")
    check("temp gone after delete", g.get_node("temp") is None)
    check("node:a still v=5", g.get_node("a")["properties"]["v"] == 5)
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 9. put_raw (spec §2.3) — zero-copy share of an existing blob
# ---------------------------------------------------------------------------
print("\n=== 9. put_raw zero-copy ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g9")
    g.add_node("n1", "t", {"k": "v"})
    g.commit("c0")
    # Find n1's blob hash, then put_raw it under a different key.
    state = g._read_state_at_commit(g._head_hash())
    n1_blob = state[g._node_key("n1")]
    g.put_raw("node:n1_copy", n1_blob)
    g.commit("c1: copy via put_raw")
    copy = g.get("node:n1_copy")
    check("put_raw copied value matches original",
          copy is not None and copy["properties"]["k"] == "v")
    check("put_raw did NOT re-encode (same blob hash in state)",
          g._read_state_at_commit(g._head_hash())["node:n1_copy"] == n1_blob)
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 10. Empty commit raises (spec §2.4)
# ---------------------------------------------------------------------------
print("\n=== 10. Empty commit raises ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g10")
    raised = False
    try:
        g.commit("nothing")
    except ValueError:
        raised = True
    check("commit with no staging raises ValueError", raised)

    g.add_node("n", "t", {})
    g.commit("c0")
    raised2 = False
    try:
        g.commit("nothing again")
    except ValueError:
        raised2 = True
    check("second commit with no staging raises ValueError", raised2)
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 11. branch with no commits raises (spec §5.1)
# ---------------------------------------------------------------------------
print("\n=== 11. branch with no HEAD raises ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g11")
    raised = False
    try:
        g.branch("x")
    except ValueError:
        raised = True
    check("branch before any commit raises ValueError", raised)
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# 12. Checkout clears staging (spec §5.2)
# ---------------------------------------------------------------------------
print("\n=== 12. Checkout clears staging ===")
kernel, kd = fresh_kernel()
try:
    g = GraphView(kernel, "g12")
    g.add_node("a", "t", {}); g.commit("c0")
    g.branch("feat")
    g.add_node("b", "t", {})  # staged, not committed
    check("staging has node:b before checkout",
          g._node_key("b") in g.staging)
    g.checkout("feat")
    check("staging cleared by checkout", g.staging == {})
    # Commit on feat — node:b should NOT be there.
    g.add_node("c", "t", {}); g.commit("feat c1")
    check("node:b not on feat branch", g.get_node("b") is None)
    check("node:c on feat branch", g.get_node("c") is not None)
finally:
    kernel.close()
    shutil.rmtree(kd, ignore_errors=True)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print(f"\n=== Summary: {PASS} passed, {FAIL} failed ===")
sys.exit(0 if FAIL == 0 else 1)
