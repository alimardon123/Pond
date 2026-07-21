"""
Test harness for VectorView.

Runs the required scenario:
    1. insert 5 vectors
    2. search for nearest 2
    3. get by ID
    4. delete one
    5. branch, insert, merge
    6. history

Also verifies that vectors are stored as packed binary, not JSON.
"""

import json
import struct
import sys
import os

# Make sure the mock SDK modules on on the path.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pond_minimal import PondMinimal
from vector_view import VectorView

def banner(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def check(label: str, condition: bool) -> None:
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}")
    if not condition:
        check.failures += 1
check.failures = 0


def main() -> int:
    kernel = PondMinimal()
    view = VectorView(kernel, "vectors")

    # ------------------------------------------------------------------
    banner("1. Insert 5 vectors")
    # ------------------------------------------------------------------
    vectors = [
        ("v1", [1.0, 1.0], {"label": "a"}),
        ("v2", [2.0, 2.0], {"label": "b"}),
        ("v3", [3.0, 3.0], {"label": "c"}),
        ("v4", [4.0, 4.0], {"label": "d"}),
        ("v5", [5.0, 5.0], {"label": "e"}),
    ]
    for vid, vec, meta in vectors:
        h = view.insert(vid, vec, meta)
        print(f"  inserted {vid} -> commit {h[:12]}…")

    check("count == 5 after inserts", view.count() == 5)
    check("list_vectors has 5 ids", len(view.list_vectors()) == 5)
    check("v1 in list", "v1" in view.list_vectors())

    # Verify binary storage: read the raw blob for v1 from the kernel.
    snapshot = view._get_snapshot()
    raw_blob = kernel.read(snapshot["v1"])
    is_json = raw_blob[:1] == b"{"
    (vec_len,) = struct.unpack_from("<I", raw_blob, 0)
    check("raw blob is NOT JSON", not is_json)
    check("raw blob starts with vec_len==2", vec_len == 2)
    print(f"  raw blob for v1 (first 24 bytes): {raw_blob[:24]!r}")

    # ------------------------------------------------------------------
    banner("2. Search for nearest 2 to [1.5, 1.5]")
    # ------------------------------------------------------------------
    results = view.search([1.5, 1.5], k=2)
    print(f"  query = [1.5, 1.5]")
    for r in results:
        print(f"    id={r['id']}  dist={r['distance']:.4f}  "
              f"vec={r['vector']}  meta={r['metadata']}")
    check("search returns 2 results", len(results) == 2)
    check("nearest is v1", results[0]["id"] == "v1")
    check("second nearest is v2", results[1]["id"] == "v2")
    check("v1 distance ~ 0.707",
          abs(results[0]["distance"] - 0.7071) < 0.001)

    # ------------------------------------------------------------------
    banner("3. Get by ID")
    # ------------------------------------------------------------------
    rec = view.get("v3")
    print(f"  get('v3') = {rec}")
    check("get v3 returns vector [3,3]", rec["vector"] == [3.0, 3.0])
    check("get v3 metadata label == c", rec["metadata"]["label"] == "c")

    # Also test the index-backed lookup.
    rec2 = view.find_by_id("v3")
    check("find_by_id('v3') matches get", rec2 == rec)

    missing = view.get("nonexistent")
    check("get missing returns None", missing is None)

    # ------------------------------------------------------------------
    banner("4. Delete one vector")
    # ------------------------------------------------------------------
    view.delete("v2")
    print(f"  deleted v2")
    check("count == 4 after delete", view.count() == 4)
    check("v2 no longer exists", view.get("v2") is None)
    check("v2 not in list", "v2" not in view.list_vectors())

    # Search again — should only return from the 4 remaining.
    results2 = view.search([2.0, 2.0], k=2)
    print(f"  search after delete:")
    for r in results2:
        print(f"    id={r['id']}  dist={r['distance']:.4f}")
    check("deleted v2 not in search results",
          all(r["id"] != "v2" for r in results2))

    # ------------------------------------------------------------------
    banner("5. Branch, insert, merge")
    # ------------------------------------------------------------------
    # Create and checkout a branch.
    view.create_branch("experiment")
    view.checkout_branch("experiment")
    print("  created + checked out branch 'experiment'")

    view.insert("v6", [6.0, 6.0], {"label": "f", "branch": "experiment"})
    print("  inserted v6 on 'experiment' branch")

    check("experiment has v6", view.get("v6") is not None)
    check("experiment count == 5", view.count() == 5)

    # Switch back to main — v6 should not be visible.
    view.checkout_branch("vectors")
    print("  checked out 'vectors' (main)")
    check("main does not have v6 before merge", view.get("v6") is None)
    check("main count == 4 before merge", view.count() == 4)

    # Merge experiment into main.
    merge_hash = view.merge_branch("experiment")
    print(f"  merged 'experiment' -> {merge_hash[:12]}…")
    check("v6 visible after merge", view.get("v6") is not None)
    check("count == 5 after merge", view.count() == 5)

    # ------------------------------------------------------------------
    banner("6. History")
    # ------------------------------------------------------------------
    hist = view.get_history(limit=20)
    print(f"  {len(hist)} commits in history:")
    for entry in hist:
        msg = entry.get("message", "")
        print(f"    {entry['hash'][:12]}…  {msg}")
    check("history has at least 7 commits", len(hist) >= 7)

    # Check we can see insert and delete messages.
    messages = [e.get("message", "") for e in hist]
    check("history contains insert messages",
          any("insert" in m for m in messages))
    check("history contains delete message",
          any("delete" in m for m in messages))
    check("history contains merge message",
          any("Merge" in m for m in messages))

    # ------------------------------------------------------------------
    banner("Summary")
    # ------------------------------------------------------------------
    total = 0
    # Recount checks (we can't easily know how many ran, so use failures).
    if check.failures == 0:
        print("  ALL CHECKS PASSED")
        return 0
    else:
        print(f"  {check.failures} CHECK(S) FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
