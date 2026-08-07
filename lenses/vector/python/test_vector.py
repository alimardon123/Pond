"""
Test harness for VectorLens.

Verifies:
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
import shutil

# Make bindings/python/core and bindings/python/sdk importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "..", "bindings/python/core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "..", "bindings/python/sdk"))

from kernel import PondMinimal
from vector_lens import VectorLens


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
    test_dir = "/tmp/pond_vector_test"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    kernel = PondMinimal(test_dir)
    lens = VectorLens(kernel)
    collection = "vectors"

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
        h = lens.insert(collection, vid, vec, meta)
        print(f"  inserted {vid} -> commit {h[:12]}…")

    check("count == 5 after inserts", lens.count(collection) == 5)
    check("list_vectors has 5 ids", len(lens.list_vectors(collection)) == 5)
    check("v1 in list", "v1" in lens.list_vectors(collection))

    # Verify binary storage: read the raw blob for v1 from the kernel.
    raw_blob = lens.get_raw(collection, "v1")
    is_json = raw_blob[:1] == b"{" if raw_blob else False
    (vec_len,) = struct.unpack_from("<I", raw_blob, 0)
    check("raw blob is NOT JSON", not is_json)
    check("raw blob starts with vec_len==2", vec_len == 2)

    # ------------------------------------------------------------------
    banner("2. Search for nearest 2 to [1.5, 1.5]")
    # ------------------------------------------------------------------
    results = lens.search(collection, [1.5, 1.5], k=2)
    check("search returns 2 results", len(results) == 2)
    check("nearest is v1", results[0]["id"] == "v1")
    check("second nearest is v2", results[1]["id"] == "v2")
    check("v1 distance ~ 0.707", abs(results[0]["distance"] - 0.7071) < 0.01)

    # ------------------------------------------------------------------
    banner("3. Get by ID + index lookup")
    # ------------------------------------------------------------------
    rec = lens.get_vector(collection, "v3")
    check("get v3 returns vector [3,3]", rec["vector"] == [3.0, 3.0])
    check("get v3 metadata label == c", rec["metadata"]["label"] == "c")

    rec2 = lens.find_by_id(collection, "v3")
    check("find_by_id('v3') matches get", rec2 == rec)

    check("get missing returns None", lens.get_vector(collection, "nope") is None)

    # ------------------------------------------------------------------
    banner("4. Delete v2")
    # ------------------------------------------------------------------
    lens.delete_vector(collection, "v2")
    check("v2 no longer exists", lens.get_vector(collection, "v2") is None)
    check("v2 not in list", "v2" not in lens.list_vectors(collection))

    # Search again — v2 should not be in results
    results2 = lens.search(collection, [1.5, 1.5], k=3)
    ids = {r["id"] for r in results2}
    check("deleted v2 not in search results", "v2" not in ids)

    # ------------------------------------------------------------------
    banner("5. Branch, insert, merge")
    # ------------------------------------------------------------------
    lens.create_branch(collection, "experiment")
    lens.checkout_branch(collection, "experiment")

    lens.insert(collection, "v6", [6.0, 6.0], {"label": "f"})
    print("  inserted v6 on 'experiment' branch")

    check("experiment has v6", lens.get_vector(collection, "v6") is not None)

    # Merge experiment into main
    merge_hash = lens.merge_branch(collection, "experiment")
    print(f"  merged 'experiment' -> {merge_hash[:12]}…")
    check("v6 visible after merge", lens.get_vector(collection, "v6") is not None)

    # ------------------------------------------------------------------
    banner("6. History")
    # ------------------------------------------------------------------
    hist = lens.get_history(collection, limit=20)
    print(f"  {len(hist)} commits in history:")
    for entry in hist:
        msg = entry.get("message", "")
        h = entry.get("hash", entry.get("commit", ""))[:12]
        print(f"    {h}…  {msg}")
    check("history has at least 7 commits", len(hist) >= 7)

    messages = [e.get("message", "") for e in hist]
    check("history contains insert messages",
          any("insert" in m for m in messages))
    check("history contains delete message",
          any("delete" in m for m in messages))

    # ------------------------------------------------------------------
    banner("Summary")
    # ------------------------------------------------------------------
    if check.failures == 0:
        print("  ALL CHECKS PASSED")
    else:
        print(f"  {check.failures} CHECK(S) FAILED")

    kernel.close()
    shutil.rmtree(test_dir, ignore_errors=True)
    return check.failures


if __name__ == "__main__":
    sys.exit(main())
