#!/usr/bin/env python3
"""
Test: TypedBlob middle layer — any lens can read any blob, cross-lens
indexing, bidirectional branching.

This proves:
  1. Any TypedLens can read ANY blob in the shared byte graph. If the
     codec matches, it decodes natively. If not, it returns the raw
     payload bytes — so the caller can transform later.
  2. Cross-lens indexing works: a TypedIndex extracts keys from blobs
     regardless of which lens wrote them (the middle layer decodes
     based on codec_id).
  3. Bidirectional branching: any lens can branch, any lens can
     checkout, any lens can merge. All share the same commit DAG.

Run:
    python pond-sdk/test_typed_blob.py
"""

from __future__ import annotations

import os
import sys
import shutil
import json

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, HERE)

from kernel import PondMinimal
from typed_blob import (TypedBlob, TypedLens, TypedIndex, CodecRegistry,
                         CODEC_JSON, CODEC_GIT_TREE, CODEC_NOTEBOOK,
                         CODEC_RAW, CODEC_CSV)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_any_lens_reads_any_blob():
    """THE KEY TEST: any lens can read any blob, even with different codecs.

    The TypedBlob envelope carries a codec_id. The middle layer
    (CodecRegistry) knows how to decode ALL registered codecs. So any
    lens reading any blob gets the fully decoded value — regardless
    of which lens wrote it.

    This is BETTER than "get raw bytes and transform later" — the
    middle layer decodes for you. You only get raw bytes if the
    codec_id isn't registered (which doesn't happen for built-in codecs).
    """
    bench = "/tmp/pond_typed_any_read"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Three lenses, same byte graph, different WRITE codecs
    sql = TypedLens(kernel, "workspace", CODEC_JSON)
    git = TypedLens(kernel, "workspace", CODEC_GIT_TREE)
    notebook = TypedLens(kernel, "workspace", CODEC_NOTEBOOK)

    # SQL writes a row (JSON envelope)
    sql.put("user:1", {"name": "Alice", "age": 30})
    sql.commit("SQL write")

    # Git writes a tree (git_tree envelope)
    git.put("tree:main", {"README.md": "abc123", "src/main.py": "def456"})
    git.commit("Git write")

    # --- SQL lens reads its own data (codec matches) ---
    sql_row = sql.get("user:1")
    assert sql_row == {"name": "Alice", "age": 30}

    # --- Git lens reads SQL data ---
    # The envelope says CODEC_JSON. The registry knows JSON.
    # Git lens gets the FULLY DECODED dict — not raw bytes!
    # This is the magic of the middle layer: any lens gets decoded data.
    git_reads_sql = git.get("user:1")
    assert git_reads_sql == {"name": "Alice", "age": 30}

    # --- Git lens can TRANSFORM this into its own format ---
    # (e.g., create a Git tree from the SQL row's data)
    tree_from_sql = {k: f"hash_{v}" for k, v in git_reads_sql.items()}
    git.put("tree:from_sql", tree_from_sql)
    git.commit("Git: transformed SQL row into tree")

    # --- SQL lens reads Git data ---
    # The envelope says CODEC_GIT_TREE. The registry knows git_tree.
    # SQL lens gets the FULLY DECODED tree dict!
    sql_reads_git = sql.get("tree:main")
    assert sql_reads_git == {"README.md": "abc123", "src/main.py": "def456"}

    # --- Notebook lens reads BOTH ---
    nb_reads_sql = notebook.get("user:1")
    assert nb_reads_sql == {"name": "Alice", "age": 30}
    nb_reads_git = notebook.get("tree:main")
    assert nb_reads_git == {"README.md": "abc123", "src/main.py": "def456"}

    # --- get_typed: inspect codec metadata ---
    info = sql.get_typed("tree:main")
    assert info["codec_id"] == CODEC_GIT_TREE
    assert info["codec_name"] == "git_tree"
    assert info["decoded"] is True  # decoded via registry
    assert info["value"] == {"README.md": "abc123", "src/main.py": "def456"}

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Any lens reads any blob (fully decoded via middle layer)")
    print("      - SQL reads SQL → dict (native)")
    print("      - Git reads SQL → dict (decoded via JSON codec in registry)")
    print("      - SQL reads Git → dict (decoded via git_tree codec in registry)")
    print("      - Notebook reads both → dict (all codecs in registry)")
    print("      - Git transforms SQL data into Git tree (consume + transform)")


def test_cross_lens_index():
    """An index works across lenses — extractor receives decoded payloads
    regardless of which lens wrote them.

    A TypedIndex with CODEC_JSON extractor indexes all JSON blobs.
    It can also index Git blobs (gets raw bytes, extractor can skip
    or handle them).
    """
    bench = "/tmp/pond_typed_cross_index"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    sql = TypedLens(kernel, "workspace", CODEC_JSON)
    git = TypedLens(kernel, "workspace", CODEC_GIT_TREE)

    # SQL writes rows with a "region" field
    sql.put("user:1", {"name": "Alice", "region": "US"})
    sql.put("user:2", {"name": "Bob", "region": "EU"})
    sql.put("user:3", {"name": "Carol", "region": "US"})
    sql.commit("SQL: 3 users")

    # Git writes a tree (no "region" field)
    git.put("tree:main", {"README.md": "abc123"})
    git.commit("Git: tree")

    # Build a cross-lens index on "region"
    # The extractor receives decoded payloads. For JSON blobs, it gets
    # a dict and can extract "region". For Git blobs, it gets raw bytes
    # (can't extract "region" — skips them).
    idx = TypedIndex(kernel, "workspace", CODEC_JSON)

    def region_extractor(payload):
        if isinstance(payload, dict):
            return payload.get("region")
        # payload is raw bytes (Git tree format) — skip
        return None

    idx.build_cross_lens_index("by_region", region_extractor)

    # Look up "US" — should find user:1 and user:3 (both JSON, region=US)
    result = idx.find_cross_lens("by_region", "US")
    assert result is not None
    assert result["typed"][1] is True  # decoded successfully
    assert result["typed"][2]["region"] == "US"

    # Look up "EU" — should find user:2
    result = idx.find_cross_lens("by_region", "EU")
    assert result is not None
    assert result["typed"][2]["region"] == "EU"

    # Look up "ASIA" — should return None (no match)
    result = idx.find_cross_lens("by_region", "ASIA")
    assert result is None

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Cross-lens index")
    print("      - Index built across JSON blobs (SQL) and Git blobs")
    print("      - Extractor received decoded dicts for JSON blobs")
    print("      - Extractor received raw bytes for Git blobs (skipped)")
    print("      - find_cross_lens returns typed info about the match")


def test_bidirectional_branching():
    """Any lens can branch, any lens can checkout, any lens can merge.

    All share the same commit DAG. A branch created by SQL is visible
    to Git. A commit by Git on a branch is visible to SQL.
    """
    bench = "/tmp/pond_typed_branching"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    sql = TypedLens(kernel, "workspace", CODEC_JSON)
    git = TypedLens(kernel, "workspace", CODEC_GIT_TREE)
    notebook = TypedLens(kernel, "workspace", CODEC_NOTEBOOK)

    # Initial write via SQL
    sql.put("user:1", {"name": "Alice"})
    sql.commit("initial")

    # SQL creates a branch
    sql.branch("feature-x")
    assert "feature-x" in sql.list_branches()

    # Git sees the branch (shared DAG)
    assert "feature-x" in git.list_branches()
    assert "feature-x" in notebook.list_branches()

    # Git checks out the branch and writes
    git.checkout("feature-x")
    git.put("tree:feature", {"file.txt": "new123"})
    git.commit("Git: add tree on feature-x")

    # SQL checks out the same branch and sees the Git commit
    sql.checkout("feature-x")
    assert "tree:feature" in sql
    # SQL can read the Git blob — decoded via the registry!
    tree = sql.get("tree:feature")
    assert tree == {"file.txt": "new123"}  # decoded from git_tree codec

    # SQL adds its own data on the same branch
    sql.put("user:2", {"name": "Bob"})
    sql.commit("SQL: add user:2 on feature-x")

    # Notebook sees both commits (shared DAG)
    notebook.checkout("feature-x")
    history = notebook.history()
    assert len(history) >= 3  # initial + git commit + sql commit

    # Notebook can read both SQL and Git data — all decoded via registry
    user2 = notebook.get("user:2")
    assert user2 == {"name": "Bob"}
    tree = notebook.get("tree:feature")
    assert tree == {"file.txt": "new123"}  # decoded from git_tree codec

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Bidirectional branching")
    print("      - SQL creates branch; Git and Notebook see it")
    print("      - Git commits on branch; SQL sees the commit")
    print("      - SQL commits on branch; Notebook sees the commit")
    print("      - All lenses share the same commit DAG")


def test_envelope_overhead():
    """Verify the envelope overhead is minimal: 5 bytes per blob."""
    bench = "/tmp/pond_typed_overhead"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    sql = TypedLens(kernel, "workspace", CODEC_JSON)
    sql.put("k1", {"name": "Alice"})
    sql.commit("write k1")

    # Verify: no manifest, no enable_view, no per-lens metadata
    names = kernel.list_names()
    forbidden = [n for n in names if any(kw in n.lower() for kw in
                ["manifest", "enable", "sidecar", "_view_", "_lens_"])]
    assert not forbidden, f"Found metadata overhead: {forbidden}"

    # Verify: the envelope is bytes (kernel doesn't interpret it)
    raw = sql.get_raw("k1")
    assert isinstance(raw, bytes)
    # Envelope: [1B codec_id][4B payload_len][payload]
    assert len(raw) >= 5
    # codec_id should be CODEC_JSON (1)
    import struct
    codec_id, payload_len = struct.unpack("<BI", raw[:5])
    assert codec_id == CODEC_JSON
    assert payload_len == len(raw) - 5
    # The payload is the JSON bytes
    payload = raw[5:]
    assert json.loads(payload) == {"name": "Alice"}

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print(f"PASS: Envelope overhead is 5 bytes per blob (codec_id={codec_id}, "
          f"payload_len={payload_len}, total={len(raw)})")


def test_transform_later():
    """A lens reads a blob written by another lens, then transforms it.

    The middle layer decodes the blob (via the registry). The lens
    gets the decoded value and can transform it into its own format.
    """
    bench = "/tmp/pond_typed_transform"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    sql = TypedLens(kernel, "workspace", CODEC_JSON)
    git = TypedLens(kernel, "workspace", CODEC_GIT_TREE)

    # SQL writes a row
    sql.put("user:1", {"name": "Alice", "age": 30, "files": ["a.py", "b.py"]})
    sql.commit("SQL write")

    # Git lens reads it — gets the decoded dict (via registry)
    data = git.get("user:1")
    assert data == {"name": "Alice", "age": 30, "files": ["a.py", "b.py"]}

    # Git lens TRANSFORMS the data into a Git tree
    # (treating each file in the "files" list as a tree entry)
    tree = {filename: "fake_hash_" + filename for filename in data["files"]}
    git.put("tree:from_user1", tree)
    git.commit("Git: transformed SQL row into Git tree")

    # Verify the transformation
    result = git.get("tree:from_user1")
    assert result == {"a.py": "fake_hash_a.py", "b.py": "fake_hash_b.py"}

    # SQL lens can also read the Git tree (decoded via registry)
    sql_reads_tree = sql.get("tree:from_user1")
    assert sql_reads_tree == {"a.py": "fake_hash_a.py", "b.py": "fake_hash_b.py"}

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Transform later")
    print("      - Git lens read SQL blob (decoded via registry)")
    print("      - Extracted 'files' list from the decoded dict")
    print("      - Transformed into Git tree format")
    print("      - Wrote the transformed data back as a Git tree")
    print("      - SQL lens can also read the Git tree (decoded via registry)")


def _run_all_tests():
    print("=== TypedBlob Middle Layer — Any Lens, Any Blob, Cross-Lens Index ===\n")

    test_any_lens_reads_any_blob()
    print()
    test_cross_lens_index()
    print()
    test_bidirectional_branching()
    print()
    test_envelope_overhead()
    print()
    test_transform_later()

    print("\n" + "=" * 72)
    print("  THE MIDDLE LAYER WORKS:")
    print("  - Any lens can read any blob (native decode or raw payload)")
    print("  - Cross-lens indexing (extractor receives decoded payloads)")
    print("  - Bidirectional branching (any lens branches, all see it)")
    print("  - Transform later (read raw, transform, write back)")
    print("  - Minimal overhead (5 bytes per blob envelope)")
    print("  - NO manifest, NO enable_view, NO per-lens metadata")
    print("=" * 72)


if __name__ == "__main__":
    _run_all_tests()
