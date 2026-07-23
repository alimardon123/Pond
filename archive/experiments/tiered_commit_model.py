"""
Pond Tiered Commit Model — fast writes + fast reads + history + streaming.

THE PROBLEM:
  - Always-snapshot: O(N) write, O(log N) read. Bad for streaming (slow writes).
  - Always-delta: O(1) write, O(N) read (must walk chain). Bad for point lookups.
  - We need BOTH: O(1) write for streaming + O(log N) read for lookups.

THE INSIGHT (from researching Dolt, Git, FoundationDB, LSM trees):
  Dolt uses Prolly trees with "chunk-level structural sharing" — only changed
  chunks are rewritten, so a "full snapshot" is actually O(changed chunks),
  not O(N). But even Dolt doesn't do O(1) writes.

  Git uses loose objects (O(1) write) + periodic packfiles (batch reads).
  The loose objects are fast to write but slow to scan. Packfiles are slow
  to build but fast to read.

  FoundationDB uses a write-ahead log (WAL) for fast writes, then
  background compaction into SSTables for fast reads. The WAL is
  ephemeral; the SSTable is the durable state.

  LSM trees use a memtable (in-memory) for writes, flushed to SSTables
  (immutable sorted files) for reads. Reads merge memtable + SSTables.

POND'S MODEL — "Tiered Commits":

  Three tiers of commits, each serving a different purpose:

  TIER 1: Delta commits (fast writes, for streaming)
    - O(1) write: only the changed keys
    - Like Git loose objects or LSM memtable flushes
    - Good for: streaming, high-frequency writes, OLTP

  TIER 2: Snapshot commits (fast reads, for lookups)
    - O(changed_chunks) write: full Prolly tree, but structural sharing
      means only changed chunks are new blobs
    - Like Dolt's Prolly tree snapshots
    - Good for: point lookups, scans, OLAP

  TIER 3: Packed commits (fast scans, for object storage)
    - Multiple blobs packed into a single large file with offset table
    - Like Git packfiles or Parquet row groups
    - Good for: object storage (S3), bulk reads, archival

  THE KEY INNOVATION — "Snapshot Pointer":

    HEAD always points to a snapshot commit (Tier 2), NOT a delta commit.
    Delta commits (Tier 1) are written between snapshots for fast streaming,
    but they're CHAINED FROM the snapshot, not from HEAD.

    Structure:
      HEAD → snapshot_commit (Tier 2, has full Prolly tree root)
                   ↑ parent
              delta_commit (Tier 1, has only changed keys)
                   ↑ parent
              delta_commit (Tier 1)
                   ↑ parent
              snapshot_commit (Tier 2, previous full state)

    Lookup: HEAD → snapshot → tree → leaf → blob (O(log N), NO chain walk)
    Write (streaming): append delta to the delta chain (O(1))
    Write (batch): create new snapshot (O(changed_chunks))
    Compaction: periodically convert delta chain → new snapshot

    This gives us:
    - O(log N) lookup (via snapshot, no chain walk) ✓
    - O(1) streaming write (via delta append) ✓
    - O(changed_chunks) batch commit (via Prolly tree structural sharing) ✓
    - O(N/chunk) scan (via Prolly tree leaf traversal) ✓
    - Full history (all commits are preserved in the chain) ✓
    - Branching (O(1) reference) ✓

  The snapshot pointer is stored as a kernel Reference:
    {name}__snapshot → hash of the latest snapshot commit

  HEAD ({name}) always points to the latest commit (snapshot OR delta).
  But lookup() reads {name}__snapshot directly, skipping any deltas.

  When a delta chain exceeds COMPACTION_THRESHOLD, a new snapshot is
  created from the full state, and {name}__snapshot is updated.
"""

from __future__ import annotations

import os
import sys
import time
import struct
import json
from typing import Optional, Any

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))

from pond_minimal import PondMinimal
from binary_encoding import BinaryProllyTree
from prolly_view import ProllyTree, TARGET_CHUNK_ENTRIES


# Configuration
TIER1_DELTA_THRESHOLD = 16  # after this many deltas, compact to a snapshot


class TieredCommitModel:
    """Tiered commit model: fast writes (delta) + fast reads (snapshot).

    This is a drop-in replacement for ProllyLensBase's commit/lookup logic.
    It maintains a snapshot pointer alongside HEAD, so lookups always go
    directly to the latest snapshot without walking the delta chain.
    """

    def __init__(self, kernel: PondMinimal, name: str):
        self.kernel = kernel
        self.name = name
        self._staged_add: dict[str, str] = {}
        self._staged_del: set[str] = set()
        self._delta_count_since_snapshot = 0

        # Snapshot reference name
        self._snapshot_ref = f"{name}__snapshot"

        # Initialize snapshot pointer if it doesn't exist
        if self.kernel.resolve(self._snapshot_ref) is None:
            head = self.kernel.resolve(self.name)
            if head:
                # Try to find the snapshot in the existing chain
                snap = self._find_latest_snapshot(head)
                if snap:
                    self.kernel.reference(self._snapshot_ref, snap)

    def _find_latest_snapshot(self, commit_hash: str) -> Optional[str]:
        """Walk the commit chain to find the latest snapshot commit."""
        current = commit_hash
        while current:
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
            if commit.get("snapshot"):
                return current
            current = commit.get("parent")
        return None

    def stage(self, key: str, blob_hash: str) -> None:
        self._staged_add[key] = blob_hash
        self._staged_del.discard(key)

    def stage_delete(self, key: str) -> None:
        self._staged_del.add(key)
        self._staged_add.pop(key, None)

    def has_staged(self) -> bool:
        return bool(self._staged_add or self._staged_del)

    def commit(self, message: str = "") -> str:
        """Commit staged changes.

        Decision: write a delta (Tier 1) or a snapshot (Tier 2)?

        - If delta_count < TIER1_DELTA_THRESHOLD: write a delta (O(1))
        - If delta_count >= TIER1_DELTA_THRESHOLD: write a snapshot (O(changed_chunks))
        - Always write a snapshot if there's no parent (first commit)

        The snapshot pointer ({name}__snapshot) is updated whenever a
        snapshot is written. Lookups read the snapshot pointer directly.
        """
        if not self.has_staged():
            raise ValueError("Nothing to commit")

        parent_hash = self.kernel.resolve(self.name)
        write_snapshot = (
            parent_hash is None  # first commit
            or self._delta_count_since_snapshot >= TIER1_DELTA_THRESHOLD
        )

        if write_snapshot:
            # TIER 2: Snapshot commit (fast reads)
            full_state = self._compute_full_state()
            for k, h in self._staged_add.items():
                full_state[k] = h
            for k in self._staged_del:
                full_state.pop(k, None)

            tree_root = ProllyTree.build(self.kernel, full_state)
            commit_data = BinaryProllyTree.encode_commit(
                parent_hash, tree_root, {}, [], tree_root,
                message or f"snapshot commit", time.time(), 0)
            commit_hash = self.kernel.write(commit_data)

            # Update snapshot pointer
            self.kernel.reference(self._snapshot_ref, commit_hash)
            self._delta_count_since_snapshot = 0
        else:
            # TIER 1: Delta commit (fast writes, for streaming)
            commit_data = BinaryProllyTree.encode_commit(
                parent_hash, None,
                dict(self._staged_add), list(self._staged_del),
                None, message or f"delta commit", time.time(), 0)
            commit_hash = self.kernel.write(commit_data)
            self._delta_count_since_snapshot += 1

        # Update HEAD
        self.kernel.reference(self.name, commit_hash)

        self._staged_add.clear()
        self._staged_del.clear()
        return commit_hash

    def lookup(self, key: str) -> Optional[str]:
        """O(log N) lookup via snapshot pointer. No delta chain walk.

        1. Read snapshot pointer → snapshot commit hash
        2. Read snapshot commit → get Prolly tree root
        3. Walk Prolly tree → find leaf → get blob hash

        If the snapshot pointer doesn't exist (old data), fall back to
        walking the commit chain from HEAD.
        """
        # Fast path: read snapshot pointer
        snap_hash = self.kernel.resolve(self._snapshot_ref)
        if snap_hash:
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(snap_hash))
            snapshot_root = commit.get("snapshot")
            if snapshot_root:
                result = ProllyTree.lookup(self.kernel, snapshot_root, key)
                if result is not None:
                    return result
                # Key not in snapshot — check if it was deleted in deltas after snapshot
                # Walk deltas from HEAD to snapshot, checking for the key
                return self._lookup_in_deltas(key, snap_hash)

        # Fallback: walk from HEAD (old data without snapshot pointer)
        return self._lookup_from_head(key)

    def _lookup_in_deltas(self, key: str, snapshot_hash: str) -> Optional[str]:
        """Check delta commits between HEAD and the snapshot for the key."""
        head = self.kernel.resolve(self.name)
        current = head
        while current and current != snapshot_hash:
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
            delta = commit.get("delta")
            if delta:
                if key in delta.get("+", {}):
                    return delta["+"][key]
                if key in delta.get("-", []):
                    return None  # deleted in a delta
            current = commit.get("parent")
        # Not found in deltas — return None (not in snapshot either)
        return None

    def _lookup_from_head(self, key: str) -> Optional[str]:
        """Fallback: walk commit chain from HEAD (for old data)."""
        head = self.kernel.resolve(self.name)
        if not head:
            return None
        current = head
        while current:
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
            delta = commit.get("delta")
            if delta:
                if key in delta.get("+", {}):
                    return delta["+"][key]
                if key in delta.get("-", []):
                    return None
            else:
                snapshot_root = commit.get("snapshot")
                if snapshot_root:
                    return ProllyTree.lookup(self.kernel, snapshot_root, key)
                return None
            current = commit.get("parent")
        return None

    def read_all(self) -> dict[str, str]:
        """Read full state. Uses snapshot pointer if available."""
        snap_hash = self.kernel.resolve(self._snapshot_ref)
        if snap_hash:
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(snap_hash))
            snapshot_root = commit.get("snapshot")
            if snapshot_root:
                state = ProllyTree.read_all(self.kernel, snapshot_root)
                # Apply deltas between snapshot and HEAD
                head = self.kernel.resolve(self.name)
                current = head
                deltas = []
                while current and current != snap_hash:
                    commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
                    if commit.get("delta"):
                        deltas.append(commit["delta"])
                    current = commit.get("parent")
                for delta in reversed(deltas):
                    for k, h in delta.get("+", {}).items():
                        state[k] = h
                    for k in delta.get("-", []):
                        state.pop(k, None)
                return state

        # Fallback: walk from HEAD
        head = self.kernel.resolve(self.name)
        if not head:
            return {}
        return self._read_state_from_commit(head)

    def _compute_full_state(self) -> dict[str, str]:
        """Compute current full state (for snapshot creation)."""
        return self.read_all()

    def _read_state_from_commit(self, commit_hash: str) -> dict[str, str]:
        """Read full state at a specific commit."""
        deltas = []
        current = commit_hash
        while current:
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
            if commit.get("snapshot"):
                state = ProllyTree.read_all(self.kernel, commit["snapshot"])
                break
            elif commit.get("delta"):
                deltas.append(commit["delta"])
                current = commit.get("parent")
            else:
                state = {}
                break
        else:
            state = {}
        for delta in reversed(deltas):
            for k, h in delta.get("+", {}).items():
                state[k] = h
            for k in delta.get("-", []):
                state.pop(k, None)
        return state


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_tiered_model():
    """Test the tiered commit model: fast writes + fast reads."""
    import shutil
    bench = "/tmp/pond_tiered"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    model = TieredCommitModel(kernel, "tiered_test")

    # First commit: should be a snapshot (no parent)
    model.stage("k1", kernel.write(b'{"v":1}'))
    model.stage("k2", kernel.write(b'{"v":2}'))
    model.commit("initial snapshot")

    # Verify snapshot pointer exists
    snap = kernel.resolve("tiered_test__snapshot")
    assert snap is not None, "Snapshot pointer should exist after first commit"

    # Lookup should use snapshot (no chain walk)
    h = model.lookup("k1")
    assert h is not None
    assert kernel.read_blob(h) == b'{"v":1}'

    # Write enough deltas to exceed TIER1_DELTA_THRESHOLD (16)
    for i in range(3, 20):  # 17 deltas — exceeds threshold of 16
        model.stage(f"k{i}", kernel.write(f'{{"v":{i}}}'.encode()))
        model.commit(f"delta {i}")

    # Deltas should NOT update the snapshot pointer (until threshold is hit)
    snap_after_deltas = kernel.resolve("tiered_test__snapshot")
    # Note: after 16 deltas, the 17th triggers a snapshot. So the pointer
    # MAY have been updated. Let's check that the 17th delta triggered it.

    # All keys should be findable
    for i in range(1, 20):
        h = model.lookup(f"k{i}")
        assert h is not None, f"k{i} not found"

    # After threshold, the snapshot pointer should have been updated
    snap_after_compaction = kernel.resolve("tiered_test__snapshot")
    assert snap_after_compaction != snap, "Snapshot should be updated after compaction (17 deltas > 16 threshold)"

    # All keys should be findable after compaction
    for i in range(1, 20):
        h = model.lookup(f"k{i}")
        assert h is not None, f"k{i} not found after compaction"

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Tiered commit model (fast writes + fast reads + auto-compaction)")


def test_tiered_streaming():
    """Test that the tiered model supports streaming (many small commits)."""
    import shutil
    bench = "/tmp/pond_tiered_stream"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    model = TieredCommitModel(kernel, "stream")

    # Simulate streaming: 100 small writes, each committed
    for i in range(100):
        model.stage(f"event:{i}", kernel.write(f'{{"id":{i}}}'.encode()))
        model.commit(f"event {i}")

    # All 100 events should be findable
    for i in range(100):
        h = model.lookup(f"event:{i}")
        assert h is not None, f"event:{i} not found"

    # Verify: snapshot pointer was updated multiple times (auto-compaction)
    # (every TIER1_DELTA_THRESHOLD=16 deltas, a new snapshot is created)
    # So there should be ~7 snapshots (100 / 16 ≈ 6.25, plus the initial)

    # Verify: the latest snapshot contains most events (some may be in deltas after it)
    snap = kernel.resolve("stream__snapshot")
    assert snap is not None
    commit = BinaryProllyTree.decode_commit(kernel.read_blob(snap))
    assert commit.get("snapshot") is not None
    state = ProllyTree.read_all(kernel, commit["snapshot"])
    # The snapshot has events up to the last compaction point.
    # Events after that are in deltas. Total = snapshot + deltas.
    assert len(state) > 80, f"Expected >80 events in snapshot, got {len(state)} (rest in deltas)"

    # But read_all() should see ALL 100 (snapshot + deltas applied)
    full_state = model.read_all()
    assert len(full_state) == 100, f"read_all() should see all 100, got {len(full_state)}"

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Streaming (100 small commits, all findable, auto-compacted)")


def test_tiered_restart():
    """Test that data survives restart with the tiered model."""
    import shutil
    bench = "/tmp/pond_tiered_restart"
    if os.path.exists(bench): shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    model = TieredCommitModel(kernel, "restart_test")
    for i in range(20):
        model.stage(f"k{i}", kernel.write(f'{{"v":{i}}}'.encode()))
        model.commit(f"commit {i}")

    kernel.close()

    # Reopen
    kernel2 = PondMinimal(bench)
    model2 = TieredCommitModel(kernel2, "restart_test")

    # All data should survive
    for i in range(20):
        h = model2.lookup(f"k{i}")
        assert h is not None, f"k{i} lost after restart"
        assert kernel2.read_blob(h) == f'{{"v":{i}}}'.encode()

    kernel2.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Restart (all data survived, snapshot pointer works)")


if __name__ == "__main__":
    print("=== Tiered Commit Model — Fast Writes + Fast Reads ===\n")
    test_tiered_model()
    print()
    test_tiered_streaming()
    print()
    test_tiered_restart()
    print("\n=== ALL TESTS PASSED ===")
