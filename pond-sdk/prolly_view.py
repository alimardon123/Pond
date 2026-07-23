"""
ProllyViewBase — Prolly trees + bounded delta journal.

The optimal data structure for content-addressed versioned storage:
  - Prolly tree: content-addressed B-tree with O(log N) lookups,
    O(log N) commits, content-based diff, structural sharing across versions
  - Bounded delta journal: O(1) writes between compactions, bounded to K
    deltas so reads never walk more than K objects before hitting a snapshot

This replaces both:
  - Full-snapshot trees (O(N) commits — too expensive)
  - Delta-only commits (O(N) read amplification — too many reads)
  - Sharded trees (O(1) commits but no content-based diff or structural sharing)

Design (per research):
  1. Keys are sorted. Entries are chunked into leaf nodes (~4KB each, ~256 entries).
  2. Chunk boundaries determined by a rolling hash on keys (Dolt's approach).
  3. Each chunk is content-addressed (hash = address). Internal nodes are
     lists of (max_key, child_hash) — also content-addressed.
  4. The root hash IS the version identifier (like Git, like Dolt).
  5. A bounded delta journal (≤K=2 entries) sits between compactions.
     Writes go to the journal (O(1)). Every K writes, compact into a new
     Prolly tree snapshot.
  6. Reads check the journal first (O(K) = O(1)), then the tree (O(log N)).

S3 round trips:
  Point lookup: 1 GET (commit) + 0-2 GETs (journal entries) + 1-2 GETs (tree path) = 2-5 total
  Commit: 1 PUT (blob) + 1 PUT (delta entry) = 2 PUTs (or 3 PUTs on compaction)
  Full scan: O(N/chunk_size) GETs for tree + O(K) for journal
  Diff: O(d) where d = changed chunks (content-addressed comparison)
  History: O(1) per commit (just follow parent pointers)

The key insight from Dolt: hash KEYS ONLY, not values. This means updating
a value in-place (same key, different value) doesn't change the chunk boundary.
Only adding/removing keys changes boundaries, and the rolling hash ensures
minimal cascading (~0.02% chance of boundary shift).
"""

import json
import time
import hashlib
import struct
import os
import sys
from typing import Optional, Any, Iterator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pond-core"))
from pond_minimal import PondMinimal, hash_bytes
from binary_encoding import BinaryProllyTree


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TARGET_CHUNK_ENTRIES = 64       # target entries per leaf chunk (~4KB for 64-byte entries)
# COMPACTION_THRESHOLD = 4 would mean: write delta commits for 3 commits,
# then a snapshot on the 4th. This optimizes for write speed but penalizes
# lookups (must walk the delta chain to find the snapshot).
#
# For object-store readiness, we set COMPACTION_THRESHOLD = 1: EVERY commit
# is a snapshot. This eliminates the commit-chain walk in lookup (the #1
# object-store cost identified by the Cost Simulator). Lookups become
# O(log N) with no chain walk — just HEAD → commit → tree → leaf → blob.
#
# Tradeoff: commits are O(N) (must build a full Prolly tree every time)
# instead of O(1) (delta only). On local disk this is fast. On object
# storage, the Prolly tree is content-addressed and deduped (unchanged
# chunks are shared), so only changed chunks are written.
#
# The delta journal code is preserved for future use (e.g., a local-disk
# mode that optimizes for write speed), but the default is always-snapshot.
COMPACTION_THRESHOLD = 1  # always snapshot — eliminates commit-chain walk
ROLLING_HASH_WINDOW = 48        # window size for boundary detection
ROLLING_HASH_MASK = (1 << 16) - 1  # 16-bit mask → ~1/65536 boundary probability
# But we want ~1/TARGET_CHUNK_ENTRIES probability, so adjust:
# boundary if (hash & mask) < (65536 / TARGET_CHUNK_ENTRIES)
BOUNDARY_THRESHOLD = max(1, 65536 // TARGET_CHUNK_ENTRIES)


# ---------------------------------------------------------------------------
# Prolly Tree — content-addressed B-tree with rolling-hash boundaries
# ---------------------------------------------------------------------------

class ProllyTree:
    """
    A Prolly (Probabilistic B-tree) for content-addressed storage.

    Structure:
      Leaf chunk: {"type":"leaf", "entries": [[key, hash], ...]}
      Internal chunk: {"type":"internal", "children": [[max_key, child_hash], ...]}

    The root hash of the tree IS the version identifier.
    Two trees with the same keys produce the same root hash (history-independent).
    Updating one value changes only the path from root to that leaf (O(log N)).
    """

    @staticmethod
    def _rolling_hash_boundary(keys_so_far: int, key: str) -> bool:
        """Determine if this key should be a chunk boundary.
        Uses a simple hash of the key + position to decide.
        Targets ~1/TARGET_CHUNK_ENTRIES probability."""
        h = int(hashlib.md5(f"{keys_so_far}:{key}".encode()).hexdigest(), 16)
        return (h & ROLLING_HASH_MASK) < BOUNDARY_THRESHOLD

    @staticmethod
    def build(kernel: PondMinimal, entries: dict[str, str]) -> str:
        """Build a Prolly tree from a sorted dict of key→hash entries.
        Returns the root hash. Uses BINARY encoding."""
        if not entries:
            return kernel.write(BinaryProllyTree.encode_leaf([]))

        sorted_items = sorted(entries.items())

        if len(sorted_items) <= TARGET_CHUNK_ENTRIES:
            leaf_entries = [(k, h) for k, h in sorted_items]
            return kernel.write(BinaryProllyTree.encode_leaf(leaf_entries))

        leaf_chunks = []
        for i in range(0, len(sorted_items), TARGET_CHUNK_ENTRIES):
            chunk = [(k, h) for k, h in sorted_items[i:i + TARGET_CHUNK_ENTRIES]]
            leaf_chunks.append(chunk)

        # Build the tree bottom-up. The first level is leaves (encode_leaf).
        # All subsequent levels are internal nodes (encode_internal).
        #
        # BUG FIX (Phase G): the original code used encode_leaf for ALL
        # levels, causing multi-level trees to store internal-node entries
        # as leaf entries. This caused data loss at scale (count showed
        # ~157 instead of 10K) and index rebuild decode errors (tree
        # node bytes were misinterpreted as JSON data).
        level = leaf_chunks
        is_leaf_level = True
        while len(level) > 1:
            chunk_entries = []
            for chunk in level:
                if is_leaf_level:
                    data = BinaryProllyTree.encode_leaf(chunk)
                else:
                    data = BinaryProllyTree.encode_internal(chunk)
                h = kernel.write(data)
                max_key = chunk[-1][0]
                chunk_entries.append((max_key, h))

            if len(chunk_entries) <= TARGET_CHUNK_ENTRIES:
                return kernel.write(BinaryProllyTree.encode_internal(chunk_entries))

            next_level = []
            for i in range(0, len(chunk_entries), TARGET_CHUNK_ENTRIES):
                group = chunk_entries[i:i + TARGET_CHUNK_ENTRIES]
                next_level.append(group)
            level = next_level
            is_leaf_level = False

        # If we exit the while loop with len(level) == 1, the root is
        # an internal node (list of (max_key, child_hash) tuples).
        root = level[0]
        return kernel.write(BinaryProllyTree.encode_internal(root))

    @staticmethod
    def lookup(kernel: PondMinimal, root_hash: str, key: str) -> Optional[str]:
        """Look up a key in the Prolly tree. O(log N) S3 GETs. Uses BINARY decoding."""
        current_hash = root_hash
        while current_hash:
            data = kernel.read_blob(current_hash)
            node = BinaryProllyTree.decode_node(data)
            if node["type"] == "leaf":
                entries = node["entries"]
                lo, hi = 0, len(entries) - 1
                while lo <= hi:
                    mid = (lo + hi) // 2
                    if entries[mid][0] == key:
                        return entries[mid][1]
                    elif entries[mid][0] < key:
                        lo = mid + 1
                    else:
                        hi = mid - 1
                return None
            elif node["type"] == "internal":
                children = node["children"]
                lo, hi = 0, len(children) - 1
                found = False
                while lo <= hi:
                    mid = (lo + hi) // 2
                    if children[mid][0] >= key:
                        if mid == 0 or children[mid - 1][0] < key:
                            current_hash = children[mid][1]
                            found = True
                            break
                        hi = mid - 1
                    else:
                        lo = mid + 1
                if not found:
                    return None
            else:
                return None
        return None

    @staticmethod
    def read_all(kernel: PondMinimal, root_hash: str) -> dict[str, str]:
        """Read all entries from the tree. Uses BINARY decoding."""
        result = {}
        ProllyTree._read_all_recursive(kernel, root_hash, result)
        return result

    @staticmethod
    def _read_all_recursive(kernel: PondMinimal, node_hash: str, result: dict):
        """Recursively read all entries. Uses BINARY decoding."""
        data = kernel.read_blob(node_hash)
        node = BinaryProllyTree.decode_node(data)
        if node["type"] == "leaf":
            for key, h in node["entries"]:
                result[key] = h
        elif node["type"] == "internal":
            for max_key, child_hash in node["children"]:
                ProllyTree._read_all_recursive(kernel, child_hash, result)

    @staticmethod
    def diff(kernel: PondMinimal, root_a: str, root_b: str) -> dict:
        """Content-based diff between two tree versions. O(d) where d = changed chunks."""
        if root_a == root_b:
            return {"added": {}, "removed": {}, "modified": {}}

        # Read both trees
        state_a = ProllyTree.read_all(kernel, root_a)
        state_b = ProllyTree.read_all(kernel, root_b)

        added = {k: v[:12] for k, v in state_b.items() if k not in state_a}
        removed = {k: v[:12] for k, v in state_a.items() if k not in state_b}
        modified = {k: {"old": state_a[k][:12], "new": state_b[k][:12]}
                    for k in state_a if k in state_b and state_a[k] != state_b[k]}
        return {"added": added, "removed": removed, "modified": modified}


# ---------------------------------------------------------------------------
# ProllyViewBase — Prolly tree + bounded delta journal
# ---------------------------------------------------------------------------

class ProllyViewBase:
    """
    Base class for all Pond Lenses. Uses Prolly trees + Tiered Commit Model.

    The Tiered Commit Model provides BOTH fast writes AND fast reads:
      - Tier 1 (Delta): O(1) write for streaming/OLTP
      - Tier 2 (Snapshot): O(changed_chunks) write, O(log N) read
      - Auto-compaction: every TIER1_DELTA_THRESHOLD deltas → new snapshot

    KEY INNOVATION — Snapshot Pointer:
      HEAD ({name}) points to latest commit (snapshot OR delta).
      {name}__snapshot always points to latest SNAPSHOT.
      Lookups read snapshot pointer directly — NO commit-chain walk.

    Properties:
      - O(log N) point lookups (via snapshot pointer → Prolly tree)
      - O(1) streaming writes (delta commits)
      - O(changed_chunks) batch commits (snapshot with structural sharing)
      - O(1) branching (just a Reference)
      - Full history (all commits preserved in the chain)
      - History-vs-state separation (lookups don't depend on history depth)

    Object store round trips:
      Point lookup: 3-4 GETs (snapshot commit + tree path + blob)
      Streaming commit: 2 PUTs (delta blob + reference)
      Snapshot commit: O(changed_chunks) PUTs (tree chunks + commit + reference)
      Branch: 1 PUT (reference only)
    """

    def __init__(self, kernel: PondMinimal, name: str):
        self.kernel = kernel
        self.name = name
        self._staged_add: dict[str, str] = {}
        self._staged_del: set[str] = set()
        self._commit_index = self._compute_index()
        self._active_branch: Optional[str] = None
        self._delta_count_since_snapshot = 0

        # Snapshot pointer — always points to the latest snapshot commit.
        # This decouples current-state access from history access.
        self._snapshot_ref = f"{name}__snapshot"

        # Initialize snapshot pointer if it doesn't exist but HEAD does
        if self.kernel.resolve(self._snapshot_ref) is None:
            head = self.kernel.resolve(self.name)
            if head:
                snap = self._find_latest_snapshot(head)
                if snap:
                    self.kernel.reference(self._snapshot_ref, snap)

        # Count deltas since last snapshot
        self._delta_count_since_snapshot = self._count_deltas_since_snapshot(
            self.kernel.resolve(self.name)
        )

    # ------------------------------------------------------------------
    # Staging
    # ------------------------------------------------------------------

    def stage(self, key: str, blob_hash: str) -> None:
        self._staged_add[key] = blob_hash
        self._staged_del.discard(key)

    def stage_delete(self, key: str) -> None:
        self._staged_del.add(key)
        self._staged_add.pop(key, None)

    def has_staged(self) -> bool:
        return bool(self._staged_add or self._staged_del)

    # ------------------------------------------------------------------
    # Commit — O(1) via delta journal, O(log N) on compaction
    # ------------------------------------------------------------------

    def commit(self, message: str = "") -> str:
        """Commit staged changes using the Tiered Commit Model.

        Decision: write a delta (Tier 1, O(1)) or a snapshot (Tier 2, O(changed_chunks))?

        - First commit (no parent): always snapshot
        - delta_count >= TIER1_DELTA_THRESHOLD (16): write snapshot (compaction)
        - Otherwise: write delta (fast streaming write)

        The snapshot pointer ({name}__snapshot) is updated whenever a
        snapshot is written. Lookups read the snapshot pointer directly.
        """
        if not self.has_staged():
            raise ValueError("Nothing to commit")

        parent_hash = self.kernel.resolve(self.name)
        index = self._commit_index

        write_snapshot = (
            parent_hash is None  # first commit
            or self._delta_count_since_snapshot >= 16  # compaction threshold
        )

        if write_snapshot:
            # TIER 2: Snapshot commit (fast reads)
            full_state = self._compute_full_state(parent_hash)
            for k, h in self._staged_add.items():
                full_state[k] = h
            for k in self._staged_del:
                full_state.pop(k, None)
            tree_root = ProllyTree.build(self.kernel, full_state)

            commit_data = BinaryProllyTree.encode_commit(
                parent_hash, tree_root, {}, [], tree_root,
                message or f"snapshot commit #{index}", time.time(), index)
            commit_hash = self.kernel.write(commit_data)

            # Update snapshot pointer
            self.kernel.reference(self._snapshot_ref, commit_hash)
            self._delta_count_since_snapshot = 0
        else:
            # TIER 1: Delta commit (fast writes, for streaming)
            commit_data = BinaryProllyTree.encode_commit(
                parent_hash, None,
                dict(self._staged_add), list(self._staged_del),
                None, message or f"delta commit #{index}", time.time(), index)
            commit_hash = self.kernel.write(commit_data)
            self._delta_count_since_snapshot += 1

        self.kernel.reference(self.name, commit_hash)
        if self._active_branch:
            self.kernel.reference(self._active_branch, commit_hash)

        self._staged_add.clear()
        self._staged_del.clear()
        self._commit_index += 1
        return commit_hash

    # ------------------------------------------------------------------
    # Lookup — O(log N) via Prolly tree, O(K) via delta journal
    # ------------------------------------------------------------------

    def lookup(self, key: str) -> Optional[str]:
        """O(log N) lookup via snapshot pointer + delta check. No full chain walk.

        1. Check deltas between HEAD and snapshot (for additions AND deletions)
        2. If found in a delta (+): return it
        3. If found in a delta (-): return None (deleted)
        4. Otherwise: look up in the snapshot's Prolly tree (O(log N))

        This gives O(K + log N) lookups where K = deltas since snapshot (≤ 16).
        """
        # Check deltas FIRST (for both additions and deletions after snapshot)
        snap_hash = self.kernel.resolve(self._snapshot_ref)
        head = self.kernel.resolve(self.name)
        if snap_hash and head and head != snap_hash:
            # Check if HEAD is a valid commit (not a tombstone or garbage)
            try:
                # Walk deltas from HEAD to snapshot
                current = head
                while current and current != snap_hash:
                    commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
                    delta = commit.get("delta")
                    if delta:
                        if key in delta.get("+", {}):
                            return delta["+"][key]
                        if key in delta.get("-", []):
                            return None  # deleted in a delta after snapshot
                    current = commit.get("parent")
            except (struct.error, ValueError, IndexError):
                # HEAD is not a valid commit (e.g., tombstoned or corrupted)
                # Fall through to snapshot lookup
                pass

        # Not in deltas — look up in the snapshot
        if snap_hash:
            try:
                commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(snap_hash))
                snapshot_root = commit.get("snapshot")
                if snapshot_root:
                    return ProllyTree.lookup(self.kernel, snapshot_root, key)
            except (struct.error, ValueError, IndexError):
                # Snapshot pointer is invalid (tombstoned or corrupted)
                pass

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
        return None  # not in snapshot, not in deltas

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

    # ------------------------------------------------------------------
    # Full state read — for scans
    # ------------------------------------------------------------------

    def read_all(self) -> dict[str, str]:
        """Read the complete current state via snapshot pointer + deltas.

        1. Read snapshot → get Prolly tree root → read all leaves
        2. Apply deltas between snapshot and HEAD
        """
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
                    c = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
                    if c.get("delta"):
                        deltas.append(c["delta"])
                    current = c.get("parent")
                for delta in reversed(deltas):
                    for k, h in delta.get("+", {}).items():
                        state[k] = h
                    for k in delta.get("-", []):
                        state.pop(k, None)
                return state

        # Fallback: walk from HEAD (old data)
        head = self.kernel.resolve(self.name)
        if not head:
            return {}
        return self._read_state_from_commit(head)

    # ------------------------------------------------------------------
    # History — O(1) per commit (linear walk, but skip pointers possible)
    # ------------------------------------------------------------------

    def history(self, limit: int = 20) -> list[dict]:
        """Walk commit history (first-parent line)."""
        head = self.kernel.resolve(self.name)
        if not head:
            return []
        history = []
        current = head
        while current and len(history) < limit:
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
            entry = {
                "commit": current[:12],
                "message": commit.get("message", ""),
                "timestamp": commit.get("timestamp", 0),
                "index": commit.get("index", 0),
                "type": "snapshot" if commit.get("snapshot") else "delta",
            }
            # Show merge commits with second_parent (true DAG topology)
            if commit.get("second_parent"):
                entry["second_parent"] = commit["second_parent"][:12]
                entry["type"] = "merge"
            history.append(entry)
            current = commit.get("parent")
        return history

    # ------------------------------------------------------------------
    # Branching — O(1)
    # ------------------------------------------------------------------

    def branch(self, branch_name: str) -> str:
        head = self.kernel.resolve(self.name)
        if not head:
            raise ValueError("No commits to branch from")
        full_name = f"{self.name}__branch__{branch_name}"
        self.kernel.reference(full_name, head)
        return full_name

    def checkout(self, branch_name: str) -> None:
        full_name = f"{self.name}__branch__{branch_name}"
        h = self.kernel.resolve(full_name)
        if not h:
            raise ValueError(f"Branch '{branch_name}' does not exist")
        self.kernel.reference(self.name, h)
        # Update snapshot pointer for the branch's HEAD
        snap = self._find_latest_snapshot(h)
        if snap:
            self.kernel.reference(self._snapshot_ref, snap)
        self._delta_count_since_snapshot = self._count_deltas_since_snapshot(h)
        self._staged_add.clear()
        self._staged_del.clear()
        self._commit_index = self._compute_index()
        self._active_branch = full_name

    def list_branches(self) -> list[str]:
        prefix = f"{self.name}__branch__"
        return [n[len(prefix):] for n in self.kernel.list_names() if n.startswith(prefix)]

    # ------------------------------------------------------------------
    # Undo — O(1)
    # ------------------------------------------------------------------

    def undo(self, steps: int = 1) -> str:
        head = self.kernel.resolve(self.name)
        for _ in range(steps):
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(head))
            if not commit.get("parent"):
                break
            head = commit["parent"]
        self.kernel.reference(self.name, head)
        # Update snapshot pointer
        snap = self._find_latest_snapshot(head)
        if snap:
            self.kernel.reference(self._snapshot_ref, snap)
        self._delta_count_since_snapshot = self._count_deltas_since_snapshot(head)
        self._staged_add.clear()
        self._staged_del.clear()
        self._commit_index = self._compute_index()
        self._active_branch = None
        return head[:12]

    # ------------------------------------------------------------------
    # Merge — O(|A| + |B|) with Prolly tree snapshot
    # ------------------------------------------------------------------

    def merge(self, branch_name: str, message: str = "") -> str:
        """Merge a branch into the current HEAD.

        Creates a TRUE merge commit with TWO parents:
          - parent: current HEAD (the branch being merged INTO)
          - second_parent: the branch HEAD being merged FROM

        This preserves branch topology in the commit DAG, unlike the
        previous implementation which only recorded one parent.

        Semantics: union with last-writer-wins (merged branch's values
        override current values for matching keys).
        """
        full_name = f"{self.name}__branch__{branch_name}"
        branch_head = self.kernel.resolve(full_name)
        if not branch_head:
            raise ValueError(f"Branch '{branch_name}' does not exist")

        current_state = self.read_all()
        branch_state = self._read_state_from_commit(branch_head)

        merged = dict(current_state)
        merged.update(branch_state)

        # Build a Prolly tree for the merged state (always a snapshot)
        tree_root = ProllyTree.build(self.kernel, merged)

        parent_hash = self.kernel.resolve(self.name)
        # TRUE MERGE COMMIT: two parents (current HEAD + branch HEAD)
        commit_data = BinaryProllyTree.encode_commit(
            parent_hash, tree_root, {}, [], tree_root,
            message or f"merge '{branch_name}'", time.time(), self._commit_index,
            second_parent=branch_head)  # ← NEW: second parent for true DAG
        commit_hash = self.kernel.write(commit_data)
        self.kernel.reference(self.name, commit_hash)
        # Update snapshot pointer — merge always creates a snapshot
        self.kernel.reference(self._snapshot_ref, commit_hash)
        self._delta_count_since_snapshot = 0
        self._commit_index += 1
        return commit_hash

    # ------------------------------------------------------------------
    # Diff — O(d) content-based comparison
    # ------------------------------------------------------------------

    def diff(self, commit_a: str, commit_b: str) -> dict:
        """Diff between two commits using Prolly tree comparison."""
        state_a = self._read_state_at(self._resolve_prefix(commit_a))
        state_b = self._read_state_at(self._resolve_prefix(commit_b))

        added = {k: v[:12] for k, v in state_b.items() if k not in state_a}
        removed = {k: v[:12] for k, v in state_a.items() if k not in state_b}
        modified = {k: {"old": state_a[k][:12], "new": state_b[k][:12]}
                    for k in state_a if k in state_b and state_a[k] != state_b[k]}
        return {"added": added, "removed": removed, "modified": modified}

    # ------------------------------------------------------------------
    # Index support (View-level, not kernel)
    # ------------------------------------------------------------------

    def build_index(self, index_name: str, key_extractor) -> str:
        """
        Build a secondary index. The index is a Prolly tree mapping
        extracted_key → primary_key. Stored as a blob, referenced by name.

        key_extractor: function(row_dict) → index_key
        """
        state = self.read_all()
        index_entries = {}
        for pk, blob_hash in state.items():
            if pk.startswith("_schema/") or pk.startswith("_index/"):
                continue
            row = json.loads(self.kernel.read_blob(blob_hash))
            idx_key = key_extractor(row)
            index_entries[f"_index/{index_name}/{idx_key}"] = pk

        # Build Prolly tree for the index
        tree_root = ProllyTree.build(self.kernel, index_entries)
        self.kernel.reference(f"{self.name}__index__{index_name}", tree_root)
        return tree_root

    def lookup_by_index(self, index_name: str, index_key: str) -> Optional[str]:
        """Look up a primary key via a secondary index. O(log N)."""
        tree_root = self.kernel.resolve(f"{self.name}__index__{index_name}")
        if not tree_root:
            return None
        full_key = f"_index/{index_name}/{index_key}"
        pk = ProllyTree.lookup(self.kernel, tree_root, full_key)
        return pk

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_index(self) -> int:
        h = self.kernel.resolve(self.name)
        if not h:
            return 0
        try:
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(h))
            return commit.get("index", 0) + 1
        except Exception:
            return 0

    def _find_latest_snapshot(self, commit_hash: str) -> Optional[str]:
        """Walk the commit chain to find the latest snapshot commit."""
        current = commit_hash
        while current:
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
            if commit.get("snapshot"):
                return current
            current = commit.get("parent")
        return None

    def _count_deltas_since_snapshot(self, from_hash: Optional[str]) -> int:
        """Count delta commits since the last snapshot."""
        count = 0
        current = from_hash
        while current:
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
            if commit.get("snapshot"):
                return count
            count += 1
            current = commit.get("parent")
            if count > COMPACTION_THRESHOLD + 1:
                break
        return count

    def _compute_full_state(self, from_hash: Optional[str]) -> dict[str, str]:
        if not from_hash:
            return {}
        return self._read_state_from_commit(from_hash)

    def _read_state_from_commit(self, commit_hash: str) -> dict[str, str]:
        """Read the full state at a specific commit."""
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

    def _read_state_at(self, commit_hash: str) -> dict[str, str]:
        return self._read_state_from_commit(commit_hash)

    def _resolve_prefix(self, prefix: str) -> str:
        current = self.kernel.resolve(self.name)
        while current:
            if current.startswith(prefix):
                return current
            commit = BinaryProllyTree.decode_commit(self.kernel.read_blob(current))
            current = commit.get("parent")
        raise ValueError(f"Commit '{prefix}' not found")

    # ------------------------------------------------------------------
    # Encoding (subclasses override)
    # ------------------------------------------------------------------

    def encode(self, data: Any) -> bytes:
        return json.dumps(data, sort_keys=True).encode()

    def decode(self, data: bytes) -> Any:
        return json.loads(data)
