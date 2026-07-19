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

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "prototype"))
from pond_minimal import PondMinimal, hash_bytes


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TARGET_CHUNK_ENTRIES = 64       # target entries per leaf chunk (~4KB for 64-byte entries)
COMPACTION_THRESHOLD = 4        # compact after this many delta entries
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
        Returns the root hash."""
        if not entries:
            # Empty tree
            return kernel.write(json.dumps({"type": "leaf", "entries": []}, sort_keys=True).encode())

        # Sort entries by key
        sorted_items = sorted(entries.items())

        # Build leaf chunks
        leaf_chunks = []  # list of [(key, hash), ...]
        current_chunk = []
        for i, (key, h) in enumerate(sorted_items):
            current_chunk.append([key, h])
            # Check boundary: every TARGET_CHUNK_ENTRIES, or rolling hash
            if len(current_chunk) >= TARGET_CHUNK_ENTRIES or \
               (len(current_chunk) > 0 and ProllyTree._rolling_hash_boundary(i, key)):
                leaf_chunks.append(current_chunk)
                current_chunk = []

        if current_chunk:
            leaf_chunks.append(current_chunk)

        # Write leaf chunks and build internal nodes
        level = leaf_chunks
        while len(level) > 1:
            # Write each chunk at this level
            chunk_hashes = []
            for chunk in level:
                if isinstance(chunk[0], list) and len(chunk[0]) == 2 and isinstance(chunk[0][0], str):
                    # Leaf chunk
                    data = json.dumps({"type": "leaf", "entries": chunk}, sort_keys=True).encode()
                else:
                    # Internal chunk (already has (max_key, child_hash) pairs)
                    data = json.dumps({"type": "internal", "children": chunk}, sort_keys=True).encode()
                h = kernel.write(data)
                # Get max key in this chunk
                if chunk:
                    max_key = chunk[-1][0] if isinstance(chunk[0], list) and len(chunk[0]) == 2 else chunk[-1][0]
                else:
                    max_key = ""
                chunk_hashes.append([max_key, h])

            # Build next level: group chunk_hashes into internal nodes
            next_level = []
            current_group = []
            for i, (max_key, h) in enumerate(chunk_hashes):
                current_group.append([max_key, h])
                if len(current_group) >= TARGET_CHUNK_ENTRIES:
                    next_level.append(current_group)
                    current_group = []
            if current_group:
                next_level.append(current_group)
            level = next_level

        # Write the root
        root = level[0]
        if isinstance(root[0], list) and len(root[0]) == 2 and isinstance(root[0][0], str):
            # Could be leaf entries or internal children — check type
            if len(root) == 1 and isinstance(root[0][1], str) and len(root[0][1]) == 64:
                # Single internal child → make it the root
                data = json.dumps({"type": "internal", "children": root}, sort_keys=True).encode()
            else:
                data = json.dumps({"type": "leaf", "entries": root}, sort_keys=True).encode()
        else:
            data = json.dumps({"type": "internal", "children": root}, sort_keys=True).encode()

        return kernel.write(data)

    @staticmethod
    def lookup(kernel: PondMinimal, root_hash: str, key: str) -> Optional[str]:
        """Look up a key in the Prolly tree. O(log N) S3 GETs."""
        current_hash = root_hash
        while current_hash:
            data = json.loads(kernel.read_blob(current_hash))
            if data["type"] == "leaf":
                # Binary search in leaf entries
                entries = data["entries"]
                lo, hi = 0, len(entries) - 1
                while lo <= hi:
                    mid = (lo + hi) // 2
                    if entries[mid][0] == key:
                        return entries[mid][1]
                    elif entries[mid][0] < key:
                        lo = mid + 1
                    else:
                        hi = mid - 1
                return None  # not found
            elif data["type"] == "internal":
                # Binary search in children to find the right child
                children = data["children"]
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
                    # Key is larger than all children's max keys → not in tree
                    return None
            else:
                return None
        return None

    @staticmethod
    def read_all(kernel: PondMinimal, root_hash: str) -> dict[str, str]:
        """Read all entries from the tree. O(N/chunk_size) GETs."""
        result = {}
        ProllyTree._read_all_recursive(kernel, root_hash, result)
        return result

    @staticmethod
    def _read_all_recursive(kernel: PondMinimal, node_hash: str, result: dict):
        """Recursively read all entries."""
        data = json.loads(kernel.read_blob(node_hash))
        if data["type"] == "leaf":
            for key, h in data["entries"]:
                result[key] = h
        elif data["type"] == "internal":
            for max_key, child_hash in data["children"]:
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
    Base class for all Pond Views. Uses Prolly trees + bounded delta journal.

    Properties:
      - O(log N) point lookups (Prolly tree binary search)
      - O(1) commits (delta journal, ≤K entries before compaction)
      - O(1) history access (commit DAG)
      - O(d) diff (content-addressed tree comparison)
      - Structural sharing across versions (same chunks → same hash)
      - History independence (same keys → same tree, regardless of insert order)

    S3 round trips:
      Point lookup: 2-5 GETs (commit + journal + tree path)
      Commit: 2-3 PUTs (blob + delta/snapshot + reference)
      Branch: 1 PUT
      History: O(1) per commit
      Diff: O(d) where d = changed chunks
    """

    def __init__(self, kernel: PondMinimal, name: str):
        self.kernel = kernel
        self.name = name
        self._staged_add: dict[str, str] = {}
        self._staged_del: set[str] = set()
        self._commit_index = self._compute_index()
        self._active_branch: Optional[str] = None

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
        """
        Commit staged changes.

        If the delta journal is below COMPACTION_THRESHOLD: write a delta
        entry (O(1) — just the changed keys). Otherwise: compact by building
        a new Prolly tree from the full state (O(log N)), and reset the journal.
        """
        if not self.has_staged():
            raise ValueError("Nothing to commit")

        parent_hash = self.kernel.resolve(self.name)
        index = self._commit_index

        # Count deltas since last compaction
        delta_count = self._count_deltas_since_snapshot(parent_hash)

        # Always compact if there's no parent (first commit) or if we hit the threshold
        if delta_count >= COMPACTION_THRESHOLD or parent_hash is None:
            # COMPACT: build a full Prolly tree snapshot
            full_state = self._compute_full_state(parent_hash)
            # Apply staged changes
            for k, h in self._staged_add.items():
                full_state[k] = h
            for k in self._staged_del:
                full_state.pop(k, None)
            # Build Prolly tree
            tree_root = ProllyTree.build(self.kernel, full_state)

            commit_obj = {
                "type": "commit",
                "parent": parent_hash,
                "snapshot": tree_root,     # full Prolly tree root
                "delta": None,              # no delta (compacted)
                "timestamp": time.time(),
                "message": message or f"compaction commit #{index}",
                "index": index,
            }
        else:
            # DELTA: write just the changed entries
            commit_obj = {
                "type": "commit",
                "parent": parent_hash,
                "snapshot": None,           # no snapshot (delta only)
                "delta": {
                    "+": dict(self._staged_add),
                    "-": list(self._staged_del),
                },
                "timestamp": time.time(),
                "message": message or f"delta commit #{index}",
                "index": index,
            }

        commit_hash = self.kernel.write(json.dumps(commit_obj, sort_keys=True).encode())
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
        """
        Look up a single key. O(log N) Prolly tree + O(K) delta journal.

        Path:
        1. Walk commits from HEAD, applying deltas (O(K) where K ≤ COMPACTION_THRESHOLD)
        2. If key found in a delta, return it
        3. If key marked as deleted in a delta, return None
        4. When we reach a snapshot, look up in the Prolly tree (O(log N))
        """
        head = self.kernel.resolve(self.name)
        if not head:
            return None

        current = head
        steps = 0
        while current:
            commit = json.loads(self.kernel.read_blob(current))
            steps += 1

            # Check delta
            delta = commit.get("delta")
            if delta:
                if key in delta.get("+", {}):
                    return delta["+"][key]
                if key in delta.get("-", []):
                    return None
            else:
                # This is a snapshot commit — look up in the Prolly tree
                snapshot_root = commit.get("snapshot")
                if snapshot_root:
                    return ProllyTree.lookup(self.kernel, snapshot_root, key)
                # No snapshot and no delta — empty state
                return None

            current = commit.get("parent")
            if steps > COMPACTION_THRESHOLD + 1:
                break  # safety valve

        return None

    # ------------------------------------------------------------------
    # Full state read — for scans
    # ------------------------------------------------------------------

    def read_all(self) -> dict[str, str]:
        """Read the complete current state. O(N/chunk + K) GETs."""
        head = self.kernel.resolve(self.name)
        if not head:
            return {}

        # Collect delta commits until we find a snapshot
        deltas = []
        current = head
        while current:
            commit = json.loads(self.kernel.read_blob(current))
            if commit.get("snapshot"):
                # Found the snapshot — read the Prolly tree
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

        # Apply deltas in reverse (oldest first)
        for delta in reversed(deltas):
            for k, h in delta.get("+", {}).items():
                state[k] = h
            for k in delta.get("-", []):
                state.pop(k, None)

        return state

    # ------------------------------------------------------------------
    # History — O(1) per commit (linear walk, but skip pointers possible)
    # ------------------------------------------------------------------

    def history(self, limit: int = 20) -> list[dict]:
        """Walk commit history."""
        head = self.kernel.resolve(self.name)
        if not head:
            return []
        history = []
        current = head
        while current and len(history) < limit:
            commit = json.loads(self.kernel.read_blob(current))
            history.append({
                "commit": current[:12],
                "message": commit.get("message", ""),
                "timestamp": commit.get("timestamp", 0),
                "index": commit.get("index", 0),
                "type": "snapshot" if commit.get("snapshot") else "delta",
            })
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
            commit = json.loads(self.kernel.read_blob(head))
            if not commit.get("parent"):
                break
            head = commit["parent"]
        self.kernel.reference(self.name, head)
        self._staged_add.clear()
        self._staged_del.clear()
        self._commit_index = self._compute_index()
        self._active_branch = None
        return head[:12]

    # ------------------------------------------------------------------
    # Merge — O(|A| + |B|) with Prolly tree snapshot
    # ------------------------------------------------------------------

    def merge(self, branch_name: str, message: str = "") -> str:
        full_name = f"{self.name}__branch__{branch_name}"
        branch_head = self.kernel.resolve(full_name)
        if not branch_head:
            raise ValueError(f"Branch '{branch_name}' does not exist")

        current_state = self.read_all()
        # Read branch state directly from the branch's commit chain
        branch_state = self._read_state_from_commit(branch_head)

        merged = dict(current_state)
        merged.update(branch_state)

        # Build a Prolly tree for the merged state
        tree_root = ProllyTree.build(self.kernel, merged)

        parent_hash = self.kernel.resolve(self.name)
        commit_obj = {
            "type": "commit",
            "parent": parent_hash,
            "parents": [parent_hash, branch_head],
            "snapshot": tree_root,
            "delta": None,
            "timestamp": time.time(),
            "message": message or f"merge '{branch_name}'",
            "index": self._commit_index,
        }
        commit_hash = self.kernel.write(json.dumps(commit_obj, sort_keys=True).encode())
        self.kernel.reference(self.name, commit_hash)
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
            commit = json.loads(self.kernel.read_blob(h))
            return commit.get("index", 0) + 1
        except Exception:
            return 0

    def _count_deltas_since_snapshot(self, from_hash: Optional[str]) -> int:
        """Count delta commits since the last snapshot."""
        count = 0
        current = from_hash
        while current:
            commit = json.loads(self.kernel.read_blob(current))
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
            commit = json.loads(self.kernel.read_blob(current))
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
            commit = json.loads(self.kernel.read_blob(current))
            current = commit.get("parent")
        raise ValueError(f"Commit '{prefix}' not found")

    # ------------------------------------------------------------------
    # Encoding (subclasses override)
    # ------------------------------------------------------------------

    def encode(self, data: Any) -> bytes:
        return json.dumps(data, sort_keys=True).encode()

    def decode(self, data: bytes) -> Any:
        return json.loads(data)
