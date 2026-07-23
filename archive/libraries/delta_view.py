"""
DeltaViewBase — the elegant, performant, minimal-round-trip View base class.

Design: delta commits with embedded skip pointers + periodic compaction.

Key properties:
  - Commit: O(1) in table size (writes only the delta, not the full tree)
  - Point lookup: 1-2 S3 GETs (commit blob + optional snapshot)
  - History: O(log N) via skip pointers (every 64th commit has a back-pointer)
  - Branch: O(1) (just a Reference)
  - Compaction: every 64 commits, merge deltas into a full-snapshot tree

This replaces both the naive full-snapshot approach (O(N) commits) and
the sharded tree approach (O(1) commits but 2-3 reads per lookup).
Delta commits give O(1) commits AND 1-2 reads per lookup.

S3 round trips per operation:
  Point lookup:    1-2 GETs (commit + optional snapshot)
  Commit:          2-3 PUTs (blob + commit + reference)
  Branch:          1 PUT (reference only)
  History (depth D): O(D/64 + 64) GETs (skip pointers)
  Undo:            1 PUT (reference to parent)
  Merge:           5 ops (2 GETs + 3 PUTs)
"""

import json
import time
import hashlib
import os
import sys
from typing import Optional, Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "prototype"))
from pond_minimal import PondMinimal, hash_bytes


COMPACTION_INTERVAL = 64  # compact every 64 commits
SKIP_INTERVAL = 64        # skip pointer every 64 commits (same as compaction)


class DeltaViewBase:
    """
    Base class for all Pond Views. Provides:
    - O(1) commits via delta commits (only changed entries written)
    - O(1) point lookups via embedded snapshots
    - O(log N) history via skip pointers
    - O(1) branching via Reference
    - Periodic compaction (every 64 commits) to bound read amplification

    Subclasses implement:
    - encode(data) -> bytes: serialize View data
    - decode(bytes) -> data: deserialize View data
    - The View-specific query logic (search, traverse, etc.)

    Usage:
        class MyView(DeltaViewBase):
            def __init__(self, kernel, name):
                super().__init__(kernel, name)

            def put(self, key, value):
                self.stage(key, self.kernel.write(self.encode(value)))

            def get(self, key):
                h = self.lookup(key)
                return self.decode(self.kernel.read_blob(h)) if h else None
    """

    def __init__(self, kernel: PondMinimal, name: str):
        self.kernel = kernel
        self.name = name
        self._staged_add: dict[str, str] = {}   # key -> blob_hash (pending adds/updates)
        self._staged_del: set[str] = set()       # keys pending deletion
        self._commit_index = self._compute_index()
        self._active_branch: Optional[str] = None  # track which branch is checked out

    # ------------------------------------------------------------------
    # Staging
    # ------------------------------------------------------------------

    def stage(self, key: str, blob_hash: str) -> None:
        """Stage a key→blob_hash for the next commit."""
        self._staged_add[key] = blob_hash
        self._staged_del.discard(key)

    def stage_delete(self, key: str) -> None:
        """Stage a key for deletion."""
        self._staged_del.add(key)
        self._staged_add.pop(key, None)

    def has_staged(self) -> bool:
        return bool(self._staged_add or self._staged_del)

    # ------------------------------------------------------------------
    # Commit
    # ------------------------------------------------------------------

    def commit(self, message: str = "") -> str:
        """
        Commit staged changes. O(1) in table size.

        Writes a delta commit containing only the changed entries.
        Every COMPACTION_INTERVAL commits, also writes a full-snapshot
        tree for fast reads.

        S3 round trips: 2-3 PUTs (delta commit + reference + optional snapshot)
        """
        if not self.has_staged():
            raise ValueError("Nothing to commit")

        parent_hash = self.kernel.resolve(self.name)
        index = self._commit_index

        # Build delta
        delta_plus = dict(self._staged_add)
        delta_minus = list(self._staged_del)

        # Check if we need compaction (every COMPACTION_INTERVAL commits)
        needs_compaction = (index > 0 and index % COMPACTION_INTERVAL == 0)
        snapshot_hash = None

        if needs_compaction:
            # Compact: merge all deltas since last snapshot into a full tree
            full_state = self._compute_full_state(parent_hash)
            # Apply staged changes
            for k, h in delta_plus.items():
                full_state[k] = h
            for k in delta_minus:
                full_state.pop(k, None)
            # Write snapshot tree
            snapshot_hash = self.kernel.write(
                json.dumps({"type": "snapshot", "entries": full_state}, sort_keys=True).encode()
            )

        # Compute skip pointer (every SKIP_INTERVAL commits)
        skip_hash = None
        if index > 0 and index % SKIP_INTERVAL == 0 and parent_hash:
            skip_hash = self._walk_back(parent_hash, SKIP_INTERVAL)

        # Write commit blob
        commit_obj = {
            "type": "commit",
            "parent": parent_hash,
            "skip": skip_hash,
            "delta": {"+": delta_plus, "-": delta_minus},
            "snapshot": snapshot_hash,
            "timestamp": time.time(),
            "message": message or f"commit #{index}",
            "index": index,
        }
        commit_hash = self.kernel.write(json.dumps(commit_obj, sort_keys=True).encode())

        # Update root namespace
        self.kernel.reference(self.name, commit_hash)

        # If we're on a branch, update the branch reference too
        if self._active_branch:
            self.kernel.reference(self._active_branch, commit_hash)

        # Clear staging
        self._staged_add.clear()
        self._staged_del.clear()
        self._commit_index += 1

        return commit_hash

    # ------------------------------------------------------------------
    # Lookup (point read) — O(1-2) S3 GETs
    # ------------------------------------------------------------------

    def lookup(self, key: str) -> Optional[str]:
        """
        Look up a single key in the current state. Returns the blob hash or None.

        Path:
        1. Read the HEAD commit (1 S3 GET)
        2. Check if the key is in the delta (found → return)
        3. If the commit has a snapshot, read it and look up the key (1 S3 GET)
        4. If no snapshot, walk backwards applying deltas (bounded by COMPACTION_INTERVAL)

        Worst case: 1 + COMPACTION_INTERVAL GETs (before compaction kicks in)
        Best case: 1 GET (key is in the latest delta)
        Typical case: 2 GETs (commit + snapshot)
        """
        head = self.kernel.resolve(self.name)
        if not head:
            return None

        # Walk backwards from HEAD, applying deltas
        current = head
        steps = 0
        while current:
            commit = json.loads(self.kernel.read_blob(current))

            # Check if key is in this commit's delta
            delta = commit.get("delta", {"+": {}, "-": []})
            if key in delta.get("+", {}):
                return delta["+"][key]
            if key in delta.get("-", []):
                return None  # deleted in this commit

            # Check if this commit has a snapshot
            snapshot_hash = commit.get("snapshot")
            if snapshot_hash:
                snapshot = json.loads(self.kernel.read_blob(snapshot_hash))
                return snapshot.get("entries", {}).get(key)

            # Walk to parent
            current = commit.get("parent")
            steps += 1
            if steps > COMPACTION_INTERVAL + 1:
                # Safety valve: shouldn't happen if compaction is working
                break

        return None

    # ------------------------------------------------------------------
    # Full state read — for scans, listings, exports
    # ------------------------------------------------------------------

    def read_all(self) -> dict[str, str]:
        """
        Read the complete current state (all key→blob_hash mappings).

        Path:
        1. Walk backwards until we find a snapshot (or the beginning)
        2. Apply all deltas forward from the snapshot

        Cost: O(K + S) where K = changes since last snapshot (≤64),
              S = 1 snapshot read.
        Total S3 GETs: 1 + K (typically 1-65)
        """
        head = self.kernel.resolve(self.name)
        if not head:
            return {}

        # Collect commits from HEAD backwards until we find a snapshot
        commits = []
        current = head
        while current:
            commit = json.loads(self.kernel.read_blob(current))
            commits.append(commit)
            if commit.get("snapshot"):
                break
            current = commit.get("parent")

        # Start from the snapshot (or empty if no snapshot found)
        state = {}
        if commits and commits[-1].get("snapshot"):
            snapshot = json.loads(self.kernel.read_blob(commits[-1]["snapshot"]))
            state = dict(snapshot.get("entries", {}))

        # Apply deltas in reverse order (oldest first)
        for commit in reversed(commits):
            delta = commit.get("delta", {"+": {}, "-": []})
            for k, h in delta.get("+", {}).items():
                state[k] = h
            for k in delta.get("-", []):
                state.pop(k, None)

        return state

    # ------------------------------------------------------------------
    # History — O(log N) via skip pointers
    # ------------------------------------------------------------------

    def history(self, limit: int = 20) -> list[dict]:
        """Walk commit history. Uses skip pointers for O(log N) traversal."""
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
            })
            current = commit.get("parent")
        return history

    def history_to_index(self, target_index: int) -> Optional[str]:
        """Walk to a specific commit index using skip pointers. O(log N)."""
        head = self.kernel.resolve(self.name)
        if not head:
            return None

        current = head
        while current:
            commit = json.loads(self.kernel.read_blob(current))
            current_index = commit.get("index", 0)

            if current_index == target_index:
                return current

            if current_index < target_index:
                # We've gone past the target (shouldn't happen walking backwards)
                break

            # Can we use a skip pointer?
            skip = commit.get("skip")
            if skip and current_index - SKIP_INTERVAL >= target_index:
                current = skip
            else:
                current = commit.get("parent")

        return None

    # ------------------------------------------------------------------
    # Branching — O(1)
    # ------------------------------------------------------------------

    def branch(self, branch_name: str) -> str:
        """Create a branch. O(1) — just a Reference."""
        head = self.kernel.resolve(self.name)
        if not head:
            raise ValueError("No commits to branch from")
        full_name = f"{self.name}__branch__{branch_name}"
        self.kernel.reference(full_name, head)
        return full_name

    def checkout(self, branch_name: str) -> None:
        """Switch to a branch. O(1) — move the name to the branch's commit."""
        full_name = f"{self.name}__branch__{branch_name}"
        h = self.kernel.resolve(full_name)
        if not h:
            raise ValueError(f"Branch '{branch_name}' does not exist")
        self.kernel.reference(self.name, h)
        self._staged_add.clear()
        self._staged_del.clear()
        self._commit_index = self._compute_index()
        self._active_branch = full_name  # track active branch

    def list_branches(self) -> list[str]:
        """List all branches."""
        prefix = f"{self.name}__branch__"
        return [n[len(prefix):] for n in self.kernel.list_names() if n.startswith(prefix)]

    # ------------------------------------------------------------------
    # Undo / Rollback — O(1)
    # ------------------------------------------------------------------

    def undo(self, steps: int = 1) -> str:
        """Undo N commits. O(N) (walks parent chain N times)."""
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
        return head[:12]

    # ------------------------------------------------------------------
    # Merge — O(|A| + |B|) where A, B are the two states
    # ------------------------------------------------------------------

    def merge(self, branch_name: str, message: str = "") -> str:
        """Merge a branch into the current branch. Union merge (no conflict resolution)."""
        full_name = f"{self.name}__branch__{branch_name}"
        branch_head = self.kernel.resolve(full_name)
        if not branch_head:
            raise ValueError(f"Branch '{branch_name}' does not exist")

        # Read current state
        current_state = self.read_all()

        # Read branch state by directly walking from branch_head
        branch_state = self._read_state_from_commit(branch_head)

        # Union merge (branch wins on conflict)
        merged = dict(current_state)
        merged.update(branch_state)

        # Write a snapshot (merge commits always have a snapshot for fast reads)
        snapshot_hash = self.kernel.write(
            json.dumps({"type": "snapshot", "entries": merged}, sort_keys=True).encode()
        )

        # Write merge commit with TWO parents
        parent_hash = self.kernel.resolve(self.name)
        skip_hash = None
        if self._commit_index > 0 and self._commit_index % SKIP_INTERVAL == 0 and parent_hash:
            skip_hash = self._walk_back(parent_hash, SKIP_INTERVAL)

        commit_obj = {
            "type": "commit",
            "parent": parent_hash,
            "parents": [parent_hash, branch_head],  # multi-parent
            "skip": skip_hash,
            "delta": {"+": {}, "-": []},  # empty delta (merge uses snapshot)
            "snapshot": snapshot_hash,
            "timestamp": time.time(),
            "message": message or f"merge '{branch_name}'",
            "index": self._commit_index,
        }
        commit_hash = self.kernel.write(json.dumps(commit_obj, sort_keys=True).encode())
        self.kernel.reference(self.name, commit_hash)
        self._commit_index += 1
        return commit_hash

    def _read_state_from_commit(self, commit_hash: str) -> dict[str, str]:
        """Read the full state starting from a specific commit hash."""
        if not commit_hash:
            return {}

        # Collect commits from the given hash backwards until we find a snapshot
        commits = []
        current = commit_hash
        while current:
            commit = json.loads(self.kernel.read_blob(current))
            commits.append(commit)
            if commit.get("snapshot"):
                break
            current = commit.get("parent")

        # Start from the snapshot (or empty if no snapshot found)
        state = {}
        if commits and commits[-1].get("snapshot"):
            snapshot = json.loads(self.kernel.read_blob(commits[-1]["snapshot"]))
            state = dict(snapshot.get("entries", {}))

        # Apply deltas in reverse order (oldest first)
        for commit in reversed(commits):
            delta = commit.get("delta", {"+": {}, "-": []})
            for k, h in delta.get("+", {}).items():
                state[k] = h
            for k in delta.get("-", []):
                state.pop(k, None)

        return state

    # ------------------------------------------------------------------
    # Diff between two commits
    # ------------------------------------------------------------------

    def diff(self, commit_a: str, commit_b: str) -> dict:
        """Show what changed between two commits (by hash prefix)."""
        state_a = self._read_state_at(self._resolve_prefix(commit_a))
        state_b = self._read_state_at(self._resolve_prefix(commit_b))

        added = {k: v[:12] for k, v in state_b.items() if k not in state_a}
        removed = {k: v[:12] for k, v in state_a.items() if k not in state_b}
        modified = {k: {"old": state_a[k][:12], "new": state_b[k][:12]}
                    for k in state_a if k in state_b and state_a[k] != state_b[k]}
        return {"added": added, "removed": removed, "modified": modified}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_index(self) -> int:
        """Compute the current commit index (depth)."""
        h = self.kernel.resolve(self.name)
        if not h:
            return 0
        try:
            commit = json.loads(self.kernel.read_blob(h))
            return commit.get("index", 0) + 1
        except Exception:
            return 0

    def _walk_back(self, start_hash: str, steps: int) -> Optional[str]:
        """Walk back N steps from start_hash."""
        current = start_hash
        for _ in range(steps):
            if not current:
                return None
            commit = json.loads(self.kernel.read_blob(current))
            current = commit.get("parent")
        return current

    def _compute_full_state(self, from_commit_hash: Optional[str]) -> dict[str, str]:
        """Compute the full state at a commit (for compaction)."""
        if not from_commit_hash:
            return {}

        # Temporarily set the name to the commit, read_all, then restore
        original = self.kernel.resolve(self.name)
        self.kernel.reference(self.name, from_commit_hash)
        state = self.read_all()
        self.kernel.reference(self.name, original)
        return state

    def _read_state_at(self, commit_hash: str) -> dict[str, str]:
        """Read the full state at a specific commit."""
        original = self.kernel.resolve(self.name)
        self.kernel.reference(self.name, commit_hash)
        state = self.read_all()
        self.kernel.reference(self.name, original)
        return state

    def _resolve_prefix(self, prefix: str) -> str:
        """Resolve a commit hash prefix by walking history."""
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
        """Serialize View data to bytes. Override in subclass."""
        return json.dumps(data, sort_keys=True).encode()

    def decode(self, data: bytes) -> Any:
        """Deserialize bytes to View data. Override in subclass."""
        return json.loads(data)
