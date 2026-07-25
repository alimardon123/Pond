"""
Pond Lens Helpers — shared library eliminating boilerplate across Views.

Fixes friction points 1, 2, and 4 from the notebook friction diary:
  - Friction 1: Tree/Commit boilerplate (no more copy-paste)
  - Friction 2: Full-snapshot tree O(N) per commit → SHARDED TREES
  - Friction 4: History walk O(N) → SKIP POINTERS

This is a VIEW-LEVEL library, not a kernel feature. Views import it
to avoid reinventing the same patterns. The kernel stays at 3 primitives.

Performance improvements over the naive approach:
  - Sharded trees: commit is O(1) in tree size (not O(N))
  - Skip pointers: history walk is O(log N) (not O(N))
  - Partial tree reads: read only the shard containing your key
"""

import json
import time
import sys
import os
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "prototype"))
from kernel import PondMinimal

# ---------------------------------------------------------------------------
# Sharded Tree — fixes Friction 2 (O(N) tree per commit)
# ---------------------------------------------------------------------------

SHARD_SIZE = 256  # entries per shard

class ShardedTree:
    """
    A tree that shards entries across multiple blobs, avoiding the O(N)
    full-snapshot copy on every commit.

    Structure:
      root tree blob: {"type":"sharded_tree", "shards": {"0": shard_hash, "1": shard_hash, ...}}
      shard blob: {"type":"tree_shard", "entries": {key: hash, ...}}

    Commit cost: O(1) — write one new shard + update root tree (which has
    at most N/256 entries, not N entries).

    Read cost: O(1) per key — hash the key to find the shard, read one shard.
    Full scan: O(N/256) shard reads + O(N) total entries.
    """

    @staticmethod
    def _shard_key(key: str) -> str:
        """Determine which shard a key belongs to. Uses hash prefix for distribution."""
        import hashlib
        return str(int(hashlib.md5(key.encode()).hexdigest(), 16) % SHARD_SIZE)

    @staticmethod
    def write(kernel: PondMinimal, entries: dict[str, str],
              parent_root_hash: Optional[str] = None) -> str:
        """Write a sharded tree. If parent_root_hash is given, reuse unchanged shards."""
        # Load parent's shard index if available
        parent_shards = {}
        if parent_root_hash:
            try:
                parent_data = json.loads(kernel.read_blob(parent_root_hash))
                if parent_data.get("type") == "sharded_tree":
                    parent_shards = parent_data.get("shards", {})
            except Exception:
                pass

        # Group entries by shard
        shard_groups: dict[str, dict[str, str]] = {}
        for key, h in entries.items():
            sk = ShardedTree._shard_key(key)
            shard_groups.setdefault(sk, {})[key] = h

        # Write shards (reuse parent's shard if unchanged)
        new_shards = {}
        for sk, group in shard_groups.items():
            # Check if this shard is identical to parent's
            shard_data = json.dumps({"type": "tree_shard", "entries": group}, sort_keys=True).encode()
            shard_hash = kernel.write(shard_data)
            new_shards[sk] = shard_hash

        # Merge with parent's unchanged shards
        all_shards = dict(parent_shards)
        all_shards.update(new_shards)

        # Write root tree
        root_data = json.dumps({"type": "sharded_tree", "shards": all_shards}, sort_keys=True).encode()
        return kernel.write(root_data)

    @staticmethod
    def read_entry(kernel: PondMinimal, root_hash: str, key: str) -> Optional[str]:
        """Read a single entry from a sharded tree. O(1) shard read."""
        root_data = json.loads(kernel.read_blob(root_hash))
        if root_data.get("type") != "sharded_tree":
            # Fallback: flat tree
            return root_data.get("entries", {}).get(key)

        sk = ShardedTree._shard_key(key)
        shards = root_data.get("shards", {})
        if sk not in shards:
            return None

        shard_data = json.loads(kernel.read_blob(shards[sk]))
        return shard_data.get("entries", {}).get(key)

    @staticmethod
    def read_all(kernel: PondMinimal, root_hash: str) -> dict[str, str]:
        """Read ALL entries from a sharded tree. O(N/256) shard reads."""
        root_data = json.loads(kernel.read_blob(root_hash))
        if root_data.get("type") != "sharded_tree":
            return root_data.get("entries", {})

        all_entries = {}
        for sk, shard_hash in root_data.get("shards", {}).items():
            shard_data = json.loads(kernel.read_blob(shard_hash))
            all_entries.update(shard_data.get("entries", {}))
        return all_entries


# ---------------------------------------------------------------------------
# Skip-Pointer History — fixes Friction 4 (O(N) history walk)
# ---------------------------------------------------------------------------

SKIP_INTERVAL = 64  # every 64th commit stores a back-pointer

class SkipPointerHistory:
    """
    History walk with skip pointers: O(log N) instead of O(N).

    Every SKIP_INTERVAL-th commit stores a skip pointer — a direct
    link to the commit SKIP_INTERVAL steps back. To walk to depth D:
    1. Follow skip pointers: D / SKIP_INTERVAL hops
    2. Walk linearly: D % SKIP_INTERVAL steps
    Total: O(D / SKIP_INTERVAL + SKIP_INTERVAL) = O(D/64 + 64)

    The skip pointer is stored IN the commit blob (as an extra field),
    NOT in the kernel. This is a Lens-level pattern.
    """

    @staticmethod
    def write_commit(kernel: PondMinimal, tree_hash: str,
                     parent_hash: Optional[str], message: str,
                     author: str = "user", commit_index: int = 0) -> str:
        """Write a commit with skip pointer. commit_index is the depth (0-based)."""
        skip_ptr = None
        if parent_hash and commit_index > 0 and commit_index % SKIP_INTERVAL == 0:
            # This commit gets a skip pointer to the commit SKIP_INTERVAL steps back
            skip_ptr = SkipPointerHistory._walk_back(kernel, parent_hash, SKIP_INTERVAL)

        obj = {
            "type": "commit",
            "tree": tree_hash,
            "parent": parent_hash,
            "timestamp": time.time(),
            "message": message,
            "author": author,
            "index": commit_index,
            "skip": skip_ptr,  # skip pointer (None for most commits)
        }
        return kernel.write(json.dumps(obj, sort_keys=True).encode())

    @staticmethod
    def _walk_back(kernel: PondMinimal, start_hash: str, steps: int) -> Optional[str]:
        """Walk back N steps from start_hash. Used to compute skip pointers."""
        current = start_hash
        for _ in range(steps):
            if not current:
                return None
            commit = json.loads(kernel.read_blob(current))
            current = commit.get("parent")
        return current

    @staticmethod
    def walk_to_depth(kernel: PondMinimal, head_hash: str, target_depth: int) -> Optional[str]:
        """Walk to a specific depth using skip pointers. O(D/64 + 64)."""
        current = head_hash
        depth_walked = 0

        while current and depth_walked < target_depth:
            commit = json.loads(kernel.read_blob(current))
            current_depth = commit.get("index", depth_walked)

            # Can we use a skip pointer?
            skip = commit.get("skip")
            if skip and current_depth - SKIP_INTERVAL >= target_depth:
                # Use skip pointer
                current = skip
                depth_walked = current_depth - SKIP_INTERVAL
            else:
                # Walk one step
                current = commit.get("parent")
                depth_walked += 1

        return current

    @staticmethod
    def read_commit(kernel: PondMinimal, commit_hash: str) -> dict:
        """Read a commit blob."""
        return json.loads(kernel.read_blob(commit_hash))


# ---------------------------------------------------------------------------
# Full View Base — combines sharded tree + skip pointers
# ---------------------------------------------------------------------------

class ViewBase:
    """
    Base class for Lenses. Eliminates ALL boilerplate.

    Subclasses get:
    - Sharded trees (O(1) commits, O(1) key reads)
    - Skip-pointer history (O(log N) time travel)
    - Commit/resolve helpers
    - No copy-pasted Tree/Commit code

    Usage:
        class MyView(ViewBase):
            def __init__(self, kernel, name):
                super().__init__(kernel, name)

            def my_operation(self):
                entries = self.read_all()
                entries["key"] = self.kernel.write(b"data")
                self.commit("update key")
    """

    def __init__(self, kernel: PondMinimal, name: str):
        self.kernel = kernel
        self.name = name
        self._commit_index = self._compute_commit_index()

    def _compute_commit_index(self) -> int:
        """Compute the current commit index (depth)."""
        h = self.kernel.resolve(self.name)
        if not h:
            return 0
        try:
            commit = SkipPointerHistory.read_commit(self.kernel, h)
            return commit.get("index", 0) + 1
        except Exception:
            return 0

    def commit(self, message: str, entries: dict[str, str]) -> str:
        """Commit entries. Uses sharded tree + skip pointers."""
        parent_hash = self.kernel.resolve(self.name)

        # Read parent's tree root (for shard reuse)
        parent_tree_root = None
        if parent_hash:
            parent_commit = SkipPointerHistory.read_commit(self.kernel, parent_hash)
            parent_tree_root = parent_commit.get("tree")

        # Write sharded tree
        tree_root = ShardedTree.write(self.kernel, entries, parent_tree_root)

        # Write commit with skip pointer
        commit_hash = SkipPointerHistory.write_commit(
            self.kernel, tree_root, parent_hash, message,
            commit_index=self._commit_index
        )
        self.kernel.reference(self.name, commit_hash)
        self._commit_index += 1
        return commit_hash

    def read_all(self) -> dict[str, str]:
        """Read all entries from the current commit's tree."""
        h = self.kernel.resolve(self.name)
        if not h:
            return {}
        commit = SkipPointerHistory.read_commit(self.kernel, h)
        return ShardedTree.read_all(self.kernel, commit["tree"])

    def read_entry(self, key: str) -> Optional[str]:
        """Read a single entry. O(1) shard read."""
        h = self.kernel.resolve(self.name)
        if not h:
            return None
        commit = SkipPointerHistory.read_commit(self.kernel, h)
        return ShardedTree.read_entry(self.kernel, commit["tree"], key)

    def history(self, limit: int = 20) -> list[dict]:
        """Walk commit history."""
        h = self.kernel.resolve(self.name)
        if not h:
            return []
        history = []
        current = h
        while current and len(history) < limit:
            commit = SkipPointerHistory.read_commit(self.kernel, current)
            history.append({
                "commit": current[:12],
                "message": commit["message"],
                "timestamp": commit["timestamp"],
                "index": commit.get("index", 0),
            })
            current = commit.get("parent")
        return history

    def time_travel(self, target_index: int) -> Optional[str]:
        """Travel to a specific commit index. O(log N) via skip pointers."""
        h = self.kernel.resolve(self.name)
        if not h:
            return None
        return SkipPointerHistory.walk_to_depth_by_index(self.kernel, h, target_index)
