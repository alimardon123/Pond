"""
Pond Git — a real Git replacement built on Pond.

This is NOT a toy Lens. It's a functional Git-like version control
system that stores files, commits, branches, and history in the Pond
kernel. It demonstrates that Pond can replace Git's storage layer.

Supported operations:
  - init: initialize a repository
  - add: stage a file
  - commit: create a commit with staged files
  - log: show commit history
  - branch: create/list branches
  - checkout: switch to a branch
  - merge: merge a branch into the current branch
  - diff: show what changed between two commits
  - cat: read a file at the current commit
  - ls: list files at the current commit

Uses the LensBase library (sharded trees + skip pointers) to avoid
boilerplate and get O(1) commits and O(log N) history.
"""

import os
import sys
import time
import json

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "prototype"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "libraries"))
from pond_minimal import PondMinimal
from view_helpers import ViewBase, ShardedTree, SkipPointerHistory


class PondGit:
    """
    A Git-like version control system on Pond.

    Uses ViewBase for sharded trees + skip-pointer history.
    Adds Git-specific semantics: branches, merge, diff.
    """

    def __init__(self, kernel: PondMinimal, repo_name: str = "repo"):
        self.kernel = kernel
        self.repo_name = repo_name
        self.base = ViewBase(kernel, repo_name)
        self._staged: dict[str, bytes] = {}  # path -> file content
        self._deleted: set[str] = set()

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------

    def init(self) -> str:
        """Initialize the repository with an empty commit."""
        # Empty tree
        tree_root = ShardedTree.write(self.kernel, {}, None)
        commit = SkipPointerHistory.write_commit(
            self.kernel, tree_root, None, "initial commit", commit_index=0
        )
        self.kernel.reference(self.repo_name, commit)
        self.kernel.reference(f"{self.repo_name}_HEAD", commit)
        return commit

    # ------------------------------------------------------------------
    # Staging
    # ------------------------------------------------------------------

    def add(self, path: str, content: bytes) -> None:
        """Stage a file for commit."""
        self._staged[path] = content
        self._deleted.discard(path)

    def rm(self, path: str) -> None:
        """Mark a file for deletion."""
        self._deleted.add(path)
        self._staged.pop(path, None)

    # ------------------------------------------------------------------
    # Commit
    # ------------------------------------------------------------------

    def commit(self, message: str, author: str = "user") -> str:
        """Commit staged changes."""
        if not self._staged and not self._deleted:
            raise ValueError("Nothing to commit")

        # Get current tree entries
        entries = self.base.read_all()

        # Apply staged changes
        for path, content in self._staged.items():
            h = self.kernel.write(content)
            entries[path] = h

        # Apply deletions
        for path in self._deleted:
            entries.pop(path, None)

        # Commit via LensBase (sharded tree + skip pointer)
        commit_hash = self.base.commit(message, entries)

        # Update HEAD
        self.kernel.reference(f"{self.repo_name}_HEAD", commit_hash)

        # Clear staging
        self._staged.clear()
        self._deleted.clear()

        return commit_hash

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def cat(self, path: str) -> bytes:
        """Read a file at the current commit. O(1) shard read."""
        h = self.base.read_entry(path)
        if h is None:
            raise ValueError(f"File '{path}' not found")
        return self.kernel.read_blob(h)

    def ls(self) -> list[str]:
        """List all files at the current commit."""
        entries = self.base.read_all()
        return sorted(entries.keys())

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def log(self, limit: int = 20) -> list[dict]:
        """Show commit history."""
        return self.base.history(limit)

    # ------------------------------------------------------------------
    # Branching
    # ------------------------------------------------------------------

    def branch(self, name: str) -> str:
        """Create a branch."""
        head = self.kernel.resolve(f"{self.repo_name}_HEAD")
        full_name = f"{self.repo_name}_branch_{name}"
        self.kernel.reference(full_name, head)
        return full_name

    def checkout(self, name: str) -> None:
        """Switch to a branch."""
        full_name = f"{self.repo_name}_branch_{name}"
        h = self.kernel.resolve(full_name)
        if not h:
            raise ValueError(f"Branch '{name}' does not exist")
        # Move HEAD and repo_name to the branch's commit
        self.kernel.reference(self.repo_name, h)
        self.kernel.reference(f"{self.repo_name}_HEAD", h)
        # Reset ViewBase index
        self.base = ViewBase(self.kernel, self.repo_name)
        self._staged.clear()
        self._deleted.clear()

    def branches(self) -> list[str]:
        """List all branches."""
        all_names = self.kernel.list_names()
        prefix = f"{self.repo_name}_branch_"
        return [n[len(prefix):] for n in all_names if n.startswith(prefix)]

    # ------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------

    def merge(self, branch_name: str, author: str = "user") -> str:
        """Merge a branch into the current branch. Uses union merge (no conflict resolution)."""
        # Get branch commit
        branch_full = f"{self.repo_name}_branch_{branch_name}"
        branch_commit_hash = self.kernel.resolve(branch_full)
        if not branch_commit_hash:
            raise ValueError(f"Branch '{branch_name}' does not exist")

        # Read both trees
        current_entries = self.base.read_all()
        branch_commit = SkipPointerHistory.read_commit(self.kernel, branch_commit_hash)
        branch_entries = ShardedTree.read_all(self.kernel, branch_commit["tree"])

        # Union merge: branch wins on conflict (simplification)
        merged = dict(current_entries)
        for path, h in branch_entries.items():
            if path not in merged or merged[path] != h:
                merged[path] = h

        # Write merge commit with TWO parents
        parent_hash = self.kernel.resolve(self.repo_name)
        tree_root = ShardedTree.write(self.kernel, merged,
                                       parent_hash if parent_hash else None)

        # Custom merge commit with multi-parent
        merge_data = json.dumps({
            "type": "commit",
            "tree": tree_root,
            "parents": [parent_hash, branch_commit_hash],
            "timestamp": time.time(),
            "message": f"merge '{branch_name}'",
            "author": author,
            "index": self.base._commit_index,
            "skip": None,
        }, sort_keys=True).encode()
        merge_hash = self.kernel.write(merge_data)
        self.kernel.reference(self.repo_name, merge_hash)
        self.kernel.reference(f"{self.repo_name}_HEAD", merge_hash)
        self.base._commit_index += 1

        return merge_hash

    # ------------------------------------------------------------------
    # Diff
    # ------------------------------------------------------------------

    def diff(self, commit_a: str, commit_b: str) -> dict:
        """Show what changed between two commits (by hash prefix)."""
        # Resolve prefixes
        hash_a = self._resolve_prefix(commit_a)
        hash_b = self._resolve_prefix(commit_b)

        commit_a_obj = SkipPointerHistory.read_commit(self.kernel, hash_a)
        commit_b_obj = SkipPointerHistory.read_commit(self.kernel, hash_b)

        tree_a = ShardedTree.read_all(self.kernel, commit_a_obj["tree"])
        tree_b = ShardedTree.read_all(self.kernel, commit_b_obj["tree"])

        added = {k: v[:12] for k, v in tree_b.items() if k not in tree_a}
        removed = {k: v[:12] for k, v in tree_a.items() if k not in tree_b}
        modified = {k: {"old": tree_a[k][:12], "new": tree_b[k][:12]}
                    for k in tree_a if k in tree_b and tree_a[k] != tree_b[k]}

        return {"added": added, "removed": removed, "modified": modified}

    def _resolve_prefix(self, prefix: str) -> str:
        """Resolve a commit hash prefix to full hash by walking history."""
        current = self.kernel.resolve(self.repo_name)
        while current:
            if current.startswith(prefix):
                return current
            commit = SkipPointerHistory.read_commit(self.kernel, current)
            current = commit.get("parent")
        raise ValueError(f"Commit '{prefix}' not found")
