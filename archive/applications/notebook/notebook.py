"""
Pond Notebook — a production-quality application built on the Pond kernel.

This is NOT a toy Lens. It's a real application that a developer might
actually want to use: a personal notebook with pages, full-text search,
version history, branching for draft experiments, and undo.

The goal is to answer: "Would anyone voluntarily build this on Pond?
Is it elegant, or does everything become awkward?"

Every awkwardness is documented in friction_diary.md.

Architecture:
  - NotebookLens manages pages (title, body, tags, attachments)
  - Each page is a JSON blob (content-addressed)
  - A "notebook commit" is a tree of page_name -> page_hash
  - Branching = Reference to a commit (experimental drafts)
  - Search = scan all pages in current commit (linear; no index)
  - History = walk parent chain (O(N); no skip pointers)
  - Undo = Reference to past commit
  - Attachments = binary blobs (images, PDFs) stored via Write

Uses ONLY: kernel.Write, kernel.Read, kernel.Reference, kernel.resolve
"""

import os
import sys
import json
import time
import re
from typing import Optional
from dataclasses import dataclass, field, asdict

# Add prototype to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "prototype"))
from pond_minimal import PondMinimal


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Page:
    """A notebook page."""
    title: str
    body: str
    tags: list[str] = field(default_factory=list)
    attachments: dict[str, str] = field(default_factory=dict)  # filename -> blob_hash
    created_at: float = 0.0
    updated_at: float = 0.0

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self), sort_keys=True).encode()

    @staticmethod
    def from_bytes(data: bytes) -> "Page":
        d = json.loads(data)
        return Page(**d)


# ---------------------------------------------------------------------------
# Lens-level helpers (Tree + Commit patterns)
# ---------------------------------------------------------------------------

def write_tree(kernel: PondMinimal, entries: dict[str, str]) -> str:
    """A Tree is a blob containing serialized {name -> hash} mappings."""
    data = json.dumps({"type": "tree", "entries": entries}, sort_keys=True).encode()
    return kernel.write(data)

def read_tree(kernel: PondMinimal, tree_hash: str) -> dict[str, str]:
    """Read a Tree blob and return its entries."""
    data = kernel.read_blob(tree_hash)
    obj = json.loads(data)
    return obj.get("entries", {})

def write_commit(kernel: PondMinimal, tree_hash: str, parent_hash: Optional[str],
                 message: str, author: str = "user") -> str:
    """A Commit is a blob containing {tree, parent, timestamp, message, author}."""
    obj = {
        "type": "commit",
        "tree": tree_hash,
        "parent": parent_hash,
        "timestamp": time.time(),
        "message": message,
        "author": author,
    }
    data = json.dumps(obj, sort_keys=True).encode()
    return kernel.write(data)

def read_commit(kernel: PondMinimal, commit_hash: str) -> dict:
    """Read a Commit blob."""
    data = kernel.read_blob(commit_hash)
    return json.loads(data)


# ---------------------------------------------------------------------------
# Notebook View — production quality
# ---------------------------------------------------------------------------

class NotebookLens:
    """
    A personal notebook with pages, search, history, branching, and undo.

    This is a REAL application, not a demo. It implements:
    - Page CRUD (create, read, update, delete)
    - Full-text search across all pages
    - Version history (walk commit chain)
    - Branching (experimental drafts)
    - Undo (move to past commit)
    - Attachments (binary blobs)
    - Tags

    Built on the Pond kernel (Write/Read/Reference only).
    """

    def __init__(self, kernel: PondMinimal, notebook_name: str = "notebook"):
        self.kernel = kernel
        self.notebook_name = notebook_name
        self._staged: dict[str, Page] = {}  # path -> Page (pending changes)
        self._deleted: set[str] = set()     # paths marked for deletion

    # ------------------------------------------------------------------
    # Page operations
    # ------------------------------------------------------------------

    def create_page(self, path: str, title: str, body: str = "",
                    tags: list[str] = None) -> Page:
        """Create a new page (staged, not committed yet)."""
        if path in self._staged:
            raise ValueError(f"Page '{path}' already staged")
        # Check if page already exists in current commit
        current = self._get_current_tree()
        if path in current and path not in self._deleted:
            raise ValueError(f"Page '{path}' already exists. Use update_page().")
        page = Page(
            title=title,
            body=body,
            tags=tags or [],
            created_at=time.time(),
            updated_at=time.time(),
        )
        self._staged[path] = page
        self._deleted.discard(path)  # un-delete if was deleted
        return page

    def update_page(self, path: str, title: str = None, body: str = None,
                    tags: list[str] = None) -> Page:
        """Update an existing page (staged, not committed yet)."""
        # Get current version
        page = self._staged.get(path)
        if page is None:
            # Load from current commit
            page = self.read_page(path)
        if page is None:
            raise ValueError(f"Page '{path}' does not exist")
        if title is not None:
            page.title = title
        if body is not None:
            page.body = body
        if tags is not None:
            page.tags = tags
        page.updated_at = time.time()
        self._staged[path] = page
        return page

    def delete_page(self, path: str) -> None:
        """Mark a page for deletion (staged, not committed yet)."""
        current = self._get_current_tree()
        if path not in current and path not in self._staged:
            raise ValueError(f"Page '{path}' does not exist")
        self._deleted.add(path)
        self._staged.pop(path, None)

    def read_page(self, path: str) -> Optional[Page]:
        """Read a page from the current commit."""
        current = self._get_current_tree()
        if path not in current:
            return None
        data = self.kernel.read_blob(current[path])
        return Page.from_bytes(data)

    def list_pages(self) -> list[dict]:
        """List all pages in the current commit."""
        tree = self._get_current_tree()
        pages = []
        for path, h in sorted(tree.items()):
            if path.startswith("_attachments/"):
                continue
            page = Page.from_bytes(self.kernel.read_blob(h))
            pages.append({
                "path": path,
                "title": page.title,
                "tags": page.tags,
                "updated_at": page.updated_at,
            })
        return pages

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search(self, query: str) -> list[dict]:
        """Full-text search across all pages. Returns matching pages with context."""
        query_lower = query.lower()
        results = []
        tree = self._get_current_tree()
        for path, h in tree.items():
            if path.startswith("_attachments/"):
                continue
            page = Page.from_bytes(self.kernel.read_blob(h))
            # Search in title and body
            title_match = query_lower in page.title.lower()
            body_match = query_lower in page.body.lower()
            tag_match = any(query_lower in tag.lower() for tag in page.tags)
            if title_match or body_match or tag_match:
                # Extract context (snippet around match)
                context = ""
                if body_match:
                    idx = page.body.lower().index(query_lower)
                    start = max(0, idx - 30)
                    end = min(len(page.body), idx + len(query) + 30)
                    context = f"...{page.body[start:end]}..."
                results.append({
                    "path": path,
                    "title": page.title,
                    "context": context,
                    "tags": page.tags,
                })
        return results

    # ------------------------------------------------------------------
    # Attachments
    # ------------------------------------------------------------------

    def add_attachment(self, filename: str, data: bytes) -> str:
        """Add a binary attachment (image, PDF, etc.). Returns the blob hash."""
        h = self.kernel.write(data)
        # Stage the attachment reference
        self._staged[f"_attachments/{filename}"] = Page(
            title=filename,
            body=h,  # store the hash in the body for retrieval
            created_at=time.time(),
            updated_at=time.time(),
        )
        return h

    def get_attachment(self, filename: str) -> bytes:
        """Retrieve an attachment by filename."""
        tree = self._get_current_tree()
        key = f"_attachments/{filename}"
        if key not in tree:
            raise ValueError(f"Attachment '{filename}' not found")
        page = Page.from_bytes(self.kernel.read_blob(tree[key]))
        return self.kernel.read_blob(page.body)  # body contains the hash

    # ------------------------------------------------------------------
    # Commit / History
    # ------------------------------------------------------------------

    def commit(self, message: str = "") -> str:
        """Commit staged changes. Returns the new commit hash."""
        if not self._staged and not self._deleted:
            raise ValueError("Nothing to commit")

        # Inherit parent tree
        parent_hash = self.kernel.resolve(self.notebook_name)
        tree_entries = {}
        if parent_hash:
            parent_commit = read_commit(self.kernel, parent_hash)
            tree_entries = dict(read_tree(self.kernel, parent_commit["tree"]))

        # Stage new/updated pages
        for path, page in self._staged.items():
            h = self.kernel.write(page.to_bytes())
            tree_entries[path] = h

        # Handle deletions
        for path in self._deleted:
            tree_entries.pop(path, None)

        # Build tree and commit
        tree_hash = write_tree(self.kernel, tree_entries)
        msg = message or f"commit: {len(self._staged)} updates, {len(self._deleted)} deletions"
        commit_hash = write_commit(self.kernel, tree_hash, parent_hash, msg)
        self.kernel.reference(self.notebook_name, commit_hash)

        # Clear staging
        self._staged.clear()
        self._deleted.clear()
        return commit_hash

    def history(self, limit: int = 20) -> list[dict]:
        """Walk the commit history."""
        commit_hash = self.kernel.resolve(self.notebook_name)
        if not commit_hash:
            return []
        history = []
        current = commit_hash
        while current and len(history) < limit:
            commit = read_commit(self.kernel, current)
            history.append({
                "commit": current[:12],
                "message": commit["message"],
                "timestamp": commit["timestamp"],
                "parent": commit["parent"][:12] if commit["parent"] else None,
            })
            current = commit["parent"]
        return history

    # ------------------------------------------------------------------
    # Branching
    # ------------------------------------------------------------------

    def create_branch(self, branch_name: str) -> str:
        """Create a branch (experimental draft)."""
        commit_hash = self.kernel.resolve(self.notebook_name)
        if not commit_hash:
            raise ValueError("No commits to branch from")
        full_name = f"{self.notebook_name}_branch_{branch_name}"
        self.kernel.reference(full_name, commit_hash)
        return full_name

    def checkout_branch(self, branch_name: str) -> None:
        """Switch to a branch."""
        full_name = f"{self.notebook_name}_branch_{branch_name}"
        commit_hash = self.kernel.resolve(full_name)
        if not commit_hash:
            raise ValueError(f"Branch '{branch_name}' does not exist")
        self.kernel.reference(self.notebook_name, commit_hash)
        self._staged.clear()
        self._deleted.clear()

    def list_branches(self) -> list[str]:
        """List all branches."""
        all_names = self.kernel.list_names()
        prefix = f"{self.notebook_name}_branch_"
        return [n[len(prefix):] for n in all_names if n.startswith(prefix)]

    # ------------------------------------------------------------------
    # Undo / Rollback
    # ------------------------------------------------------------------

    def undo(self, steps: int = 1) -> str:
        """Undo N commits by moving the reference back."""
        commit_hash = self.kernel.resolve(self.notebook_name)
        for _ in range(steps):
            commit = read_commit(self.kernel, commit_hash)
            if not commit["parent"]:
                break
            commit_hash = commit["parent"]
        self.kernel.reference(self.notebook_name, commit_hash)
        self._staged.clear()
        self._deleted.clear()
        return commit_hash[:12]

    def time_travel(self, commit_hash: str) -> None:
        """Travel to a specific commit (by hash prefix)."""
        # Resolve prefix to full hash
        current = self.kernel.resolve(self.notebook_name)
        while current:
            if current.startswith(commit_hash):
                self.kernel.reference(self.notebook_name, current)
                self._staged.clear()
                self._deleted.clear()
                return
            commit = read_commit(self.kernel, current)
            current = commit["parent"]
        raise ValueError(f"Commit '{commit_hash}' not found in history")

    def read_page_at(self, path: str, commit_hash: str) -> Optional[Page]:
        """Read a page at a specific point in history (time travel read)."""
        # Resolve prefix
        full_hash = self._resolve_commit_prefix(commit_hash)
        commit = read_commit(self.kernel, full_hash)
        tree = read_tree(self.kernel, commit["tree"])
        if path not in tree:
            return None
        return Page.from_bytes(self.kernel.read_blob(tree[path]))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_current_tree(self) -> dict[str, str]:
        """Get the tree entries for the current commit."""
        commit_hash = self.kernel.resolve(self.notebook_name)
        if not commit_hash:
            return {}
        commit = read_commit(self.kernel, commit_hash)
        return read_tree(self.kernel, commit["tree"])

    def _resolve_commit_prefix(self, prefix: str) -> str:
        """Resolve a commit hash prefix to full hash."""
        current = self.kernel.resolve(self.notebook_name)
        while current:
            if current.startswith(prefix):
                return current
            commit = read_commit(self.kernel, current)
            current = commit["parent"]
        raise ValueError(f"Commit '{prefix}' not found")

    # ------------------------------------------------------------------
    # Diff (what changed between two commits)
    # ------------------------------------------------------------------

    def diff(self, commit_a: str, commit_b: str) -> dict:
        """Show what changed between two commits."""
        # Resolve prefixes
        hash_a = self._resolve_commit_prefix(commit_a)
        hash_b = self._resolve_commit_prefix(commit_b)

        tree_a = read_tree(self.kernel, read_commit(self.kernel, hash_a)["tree"])
        tree_b = read_tree(self.kernel, read_commit(self.kernel, hash_b)["tree"])

        added = {}
        removed = {}
        modified = {}

        for path in set(tree_a) | set(tree_b):
            if path in tree_a and path not in tree_b:
                removed[path] = tree_a[path]
            elif path not in tree_a and path in tree_b:
                added[path] = tree_b[path]
            elif tree_a[path] != tree_b[path]:
                modified[path] = {"old": tree_a[path][:12], "new": tree_b[path][:12]}

        return {"added": added, "removed": removed, "modified": modified}
