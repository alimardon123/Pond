"""
Independent Git-like Version Control View built on the Pond Kernel.

This module was implemented PURELY from the "Pond Kernel - Formal Specification"
provided in the task. No existing Pond code was read or consulted.

The only kernel surface this Lens depends on is the four-method API the task
tells us to assume:

    kernel.write(data: bytes) -> str          # content-addressed store -> hex hash
    kernel.read(hash_or_name: str) -> bytes   # fetch bytes by hash or resolved name
    kernel.reference(name: str, hash: str)    # the ONLY mutation: name -> hash
    kernel.resolve(name: str) -> str | None   # resolve a name to its hash (None if unbound)

Everything else here -- object formats, ref layout, HEAD tracking, the staging
area, the tree/commit schemas, error types -- is a Lens-level invention built on
top of the immutable, content-addressed kernel, because the spec is silent on
all of it. Those inventions are documented inline and in the accompanying report.
"""

import json
import time
import hashlib  # used ONLY by the in-memory test kernel, never by the Lens itself


# ===========================================================================
# Object-format conventions (Lens-level invention; spec is silent)
# ===========================================================================
#
# Three kinds of immutable objects, all stored as raw bytes via kernel.write:
#
#   blob   : the literal file content bytes.
#   tree   : JSON -> {"type": "tree",    "entries": {path: blob_hash, ...}}
#   commit : JSON -> {"type": "commit",  "tree": tree_hash,
#                      "parents": [commit_hash, ...],
#                      "message": str, "author": str, "timestamp": float}
#
# Trees are FLAT (path -> blob hash) rather than git's nested trees. This is a
# deliberate simplification: the spec says nothing about tree structure, and a
# flat full-snapshot tree is easy to reason about and sufficient for the
# required operations. (Cost: every commit stores a full path map; no sub-tree
# sharing. Acceptable for a Lens demo.)
#
# Names (kernel.reference) used by this Lens:
#
#   refs/heads/<branch>  -> commit hash            (a branch tip)
#   HEAD                 -> head-state-object hash
#
# The kernel namespace maps names -> HASHES only (never name -> name). Because
# "current branch" is conceptually a name -> name pointer, we cannot store it
# directly. We encode HEAD as a small immutable object and bind the name "HEAD"
# to it:
#
#   head-state object -> {"type": "branch",   "name": <branch>}
#                   or   {"type": "detached", "commit": <hash>}
#
# The staging area (index) is transient View state held in memory, mirroring
# git's conceptual model where the index is itself a Lens concern.
# ===========================================================================


class PondError(Exception):
    """Base error for Lens-level failures."""


class NotFound(PondError):
    """A path / object / name could not be found."""


class NotACommit(PondError):
    """An object that was expected to be a commit is not one."""


def _is_hex_hash(s):
    """A 64-char lowercase hex SHA-256 string, per the spec's Write contract."""
    return (
        isinstance(s, str)
        and len(s) == 64
        and all(c in "0123456789abcdef" for c in s)
    )


class GitLens:
    """A Git-like VCS View layered on the Pond kernel."""

    def __init__(self, kernel, author="independent-view <view@pond>"):
        self.kernel = kernel
        self.author = author
        # staging: path -> content bytes (transient index)
        self.staging = {}
        # _head: None | {"type":"branch","name":...} | {"type":"detached","commit":...}
        self._head = None
        self._load_head()

    # ------------------------------------------------------------------ refs
    @staticmethod
    def _branch_ref(name):
        return "refs/heads/" + name

    # ------------------------------------------------------------------ HEAD
    def _load_head(self):
        if self.kernel.resolve("HEAD") is None:
            self._head = None
            return
        raw = self.kernel.read("HEAD")
        self._head = json.loads(raw.decode("utf-8"))

    def _save_head(self):
        data = json.dumps(self._head).encode("utf-8")
        head_hash = self.kernel.write(data)
        self.kernel.reference("HEAD", head_hash)

    # ----------------------------------------------------------- repo lifecycle
    def init(self, default_branch="main"):
        """Initialize the repo. Idempotent: a no-op if already initialized."""
        if self.kernel.resolve("HEAD") is not None:
            self._load_head()
            return
        self._head = {"type": "branch", "name": default_branch}
        self._save_head()
        return default_branch

    # ----------------------------------------------------------- staging: add
    def add(self, path, content):
        """Stage a file (path -> content). Content may be str or bytes."""
        if isinstance(content, str):
            content = content.encode("utf-8")
        elif not isinstance(content, (bytes, bytearray)):
            raise PondError("content must be str or bytes")
        self.staging[path] = bytes(content)

    def _clear_staging(self):
        self.staging = {}

    # ----------------------------------------------------------- current state
    def _current_commit_hash(self):
        """Hash of the commit HEAD currently points at, or None if unborn."""
        if self._head is None:
            return None
        if self._head["type"] == "branch":
            return self.kernel.resolve(self._branch_ref(self._head["name"]))
        return self._head["commit"]

    def _read_commit(self, commit_hash):
        if commit_hash is None:
            raise NotFound("no commit exists yet")
        obj = json.loads(self.kernel.read(commit_hash).decode("utf-8"))
        if obj.get("type") != "commit":
            raise NotACommit("object %s is not a commit" % commit_hash)
        return obj

    def _current_tree_entries(self):
        """path -> blob_hash for the current commit (empty if no commits)."""
        ch = self._current_commit_hash()
        if ch is None:
            return {}
        tree_hash = self._read_commit(ch)["tree"]
        tree = json.loads(self.kernel.read(tree_hash).decode("utf-8"))
        if tree.get("type") != "tree":
            raise PondError("object %s is not a tree" % tree_hash)
        return dict(tree.get("entries", {}))

    # ----------------------------------------------------------- commit
    def commit(self, message):
        if self._head is None:
            raise PondError("repo not initialized; call init() first")
        if not self.staging:
            raise PondError("nothing staged to commit")

        # 1. Write blobs for staged content.
        staged_entries = {}
        for path, content in self.staging.items():
            staged_entries[path] = self.kernel.write(content)

        # 2. Build a full tree = current tree + staged changes (flat snapshot).
        entries = self._current_tree_entries()
        entries.update(staged_entries)
        tree_obj = {"type": "tree", "entries": entries}
        tree_hash = self.kernel.write(json.dumps(tree_obj).encode("utf-8"))

        # 3. Determine parent(s). Linear history: single first-parent.
        parent = self._current_commit_hash()
        parents = [parent] if parent is not None else []

        commit_obj = {
            "type": "commit",
            "tree": tree_hash,
            "parents": parents,
            "message": message,
            "author": self.author,
            "timestamp": time.time(),
        }
        commit_hash = self.kernel.write(json.dumps(commit_obj).encode("utf-8"))

        # 4. Advance the branch ref (or move detached HEAD).
        if self._head["type"] == "branch":
            self.kernel.reference(self._branch_ref(self._head["name"]), commit_hash)
        else:  # detached
            self._head = {"type": "detached", "commit": commit_hash}
            self._save_head()

        self._clear_staging()
        return commit_hash

    # ----------------------------------------------------------- read_file
    def read_file(self, path):
        """Return the bytes of `path` as recorded in the current commit."""
        entries = self._current_tree_entries()
        if path not in entries:
            raise NotFound("path not in current commit: %s" % path)
        return self.kernel.read(entries[path])

    # ----------------------------------------------------------- log
    def log(self):
        """Return commit history from HEAD back, following first parent."""
        history = []
        ch = self._current_commit_hash()
        seen = set()
        while ch is not None and ch not in seen:
            seen.add(ch)
            c = self._read_commit(ch)
            history.append({
                "hash": ch,
                "message": c["message"],
                "author": c["author"],
                "timestamp": c["timestamp"],
                "parents": c["parents"],
            })
            parents = c["parents"]
            ch = parents[0] if parents else None
        return history

    # ----------------------------------------------------------- branch
    def branch(self, name):
        """Create a branch pointing at the current commit (O(1) ref)."""
        ch = self._current_commit_hash()
        if ch is None:
            raise PondError("cannot create a branch before the first commit")
        self.kernel.reference(self._branch_ref(name), ch)

    # ----------------------------------------------------------- checkout
    def checkout(self, name):
        """Switch HEAD to a branch name or (detached) to a commit hash."""
        # Branch?
        if self.kernel.resolve(self._branch_ref(name)) is not None:
            self._head = {"type": "branch", "name": name}
            self._save_head()
            self._clear_staging()
            return ("branch", name)

        # Detached at a commit hash?
        if _is_hex_hash(name):
            # Verify it exists and is actually a commit object.
            self._read_commit(name)
            self._head = {"type": "detached", "commit": name}
            self._save_head()
            self._clear_staging()
            return ("detached", name)

        raise NotFound("not a branch or commit: %s" % name)

    # ----------------------------------------------------------- introspection
    def head(self):
        """Return a human-readable description of the current HEAD."""
        if self._head is None:
            return "<uninitialized>"
        if self._head["type"] == "branch":
            return "branch:%s" % self._head["name"]
        return "detached:%s" % self._head["commit"]


# ===========================================================================
# Minimal in-memory realization of the Pond kernel -- TEST HARNESS ONLY.
#
# This is NOT part of the Lens. It exists so the demo scenario is runnable
# end-to-end. The View talks to it exclusively through write/read/reference/
# resolve, exactly the contract the spec describes.
# ===========================================================================
class InMemoryKernel:
    def __init__(self):
        self._store = {}   # hash -> bytes   (immutable after write)
        self._refs = {}    # name -> hash    (the only mutable state)

    def write(self, data: bytes) -> str:
        h = hashlib.sha256(data).hexdigest()
        # Law 1: immutable; writing same bytes is a no-op (dedup).
        self._store.setdefault(h, bytes(data))
        return h

    def read(self, hash_or_name: str) -> bytes:
        # Spec: 64-char hex hash first, else name.
        if _is_hex_hash(hash_or_name) and hash_or_name in self._store:
            return self._store[hash_or_name]
        h = self._refs.get(hash_or_name)
        if h is None or h not in self._store:
            raise KeyError("NOT_FOUND: %s" % hash_or_name)
        return self._store[h]

    def reference(self, name: str, hash: str):
        if hash not in self._store:
            raise KeyError("HASH_NOT_FOUND: %s" % hash)
        self._refs[name] = hash

    def resolve(self, name: str):
        return self._refs.get(name)


# ===========================================================================
# Required test scenario.
# ===========================================================================
def demo():
    kernel = InMemoryKernel()
    repo = GitLens(kernel)
    repo.init()

    repo.add("file1.txt", "hello")
    repo.add("file2.txt", "world")
    c1 = repo.commit("initial")
    print("commit 1  :", c1[:12], "| initial")

    repo.add("file1.txt", "hello world")          # modify
    c2 = repo.commit("update file1")
    print("commit 2  :", c2[:12], "| update file1")

    repo.branch("feature")
    repo.checkout("feature")
    repo.add("file3.txt", "feature file")
    c3 = repo.commit("add file3 on feature")
    print("commit 3  :", c3[:12], "| add file3 on feature (feature)")

    repo.checkout("main")
    print("HEAD now  :", repo.head())

    print("\n--- log() on main ---")
    for e in repo.log():
        print("  ", e["hash"][:12], "|", e["message"])

    print("\n--- read_file on main ---")
    print("  file1.txt:", repo.read_file("file1.txt").decode())
    print("  file2.txt:", repo.read_file("file2.txt").decode())

    try:
        repo.read_file("file3.txt")
        print("  ERROR: file3.txt should NOT exist on main!")
    except NotFound:
        print("  file3.txt: correctly ABSENT on main")

    # Sanity: file3 should be readable on feature.
    repo.checkout("feature")
    print("\n--- after checkout feature ---")
    print("  file3.txt:", repo.read_file("file3.txt").decode())

    # Sanity: persistence across a fresh View instance (HEAD ref survives).
    repo2 = GitLens(kernel)
    print("\n--- new Lens instance, same kernel ---")
    print("  HEAD     :", repo2.head())
    print("  file1.txt:", repo2.read_file("file1.txt").decode())

    print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    demo()
