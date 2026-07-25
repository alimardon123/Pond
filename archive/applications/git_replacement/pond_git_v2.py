"""
Reworked Git View on ProllyLensBase.

Uses Prolly trees for O(log N) file lookups, bounded delta journal
for O(1) commits, and content-based diff between versions.
"""

import json, time, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "prototype"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "libraries"))
from kernel import PondMinimal
from prolly_tree import ProllyLensBase, ProllyTree


class PondGit:
    """Git-like VCS on Pond. Uses ProllyLensBase."""

    def __init__(self, kernel, repo_name="repo"):
        self.kernel = kernel
        self.repo_name = repo_name
        self.base = ProllyLensBase(kernel, repo_name)

    def init(self):
        """Initialize with empty snapshot."""
        tree_root = ProllyTree.build(self.kernel, {})
        commit_obj = {"type":"commit","parent":None,"snapshot":tree_root,"delta":None,
                      "timestamp":time.time(),"message":"initial commit","index":0}
        h = self.kernel.write(json.dumps(commit_obj, sort_keys=True).encode())
        self.kernel.reference(self.repo_name, h)
        return h

    def add(self, path, content):
        h = self.kernel.write(content if isinstance(content, bytes) else content.encode())
        self.base.stage(path, h)

    def rm(self, path):
        self.base.stage_delete(path)

    def commit(self, message, author="user"):
        return self.base.commit(message)

    def cat(self, path):
        """O(log N) file lookup via Prolly tree."""
        h = self.base.lookup(path)
        if not h: raise ValueError(f"File '{path}' not found")
        return self.kernel.read_blob(h)

    def ls(self):
        return sorted(k for k in self.base.read_all() if not k.startswith("_"))

    def log(self, limit=20):
        return self.base.history(limit)

    def branch(self, name): return self.base.branch(name)
    def checkout(self, name): self.base.checkout(name)
    def branches(self): return self.base.list_branches()
    def merge(self, name): return self.base.merge(name)
    def undo(self, steps=1): return self.base.undo(steps)

    def diff(self, a, b):
        """Content-based diff via Prolly tree comparison."""
        return self.base.diff(a, b)
