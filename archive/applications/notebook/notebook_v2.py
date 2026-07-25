"""
Reworked Notebook View on ProllyLensBase.

Uses Prolly trees for O(log N) page lookups, bounded delta journal
for O(1) commits, and Lens-level search index.
"""

import json, time, sys, os
from dataclasses import dataclass, asdict
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "prototype"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "libraries"))
from kernel import PondMinimal
from prolly_tree import ProllyLensBase, ProllyTree


@dataclass
class Page:
    title: str
    body: str
    tags: list = None
    created_at: float = 0.0
    updated_at: float = 0.0

    def to_bytes(self): return json.dumps(asdict(self), sort_keys=True).encode()
    @staticmethod
    def from_bytes(data): return Page(**json.loads(data))


class NotebookLens:
    """Personal notebook on Pond. Uses ProllyLensBase."""

    def __init__(self, kernel, name="notebook"):
        self.kernel = kernel
        self.name = name
        self.base = ProllyLensBase(kernel, name)

    def create_page(self, path, title, body="", tags=None):
        page = Page(title, body, tags or [], time.time(), time.time())
        h = self.kernel.write(page.to_bytes())
        self.base.stage(path, h)
        return page

    def update_page(self, path, title=None, body=None, tags=None):
        page = self.read_page(path)
        if not page: raise ValueError(f"Page '{path}' not found")
        if title is not None: page.title = title
        if body is not None: page.body = body
        if tags is not None: page.tags = tags
        page.updated_at = time.time()
        h = self.kernel.write(page.to_bytes())
        self.base.stage(path, h)

    def delete_page(self, path):
        self.base.stage_delete(path)

    def read_page(self, path):
        """O(log N) page lookup via Prolly tree."""
        h = self.base.lookup(path)
        return Page.from_bytes(self.kernel.read_blob(h)) if h else None

    def list_pages(self):
        state = self.base.read_all()
        pages = []
        for path, h in sorted(state.items()):
            if path.startswith("_"): continue
            page = Page.from_bytes(self.kernel.read_blob(h))
            pages.append({"path": path, "title": page.title, "tags": page.tags})
        return pages

    def search(self, query):
        """Full-text search. Uses scan (could use an inverted index)."""
        q = query.lower()
        results = []
        for path, h in self.base.read_all().items():
            if path.startswith("_"): continue
            page = Page.from_bytes(self.kernel.read_blob(h))
            if q in page.title.lower() or q in page.body.lower() or any(q in t.lower() for t in page.tags):
                results.append({"path": path, "title": page.title})
        return results

    def commit(self, message=""):
        return self.base.commit(message)

    def history(self, limit=20): return self.base.history(limit)
    def branch(self, name): return self.base.branch(name)
    def checkout(self, name): self.base.checkout(name)
    def list_branches(self): return self.base.list_branches()
    def undo(self, steps=1): return self.base.undo(steps)
    def merge(self, name): return self.base.merge(name)
    def diff(self, a, b): return self.base.diff(a, b)
