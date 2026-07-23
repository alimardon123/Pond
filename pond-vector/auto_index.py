"""
Mock implementation of View (Layer 1) and IndexedView (Layer 2),
built ONLY from the SDK specification.

NOT the real SDK.  Used for testing VectorView.
"""

import json

from mock_kernel import PondMinimal  # noqa: F401  (re-exported for convenience)


# ---------------------------------------------------------------------------
# Layer 1: View
# ---------------------------------------------------------------------------

class View:
    """
    A versioned, content-addressed key/value view on top of the kernel.

    Each commit stores a snapshot: {key -> blob_hash}.
    Branches are kernel names:  "<branch>:head" -> commit_hash.
    """

    def __init__(self, kernel, name: str):
        self._kernel = kernel
        self._name = name
        self._staged: dict[str, str | None] = {}   # key -> blob_hash | None(del)
        self._branch = name
        self._current_commit: str | None = kernel.resolve(f"{name}:head")

    # ---- internal helpers ----

    def _get_snapshot(self) -> dict[str, str]:
        if self._current_commit is None:
            return {}
        commit = json.loads(self._kernel.read(self._current_commit))
        return commit["snapshot"]

    def _read_commit(self, commit_hash: str | None) -> dict | None:
        if commit_hash is None:
            return None
        return json.loads(self._kernel.read(commit_hash))

    # ---- Write path ----

    def put(self, key: str, data) -> str:
        blob = self.encode(data)
        blob_hash = self._kernel.write(blob)
        self.put_raw(key, blob_hash)
        return blob_hash

    def put_raw(self, key: str, blob_hash: str) -> None:
        self._staged[key] = blob_hash

    def delete(self, key: str) -> None:
        self._staged[key] = None  # None == tombstone

    def commit(self, message: str = "") -> str:
        current = self._get_snapshot()
        new_snapshot = dict(current)
        for key, val in self._staged.items():
            if val is None:
                new_snapshot.pop(key, None)
            else:
                new_snapshot[key] = val
        self._staged.clear()

        commit_obj = {
            "snapshot": new_snapshot,
            "parent": self._current_commit,
            "message": message,
            "branch": self._branch,
        }
        commit_bytes = json.dumps(commit_obj).encode("utf-8")
        commit_hash = self._kernel.write(commit_bytes)
        self._current_commit = commit_hash
        self._kernel.reference(f"{self._branch}:head", commit_hash)
        return commit_hash

    # ---- Read path ----

    def get(self, key: str):
        snapshot = self._get_snapshot()
        if key not in snapshot:
            return None
        blob = self._kernel.read(snapshot[key])
        return self.decode(blob)

    def get_all(self) -> dict:
        snapshot = self._get_snapshot()
        result = {}
        for key, blob_hash in snapshot.items():
            result[key] = self.decode(self._kernel.read(blob_hash))
        return result

    def keys(self) -> list[str]:
        return list(self._get_snapshot().keys())

    def exists(self, key: str) -> bool:
        return key in self._get_snapshot()

    def count(self) -> int:
        return len(self._get_snapshot())

    # ---- Version control ----

    def branch(self, name: str) -> str:
        self._kernel.reference(f"{name}:head", self._current_commit)
        return name

    def checkout(self, name: str) -> None:
        self._branch = name
        self._current_commit = self._kernel.resolve(f"{name}:head")

    def merge(self, name: str) -> str:
        other_head = self._kernel.resolve(f"{name}:head")
        other_snapshot = self._read_commit(other_head)["snapshot"]
        current_snapshot = self._get_snapshot()
        # Union merge: other branch wins on conflict.
        merged = {**current_snapshot, **other_snapshot}
        commit_obj = {
            "snapshot": merged,
            "parent": self._current_commit,
            "message": f"Merge branch '{name}'",
            "branch": self._branch,
            "merge_parent": other_head,
        }
        commit_bytes = json.dumps(commit_obj).encode("utf-8")
        commit_hash = self._kernel.write(commit_bytes)
        self._current_commit = commit_hash
        self._kernel.reference(f"{self._branch}:head", commit_hash)
        return commit_hash

    def undo(self, steps: int = 1) -> str:
        for _ in range(steps):
            commit = self._read_commit(self._current_commit)
            if commit is None:
                break
            self._current_commit = commit.get("parent")
        self._kernel.reference(f"{self._branch}:head", self._current_commit)
        return self._current_commit or ""

    def history(self, limit: int = 20) -> list[dict]:
        result = []
        h = self._current_commit
        while h is not None and len(result) < limit:
            commit = self._read_commit(h)
            if commit is None:
                break
            result.append({
                "hash": h,
                "message": commit.get("message", ""),
                "parent": commit.get("parent"),
                "branch": commit.get("branch", ""),
            })
            h = commit.get("parent")
        return result

    def diff(self, a: str, b: str) -> dict:
        snap_a = self._read_commit(a)["snapshot"]
        snap_b = self._read_commit(b)["snapshot"]
        added = {k: v for k, v in snap_b.items() if k not in snap_a}
        removed = {k: v for k, v in snap_a.items() if k not in snap_b}
        modified = {
            k: (snap_a[k], snap_b[k])
            for k in snap_a
            if k in snap_b and snap_a[k] != snap_b[k]
        }
        return {"added": added, "removed": removed, "modified": modified}

    # ---- Indexing (basic, overridden by IndexedView) ----

    def create_index(self, name: str, extractor) -> str:
        snapshot = self._get_snapshot()
        index: dict[str, list[str]] = {}
        for key, blob_hash in snapshot.items():
            data = self.decode(self._kernel.read(blob_hash))
            idx_key = extractor(data)
            index.setdefault(str(idx_key), []).append(key)
        index_hash = self._kernel.write(json.dumps(index).encode("utf-8"))
        self._kernel.reference(f"{self._name}:index:{name}", index_hash)
        return index_hash

    def drop_index(self, name: str) -> bool:
        # The kernel has no "unreference" primitive — a spec gap.
        # We can only stop tracking it; the name lingers in the kernel.
        return True

    def refresh_index(self, name: str, extractor) -> str:
        return self.create_index(name, extractor)

    def list_indexes(self) -> list[str]:
        prefix = f"{self._name}:index:"
        return [n[len(prefix):] for n in self._kernel.list_names()
                if n.startswith(prefix)]

    def lookup_by_index(self, name: str, key: str):
        idx_hash = self._kernel.resolve(f"{self._name}:index:{name}")
        if idx_hash is None:
            return None
        index = json.loads(self._kernel.read(idx_hash))
        keys = index.get(str(key), [])
        if not keys:
            return None
        return self.get(keys[0])

    # ---- Serialization (override in subclass) ----

    def encode(self, data) -> bytes:
        return json.dumps(data).encode("utf-8")

    def decode(self, data: bytes):
        return json.loads(data)


# ---------------------------------------------------------------------------
# Layer 2: IndexedView
# ---------------------------------------------------------------------------

class IndexedView(View):
    """
    View with automatic, registered indexes.

    Modes:
      "lazy"  — index rebuilt on read when staleness >= budget.
      "eager" — index rebuilt on every commit (always fresh reads).
    """

    def __init__(self, kernel, name: str):
        super().__init__(kernel, name)
        # name -> {"extractor", "mode", "staleness_budget", "last_commit"}
        self._registered: dict[str, dict] = {}

    # ---- internal ----

    def _rebuild_index(self, name: str) -> None:
        reg = self._registered[name]
        extractor = reg["extractor"]
        snapshot = self._get_snapshot()
        index: dict[str, list[str]] = {}
        for key, blob_hash in snapshot.items():
            data = self.decode(self._kernel.read(blob_hash))
            idx_key = extractor(data)
            index.setdefault(str(idx_key), []).append(key)
        index_hash = self._kernel.write(json.dumps(index).encode("utf-8"))
        self._kernel.reference(f"{self._name}:index:{name}", index_hash)
        reg["last_commit"] = self._current_commit

    def _staleness(self, name: str) -> int:
        reg = self._registered.get(name)
        if reg is None:
            return 0
        last = reg["last_commit"]
        if last == self._current_commit:
            return 0
        # Walk commit chain from current back to last (or to None).
        count = 0
        h = self._current_commit
        while h is not None and h != last:
            count += 1
            commit = self._read_commit(h)
            if commit is None:
                break
            h = commit.get("parent")
        return count

    def _ensure_fresh(self, name: str) -> None:
        reg = self._registered.get(name)
        if reg is None:
            return
        if reg["mode"] == "eager":
            if self._staleness(name) > 0:
                self._rebuild_index(name)
        else:  # lazy
            if self._staleness(name) >= reg["staleness_budget"]:
                self._rebuild_index(name)

    # ---- commit override: rebuild eager indexes ----

    def commit(self, message: str = "") -> str:
        result = super().commit(message)
        for name, reg in self._registered.items():
            if reg["mode"] == "eager":
                self._rebuild_index(name)
        return result

    # ---- public API ----

    def register_index(self, name: str, extractor,
                       mode: str = "lazy", staleness_budget: int = 5) -> None:
        self._registered[name] = {
            "extractor": extractor,
            "mode": mode,
            "staleness_budget": staleness_budget,
            "last_commit": None,
        }
        if mode == "eager":
            self._rebuild_index(name)

    def unregister_index(self, name: str) -> None:
        self._registered.pop(name, None)

    def find_by(self, index_name: str, index_key: str):
        self._ensure_fresh(index_name)
        idx_hash = self._kernel.resolve(f"{self._name}:index:{index_name}")
        if idx_hash is None:
            return None
        index = json.loads(self._kernel.read(idx_hash))
        keys = index.get(str(index_key), [])
        if not keys:
            return None
        return self.get(keys[0])

    def find_all_by(self, index_name: str, index_key: str) -> list:
        self._ensure_fresh(index_name)
        idx_hash = self._kernel.resolve(f"{self._name}:index:{index_name}")
        if idx_hash is None:
            return []
        index = json.loads(self._kernel.read(idx_hash))
        keys = index.get(str(index_key), [])
        return [self.get(k) for k in keys]

    def refresh_all_indexes(self) -> None:
        for name in self._registered:
            self._rebuild_index(name)

    def get_index_staleness(self, index_name: str) -> int:
        return self._staleness(index_name)
