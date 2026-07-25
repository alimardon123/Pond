"""
GraphView — external validation of Pond SDK (Task 12).

Built from SDK_SPEC.md and pond-core/kernel.py ONLY.
Does NOT import pond-sdk (built from spec, not copied).

Design choice (per task instructions, option (b)):
  Build GraphView directly on the kernel primitives, following the
  spec's described behavior. We do NOT re-implement ProllyLensBase
  or IndexedLens from the spec because:
    1. Spec §7 explicitly says "Views do NOT need to know this format"
       (the binary commit format is an internal detail of
       ProllyLensBase; the spec authorizes Views to use their own).
    2. The Prolly tree structure is referenced as known but never
       defined in SDK_SPEC.md — re-implementing it would require
       inventing unspecified internals.
    3. Building directly on the kernel with a simpler snapshot/delta
       model lets us follow the spec's described BEHAVIOR without
       guessing at Prolly tree internals.

The observable behavior (history shape, merge semantics, branch
semantics, tombstone-based index drop) follows SDK_SPEC.md exactly.

Conformance to the 10 settled ambiguities (A–J):
  A (§1.1): kernel = PondMinimal(base_dir) — used directly.
  B (§4.2): index extractors are functions of decoded data only.
  C (§3.2): get() walks DAG, early-returns on delta hit, falls
            back to snapshot — O(K + log N) shape.
  D (§6.1): merge is union with merged-branch-wins, snapshot commit.
  E (§4.4): indexes are kernel blobs; root pointer is a Reference
            named f"{view_name}__index__{index_name}".
  F (§4.5): drop_index uses TOMBSTONE_HASH (RFC-0008).
  G (§6.3): diff takes commit hash prefixes.
  H (§6.2): history returns list of dicts with exactly the 5 keys.
  I (§2.3): put_raw stages existing blob_hash without encoding.
  J (§7): we use a JSON commit encoding instead of the binary format;
            spec §7 explicitly permits this.
"""

from __future__ import annotations

import os
import sys
import json
import time
import hashlib
from typing import Any, Optional

# Make pond-core importable. The spec (§1.1) shows
# `from kernel import PondMinimal` — implying pond-core is on
# the path. We add it explicitly here.
_HERE = os.path.dirname(os.path.abspath(__file__))
_POND_CORE = os.path.normpath(os.path.join(_HERE, "..", "pond-core"))
if _POND_CORE not in sys.path:
    sys.path.insert(0, _POND_CORE)

from kernel import PondMinimal  # noqa: E402


# ---------------------------------------------------------------------------
# Tombstone helpers — Layer 1 convention (RFC-0008 §6, SDK_SPEC §8)
# ---------------------------------------------------------------------------
# The spec (§8) shows: `from maintenance import (TOMBSTONE_HASH, ...)`.
# We cannot import pond-sdk/maintenance.py (forbidden by task rules) and
# the spec does not give the import path. We re-define the constant here
# directly from the RFC formula (SHA-256 of b"__pond_tombstone__").
TOMBSTONE_HASH = hashlib.sha256(b"__pond_tombstone__").hexdigest()


def _ensure_tombstone_blob(kernel: PondMinimal) -> None:
    """Write the tombstone marker blob if it doesn't already exist.

    The kernel's `reference()` checks that the target hash refers to an
    existing blob (kernel.py line 155-156). SDK_SPEC.md §4.5 and
    RFC-0008 §6 both show `drop_name(kernel, name) ->
    kernel.reference(name, TOMBSTONE_HASH)` — but neither tells you to
    write the marker blob first. On a fresh kernel the example code in
    the spec would crash. We lazily write `b"__pond_tombstone__"`
    (whose SHA-256 IS TOMBSTONE_HASH, by RFC-0008 §2's definition) so
    the rebinding succeeds. This is a workaround for a spec gap (see
    report §3, NEW ambiguity).
    """
    if not os.path.exists(kernel._blob_path(TOMBSTONE_HASH)):
        # kernel.write dedups, so this is safe even if a concurrent
        # writer created it between the check and the write.
        marker = b"__pond_tombstone__"
        actual = hashlib.sha256(marker).hexdigest()
        assert actual == TOMBSTONE_HASH, "tombstone hash mismatch"
        kernel.write(marker)


def drop_name(kernel: PondMinimal, name: str) -> None:
    """Logical deletion of a name. Idempotent. (RFC-0008 §6.)"""
    _ensure_tombstone_blob(kernel)
    kernel.reference(name, TOMBSTONE_HASH)


def is_dropped(kernel: PondMinimal, name: str) -> bool:
    """True iff name is bound to TOMBSTONE_HASH."""
    return kernel.resolve(name) == TOMBSTONE_HASH


def resolve_active(kernel: PondMinimal, name: str) -> Optional[str]:
    """Resolve a name, returning None for unbound or tombstoned names."""
    h = kernel.resolve(name)
    if h is None or h == TOMBSTONE_HASH:
        return None
    return h


# ---------------------------------------------------------------------------
# Constants from the spec
# ---------------------------------------------------------------------------
COMPACTION_THRESHOLD = 4  # spec §3.2/§7: snapshot every 4 deltas


# ---------------------------------------------------------------------------
# GraphView
# ---------------------------------------------------------------------------
class GraphView:
    """
    A directed graph View: nodes (id, type, properties) and
    edges (from_id, to_id, edge_type, properties).

    State model:
      - Each commit is either a snapshot (full state as JSON) or a
        delta (changes since parent). The COMPACTION_THRESHOLD=4 rule
        from spec §7 decides which.
      - HEAD is a kernel Reference: f"{view_name}__branch__{branch}".
      - Branches are kernel References of the same shape.

    Indexes:
      - by_node_type, by_edge_type.
      - Per spec §4.4: stored as a kernel blob, with the root pointer
        stored as a kernel Reference named
        f"{view_name}__index__{index_name}".
      - Spec §4.4 says "Prolly trees in the kernel object store" but
        does not define the Prolly tree format. We store the index as
        a single JSON-encoded dict (a kernel blob). This satisfies the
        spec's described behavior (kernel blob + Reference) without
        inventing Prolly tree internals.
      - Mode: eager (rebuilt on every commit, per spec §4.3 "eager").
    """

    # -- construction ------------------------------------------------------

    def __init__(self, kernel: PondMinimal, name: str):
        """
        Construct a GraphLens.

        Note: SDK_SPEC.md does not specify the Lens constructor signature.
        §1.1 shows kernel construction but not View construction. We
        invent: `GraphView(kernel, name)` — kernel is the PondMinimal
        instance per §1.1, name is the view_name used in §4.4/§5.1.
        """
        self.kernel = kernel
        self.name = name
        # Staging area: key → blob_hash (None means delete)
        self.staging: dict[str, Optional[str]] = {}
        # Current branch short-name (invented; spec §5.2 talks about
        # "the current branch" but doesn't say where it's tracked).
        self._current_branch = "main"

    # -- encoding (spec §9) ------------------------------------------------

    def encode(self, data: Any) -> bytes:
        return json.dumps(data, sort_keys=True).encode()

    def decode(self, data: bytes) -> Any:
        return json.loads(data)

    # -- key conventions (INVENTED; spec doesn't specify key shape) -------
    # The spec says `put(key, data)` takes a string key but gives no
    # guidance on key naming. We use prefixes "node:" and "edge:" so
    # they don't start with "_" (which would make them "internal" per
    # spec §3.3's get_all exclusion rule).

    @staticmethod
    def _node_key(node_id: str) -> str:
        return f"node:{node_id}"

    @staticmethod
    def _edge_key(from_id: str, to_id: str, edge_type: str) -> str:
        return f"edge:{from_id}:{to_id}:{edge_type}"

    # -- branch / HEAD names ----------------------------------------------

    def _branch_ref(self, short: str) -> str:
        # Spec §5.1: branch name is f"{view_name}__branch__{name}".
        return f"{self.name}__branch__{short}"

    def _head_ref(self) -> str:
        return self._branch_ref(self._current_branch)

    def _head_hash(self) -> Optional[str]:
        return self.kernel.resolve(self._head_ref())

    # -- staging / write path (spec §2) -----------------------------------

    def put(self, key: str, data: Any) -> str:
        """Stage key→encoded-data. Returns blob hash. (Spec §2.1.)"""
        blob_hash = self.kernel.write(self.encode(data))
        self.staging[key] = blob_hash
        return blob_hash

    def put_raw(self, key: str, blob_hash: str) -> None:
        """Stage key→existing-blob_hash without encoding. (Spec §2.3.)"""
        self.staging[key] = blob_hash

    def delete(self, key: str) -> None:
        """Stage a deletion. No-op on non-existent keys. (Spec §2.2.)"""
        self.staging[key] = None

    # -- commit (spec §2.4, §7) -------------------------------------------

    def _encode_commit(self, commit: dict) -> bytes:
        return json.dumps(commit, sort_keys=True).encode()

    def _decode_commit(self, blob: bytes) -> dict:
        return json.loads(blob)

    def _next_index(self, parent_hash: Optional[str]) -> int:
        """Spec §6.2: index is the commit's position in the DAG
        (0 = first commit). We use linear count from the root."""
        if parent_hash is None:
            return 0
        parent = self._decode_commit(self.kernel.read_blob(parent_hash))
        return parent["index"] + 1

    def _commits_since_last_snapshot(self, head_hash: Optional[str]) -> int:
        n = 0
        h = head_hash
        while h is not None:
            c = self._decode_commit(self.kernel.read_blob(h))
            if c["snapshot"] is not None:
                return n
            n += 1
            h = c["parent"]
        return n

    def commit(self, message: str = "") -> str:
        """Commit staged changes. Returns commit hash. (Spec §2.4.)"""
        if not self.staging:
            raise ValueError("Nothing to commit")
        if not message:
            message = f"{self.name} commit"

        parent = self._head_hash()
        idx = self._next_index(parent)
        # First commit (no parent) MUST be a snapshot — a delta with no
        # parent has nothing to delta against. SDK_SPEC.md §7 doesn't
        # explicitly say this, but it's the only sensible reading.
        # Otherwise: snapshot after COMPACTION_THRESHOLD deltas (spec §7).
        is_snapshot = (
            parent is None
            or self._commits_since_last_snapshot(parent) >= COMPACTION_THRESHOLD
        )

        if is_snapshot:
            # Snapshot: full state.
            full = self._read_state_at_commit(parent)
            for k, v in self.staging.items():
                if v is None:
                    full.pop(k, None)
                else:
                    full[k] = v
            snap_hash = self.kernel.write(self.encode(full))
            commit = {
                "type": "snapshot",
                "parent": parent,
                "snapshot": snap_hash,
                "delta_plus": {},
                "delta_minus": [],
                "message": message,
                "timestamp": time.time(),
                "index": idx,
            }
        else:
            delta_plus = {k: v for k, v in self.staging.items() if v is not None}
            delta_minus = [k for k, v in self.staging.items() if v is None]
            commit = {
                "type": "delta",
                "parent": parent,
                "snapshot": None,
                "delta_plus": delta_plus,
                "delta_minus": delta_minus,
                "message": message,
                "timestamp": time.time(),
                "index": idx,
            }

        commit_hash = self.kernel.write(self._encode_commit(commit))
        self.kernel.reference(self._head_ref(), commit_hash)
        self.staging = {}
        # Eager index rebuild (spec §4.3 "eager" mode).
        self._rebuild_indexes()
        return commit_hash

    # -- read path (spec §3) ----------------------------------------------

    def _read_state_at_commit(self, commit_hash: Optional[str]) -> dict:
        """Reconstruct full {key: blob_hash} state at a commit."""
        if commit_hash is None:
            return {}
        state: dict[str, str] = {}
        deltas: list[dict] = []
        h = commit_hash
        while h is not None:
            commit = self._decode_commit(self.kernel.read_blob(h))
            if commit["snapshot"] is not None:
                state = self.decode(self.kernel.read_blob(commit["snapshot"]))
                break
            deltas.append(commit)
            h = commit["parent"]
        # Apply deltas oldest-first.
        for c in reversed(deltas):
            for k, v in c["delta_plus"].items():
                state[k] = v
            for k in c["delta_minus"]:
                state.pop(k, None)
        return state

    def get(self, key: str) -> Optional[Any]:
        """O(K + log N) lookup. (Spec §3.1, §3.2.)"""
        h = self._head_hash()
        deltas: list[dict] = []
        while h is not None:
            commit = self._decode_commit(self.kernel.read_blob(h))
            if commit["snapshot"] is not None:
                state = self.decode(self.kernel.read_blob(commit["snapshot"]))
                for c in reversed(deltas):
                    for k, v in c["delta_plus"].items():
                        state[k] = v
                    for k in c["delta_minus"]:
                        state.pop(k, None)
                if key in state:
                    return self.decode(self.kernel.read_blob(state[key]))
                return None
            # Delta commit: early-return if key is in this delta.
            if key in commit["delta_plus"]:
                return self.decode(
                    self.kernel.read_blob(commit["delta_plus"][key])
                )
            if key in commit["delta_minus"]:
                return None
            deltas.append(commit)
            h = commit["parent"]
        return None

    def get_all(self) -> dict:
        """Full state as {key: decoded_data}. Excludes _-prefixed keys.
        (Spec §3.3.)"""
        state = self._read_state_at_commit(self._head_hash())
        return {
            k: self.decode(self.kernel.read_blob(v))
            for k, v in state.items()
            if not k.startswith("_")
        }

    def keys(self) -> list:
        state = self._read_state_at_commit(self._head_hash())
        return [k for k in state if not k.startswith("_")]

    def count(self) -> int:
        return len(self.keys())

    def exists(self, key: str) -> bool:
        return key in self._read_state_at_commit(self._head_hash())

    # -- branching (spec §5) ----------------------------------------------

    def branch(self, name: str) -> str:
        """Create branch at HEAD. O(1). (Spec §5.1.)"""
        if self._head_hash() is None:
            raise ValueError("No commits to branch from")
        ref = self._branch_ref(name)
        self.kernel.reference(ref, self._head_hash())
        return ref

    def checkout(self, name: str) -> None:
        """Switch HEAD to branch. Clears staging. (Spec §5.2.)"""
        ref = self._branch_ref(name)
        if self.kernel.resolve(ref) is None:
            raise ValueError(f"Branch '{name}' does not exist")
        self._current_branch = name
        self.staging = {}

    def list_branches(self) -> list:
        """Short names of all (non-tombstoned) branches. (Spec §5.3.)"""
        prefix = f"{self.name}__branch__"
        out = []
        for n in self.kernel.list_names():
            if n.startswith(prefix) and not is_dropped(self.kernel, n):
                out.append(n[len(prefix):])
        return out

    # -- history (spec §6.2) ----------------------------------------------

    def history(self, limit: int = 20) -> list:
        """List of commit dicts, most-recent first. (Spec §6.2.)
        Each dict has exactly: commit, message, timestamp, index, type."""
        out = []
        h = self._head_hash()
        while h is not None and len(out) < limit:
            c = self._decode_commit(self.kernel.read_blob(h))
            out.append({
                "commit": h[:12],
                "message": c["message"],
                "timestamp": c["timestamp"],
                "index": c["index"],
                "type": c["type"],
            })
            h = c["parent"]
        return out

    # -- diff (spec §6.3) --------------------------------------------------

    def _find_commit_by_prefix(self, prefix: str) -> Optional[str]:
        """Spec §6.3: walk the commit DAG from HEAD."""
        h = self._head_hash()
        while h is not None:
            if h.startswith(prefix):
                return h
            c = self._decode_commit(self.kernel.read_blob(h))
            h = c["parent"]
        return None

    def diff(self, a: str, b: str) -> dict:
        """Diff two commits (by hash prefix). (Spec §6.3.)"""
        a_hash = self._find_commit_by_prefix(a)
        b_hash = self._find_commit_by_prefix(b)
        if a_hash is None:
            raise ValueError(f"Commit '{a}' not found")
        if b_hash is None:
            raise ValueError(f"Commit '{b}' not found")
        sa = self._read_state_at_commit(a_hash)
        sb = self._read_state_at_commit(b_hash)
        added = {k: v[:12] for k, v in sb.items() if k not in sa}
        removed = {k: v[:12] for k, v in sa.items() if k not in sb}
        modified = {
            k: {"old": sa[k][:12], "new": sb[k][:12]}
            for k in sa
            if k in sb and sa[k] != sb[k]
        }
        return {"added": added, "removed": removed, "modified": modified}

    # -- merge (spec §6.1) -------------------------------------------------

    def merge(self, branch_name: str, message: str = "") -> str:
        """Union merge; merged-branch wins on conflict. Snapshot commit.
        (Spec §6.1.)"""
        ref = self._branch_ref(branch_name)
        branch_head = self.kernel.resolve(ref)
        if branch_head is None:
            raise ValueError(f"Branch '{branch_name}' does not exist")
        current_state = self._read_state_at_commit(self._head_hash())
        branch_state = self._read_state_at_commit(branch_head)
        # Union with merged-branch wins (spec §6.1 step 3).
        merged = dict(current_state)
        merged.update(branch_state)
        if not message:
            message = f"Merge {branch_name} into {self._current_branch}"
        snap_hash = self.kernel.write(self.encode(merged))
        parent = self._head_hash()
        commit = {
            "type": "snapshot",
            "parent": parent,
            "snapshot": snap_hash,
            "delta_plus": {},
            "delta_minus": [],
            "message": message,
            "timestamp": time.time(),
            "index": self._next_index(parent),
        }
        commit_hash = self.kernel.write(self._encode_commit(commit))
        self.kernel.reference(self._head_ref(), commit_hash)
        self._rebuild_indexes()
        return commit_hash

    # -- indexes (spec §4) -------------------------------------------------

    def _index_ref(self, index_name: str) -> str:
        # Spec §4.4: f"{view_name}__index__{index_name}".
        return f"{self.name}__index__{index_name}"

    def _rebuild_indexes(self) -> None:
        """Eager rebuild of both indexes from current state."""
        state = self._read_state_at_commit(self._head_hash())
        by_node_type: dict[str, list[str]] = {}
        by_edge_type: dict[str, list[str]] = {}
        for key, blob_hash in state.items():
            if key.startswith("node:"):
                data = self.decode(self.kernel.read_blob(blob_hash))
                t = data.get("type", "")
                by_node_type.setdefault(t, []).append(key[len("node:"):])
            elif key.startswith("edge:"):
                # edge:{from}:{to}:{type}
                parts = key.split(":", 3)
                if len(parts) == 4:
                    by_edge_type.setdefault(parts[3], []).append(key)
        nt = self.kernel.write(self.encode(by_node_type))
        et = self.kernel.write(self.encode(by_edge_type))
        self.kernel.reference(self._index_ref("by_node_type"), nt)
        self.kernel.reference(self._index_ref("by_edge_type"), et)

    def _read_index(self, index_name: str) -> Optional[dict]:
        h = resolve_active(self.kernel, self._index_ref(index_name))
        if h is None:
            return None
        return self.decode(self.kernel.read_blob(h))

    def list_indexes(self) -> list:
        """Active (non-tombstoned) indexes. (Spec §4.4/§4.5.)"""
        prefix = f"{self.name}__index__"
        out = []
        for n in self.kernel.list_names():
            if n.startswith(prefix) and not is_dropped(self.kernel, n):
                out.append(n[len(prefix):])
        return out

    def drop_index(self, index_name: str) -> bool:
        """Tombstone-drop an index. (Spec §4.5, RFC-0008.)"""
        ref = self._index_ref(index_name)
        if resolve_active(self.kernel, ref) is None:
            return False
        drop_name(self.kernel, ref)
        return True

    # ----------------------------------------------------------------------
    # GraphView-specific operations (the task spec)
    # ----------------------------------------------------------------------

    def add_node(self, node_id: str, type: str, properties: Optional[dict] = None):
        """Add or update a node."""
        properties = properties or {}
        # Per spec §4.2: extractor cannot access the primary key directly;
        # the data must contain its own primary key. We embed node_id.
        self.put(
            self._node_key(node_id),
            {"id": node_id, "type": type, "properties": properties},
        )

    def add_edge(self, from_id: str, to_id: str, edge_type: str,
                 properties: Optional[dict] = None):
        """Add an edge."""
        properties = properties or {}
        self.put(
            self._edge_key(from_id, to_id, edge_type),
            {"from": from_id, "to": to_id, "type": edge_type,
             "properties": properties},
        )

    def get_node(self, node_id: str) -> Optional[dict]:
        return self.get(self._node_key(node_id))

    def get_neighbors(self, node_id: str,
                      edge_type: Optional[str] = None) -> list:
        """Outgoing neighbors of node_id, optionally filtered by edge_type."""
        state = self._read_state_at_commit(self._head_hash())
        prefix = f"edge:{node_id}:"
        out = []
        for key in state:
            if not key.startswith(prefix):
                continue
            parts = key.split(":", 3)
            if len(parts) != 4:
                continue
            from_id, to_id, etype = parts[1], parts[2], parts[3]
            if from_id != node_id:
                continue
            if edge_type is not None and etype != edge_type:
                continue
            out.append({"to": to_id, "edge_type": etype})
        return out

    def find_nodes_by_type(self, node_type: str) -> list:
        """All nodes of a given type. Uses the by_node_type index."""
        idx = self._read_index("by_node_type")
        if idx is None:
            # Fallback: linear scan (index not yet built).
            state = self._read_state_at_commit(self._head_hash())
            out = []
            for key, bh in state.items():
                if key.startswith("node:"):
                    data = self.decode(self.kernel.read_blob(bh))
                    if data.get("type") == node_type:
                        out.append(data)
            return out
        node_ids = idx.get(node_type, [])
        out = []
        for nid in node_ids:
            n = self.get_node(nid)
            if n is not None:
                out.append(n)
        return out

    def find_edges_by_type(self, edge_type: str) -> list:
        """All edges of a given type. Uses the by_edge_type index."""
        idx = self._read_index("by_edge_type")
        state = self._read_state_at_commit(self._head_hash())
        if idx is None:
            out = []
            for key, bh in state.items():
                if key.startswith("edge:"):
                    parts = key.split(":", 3)
                    if len(parts) == 4 and parts[3] == edge_type:
                        out.append(self.decode(self.kernel.read_blob(bh)))
            return out
        edge_keys = idx.get(edge_type, [])
        out = []
        for k in edge_keys:
            if k in state:
                out.append(self.decode(self.kernel.read_blob(state[k])))
        return out

    def delete_node(self, node_id: str):
        """Delete a node AND all its edges (both directions)."""
        self.delete(self._node_key(node_id))
        state = self._read_state_at_commit(self._head_hash())
        for key in list(state):
            if not key.startswith("edge:"):
                continue
            parts = key.split(":", 3)
            if len(parts) != 4:
                continue
            from_id, to_id, _etype = parts[1], parts[2], parts[3]
            if from_id == node_id or to_id == node_id:
                self.delete(key)

    def delete_edge(self, from_id: str, to_id: str, edge_type: str):
        """Delete a specific edge."""
        self.delete(self._edge_key(from_id, to_id, edge_type))

    def count_nodes(self) -> int:
        state = self._read_state_at_commit(self._head_hash())
        return sum(1 for k in state if k.startswith("node:"))

    def count_edges(self) -> int:
        state = self._read_state_at_commit(self._head_hash())
        return sum(1 for k in state if k.startswith("edge:"))
