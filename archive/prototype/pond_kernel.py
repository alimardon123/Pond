"""
Pond storage kernel — bytes-only, universal.

This is the substrate layer. It knows NOTHING about:
  - Parquet, Arrow, ORC, Lance, JSON, or any data format
  - SQL, tables, schemas, or relational concepts
  - Vectors, embeddings, or ML
  - Streaming, logs, or topics
  - Graphs, nodes, or edges

It exposes exactly four operations (per RFC 1):
  - Write(bytes)      append a fragment to an OPEN object
  - Seal(open_hash)   convert OPEN -> SEALED, return content hash
  - Read(hash_or_name) return bytes
  - Reference(name, hash) set a mutable name -> hash mapping

And the DAG pattern:
  - blob   = Seal(Write(bytes))
  - tree   = Seal(Write(serialized_entries))
  - commit = Seal(Write({tree_hash, parent_hash, ...}))
  - tag    = Reference(name, commit_hash)

Views (SQL, vector, streaming, graph) are built ON TOP of this kernel,
in separate files. They use only the four syscalls + DAG patterns.
The kernel never calls back into Views.

This file is the answer to the user's question:
  > Can I delete SQL capability and storage still works?

Yes. This file has no SQL. It has no Parquet. It has no Arrow.
It just stores and retrieves bytes by hash.
"""

from __future__ import annotations

import os
import json
import time
import uuid
import sqlite3
import hashlib
from dataclasses import dataclass, field, asdict
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POND_DIR_NAME = ".pond"
OBJECTS_DIR = "objects"          # sealed blobs (any format — bytes-only)
OPEN_DIR = "open"                # OPEN object byte streams (in-flight)
ROOT_STORE_DB = "roots.sqlite"   # mutable name -> hash namespace

# Hierarchical tree fanout (for tree-of-trees metadata structure)
TREE_FANOUT = 256


def hash_bytes(data: bytes) -> str:
    """SHA-256 of bytes, hex-encoded. The content address."""
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# DAG object types (patterns over the 4 syscalls, per RFC 1 section 5.2)
# ---------------------------------------------------------------------------

@dataclass
class Tree:
    """
    A directory mapping {name -> hash}. Two variants:
      - leaf tree: entries are {name -> blob_hash}
      - interior tree: entries are {name -> subtree_hash}

    The kernel doesn't interpret entry names. "sql/events/data/0",
    "vector/embeddings/0", "stream/log/0" are all just strings to the kernel.
    Views impose naming conventions; the kernel stores them opaquely.
    """
    entries: dict[str, str] = field(default_factory=dict)
    tree_type: str = "leaf"   # "leaf" | "interior"
    kind: str = "tree"


@dataclass
class Commit:
    """
    A snapshot pointer: {tree_hash, parent_hash, timestamp, message}.
    The kernel doesn't interpret message content. Views can use it for
    metadata (e.g., "schema_version=2" or "vector_dim=128").
    """
    tree_hash: str
    parent_hash: Optional[str]
    timestamp: float
    message: str
    schema_hash: Optional[str] = None  # opaque to kernel; Views use as needed
    kind: str = "commit"


# ---------------------------------------------------------------------------
# OPEN object — mutable, appendable byte stream
# ---------------------------------------------------------------------------

class OpenObject:
    """
    An OPEN object: mutable, appendable byte stream on local disk.
    Each Write appends a chunk of bytes. Seal closes the stream and
    returns the content hash.

    The kernel doesn't interpret the bytes. They could be Arrow IPC,
    Parquet, JSON, raw vectors, image bytes, anything. The View layer
    decides what to put in and how to read it out.
    """

    def __init__(self, path: str):
        self.path = path
        self.fragment_count = 0
        self.byte_count = 0
        self._sink = open(path, "wb")

    def write(self, data: bytes) -> None:
        """Append a chunk of bytes to the OPEN object."""
        # Length-prefix each fragment so reads can delimit them
        import struct
        self._sink.write(struct.pack("<Q", len(data)))
        self._sink.write(data)
        self.fragment_count += 1
        self.byte_count += len(data)

    def seal(self) -> bytes:
        """Close the stream and return all bytes."""
        self._sink.close()
        with open(self.path, "rb") as f:
            return f.read()


# ---------------------------------------------------------------------------
# The Pond storage kernel — 4 syscalls + DAG
# ---------------------------------------------------------------------------

class PondKernel:
    """
    The bytes-only storage kernel. Exposes the 4 syscalls and the DAG pattern.

    v0: single-node, local filesystem, SQLite root store, no replication.
    The kernel has NO knowledge of data formats, SQL, or workload types.
    """

    def __init__(self, base_dir: str):
        self.base_dir = os.path.abspath(base_dir)
        self.pond_dir = os.path.join(self.base_dir, POND_DIR_NAME)
        self.objects_dir = os.path.join(self.pond_dir, OBJECTS_DIR)
        self.open_dir = os.path.join(self.pond_dir, OPEN_DIR)
        self.root_store_path = os.path.join(self.pond_dir, ROOT_STORE_DB)

        os.makedirs(self.objects_dir, exist_ok=True)
        os.makedirs(self.open_dir, exist_ok=True)

        # Root pointer namespace — single-writer for v0
        # (production: Raft-replicated or external KV)
        self.root_db = sqlite3.connect(self.root_store_path, isolation_level=None)
        self.root_db.execute("""
            CREATE TABLE IF NOT EXISTS roots (
                name TEXT PRIMARY KEY,
                commit_hash TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        """)

        # In-flight OPEN objects, keyed by an opaque handle (View chooses)
        self._open_objects: dict[str, OpenObject] = {}

        self.stats = {
            "writes": 0,
            "bytes_written": 0,
            "seals": 0,
            "reads": 0,
            "bytes_read": 0,
        }

    # ------------------------------------------------------------------
    # Syscall 1: Write — append bytes to an OPEN object
    # ------------------------------------------------------------------

    def open(self, handle: str) -> str:
        """Create a new OPEN object with the given opaque handle.
        Returns the handle. The kernel doesn't interpret the handle —
        Views use it to track which OPEN object belongs to which view."""
        if handle in self._open_objects:
            raise ValueError(f"OPEN object '{handle}' already exists")
        open_path = os.path.join(self.open_dir, f"{handle}__{uuid.uuid4().hex}.bin")
        self._open_objects[handle] = OpenObject(open_path)
        return handle

    def write(self, handle: str, data: bytes) -> None:
        """Append bytes to an OPEN object identified by handle.
        Multiple writes to the same handle produce a multi-fragment stream.
        Use write_blob() for single-shot complete blobs."""
        if handle not in self._open_objects:
            self.open(handle)
        self._open_objects[handle].write(data)
        self.stats["writes"] += 1
        self.stats["bytes_written"] += len(data)

    def write_blob(self, data: bytes) -> str:
        """Single-shot: write a complete blob and seal it.
        Returns the content hash. Equivalent to open+write+seal but
        without length-prefixing (the blob IS the bytes, not a stream of fragments).
        Use this when you have a complete serialized object (e.g., a Parquet file)."""
        sealed_hash = hash_bytes(data)
        shard_dir = os.path.join(self.objects_dir, sealed_hash[:2])
        os.makedirs(shard_dir, exist_ok=True)
        final_path = os.path.join(shard_dir, sealed_hash + ".bin")
        if not os.path.exists(final_path):
            with open(final_path, "wb") as f:
                f.write(data)
        self.stats["writes"] += 1
        self.stats["bytes_written"] += len(data)
        self.stats["seals"] += 1
        return sealed_hash

    # ------------------------------------------------------------------
    # Syscall 2: Seal — convert OPEN to SEALED, return content hash
    # ------------------------------------------------------------------

    def seal(self, handle: str) -> str:
        """Seal the OPEN object. Returns the content hash of the sealed bytes.
        The kernel doesn't interpret the bytes — they could be any format."""
        if handle not in self._open_objects:
            raise ValueError(f"No OPEN object for handle '{handle}'")

        open_obj = self._open_objects[handle]
        sealed_bytes = open_obj.seal()
        sealed_hash = hash_bytes(sealed_bytes)

        # Content-addressed storage
        shard_dir = os.path.join(self.objects_dir, sealed_hash[:2])
        os.makedirs(shard_dir, exist_ok=True)
        final_path = os.path.join(shard_dir, sealed_hash + ".bin")
        # Only write if not already present (content-addressing dedup)
        if not os.path.exists(final_path):
            with open(final_path, "wb") as f:
                f.write(sealed_bytes)

        # Clean up the OPEN object's temp file
        if os.path.exists(open_obj.path):
            os.remove(open_obj.path)
        del self._open_objects[handle]

        self.stats["seals"] += 1
        return sealed_hash

    # ------------------------------------------------------------------
    # Syscall 3: Read — fetch bytes by hash or name
    # ------------------------------------------------------------------

    def read(self, hash_or_name: str) -> bytes:
        """Read a sealed blob by content hash, or resolve a name to a commit
        and read all blobs referenced by that commit's tree.

        For name reads: walks the tree and concatenates all blobs in
        entry-name-sorted order. Views that want specific interpretation
        (e.g., Parquet schema) should use read_blob(hash) directly and
        interpret the bytes themselves.
        """
        self.stats["reads"] += 1

        # If 64-char hex, treat as a blob hash directly
        if len(hash_or_name) == 64 and all(c in "0123456789abcdef" for c in hash_or_name):
            return self.read_blob(hash_or_name)

        # Otherwise, resolve as a name -> commit -> walk tree
        commit_hash = self._resolve_name(hash_or_name)
        if commit_hash is None:
            raise ValueError(f"Name '{hash_or_name}' is not bound in the root namespace")

        # Walk the tree, collect all blob hashes, concatenate bytes
        # (For real Views, you'd use read_blob on each and interpret per-format)
        commit = self.read_commit(commit_hash)
        if commit is None:
            raise ValueError(f"Commit {commit_hash} not found")

        blob_hashes = self._walk_tree_for_blobs(commit.tree_hash)
        result = b""
        for bh in blob_hashes:
            result += self.read_blob(bh)
        self.stats["bytes_read"] += len(result)
        return result

    def read_blob(self, blob_hash: str) -> bytes:
        """Read a single sealed blob by content hash. Returns raw bytes.
        The caller (Lens) interprets the bytes."""
        path = self._blob_path(blob_hash)
        if not os.path.exists(path):
            raise ValueError(f"Blob {blob_hash} not found on disk")
        with open(path, "rb") as f:
            return f.read()

    # ------------------------------------------------------------------
    # Syscall 4: Reference — set a mutable name -> hash mapping
    # ------------------------------------------------------------------

    def reference(self, name: str, commit_hash: str) -> None:
        """Set a mutable name -> commit_hash mapping in the root namespace."""
        if not self._commit_exists(commit_hash):
            raise ValueError(f"Commit hash {commit_hash} does not exist")
        self._set_root(name, commit_hash)

    # ------------------------------------------------------------------
    # DAG pattern helpers — for Lenses to build trees, commits, tags
    # ------------------------------------------------------------------

    def write_tree(self, tree: Tree) -> str:
        """Persist a Tree object. Returns its content hash."""
        data = json.dumps(asdict(tree), sort_keys=True).encode()
        h = hash_bytes(data)
        path = self._meta_path(h)
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
        return h

    def read_tree(self, tree_hash: str) -> Optional[Tree]:
        path = self._meta_path(tree_hash)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            data = json.loads(f.read())
        data.setdefault("tree_type", "leaf")
        return Tree(**{k: v for k, v in data.items() if k != "kind"})

    def write_commit(self, commit: Commit) -> str:
        """Persist a Commit object. Returns its content hash."""
        data = json.dumps(asdict(commit), sort_keys=True).encode()
        h = hash_bytes(data)
        path = self._meta_path(h)
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
        return h

    def read_commit(self, commit_hash: str) -> Optional[Commit]:
        path = self._meta_path(commit_hash)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            data = json.loads(f.read())
        return Commit(**{k: v for k, v in data.items() if k != "kind"})

    def walk_tree_for_blobs(self, tree_hash: str) -> list[str]:
        """Public helper: walk a tree (hierarchical) and return all blob hashes.
        The kernel doesn't interpret blob names — it returns all entries
        in leaf trees as blob hashes."""
        return self._walk_tree_for_blobs(tree_hash)

    def history(self, name: str) -> list[dict]:
        """Walk the commit DAG backwards from the current commit for `name`."""
        commit_hash = self._resolve_name(name)
        if commit_hash is None:
            return []
        history = []
        current = commit_hash
        visited = set()
        while current is not None and current not in visited:
            visited.add(current)
            commit = self.read_commit(current)
            if commit is None:
                break
            history.append({
                "commit": current,
                "timestamp": commit.timestamp,
                "message": commit.message,
                "parent": commit.parent_hash,
            })
            current = commit.parent_hash
        return history

    def list_names(self) -> list[str]:
        cur = self.root_db.execute("SELECT name FROM roots ORDER BY name")
        return [row[0] for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Internal: DAG traversal
    # ------------------------------------------------------------------

    def _walk_tree_for_blobs(self, tree_hash: str) -> list[str]:
        tree = self.read_tree(tree_hash)
        if tree is None:
            return []
        blobs: list[str] = []
        for name, h in tree.entries.items():
            if tree.tree_type == "interior":
                # subtree or leaf reference — recurse
                if name.startswith("subtree/") or name.startswith("leaf/"):
                    blobs.extend(self._walk_tree_for_blobs(h))
                else:
                    # Direct blob ref in an interior tree
                    blobs.append(h)
            else:
                # leaf tree — entries are blob refs
                blobs.append(h)
        return blobs

    def _commit_exists(self, commit_hash: str) -> bool:
        return os.path.exists(self._meta_path(commit_hash))

    def _meta_path(self, h: str) -> str:
        return os.path.join(self.objects_dir, h[:2], h + ".json")

    def _blob_path(self, h: str) -> str:
        return os.path.join(self.objects_dir, h[:2], h + ".bin")

    def _set_root(self, name: str, commit_hash: str) -> None:
        self.root_db.execute(
            "INSERT OR REPLACE INTO roots (name, commit_hash, updated_at) VALUES (?, ?, ?)",
            (name, commit_hash, time.time())
        )

    def _resolve_name(self, name: str) -> Optional[str]:
        cur = self.root_db.execute(
            "SELECT commit_hash FROM roots WHERE name = ?", (name,)
        )
        row = cur.fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # Stats / introspection
    # ------------------------------------------------------------------

    def storage_stats(self) -> dict:
        data_bytes = 0
        meta_bytes = 0
        blob_count = 0
        meta_count = 0

        for shard in os.listdir(self.objects_dir):
            shard_path = os.path.join(self.objects_dir, shard)
            if not os.path.isdir(shard_path):
                continue
            for f in os.listdir(shard_path):
                fpath = os.path.join(shard_path, f)
                size = os.path.getsize(fpath)
                if f.endswith(".bin"):
                    data_bytes += size
                    blob_count += 1
                elif f.endswith(".json"):
                    meta_bytes += size
                    meta_count += 1

        return {
            **self.stats,
            "data_bytes": data_bytes,
            "meta_bytes": meta_bytes,
            "root_store_bytes": os.path.getsize(self.root_store_path),
            "blob_count": blob_count,
            "meta_count": meta_count,
            "meta_to_data_ratio": (meta_bytes / data_bytes) if data_bytes > 0 else 0,
            "name_count": len(self.list_names()),
        }

    def close(self) -> None:
        # Seal any remaining OPEN objects
        for handle in list(self._open_objects.keys()):
            try:
                self.seal(handle)
            except Exception:
                pass
        self.root_db.close()
