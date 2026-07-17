"""
Pond v0 prototype — storage kernel.

Implements the four storage syscalls (Read, Write, Seal, Reference) and the
object lifecycle (OPEN -> SEALED) on a single node, local filesystem.

What this demonstrates:
  - The 4 syscalls work as specified in RFC 1
  - OPEN objects (Arrow IPC) -> SEALED objects (Parquet) state transition
  - Content-addressed DAG (blob/tree/commit/tag as patterns)
  - Root pointer namespace (mutable name -> commit hash, SQLite-backed)
  - Snapshot reads (read at a commit hash returns consistent state)
  - Time travel (read at past commit)
  - Branching (named pointer to a commit)
  - DuckDB as the Parquet reader for OLAP scans (the reference backend)

What this does NOT demonstrate (intentionally, per the prototype scope):
  - Replication (no Raft; single node)
  - Distributed execution (no Exchange)
  - Streaming (no tail-reads)
  - Cross-backend capability routing (only DuckDB)
  - HLC timestamps (uses wall clock)
  - Transactions (single-writer)
  - The Capability trait (hardcoded DuckDB backend)
  - The Planner/PassManager (no IR)
  - PB scale (local FS, not S3)

These are all v2+ concerns. v0 proves the storage abstraction works.
"""

from __future__ import annotations

import os
import json
import time
import uuid
import sqlite3
import hashlib
import shutil
from dataclasses import dataclass, field, asdict
from typing import Any, Optional, Iterator

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import duckdb


# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

POND_DIR_NAME = ".pond"
OBJECTS_DIR = "objects"          # sealed Parquet files (content-addressed)
OPEN_DIR = "open"                # OPEN object Arrow IPC files (in-flight)
ROOT_STORE_DB = "roots.sqlite"   # mutable name -> commit_hash namespace
META_DB = "meta.sqlite"          # Pond instance metadata

# Hierarchical tree fanout. Each subtree holds up to TREE_FANOUT blob refs.
# When a subtree fills, it is sealed and referenced by the parent tree.
# This gives O(log_FANOUT N) tree depth and O(N) total metadata (not O(N^2)).
TREE_FANOUT = 256


# ---------------------------------------------------------------------------
# Content addressing
# ---------------------------------------------------------------------------

def hash_bytes(data: bytes) -> str:
    """SHA-256 of bytes, hex-encoded. The content address."""
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# DAG object types (patterns over the 4 syscalls, per RFC 1 section 5.2)
# ---------------------------------------------------------------------------

@dataclass
class Blob:
    """Raw bytes — a Parquet file, an Arrow fragment, an index, a stats block."""
    hash: str
    size_bytes: int
    kind: str = "blob"   # "data" | "index" | "stats" | ...

@dataclass
class Tree:
    """
    A directory mapping {name -> hash}. Two variants:
      - leaf tree: entries are {blob_name -> blob_hash} (data blobs)
      - interior tree: entries are {subtree_name -> subtree_hash} (other trees)

    This is the Git model: a tree can reference subtrees, giving O(log N) depth
    instead of O(N) flatness. Without hierarchy, every commit copies all prior
    blob references — O(N^2) metadata growth.
    """
    entries: dict[str, str] = field(default_factory=dict)
    tree_type: str = "leaf"   # "leaf" | "interior"
    kind: str = "tree"

@dataclass
class Commit:
    """A snapshot pointer: {tree_hash, parent_commit_hash, timestamp, message}."""
    tree_hash: str
    parent_hash: Optional[str]
    timestamp: float
    message: str
    schema_hash: Optional[str] = None
    kind: str = "commit"

@dataclass
class Tag:
    """A named pointer to a commit. Branches, versions, MVs are tags."""
    name: str
    commit_hash: str
    kind: str = "tag"


# ---------------------------------------------------------------------------
# OPEN object — mutable, appendable, Arrow IPC stream
# ---------------------------------------------------------------------------

class OpenObject:
    """
    An OPEN object: mutable, appendable, Arrow IPC stream on local disk.
    The OPEN object IS the log (per RFC 1 section 4). Each Write appends a
    RecordBatch. Seal converts the stream to a Parquet file (SEALED).

    In v0, this is a single-process, single-writer artifact. In production,
    it would be Raft-replicated across nodes.
    """

    def __init__(self, path: str, schema: pa.Schema):
        self.path = path
        self.schema = schema
        self.fragment_count = 0
        self.row_count = 0
        # Open an IPC stream writer. Each Write appends a RecordBatch.
        self._sink = open(path, "wb")
        self._writer = ipc.new_stream(self._sink, schema)

    def write(self, batch: pa.RecordBatch) -> None:
        """Append a fragment (RecordBatch) to the OPEN object."""
        if batch.schema != self.schema:
            raise ValueError(
                f"Schema mismatch: batch has {batch.schema}, "
                f"OPEN object has {self.schema}"
            )
        self._writer.write_batch(batch)
        self.fragment_count += 1
        self.row_count += batch.num_rows

    def seal(self) -> bytes:
        """Close the IPC stream and return the bytes. Caller writes to S3."""
        self._writer.close()
        self._sink.close()
        with open(self.path, "rb") as f:
            return f.read()

    def close_without_sealing(self) -> None:
        """Abort: close and discard."""
        try:
            self._writer.close()
            self._sink.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# The Pond storage kernel — the 4 syscalls
# ---------------------------------------------------------------------------

class Pond:
    """
    The Pond storage kernel. Exposes the 4 syscalls (Read, Write, Seal,
    Reference) plus the lifecycle (OPEN -> SEALED) and the DAG pattern
    (blob/tree/commit/tag).

    v0: single-node, local filesystem, SQLite root store, DuckDB reader.
    """

    def __init__(self, base_dir: str):
        self.base_dir = os.path.abspath(base_dir)
        self.pond_dir = os.path.join(self.base_dir, POND_DIR_NAME)
        self.objects_dir = os.path.join(self.pond_dir, OBJECTS_DIR)
        self.open_dir = os.path.join(self.pond_dir, OPEN_DIR)
        self.root_store_path = os.path.join(self.pond_dir, ROOT_STORE_DB)
        self.meta_db_path = os.path.join(self.pond_dir, META_DB)

        os.makedirs(self.objects_dir, exist_ok=True)
        os.makedirs(self.open_dir, exist_ok=True)

        # Root pointer namespace (SQLite — single-writer for v0;
        # Raft-replicated in production per RFC 1 section 5.3)
        self.root_db = sqlite3.connect(self.root_store_path, isolation_level=None)
        self.root_db.execute("""
            CREATE TABLE IF NOT EXISTS roots (
                name TEXT PRIMARY KEY,
                commit_hash TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        """)

        # DuckDB connection for Parquet reading (the reference backend)
        self.duck = duckdb.connect(":memory:")

        # In-flight OPEN objects, keyed by table name
        self._open_objects: dict[str, OpenObject] = {}
        # Schemas, keyed by table name
        self._schemas: dict[str, pa.Schema] = {}

        # Stats (for the benchmark)
        self.stats = {
            "writes": 0,
            "rows_written": 0,
            "seals": 0,
            "reads": 0,
            "rows_read": 0,
        }

    # ------------------------------------------------------------------
    # Syscall 1: Write
    # ------------------------------------------------------------------

    def write(self, table_name: str, batch: pa.RecordBatch) -> str:
        """
        Append a fragment (RecordBatch) to the OPEN object for `table_name`.
        Returns the open_hash (a UUID for v0; in production this would be
        the hash of the OPEN object's accumulated bytes).

        Precondition: an OPEN object exists for the table (created on first
        Write if not). Postcondition: the fragment is on disk before return.
        """
        if table_name not in self._open_objects:
            # First write to this table since last seal — create a new OPEN object
            schema = batch.schema
            self._schemas[table_name] = schema
            open_path = os.path.join(
                self.open_dir, f"{table_name}__{uuid.uuid4().hex}.arrow"
            )
            self._open_objects[table_name] = OpenObject(open_path, schema)

        self._open_objects[table_name].write(batch)
        self.stats["writes"] += 1
        self.stats["rows_written"] += batch.num_rows
        return self._open_objects[table_name].path

    # ------------------------------------------------------------------
    # Syscall 2: Seal
    # ------------------------------------------------------------------

    def seal(self, table_name: str, message: str = "") -> str:
        """
        Convert the OPEN object for `table_name` into a SEALED Parquet file
        on object storage. Creates a new commit in the DAG and updates the
        root pointer namespace. Returns the new commit hash.

        Precondition: an OPEN object exists for the table.
        Postcondition: the OPEN object is no longer writable. The sealed
        Parquet is content-addressed by hash(bytes). The root pointer for
        the table now points to the new commit.
        """
        if table_name not in self._open_objects:
            raise ValueError(f"No OPEN object for table '{table_name}'")

        open_obj = self._open_objects[table_name]
        arrow_bytes = open_obj.seal()

        # Convert Arrow IPC -> Parquet bytes (the SEALED format)
        # Read the IPC stream back, write as Parquet to a buffer
        reader = ipc.open_stream(pa.BufferReader(arrow_bytes))
        table = pa.Table.from_batches(reader, open_obj.schema)

        # Use a temporary file then content-address it
        temp_parquet = open_obj.path + ".parquet"
        pq.write_table(table, temp_parquet, compression="zstd")

        with open(temp_parquet, "rb") as f:
            parquet_bytes = f.read()
        sealed_hash = hash_bytes(parquet_bytes)

        # Content-addressed storage: sharded path like ab/abcdef...
        shard_dir = os.path.join(self.objects_dir, sealed_hash[:2])
        os.makedirs(shard_dir, exist_ok=True)
        final_path = os.path.join(shard_dir, sealed_hash + ".parquet")
        os.rename(temp_parquet, final_path)

        # Build the DAG objects: blob, tree, commit
        blob = Blob(hash=sealed_hash, size_bytes=len(parquet_bytes), kind="data")

        # Parent is the previous commit for this table (if any)
        parent_hash = self._resolve_name(table_name)

        # ---- Hierarchical tree construction (Git model + delta chain) ----
        #
        # Each commit creates ONE new tiny leaf tree containing just the new
        # blob. The root tree references:
        #   - all sealed subtrees from prior commits (inherited from parent)
        #   - all unsealed single-blob leaves since the last subtree seal
        #
        # When the count of unsealed single-blob leaves reaches TREE_FANOUT,
        # they are compacted into one sealed subtree (which references all
        # their blobs in one tree). This keeps the root tree small (O(N /
        # FANOUT) entries) and each commit writes only O(1) new metadata.
        #
        # Total metadata: O(N) — each blob contributes to ~log_FANOUT(N)
        # tree references plus one leaf entry.

        parent_subtrees: list[tuple[str, str]] = []  # sealed subtrees
        parent_unsealed_leaves: list[tuple[str, str]] = []  # single-blob leaves since last seal

        if parent_hash is not None:
            parent_commit = self._read_commit(parent_hash)
            if parent_commit is not None:
                parent_root = self._read_tree(parent_commit.tree_hash)
                if parent_root is not None:
                    for name, h in sorted(parent_root.entries.items()):
                        if name.startswith("subtree/"):
                            parent_subtrees.append((name, h))
                        elif name.startswith("leaf/"):
                            parent_unsealed_leaves.append((name, h))

        # Create the new single-blob leaf for this commit
        blob_counter = len(parent_subtrees) * TREE_FANOUT + len(parent_unsealed_leaves)
        blob_name = f"{table_name}/data/{blob_counter}"
        new_leaf = Tree(
            entries={blob_name: sealed_hash},
            tree_type="leaf",
        )
        new_leaf_hash = self._write_tree(new_leaf)
        new_leaf_name = f"leaf/{blob_counter:08d}"
        parent_unsealed_leaves.append((new_leaf_name, new_leaf_hash))

        # If unsealed leaves reach FANOUT, compact them into one sealed subtree
        if len(parent_unsealed_leaves) >= TREE_FANOUT:
            compacted_entries: dict[str, str] = {}
            for _, leaf_hash in parent_unsealed_leaves:
                leaf = self._read_tree(leaf_hash)
                if leaf is not None:
                    compacted_entries.update(leaf.entries)
            sealed_subtree = Tree(entries=compacted_entries, tree_type="leaf")
            sealed_subtree_hash = self._write_tree(sealed_subtree)
            subtree_name = f"subtree/{len(parent_subtrees):08d}"
            parent_subtrees.append((subtree_name, sealed_subtree_hash))
            parent_unsealed_leaves = []

        # Build the root tree (interior) referencing all subtrees + unsealed leaves
        root_entries: dict[str, str] = {}
        for name, h in parent_subtrees:
            root_entries[name] = h
        for name, h in parent_unsealed_leaves:
            root_entries[name] = h

        root_tree = Tree(entries=root_entries, tree_type="interior")
        root_tree_hash = self._write_tree(root_tree)

        # Schema hash (for schema evolution — future)
        schema_json = str(open_obj.schema)
        schema_hash = hash_bytes(schema_json.encode())

        commit = Commit(
            tree_hash=root_tree_hash,
            parent_hash=parent_hash,
            timestamp=time.time(),
            message=message or f"seal {table_name} ({open_obj.row_count} rows, {open_obj.fragment_count} fragments)",
            schema_hash=schema_hash,
        )
        commit_hash = self._write_commit(commit)

        # Update the root pointer namespace
        self._set_root(table_name, commit_hash)

        # Clean up the OPEN object
        if os.path.exists(open_obj.path):
            os.remove(open_obj.path)
        del self._open_objects[table_name]

        self.stats["seals"] += 1
        return commit_hash

    # ------------------------------------------------------------------
    # Syscall 3: Reference
    # ------------------------------------------------------------------

    def reference(self, name: str, commit_hash: str) -> None:
        """
        Set a mutable name -> commit_hash mapping in the root pointer
        namespace. This is the only mutable operation in the storage kernel.

        Precondition: commit_hash exists.
        Postcondition: subsequent Read(name) returns the bytes at commit_hash.
        """
        if not self._commit_exists(commit_hash):
            raise ValueError(f"Commit hash {commit_hash} does not exist")
        self._set_root(name, commit_hash)

    # ------------------------------------------------------------------
    # Syscall 4: Read
    # ------------------------------------------------------------------

    def read(self, name_or_hash: str) -> pa.Table:
        """
        Read the bytes of an object. If given a content hash, return the
        Parquet bytes for that sealed object. If given a name, resolve the
        name to its current commit hash, then return the union of all
        sealed Parquet files referenced by that commit's tree.

        Consistency: linearizable for name resolution (always resolves to
        the most-recently-committed hash); content-addressed reads are by
        construction consistent.

        Returns a pyarrow Table (the union of all data blobs in the tree).
        """
        self.stats["reads"] += 1

        # If it's a 64-char hex string, treat as a commit hash
        if len(name_or_hash) == 64 and all(c in "0123456789abcdef" for c in name_or_hash):
            commit_hash = name_or_hash
        else:
            commit_hash = self._resolve_name(name_or_hash)
            if commit_hash is None:
                raise ValueError(f"Name '{name_or_hash}' is not bound in the root namespace")

        # Read the commit, get its tree, read all data blobs in the tree
        commit = self._read_commit(commit_hash)
        if commit is None:
            raise ValueError(f"Commit {commit_hash} not found")

        tree = self._read_tree(commit.tree_hash)
        if tree is None:
            raise ValueError(f"Tree {commit.tree_hash} not found")

        # Walk the tree hierarchy (interior -> leaf -> blob) and collect all data blobs
        tables = []
        for blob_hash in self._walk_tree_for_data_blobs(commit.tree_hash):
            parquet_path = self._blob_path(blob_hash)
            if not os.path.exists(parquet_path):
                raise ValueError(f"Blob {blob_hash} not found on disk")
            t = pq.read_table(parquet_path)
            tables.append(t)

        if not tables:
            # Empty table — return an empty table with the right schema
            schema = self._schemas.get(name_or_hash)
            if schema:
                return pa.table({col: pa.array([]) for col in schema.names})
            return pa.table({})

        # Concatenate all data blobs into one table
        # (In production, DuckDB would scan them in parallel without concat)
        result = pa.concat_tables(tables)
        self.stats["rows_read"] += result.num_rows
        return result

    # ------------------------------------------------------------------
    # Convenience: SQL via DuckDB (the reference backend)
    # ------------------------------------------------------------------

    def sql(self, query: str) -> Any:
        """
        Execute SQL via DuckDB over the sealed Parquet files. This is the
        reference backend — in production, the Planner would route queries
        to whichever capability advertises Execution(...) for the query shape.

        v0 limitation: only handles SELECT * FROM <table> [WHERE ...]
        patterns. A real Planner would compile SQL to Pond IR.
        """
        # Simple parser: extract table name from "FROM <table>"
        import re
        m = re.search(r"\bfrom\s+(\w+)", query, re.IGNORECASE)
        if not m:
            raise ValueError(f"v0 SQL parser only handles 'FROM <table>' queries; got: {query}")
        table_name = m.group(1)

        # Read the table as a PyArrow Table, register in DuckDB
        arrow_table = self.read(table_name)
        self.duck.register(f"_pond_{table_name}", arrow_table)

        # Rewrite the query to use the registered table
        rewritten = re.sub(rf"\b{table_name}\b", f"_pond_{table_name}", query, flags=re.IGNORECASE)
        return self.duck.execute(rewritten).fetchall()

    # ------------------------------------------------------------------
    # Time travel & branching (derived from Versioned State, per RFC 1 §5.1)
    # ------------------------------------------------------------------

    def history(self, table_name: str) -> list[dict]:
        """Walk the commit DAG backwards from the current commit."""
        commit_hash = self._resolve_name(table_name)
        if commit_hash is None:
            return []

        history = []
        current = commit_hash
        while current is not None:
            commit = self._read_commit(current)
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

    def create_branch(self, branch_name: str, from_table: str, at_commit: Optional[str] = None) -> str:
        """
        Create a branch: a named pointer to a commit. Copy-on-write — the
        branch shares all sealed objects with its parent.
        """
        if at_commit is None:
            at_commit = self._resolve_name(from_table)
            if at_commit is None:
                raise ValueError(f"Table '{from_table}' has no commits")
        self.reference(branch_name, at_commit)
        return at_commit

    def list_tables(self) -> list[str]:
        """List all names in the root pointer namespace."""
        cur = self.root_db.execute("SELECT name FROM roots ORDER BY name")
        return [row[0] for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Internal: DAG object persistence (JSON for v0; could be more compact)
    # ------------------------------------------------------------------

    def _write_tree(self, tree: Tree) -> str:
        data = json.dumps(asdict(tree), sort_keys=True).encode()
        h = hash_bytes(data)
        path = self._meta_path(h)
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
        return h

    def _read_tree(self, tree_hash: str) -> Optional[Tree]:
        path = self._meta_path(tree_hash)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            data = json.loads(f.read())
        # Backward compat: old trees don't have tree_type; assume "leaf"
        data.setdefault("tree_type", "leaf")
        return Tree(**{k: v for k, v in data.items() if k != "kind"})

    def _walk_tree_for_data_blobs(self, tree_hash: str) -> list[str]:
        """
        Walk a tree (possibly hierarchical) and return all data blob hashes.
        Interior trees reference subtrees and unsealed leaves.
        Leaf trees reference blobs directly.
        """
        tree = self._read_tree(tree_hash)
        if tree is None:
            return []

        blobs: list[str] = []
        for name, h in tree.entries.items():
            if tree.tree_type == "interior":
                # subtree or leaf reference — recurse
                if name.startswith("subtree/") or name.startswith("leaf/"):
                    blobs.extend(self._walk_tree_for_data_blobs(h))
                elif "/data/" in name or name.endswith("/data"):
                    # Direct blob ref in an interior tree (migration case)
                    blobs.append(h)
            else:
                # leaf tree — entries are blob refs
                if "/data/" in name or name.endswith("/data"):
                    blobs.append(h)
        return blobs

    def _write_commit(self, commit: Commit) -> str:
        data = json.dumps(asdict(commit), sort_keys=True).encode()
        h = hash_bytes(data)
        path = self._meta_path(h)
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
        return h

    def _read_commit(self, commit_hash: str) -> Optional[Commit]:
        path = self._meta_path(commit_hash)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            data = json.loads(f.read())
        return Commit(**{k: v for k, v in data.items() if k != "kind"})

    def _commit_exists(self, commit_hash: str) -> bool:
        return os.path.exists(self._meta_path(commit_hash))

    def _meta_path(self, h: str) -> str:
        return os.path.join(self.objects_dir, h[:2], h + ".json")

    def _blob_path(self, h: str) -> str:
        return os.path.join(self.objects_dir, h[:2], h + ".parquet")

    # ------------------------------------------------------------------
    # Internal: root pointer namespace
    # ------------------------------------------------------------------

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
        """Return storage statistics for benchmarking."""
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
                if f.endswith(".parquet"):
                    data_bytes += size
                    blob_count += 1
                elif f.endswith(".json"):
                    meta_bytes += size
                    meta_count += 1

        # Root store size
        root_store_bytes = os.path.getsize(self.root_store_path)

        return {
            **self.stats,
            "data_bytes": data_bytes,
            "meta_bytes": meta_bytes,
            "root_store_bytes": root_store_bytes,
            "blob_count": blob_count,
            "meta_count": meta_count,
            "meta_to_data_ratio": (meta_bytes / data_bytes) if data_bytes > 0 else 0,
            "table_count": len(self.list_tables()),
        }

    def close(self) -> None:
        # Seal any remaining OPEN objects
        for table_name in list(self._open_objects.keys()):
            try:
                self.seal(table_name, message="seal on close")
            except Exception:
                pass
        self.root_db.close()
        self.duck.close()
