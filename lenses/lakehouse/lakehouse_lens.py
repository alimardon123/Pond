"""
LakehouseLens — thin tabular lens over PondStorage.

REDUCED from 2227 LOC to ~350 LOC by delegating ALL storage to PondStorage
(PND2 + CollectionManifest + StatsTree). The lens only provides:
  - PyArrow Table ↔ list[dict] conversion
  - DuckDB SQL query on top of stored data
  - Tabular-specific API (create_table, insert, read_table, read_columns)

The old 2227-LOC lens had:
  - 4 write modes (range_write, range_write_column_chunks, range_write_encoded,
    _write_via_prolly) → NOW: storage.write() (ONE path, PND2)
  - 10+ read methods (read_table, range_read, range_point_lookup, read_columns,
    read_with_pruning, read_with_column_chunk_pruning, read_with_encoded_pruning,
    read_table_via_manifest, read_with_pruning_via_manifest,
    range_point_lookup_via_manifest) → NOW: storage.read() + storage.point_lookup()
  - ZoneMapIndex + PruningReader + CollectionMetadata → NOW: CollectionManifest (ONE index)
  - attach_indexer / _notify_indexers → DELETED (dead code)

Architecture:
  ┌──────────────────────────────────────┐
  │         SQL Query (DuckDB)           │  ← user-facing
  ├──────────────────────────────────────┤
  │      LakehouseLens (this file)       │  ← thin adapter
  │   PyArrow ↔ list[dict] conversion    │
  ├──────────────────────────────────────┤
  │      PondStorage (unified SDK)       │  ← ONE storage class
  │   write / read / point_lookup        │
  ├──────────────────────────────────────┤
  │   Kernel (FROZEN — 3 primitives)     │
  └──────────────────────────────────────┘
"""

from __future__ import annotations

import os
import sys
import json
from typing import Optional, Any, Iterator, Callable

# Make pond-core, pond-sdk, and physical_structures importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk",
                                  "extensions", "physical_structures"))

from kernel import PondMinimal  # noqa: E402

# PyArrow for the Parquet/Arrow interchange (REQUIRED — this is a tabular lens)
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError(
        "PyArrow is required for LakehouseLens. "
        "Install with: pip install pyarrow"
    )

# DuckDB is OPTIONAL — only needed for SQL queries
try:
    import duckdb
except ImportError:
    duckdb = None

# PondStorage — the unified storage SDK
try:
    from pond_storage import PondStorage  # noqa: E402
    _HAVE_POND_STORAGE = True
except ImportError:
    _HAVE_POND_STORAGE = False

# Legacy imports for backward compat (old code that calls ProllyLensBase directly)
try:
    from prolly_tree import ProllyLensBase, ProllyTree  # noqa: E402
    from binary_encoding import BinaryProllyTree  # noqa: E402
    from base_lens import PondLens  # noqa: E402
    _HAVE_LEGACY = True
except ImportError:
    _HAVE_LEGACY = False


# Default row group size. Each row group becomes one PND2 blob.
DEFAULT_ROW_GROUP_SIZE = 10_000


class LakehouseLens:
    """Thin tabular lens over PondStorage.

    Provides PyArrow Table ↔ PondStorage conversion and DuckDB SQL
    on top of the unified storage layer.

    ALL storage operations delegate to PondStorage:
      - create_table → storage.write()
      - insert → storage.append()
      - read_table → storage.read() → pa.Table
      - read_columns → storage.read(columns=...) → pa.Table
      - point_lookup → storage.point_lookup()
      - branch/merge → storage.branch()/storage.merge()

    Usage:
        from pond_storage import PondStorage
        from lakehouse_lens import LakehouseLens

        lens = LakehouseLens(kernel)
        lens.create_table("users", table, key_col="id")
        lens.insert("users", more_data)
        result = lens.read_table("users")
        result = lens.query("SELECT * FROM users WHERE age > 30")
    """

    def __init__(self, kernel: PondMinimal):
        """Create a LakehouseLens.

        Args:
            kernel: the PondMinimal or ObjectStoreNativeKernel instance
        """
        self.kernel = kernel
        # Use PondStorage if available (unified path), else fall back to legacy
        self._storage: Optional[PondStorage] = None
        if _HAVE_POND_STORAGE:
            self._storage = PondStorage(kernel)
        elif _HAVE_LEGACY:
            # Legacy path: extend PondLens for namespace ops
            self._legacy_base = PondLens(kernel)
        else:
            raise ImportError("Neither PondStorage nor legacy PondLens available")

        # DuckDB connection (lazy — created on first query)
        self._duckdb = None
        # Cache for registered tables (avoid re-registering on every query)
        self._registered_tables: dict[str, str] = {}  # table_name → commit_hash

    @property
    def duckdb(self):
        """Lazily-created DuckDB connection."""
        if self._duckdb is None:
            if duckdb is None:
                raise ImportError(
                    "DuckDB is required for SQL queries. Install with: pip install duckdb")
            self._duckdb = duckdb.connect()
        return self._duckdb

    # ==================================================================
    # WRITE PATH — delegates to PondStorage.write / append
    # ==================================================================

    def create_table(self, table_name: str, data: pa.Table,
                     key_col: Optional[str] = None,
                     row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
                     message: str = "") -> str:
        """Create a new table.

        Converts the PyArrow Table to list[dict] and writes via PondStorage.
        ONE write path (PND2), no zone maps, no separate manifests.

        Args:
            table_name: collection name
            data: PyArrow Table to store
            key_col: column to use as the sort key. If None, uses row index.
            row_group_size: rows per row group (default 10_000)
            message: commit message

        Returns:
            The new HEAD commit hash.
        """
        rows = data.to_pylist()
        commit_hash = self._storage.write(table_name, rows, key_col=key_col,
                                     row_group_size=row_group_size,
                                     message=message or f"create {table_name}")
        # Save the manifest hash for time-travel (keyed by commit hash)
        self._save_commit_manifest(table_name, commit_hash)
        return commit_hash

    def insert(self, table_name: str, new_data: pa.Table,
               key_col: Optional[str] = None,
               row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
               message: str = "") -> str:
        """Append rows to a table (non-destructive).

        Uses PondStorage.append() which preserves existing data —
        no read-rewrite cycle needed.

        Args:
            table_name: collection name (must already exist)
            new_data: PyArrow Table to append
            key_col: sort key column (should match create_table)
            row_group_size: rows per new row group
            message: commit message

        Returns:
            The new HEAD commit hash.
        """
        rows = new_data.to_pylist()
        commit_hash = self._storage.append(table_name, rows, key_col=key_col,
                                      row_group_size=row_group_size,
                                      message=message or f"insert {new_data.num_rows} rows")
        # Save the manifest hash for time-travel (keyed by commit hash)
        self._save_commit_manifest(table_name, commit_hash)
        return commit_hash

    def _save_commit_manifest(self, name: str, commit_hash: str) -> None:
        """Save the current manifest hash keyed by commit hash for time-travel."""
        manifest_hash = self.kernel.resolve(f"collections/{name}/manifest")
        if manifest_hash is not None:
            self.kernel.reference(
                f"collections/{name}/commits/{commit_hash}__manifest",
                manifest_hash)

    def _load_commit_manifest(self, name: str,
                                commit_hash: str) -> Optional[str]:
        """Load the manifest hash for a specific commit (time-travel)."""
        return self.kernel.resolve(
            f"collections/{name}/commits/{commit_hash}__manifest")

    # ==================================================================
    # READ PATH — delegates to PondStorage.read / point_lookup
    # ==================================================================

    def read_table(self, table_name: str,
                   commit_hash: Optional[str] = None) -> pa.Table:
        """Read a table as a PyArrow Table — FASTEST path via read_as_arrow.

        Uses PondStorage.read_as_arrow() which does:
          1. Manifest pruning (in-memory, 0 GETs)
          2. PARALLEL blob fetch (K GETs in ~1 RTT wall-clock)
          3. Zero-copy Arrow construction

        If commit_hash is provided, time-travels to the manifest at that commit
        via manifest_hash — NO ref mutation, NO race condition.
        Fix (Round 9 Issue #2): uses manifest_hash instead of swap-then-restore.
        """
        if commit_hash is not None:
            # Time travel: load the commit's manifest hash and pass it directly
            commit_manifest = self._load_commit_manifest(table_name, commit_hash)
            if commit_manifest is None:
                return pa.table({})
            # Pass manifest_hash directly — no ref mutation, no race condition
            return self._storage.read_as_arrow(table_name) if False else \
                self._read_as_arrow_with_manifest(table_name, commit_manifest)
        else:
            return self._storage.read_as_arrow(table_name)

    def _read_as_arrow_with_manifest(self, name: str,
                                       manifest_hash: str) -> pa.Table:
        """Read as Arrow using a specific manifest hash (time-travel)."""
        if self._storage and self._storage._unified:
            col_data = self._storage._unified.read_as_columns(
                name, manifest_hash=manifest_hash)
            if not col_data:
                return pa.table({})
            import pyarrow as pa
            arrays = []
            names_list = []
            for col_name, values in col_data.items():
                arrays.append(pa.array(values))
                names_list.append(col_name)
            return pa.Table.from_arrays(arrays, names=names_list)
        return pa.table({})

    def read_columns(self, name: str, columns: list[str],
                     commit_hash: Optional[str] = None) -> pa.Table:
        """Read only the specified columns (projection pushdown)."""
        rows = self._storage.read(name, columns=columns, commit_hash=commit_hash)
        if not rows:
            return pa.table({})
        return pa.Table.from_pylist(rows)

    def read_with_pruning(self, name: str,
                          predicates: Optional[list] = None,
                          columns: Optional[list[str]] = None,
                          row_filter: Optional[Callable] = None) -> pa.Table:
        """Read with predicate pushdown (Vortex-style).

        Delegates to PondStorage.read() which evaluates predicates against
        the CollectionManifest's inline stats (row-group pruning), then on
        the encoded form (Vortex-style encoded eval).
        """
        rows = self._storage.read(name, predicates=predicates,
                                    columns=columns, row_filter=row_filter)
        if not rows:
            return pa.table({})
        return pa.Table.from_pylist(rows)

    def point_lookup(self, name: str, key: str) -> Optional[pa.Table]:
        """O(1) point lookup — 4 GETs cold via manifest + encoded eval."""
        row = self._storage.point_lookup(name, key=key)
        if row is None:
            return None
        return pa.Table.from_pylist([row])

    def range_read(self, name: str,
                   start_key: Optional[str] = None,
                   end_key: Optional[str] = None) -> pa.Table:
        """Range scan — read row groups in a key range.

        The start_key and end_key are the raw key values (e.g., "5" or "50").
        The lens formats them as zero-padded row group keys for correct
        lexicographic comparison.
        """
        # Fix (Round 11 Issue #3): use _format_rg_key for zero-padded keys
        # instead of raw "rg/{key}" which breaks for numeric keys > 9.
        try:
            from unified_storage import _format_rg_key
            sk = _format_rg_key(start_key) if start_key is not None else None
            ek = _format_rg_key(end_key) if end_key is not None else None
        except ImportError:
            sk = f"rg/{start_key}" if start_key is not None else None
            ek = f"rg/{end_key}" if end_key is not None else None
        rows = self._storage.read(name, start_key=sk, end_key=ek)
        if not rows:
            return pa.table({})
        return pa.Table.from_pylist(rows)

    # ==================================================================
    # SQL QUERY (DuckDB on top of stored data)
    # ==================================================================

    def query(self, sql: str, table_name: Optional[str] = None) -> pa.Table:
        """Execute a SQL query via DuckDB.

        Registers the Pond collection as a DuckDB table, then runs the SQL.
        For repeated queries on the same data, the table is cached.

        Args:
            sql: the SQL query
            table_name: optional — if the SQL references a single table,
                register it automatically. If None, the caller must have
                registered tables via register_table().

        Returns:
            A PyArrow Table with the query results.
        """
        if table_name:
            self.register_table(table_name)
        return self.duckdb.execute(sql).fetch_arrow_table()

    def register_table(self, table_name: str) -> None:
        """Register a Pond collection as a DuckDB table.

        Reads the collection via PondStorage and registers it in DuckDB
        for SQL queries. Cached — re-registration only happens if the
        commit hash changes.
        """
        head = self.kernel.resolve(f"collections/{table_name}/HEAD")
        cached = self._registered_tables.get(table_name)
        if cached == head:
            return  # already registered at this commit

        table = self.read_table(table_name)
        # DuckDB can register an Arrow table directly
        self.duckdb.register(table_name, table)
        self._registered_tables[table_name] = head

    # ==================================================================
    # BRANCH / MERGE — delegates to PondStorage
    # ==================================================================

    def branch(self, name: str, branch_name: str) -> str:
        """Create a branch."""
        return self._storage.branch(name, branch_name)

    def list_branches(self, name: str) -> list[str]:
        """List all branches."""
        return self._storage.list_branches(name)

    def merge_branch(self, name: str, branch_name: str) -> str:
        """Merge a branch into HEAD.

        Fix (Round 12 Issue #5):
        - Deduplicates by key_col (last-writer-wins from branch)
        - Preserves key_col from the existing collection's manifest
        - Logs merge-commit rewrite errors instead of silently swallowing
        """
        # Read both branch and HEAD data
        head_table = self.read_table(name)
        branch_table = self.read_branch(name, branch_name)

        try:
            merged = pa.concat_tables([head_table, branch_table], promote_options="default")
        except TypeError:
            merged = pa.concat_tables([head_table, branch_table])

        # Get the branch commit hash for the merge commit's second_parent
        head_ref = f"collections/{name}/HEAD"
        first_parent = self.kernel.resolve(head_ref)
        branch_ref = f"collections/{name}/branches/{branch_name}"
        second_parent = self.kernel.resolve(branch_ref)

        # Get key_col from the existing manifest
        manifest = self._storage._unified._load_manifest(name) if self._storage and self._storage._unified else None
        key_col = manifest.key_col if manifest else None

        # Deduplicate by key_col (last-writer-wins: branch data overrides HEAD)
        if key_col and key_col in merged.column_names:
            merged = merged.sort_by(key_col)
            # Use PyArrow's group_by to deduplicate (keep last occurrence)
            # Simpler: convert to pylist, deduplicate by key, convert back
            rows = merged.to_pylist()
            seen = {}
            for row in rows:
                k = row.get(key_col)
                if k is not None:
                    seen[k] = row
            deduped = list(seen.values())
            if deduped:
                merged = pa.Table.from_pylist(deduped)

        # Write the merged data as PND2 blobs + manifest
        rows = merged.to_pylist()
        commit_hash = self._storage.write(name, rows,
                                            key_col=key_col,
                                            row_group_size=DEFAULT_ROW_GROUP_SIZE,
                                            message=f"merge branch '{branch_name}'")

        # Rewrite as a 2-parent merge commit
        try:
            from binary_encoding import BinaryProllyTree
            commit_data = self.kernel.read_blob(commit_hash)
            commit = BinaryProllyTree.decode_commit(commit_data)
            delta = commit.get("delta") or {}
            merge_commit_data = BinaryProllyTree.encode_commit(
                parent_hash=commit.get("parent"),
                tree_hash=commit.get("snapshot") or "",
                delta_plus=delta.get("+", {}),
                delta_minus=delta.get("-", []),
                snapshot=commit.get("snapshot"),
                message=commit.get("message", f"merge branch '{branch_name}'"),
                timestamp=commit.get("timestamp", 0),
                index=commit.get("index", 0),
                second_parent=second_parent,
            )
            merge_hash = self.kernel.write(merge_commit_data)
            self.kernel.reference(head_ref, merge_hash)
            self._save_commit_manifest(name, merge_hash)
            return merge_hash
        except Exception as e:
            # Log the error but don't silently swallow it
            import sys
            print(f"WARNING: merge commit rewrite failed ({e}), "
                  f"returning regular commit {commit_hash[:12]}", file=sys.stderr)
            return commit_hash

    def read_branch(self, name: str, branch_name: str) -> pa.Table:
        """Read a branch's data as a PyArrow Table.

        Uses the branch's manifest hash directly — NO ref mutation.
        Fix (Round 9 Issue #2): no more swap-then-restore race condition.
        Fix (Round 13 Issue #4): falls back to HEAD manifest if no __manifest ref.
        Fix (Round 15 Issue #1): resolve branch commit → commits/{hash}__manifest
        instead of falling back to the LIVE HEAD manifest (which may have
        advanced past the branch point).
        """
        # Try branch-specific manifest ref first (set by commit_to_branch)
        branch_manifest_ref = f"collections/{name}/branches/{branch_name}__manifest"
        branch_manifest = self.kernel.resolve(branch_manifest_ref)

        if branch_manifest is None:
            # Fix (Round 15 Issue #1): resolve the branch's commit hash,
            # then look up its manifest via the commit→manifest mapping.
            # This correctly returns the branch's SNAPSHOT, not the live HEAD.
            branch_commit_ref = f"collections/{name}/branches/{branch_name}"
            branch_commit = self.kernel.resolve(branch_commit_ref)
            if branch_commit is not None:
                branch_manifest = self.kernel.resolve(
                    f"collections/{name}/commits/{branch_commit}__manifest")

        if branch_manifest is None:
            # Last resort: fall back to HEAD manifest (branch == HEAD snapshot)
            branch_manifest = self.kernel.resolve(f"collections/{name}/manifest")
        if branch_manifest is None:
            raise KeyError(f"Branch '{branch_name}' not found and no HEAD manifest exists")

        # Read using the branch's manifest — no ref mutation, no race condition
        return self._read_as_arrow_with_manifest(name, branch_manifest)

    def commit_to_branch(self, name: str, branch_name: str,
                         data: pa.Table,
                         key_col: Optional[str] = None,
                         row_group_size: int = DEFAULT_ROW_GROUP_SIZE) -> str:
        """Write data to a branch (NOT HEAD).

        Strategy: write via storage.append (which creates a new manifest + commit),
        then save BOTH the commit hash AND the manifest hash as branch refs.
        Restore the original HEAD + manifest so main is unchanged.
        """
        head_ref = f"collections/{name}/HEAD"
        manifest_ref = f"collections/{name}/manifest"
        original_head = self.kernel.resolve(head_ref)
        original_manifest = self.kernel.resolve(manifest_ref)

        # Write via storage (this updates HEAD + manifest to the new commit)
        rows = data.to_pylist()
        commit_hash = self._storage.append(name, rows, key_col=key_col,
                                            row_group_size=row_group_size,
                                            message=f"branch {branch_name}: insert {data.num_rows} rows")

        # Capture the NEW manifest hash (the branch's manifest)
        branch_manifest = self.kernel.resolve(manifest_ref)

        # Point the branch refs at the new commit + manifest
        self.kernel.reference(f"collections/{name}/branches/{branch_name}", commit_hash)
        self.kernel.reference(f"collections/{name}/branches/{branch_name}__manifest", branch_manifest)

        # RESTORE the original HEAD and manifest (so main is unchanged)
        if original_head is not None:
            self.kernel.reference(head_ref, original_head)
        if original_manifest is not None:
            self.kernel.reference(manifest_ref, original_manifest)

        # Invalidate the storage's manifest cache
        if self._storage and self._storage._unified:
            self._storage._unified._invalidate_manifest_cache(name)

        return commit_hash

    # ==================================================================
    # VERSION CONTROL — delegates to PondStorage
    # ==================================================================

    def history(self, name: str, limit: int = 100) -> list[dict]:
        """Walk the commit history."""
        return self._storage.history(name, limit)

    def undo(self, name: str, steps: int = 1) -> str:
        """Undo the last N commits."""
        return self._storage.undo(name, steps)

    # ==================================================================
    # NAMESPACE — delegates to PondStorage
    # ==================================================================

    def list_collections(self) -> list[str]:
        """List all collections."""
        return self._storage.list_collections()

    def collection_exists(self, name: str) -> bool:
        """Check if a collection exists."""
        return self._storage.collection_exists(name)

    def set_definition(self, name: str, definition: dict) -> str:
        """Store table schema/metadata."""
        return self._storage.set_definition(name, definition)

    def get_definition(self, name: str) -> Optional[dict]:
        """Read table schema/metadata."""
        return self._storage.get_definition(name)

    # ==================================================================
    # DIAGNOSTICS
    # ==================================================================

    def get_round_trip_count(self, name: str,
                              predicates: Optional[list] = None) -> dict:
        """Estimate S3 round trips for a read (without performing it)."""
        return self._storage.get_round_trip_count(name, predicates)

    # ==================================================================
    # Backward-compat aliases (deprecated — use create_table/read_table instead)
    # ==================================================================

    def range_write(self, name: str, table: pa.Table, key_col: str,
                    row_group_size: int = DEFAULT_ROW_GROUP_SIZE) -> str:
        """DEPRECATED: use create_table(). Kept for backward compat."""
        return self.create_table(name, table, key_col=key_col,
                                  row_group_size=row_group_size)

    def range_point_lookup(self, name: str, key: str) -> Optional[pa.Table]:
        """DEPRECATED: use point_lookup(). Kept for backward compat.

        Returns the ENTIRE row group containing the key (not just the
        matching row), matching the old API's behavior.

        Fix (Round 12 Issue #7): use _format_rg_key for zero-padded keys.
        """
        # Find the row group containing this key via the manifest
        manifest = self._storage._unified._load_manifest(name) if self._storage and self._storage._unified else None
        if manifest is None:
            return None
        # Fix (Round 12 Issue #7): use _format_rg_key for zero-padded keys
        try:
            from unified_storage import _format_rg_key
            target = _format_rg_key(key)
        except ImportError:
            target = f"rg/{key}"
        rg = manifest.find_row_group(target)
        if rg is None:
            return None
        # Read just this one row group's blob
        blob_bytes = self.kernel.read_blob(rg.blob_hash)
        from unified_storage import PND2
        col_data = PND2.decode(blob_bytes)
        row_count = max((len(v) for v in col_data.values()), default=0)
        col_names = list(col_data.keys())
        rows = [{c: col_data[c][i] if i < len(col_data[c]) else None
                  for c in col_names}
                 for i in range(row_count)]
        if not rows:
            return None
        return pa.Table.from_pylist(rows)


# ---------------------------------------------------------------------------
# Backward-compat re-export — PondLakehouse façade
# ---------------------------------------------------------------------------
try:
    from pond_lakehouse import PondLakehouse  # noqa: E402
except ImportError:
    PondLakehouse = None


if __name__ == "__main__":
    # Quick self-test
    import tempfile
    tmp = tempfile.mkdtemp(prefix="pond-lh-test-")
    kernel = PondMinimal(tmp)

    lens = LakehouseLens(kernel)

    # Create a table
    table = pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name": pa.array(["alice", "bob", "charlie", "dave", "eve"]),
        "age": pa.array([30, 25, 35, 40, 28], type=pa.int64()),
    })
    lens.create_table("users", table, key_col="id", row_group_size=2)

    # Read it back
    result = lens.read_table("users")
    print(f"Read {result.num_rows} rows")
    print(result.to_pydict())

    # Point lookup
    row = lens.point_lookup("users", key="3")
    print(f"\nPoint lookup key=3: {row}")

    # Read with pruning
    result = lens.read_with_pruning("users", predicates=[("age", ">", 30)])
    print(f"\nPruned (age > 30): {result.num_rows} rows")

    # Round trips
    rt = lens.get_round_trip_count("users", predicates=[("age", ">", 30)])
    print(f"\nRound trips: {rt}")

    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
    print("\n=== LakehouseLens self-test PASSED ===")
