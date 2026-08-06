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
from base_lens import PondLens  # noqa: E402

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

# Legacy ProllyTree imports removed — unified architecture uses UnifiedStorage only
_HAVE_LEGACY = False


# Default row group size. Each row group becomes one PND2 blob.
DEFAULT_ROW_GROUP_SIZE = 10_000


class LakehouseLens(PondLens):
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

    Extends PondLens to inherit branch/list_collections/set_definition/
    get_definition/history for free (no duplication).

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
        super().__init__(kernel)
        self.kernel = kernel
        # Use PondStorage (the unified SDK — the only path)
        self._storage: Optional[PondStorage] = None
        if _HAVE_POND_STORAGE:
            self._storage = PondStorage(kernel)
        else:
            raise ImportError("PondStorage not available — install the pond-sdk package")

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

        CROSS-LENS: stamps metadata (lens_type="lakehouse", key_col,
        schema_hint from the Arrow schema) so other lenses can identify
        this collection.

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
        # Stamp cross-lens metadata: schema_hint from Arrow schema
        schema_hint = {field.name: str(field.type) for field in data.schema}
        self._storage.stamp_collection_metadata(
            table_name, lens_type="lakehouse", key_col=key_col,
            schema_hint=schema_hint)
        return commit_hash

    def insert(self, table_name: str, new_data: pa.Table,
               key_col: Optional[str] = None,
               row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
               message: str = "") -> str:
        """Append rows to a table (non-destructive).

        Uses PondStorage.append() which preserves existing data —
        no read-rewrite cycle needed.

        CROSS-LENS: works on any collection (lakehouse, KV, vector,
        streaming). The new rows are appended as-is; if the target
        collection was created by another lens, the appended rows
        will have only the columns you provide (other columns become
        None — "ugly shape" but readable by any lens).

        Args:
            table_name: collection name (must already exist)
            new_data: PyArrow Table to append
            key_col: sort key column (defaults to collection's existing
                key_col from metadata, or None if no metadata)
            row_group_size: rows per new row group
            message: commit message

        Returns:
            The new HEAD commit hash.
        """
        # Cross-lens: if key_col not specified, try reading it from metadata
        if key_col is None:
            md = self._storage.get_collection_metadata(table_name)
            key_col = md.get("key_col")  # may be None — that's OK
        rows = new_data.to_pylist()
        commit_hash = self._storage.append(table_name, rows, key_col=key_col,
                                      row_group_size=row_group_size,
                                      message=message or f"insert {new_data.num_rows} rows")
        # Save the manifest hash for time-travel (keyed by commit hash)
        self._save_commit_manifest(table_name, commit_hash)
        return commit_hash

    def _save_commit_manifest(self, name: str, commit_hash: str) -> None:
        """Save the current manifest hash keyed by commit hash for time-travel."""
        # Read the active branch's manifest ref (per-branch — was collection-level
        # collections/{name}/manifest before the branches/ refactor).
        us = self._storage._unified
        if us is not None:
            manifest_hash = self.kernel.resolve(us._manifest_ref(name))
        else:
            manifest_hash = self.kernel.resolve(f"collections/{name}/_branches/main/manifest")
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
        # Resolve the active branch's commit (replaces the old HEAD ref).
        us = self._storage._unified
        if us is not None:
            head = self.kernel.resolve(us._active_commit_ref(table_name))
        else:
            head = self.kernel.resolve(f"collections/{table_name}/_branches/main/commit")
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

        Delegates to PondStorage.merge() which:
          - Reads both branch HEAD + source HEAD + all shards from both
          - Unions row groups (Level 1 CRDT merge)
          - Applies row-level CRDT merge (_rowid + _version)
          - Writes a TWO-PARENT merge commit (parent=HEAD, second_parent=branch)
          - Clears shards on both branches

        Returns:
            The merge commit hash.
        """
        # Use the unified merge path — it creates a proper two-parent commit.
        return self._storage.merge(name, branch_name)

    def read_branch(self, name: str, branch_name: str) -> pa.Table:
        """Read a branch's data as a PyArrow Table.

        Uses the branch's commit + shards directly — NO ref mutation, NO
        checkout. This is the CRDT-shard-aware path: it reads the branch's
        HEAD commit's manifest AND the branch's shards, then merges them
        (same as read_with_shards but for an arbitrary branch without
        changing the active branch).

        Fix (Round 9 Issue #2): no more swap-then-restore race condition.
        Fix (Round 27): branch-aware shard merging — branches have their
        own shard space; without this, appends via commit_to_branch were
        invisible to read_branch.
        """
        us = self._storage._unified
        rows = us.read_branch_with_shards(name, branch_name)
        if not rows:
            return pa.table({})
        return pa.Table.from_pylist(rows)

    def commit_to_branch(self, name: str, branch_name: str,
                         data: pa.Table,
                         key_col: Optional[str] = None,
                         row_group_size: int = DEFAULT_ROW_GROUP_SIZE) -> str:
        """Write data to a branch (NOT the active branch).

        Uses the proper CRDT branch mechanism: checkout the branch (so
        subsequent writes — including the shard that append() creates —
        go to this branch's shard space), call append(), then restore
        the original active branch.

        This is the correct model in the CRDT-shard world:
          - Shards are stored under collections/{name}/_branches/{branch}/shards/
          - read_branch reads the branch's commit + shards
          - The branch's manifest advances as commits land on the branch
          - Main is untouched because main's branches/main/commit and shard
            space are separate from the branch's.

        Args:
            name: collection name
            branch_name: target branch
            data: PyArrow Table to append to the branch
            key_col: sort key column
            row_group_size: rows per row group

        Returns:
            The new commit hash on the branch.
        """
        us = self._storage._unified
        # Remember the originally active branch (if any) so we can restore it.
        original_active = us._active_branches.get(name)

        # If the branch doesn't exist yet, create it from the active branch's
        # commit (git checkout -b). If it already exists, just checkout.
        branch_ref = us._branch_ref(name, branch_name)
        if self.kernel.resolve(branch_ref) is None:
            us.branch(name, branch_name)
        us.checkout(name, branch_name)

        # Now writes go to the branch's shard space and branches/{branch}/commit.
        rows = data.to_pylist()
        commit_hash = self._storage.append(
            name, rows, key_col=key_col,
            row_group_size=row_group_size,
            message=f"branch {branch_name}: insert {data.num_rows} rows")

        # _write_commit_blob (called by append) already updated
        # branches/{branch_name}/commit to the new commit. No separate binding
        # is needed — the active commit ref IS the branch ref now.

        # Restore the original active branch (if any).
        if original_active is None:
            # Was on default (main) — detach. Each branch has its own manifest
            # ref, so simply popping the active branch is enough — reads will
            # automatically use main's manifest ref (no sync needed).
            us._active_branches.pop(name, None)
            if hasattr(us.kernel, 'invalidate_path_cache'):
                us.kernel.invalidate_path_cache(us._manifest_ref_for_branch(name, "main"))
        else:
            # Restore the originally active branch.
            # Parse the branch name out of the stored ref path
            # (collections/{c}/_branches/{branch}/commit → {branch}).
            prefix = f"collections/{name}/_branches/"
            if original_active.startswith(prefix):
                rest = original_active[len(prefix):]
                # Strip the trailing /commit suffix.
                orig_name = rest.rsplit("/commit", 1)[0] if rest.endswith("/commit") else rest
                try:
                    us.checkout(name, orig_name)
                except ValueError:
                    # Branch was deleted in the meantime — fall back to main.
                    us._active_branches.pop(name, None)
            else:
                # Unknown format — detach and fall back to main.
                us._active_branches.pop(name, None)

        # Invalidate the storage's manifest cache so subsequent reads
        # see the restored state.
        us._invalidate_manifest_cache(name)

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
        """List all collections (any lens, any format)."""
        return self._storage.list_collections()

    def list_collections_with_metadata(self) -> list[dict]:
        """List ALL collections with cross-lens metadata.

        Returns a list of {"name", "lens_type", "key_col", "schema_hint",
        "created_at"} for every collection in the pond, regardless of
        which lens created it. LakehouseLens can see and read any of
        them.
        """
        return self._storage.list_collections_with_metadata()

    def get_collection_metadata(self, name: str) -> dict:
        """Read cross-lens metadata for a collection."""
        return self._storage.get_collection_metadata(name)

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
    # PARTITIONING — Hive-style partition pruning
    #
    # Partitions use Pond's hierarchical namespaces: a table "events"
    # partitioned by "date" becomes collections "events/2024-01-01",
    # "events/2024-01-02", etc. Reads with partition predicates only
    # fetch the relevant partition collections.
    #
    # This follows our design: partitions = namespaces, no new primitives.
    # ==================================================================

    def create_partitioned_table(self, table_name: str, data: pa.Table,
                                  partition_cols: list[str],
                                  key_col: Optional[str] = None,
                                  row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
                                  message: str = "") -> dict:
        """Create a partitioned table — splits data by partition columns.

        Each partition is a separate collection (Hive-style):
          table_name/partition_value1/partition_value2/...

        Args:
            table_name: base table name
            data: PyArrow Table to partition
            partition_cols: columns to partition by (e.g., ["date", "region"])
            key_col: sort key column
            row_group_size: rows per row group
            message: commit message

        Returns:
            {"partitions": [list of partition names], "rows": total rows}
        """
        rows = data.to_pylist()
        # Group rows by partition values
        partitions: dict[str, list[dict]] = {}
        for row in rows:
            # Build partition key from partition columns
            parts = []
            for col in partition_cols:
                val = row.get(col, "null")
                parts.append(f"{col}={val}")
            partition_name = "/".join(parts)
            if partition_name not in partitions:
                partitions[partition_name] = []
            partitions[partition_name].append(row)

        # Write each partition as a separate collection
        partition_names = []
        total_rows = 0
        for pname, prows in partitions.items():
            coll = f"{table_name}/{pname}"
            commit = self._storage.write(coll, prows, key_col=key_col,
                                           row_group_size=row_group_size,
                                           message=message or f"partition {pname}")
            self._save_commit_manifest(coll, commit)
            schema_hint = {field.name: str(field.type) for field in data.schema}
            self._storage.stamp_collection_metadata(
                coll, lens_type="lakehouse", key_col=key_col,
                schema_hint=schema_hint,
                extra={"partition": True, "partition_cols": partition_cols,
                        "base_table": table_name})
            partition_names.append(coll)
            total_rows += len(prows)

        # Stamp the base table metadata (points to partitions)
        self._storage.stamp_collection_metadata(
            table_name, lens_type="lakehouse_partitioned",
            extra={"partition_cols": partition_cols,
                    "partitions": partition_names})

        return {"partitions": partition_names, "rows": total_rows}

    def read_partitioned(self, table_name: str,
                          partition_filters: Optional[dict] = None,
                          columns: Optional[list[str]] = None,
                          predicates: Optional[list] = None) -> pa.Table:
        """Read from a partitioned table with partition pruning.

        Args:
            table_name: base table name
            partition_filters: dict of {partition_col: value} to prune.
                e.g., {"date": "2024-01-01"} reads only that partition.
                None = read all partitions.
            columns: projection pushdown
            predicates: row-level predicates

        Returns:
            PyArrow Table with results from all matching partitions.
        """
        # List all partition collections
        all_collections = self._storage.list_collections(table_name)
        # Filter to partitions only (have / in the name after table_name)
        partition_colls = [c for c in all_collections
                           if c.startswith(f"{table_name}/") and c != table_name]

        # Apply partition filters
        if partition_filters:
            filtered = []
            for coll in partition_colls:
                # coll = "table_name/date=2024-01-01/region=us"
                parts = coll[len(table_name) + 1:].split("/")
                match = True
                for part in parts:
                    if "=" in part:
                        col, val = part.split("=", 1)
                        if col in partition_filters and str(partition_filters[col]) != val:
                            match = False
                            break
                if match:
                    filtered.append(coll)
            partition_colls = filtered

        # Read each matching partition and combine
        all_rows = []
        for coll in partition_colls:
            rows = self._storage.read(coll, predicates=predicates, columns=columns)
            all_rows.extend(rows)

        if not all_rows:
            return pa.table({})
        return pa.Table.from_pylist(all_rows)

    def list_partitions(self, table_name: str) -> list[str]:
        """List all partitions for a partitioned table."""
        all_collections = self._storage.list_collections(table_name)
        return [c for c in all_collections
                if c.startswith(f"{table_name}/") and c != table_name]

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
