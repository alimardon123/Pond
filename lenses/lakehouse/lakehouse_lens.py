"""
Pond Lakehouse (Phase Q.4 — refactored)

A lightweight alternative to Spark/Flink/Databricks built on the
Pond kernel + DuckDB. This is the flagship application that tests
whether Pond's Lens algebra covers real workloads.

Architecture:
  ┌──────────────────────────────────────┐
  │         SQL Query (DuckDB)           │  ← user-facing
  ├──────────────────────────────────────┤
  │      LakehouseLens (this file)       │  ← tabular semantics
  │   ┌────────────────────────────┐     │
  │   │  Whole-table Parquet I/O   │     │  ← fast OLAP path
  │   │  (one blob per commit)     │     │
  │   ├────────────────────────────┤     │
  │   │  Range read / range write  │     │  ← range path on top
  │   │  (rows as KV in Prolly     │     │     of ProllyTreeIndex
  │   │   tree, keyed by PK)       │     │
  │   └────────────────────────────┘     │
  ├──────────────────────────────────────┤
  │   PondLens base (shared namespace)   │  ← branch, history, defs
  ├──────────────────────────────────────┤
  │   Pond Kernel (Write/Read/Ref)       │  ← immutable substrate
  ├──────────────────────────────────────┤
  │    Object Store (S3, local disk)     │  ← backend
  └──────────────────────────────────────┘

Design (per the refactor):

  - PondLens is the SHARED NAMESPACE base for all Lenses. It is NOT
    format-aware. It provides only ref-namespace operations: branch,
    list_collections, set_definition, get_definition, history.

  - LakehouseLens is the APP-FACING lens for tabular workloads. It
    owns its OWN read/write API. Two storage paths coexist:

      1. Whole-table Parquet I/O (default for OLAP):
            create_table(name, table) → one Parquet blob, one commit
            insert(name, rows)        → append to existing Parquet blob
            read_table(name)          → return PyArrow Table

         This is the analytical fast path: DuckDB queries a single
         Parquet blob with full vectorized pushdown.

      2. Range read/write over the ProllyTreeIndex (for OLTP / streaming
         / point-lookup workloads):
            range_write(name, table, key_col)
                → split table into row groups; store each row group
                  as a Parquet blob in the ProllyTreeIndex, keyed by
                  the row group's max primary key. One commit per
                  range_write.
            range_read(name, start_key, end_key)
                → scan the ProllyTreeIndex from start_key to end_key,
                  read each Parquet blob, concat into one Arrow table.

         This is the operational fast path: point lookups are O(log N)
         via the Prolly tree, range scans are O(log N + K), and each
         row group can be updated independently (structural sharing
         across versions).

  - The two paths share the SAME namespace (collections/{name}/HEAD)
    so branching, history, and time travel work uniformly. A table
    written via range_write can be read via range_read; a table
    written via create_table can be queried via DuckDB.

  - The base class does NOT decide which path to use. The application
    code chooses by calling create_table() vs range_write().

Run tests:
    python lenses/lakehouse/lakehouse.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import tempfile
import shutil
from typing import Optional, Iterator, Callable

# Make pond-core, pond-sdk, and the physical_structures extension package
# importable. These sys.path inserts run ONCE at module import time
# (not per-method-call). A future upgrade to a real pip-installed package
# would replace these with absolute imports.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk",
                                  "extensions", "physical_structures"))
from kernel import PondMinimal  # noqa: E402
from base_lens import PondLens  # noqa: E402
from best_effort import best_effort, warn_best_effort  # noqa: E402

# DuckDB is OPTIONAL for LakehouseLens. The lens only needs DuckDB for
# range_point_lookup's "filter to exact row" convenience. Users who only
# want to write/read Parquet row groups and do time-travel can use the
# lens without DuckDB installed. The PondLakehouse façade (pond_lakehouse.py)
# is the only place DuckDB is required.
try:
    import duckdb
except ImportError:
    duckdb = None

# PyArrow for the Parquet/Arrow interchange (REQUIRED — this is a tabular lens)
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError(
        "PyArrow is required for LakehouseLens. "
        "Install with: pip install pyarrow"
    )

# ProllyTreeIndex for the range read/write path
try:
    from prolly_tree import ProllyLensBase, ProllyTree  # noqa: E402
    from binary_encoding import BinaryProllyTree  # noqa: E402
except ImportError:
    ProllyLensBase = None
    ProllyTree = None

# Pruning extensions — OPTIONAL. When present, LakehouseLens can do
# Vortex-style predicate pushdown (row-group → column-chunk → encoded →
# row-level). When absent, the lens falls back to full reads.
# HAVE_PRUNING is set once at import time so methods don't have to
# repeat the try/except dance on every call.
HAVE_PRUNING = False
try:
    from collection_metadata import CollectionMetadata  # noqa: E402
    from pruning import PruningPredicate, ColumnPredicate, ZoneMap  # noqa: E402
    from pruning_reader import PruningReader  # noqa: E402
    from column_chunk_zone_map import ColumnChunkZoneMap, ColumnChunkStats  # noqa: E402
    from column_chunk_storage import ColumnChunkStorage  # noqa: E402
    from encoded_chunk_storage import EncodedChunkStorage  # noqa: E402
    from encoding import EncodingHeader, decode_column  # noqa: E402
    HAVE_PRUNING = True
except ImportError:
    pass



# ---------------------------------------------------------------------------
# LakehouseLens — tabular semantics on Pond
# ---------------------------------------------------------------------------

# Default row group size. Each row group becomes one Parquet blob, one
# entry in the ProllyTreeIndex. Smaller groups = faster point lookups
# + more tree entries; larger groups = faster full scans + fewer tree
# entries. 10K is a balanced default (~1-2 MB per group for typical
# tabular data).
DEFAULT_RANGE_ROW_GROUP_SIZE = 10_000

# Default rows per column chunk (used by range_write_column_chunks and
# range_write_encoded). Must match DEFAULT_CHUNK_SIZE in
# pond-sdk/extensions/physical_structures/__init__.py — kept as a local
# constant to avoid an import-time dependency on the extension package.
# Mismatched chunk_size between write and read silently corrupts
# column-chunk pruning.
DEFAULT_CHUNK_SIZE = 1000

# Magic key prefix for row groups in the ProllyTreeIndex. ALL row groups
# (whether from create_table, insert, or range_write) are stored under
# this prefix. The key is f"{_RG_PREFIX}{max_pk_in_group}", where
# max_pk_in_group is the maximum primary key value in that group.
#
# For tables created via create_table() without an explicit key column,
# we use the row index (0..N-1) as the primary key. So a 1000-row table
# created via create_table() with no key_col will have ONE row group
# keyed as "rg/999" (max_pk = 999, the last row index).
#
# This unifies storage: ProllyTreeIndex is THE backend for all Lakehouse
# writes. There is no longer a separate "whole-table Parquet blob" path.
_RG_PREFIX = "rg/"


class LakehouseLens(PondLens):
    """App-facing tabular Lens with DuckDB query engine.

    Extends PondLens (the SHARED NAMESPACE base). The base class
    provides branch(), list_collections(), set_definition(),
    get_definition(), and history(). LakehouseLens adds:

      Whole-table Parquet I/O (default for OLAP):
        - create_table(name, table)   — write a new table
        - insert(name, rows)          — append rows
        - read_table(name, commit_hash=None) — read as PyArrow Table
        - commit_to_branch(name, branch, rows) — write to a branch
        - merge_branch(name, branch)  — union merge into HEAD

      Range read/write over the ProllyTreeIndex (for OLTP / streaming
      / point-lookup workloads):
        - range_write(name, table, key_col, row_group_size=...)
              Split `table` into row groups. Store each group as a
              Parquet blob in the ProllyTreeIndex, keyed by
              f"rg/{max_pk_in_group}". One snapshot commit.
        - range_read(name, start_key, end_key)
              Scan the ProllyTreeIndex from f"rg/{start_key}" to
              f"rg/{end_key}", read each Parquet blob, concat into
              one Arrow table.
        - range_point_lookup(name, key)
              O(log N) lookup of the row group containing `key`, then
              DuckDB-filter to the single row.
    """

    def __init__(self, kernel: PondMinimal):
        super().__init__(kernel)
        # DuckDB connection is created lazily — only when range_point_lookup
        # is called (the only method that uses it for "filter to exact row").
        # Most users instantiate LakehouseLens via PondLakehouse (which has
        # its own DuckDB connection); we don't want to create a second
        # connection eagerly here.
        self._duckdb = None
        self._cached_tables: dict[str, tuple[str, pa.Table]] = {}
        self._attached_indexer = None

    @property
    def duckdb(self):
        """Lazily-created DuckDB connection (only for range_point_lookup).

        Raises ImportError if DuckDB is not installed. Most users should
        use PondLakehouse (which has its own DuckDB connection) instead of
        accessing this property directly.
        """
        if self._duckdb is None:
            if duckdb is None:
                raise ImportError(
                    "DuckDB is required for this operation. Install with: "
                    "pip install duckdb. (LakehouseLens itself does not "
                    "require DuckDB — only range_point_lookup does.)"
                )
            self._duckdb = duckdb.connect()
        return self._duckdb

    def attach_indexer(self, indexer) -> None:
        """Attach a CollectionMetadata or CollectionIndexer for auto-notify.

        After attaching, every commit (create_table, insert, range_write,
        merge_branch) auto-notifies the indexer. EAGER indexes refresh
        immediately; LAZY indexes accumulate staleness.

        Usage:
            meta = CollectionMetadata(kernel)
            meta.register_eager_index('users', 'by_age', extractor, scan_fn)
            lens.attach_indexer(meta)
            lens.create_table('users', data)  # auto-refreshes EAGER index
        """
        self._attached_indexer = indexer

    # ==================================================================
    # Unified storage: ALL writes go through ProllyTreeIndex.
    #
    # Design (per the refactor): ProllyTreeIndex is THE storage backend
    # for LakehouseLens. There is no longer a separate "whole-table
    # Parquet blob" path. Every write (create_table, insert, range_write,
    # commit_to_branch, merge_branch) stores row groups as Parquet blobs
    # in the ProllyTreeIndex, keyed by f"rg/{max_pk_in_group}".
    #
    # This unifies versioning, branching, and time travel — they all
    # work the same way regardless of how the data was written.
    # ==================================================================

    def create_table(self, table_name: str, data: pa.Table,
                     key_col: Optional[str] = None,
                     row_group_size: int = 0,
                     message: str = "",
                     build_zone_maps: bool = True) -> str:
        """Create a new table. Stores data as row groups in ProllyTreeIndex.

        Args:
            table_name: collection name
            data: PyArrow Table to store
            key_col: column to use as the primary key for row group
                keys. If None, uses the row index (0..N-1) as the key.
            row_group_size: rows per row group. If 0 (default), stores
                the entire table as ONE row group (OLAP-style: fast full
                scans, no point-lookup benefit). Use a positive value
                (e.g., 10_000) for OLTP-style storage with fast point
                lookups.
            message: commit message (default: "create {table_name}").
            build_zone_maps: if True (default), auto-build zone maps for
                Vortex-style predicate pushdown. Set to False to disable
                (saves write overhead when pruning is not needed).

        Returns:
            The new HEAD commit hash.
        """
        if row_group_size == 0:
            row_group_size = max(data.num_rows, 1)  # one group = whole table
        return self._write_via_prolly(table_name, data, key_col, row_group_size,
                                        message=message or f"create {table_name}",
                                        build_zone_maps=build_zone_maps)

    def insert(self, table_name: str, new_data: pa.Table,
               key_col: Optional[str] = None,
               row_group_size: Optional[int] = None,
               message: str = "") -> str:
        """Append rows to a table. Reads current row groups, concats
        with new_data, re-writes as new row groups via ProllyTreeIndex.

        Args:
            table_name: collection name (must already exist)
            new_data: PyArrow Table to append
            key_col: column to use as primary key. If None, uses row
                index. Should match the key_col used to create the table.
            row_group_size: rows per row group for the rewritten table.
                If None, preserves the existing row group structure
                (writes new_data as one additional row group).
            message: commit message (default: "insert {N} rows").
        """
        current = self.read_table(table_name)
        try:
            combined = pa.concat_tables([current, new_data], promote_options="default")
        except TypeError:
            combined = pa.concat_tables([current, new_data])

        if row_group_size is None:
            # Preserve existing structure: write combined as one group
            # keyed by the new max row index. This is O(1) in the number
            # of existing row groups (structural sharing via Prolly tree).
            rg_size = max(combined.num_rows, 1)
        else:
            rg_size = row_group_size

        commit_hash = self._write_via_prolly(table_name, combined, key_col, rg_size,
                                        message=message or f"insert {new_data.num_rows} rows")

        self._notify_indexers(table_name)
        return commit_hash

    def read_table(self, table_name: str,
                   commit_hash: Optional[str] = None) -> pa.Table:
        """Read a table as a PyArrow Table. If commit_hash is None,
        reads HEAD (time travel via commit_hash).

        Reads ALL row groups from the ProllyTreeIndex at the given commit
        and concatenates them. This is O(N/chunk_size) tree reads + N
        blob reads — efficient for full scans.
        """
        if commit_hash is None:
            commit_hash = self.kernel.resolve(self._head_ref(table_name))
            if commit_hash is None:
                raise KeyError(f"Collection '{table_name}' not found")

        # Cache check — keyed by (table_name, commit_hash)
        cache_key = f"{table_name}:{commit_hash}"
        cached = self._cached_tables.get(cache_key)
        if cached is not None:
            return cached

        table = self._read_all_row_groups(table_name, commit_hash)
        self._cached_tables[cache_key] = table
        return table

    def commit_to_branch(self, table_name: str, branch_name: str,
                         new_data: pa.Table,
                         key_col: Optional[str] = None) -> str:
        """Commit new data to a branch (not HEAD). Reads the branch's
        current state, concatenates with new_data, writes new row groups
        to the branch ref via ProllyTreeIndex."""
        current = self.read_branch(table_name, branch_name)
        try:
            combined = pa.concat_tables([current, new_data], promote_options="default")
        except TypeError:
            combined = pa.concat_tables([current, new_data])
        # Write as one row group (preserves structure)
        rg_size = max(combined.num_rows, 1)
        # Write row groups to ProllyTreeIndex, then commit to branch ref
        return self._write_via_prolly_to_branch(table_name, branch_name, combined,
                                                  key_col, rg_size,
                                                  message=f"branch {branch_name}: insert {new_data.num_rows} rows")

    def read_branch(self, name: str, branch_name: str) -> pa.Table:
        """Read a branch's data as a PyArrow Table."""
        ref = self._branch_ref(name, branch_name)
        commit_hash = self.kernel.resolve(ref)
        if commit_hash is None:
            raise KeyError(f"Branch '{branch_name}' not found in '{name}'")
        return self.read_table(name, commit_hash)

    def merge_branch(self, name: str, branch_name: str) -> str:
        """Union merge a branch into HEAD. Concatenates HEAD and branch
        tables, writes new row groups via ProllyTreeIndex, creates a
        2-parent merge commit."""
        head = self.kernel.resolve(self._head_ref(name))
        branch_ref = self._branch_ref(name, branch_name)
        branch_head = self.kernel.resolve(branch_ref)
        if branch_head is None:
            raise KeyError(f"Branch '{branch_name}' not found")

        head_data = self.read_table(name, head)
        branch_data = self.read_table(name, branch_head)

        try:
            merged = pa.concat_tables([head_data, branch_data], promote_options="default")
        except TypeError:
            merged = pa.concat_tables([head_data, branch_data])

        # Write merged data as row groups via ProllyTreeIndex
        rg_size = max(merged.num_rows, 1)  # one group for simplicity
        # Use internal helper to write row groups + create merge commit
        commit_hash = self._write_merge_via_prolly(name, merged, head, branch_head,
                                             rg_size, f"merge branch '{branch_name}'")

        self._notify_indexers(name)
        return commit_hash

    # ==================================================================
    # Range read/write over the ProllyTreeIndex (OLTP / streaming / point
    # lookup fast path). This is the Lakehouse-specific extension that
    # sits on top of the shared ProllyTreeIndex storage backend.
    # ==================================================================

    def _range_write_generic(self, name: str, table: pa.Table,
                              key_col: str, row_group_size: int,
                              write_one_rowgroup: Callable,
                              commit_message: str) -> str:
        """Shared scaffold for range_write / range_write_column_chunks /
        range_write_encoded.

        Handles the boilerplate that is identical across all three write
        paths:
          - Validate ProllyLensBase is available
          - Validate key_col exists
          - Open ProllyLensBase + ZoneMapIndex
          - Clear old zone maps (overwrite semantics)
          - Sort by key_col
          - For each row group: call write_one_rowgroup(group_table, rg_key)
            → returns (data_blob_hash, cczm_or_None)
          - Stage the data blob, build zone map if cczm provided
          - Commit + commit zone maps + invalidate cache + notify indexers

        Args:
            name: collection name
            table: the rows to write
            key_col: the column whose values become the row-group keys
            row_group_size: rows per row group
            write_one_rowgroup: callable(group_table, rg_key) →
                (data_blob_hash, cczm_or_None). The data_blob_hash is
                staged at rg_key. If cczm is provided, it's merged into
                the zone map for column-chunk pruning.
            commit_message: the commit message string

        Returns:
            The new HEAD commit hash.
        """
        if ProllyLensBase is None:
            raise ImportError("range_write requires prolly_tree.py")

        if key_col not in table.column_names:
            raise KeyError(f"key column '{key_col}' not in table columns {table.column_names}")

        from collection_metadata import CollectionMetadata
        from pruning import ZoneMap

        base = ProllyLensBase(self.kernel, name)
        zm_index = None
        try:
            zm_index = CollectionMetadata(self.kernel).zm_index
        except ImportError:
            pass

        # Clear old zone maps for this collection (overwrite semantics).
        # Uses the public clear_zone_maps() API instead of reaching into
        # zm_index._get_base(name). Best-effort — failures are logged via
        # the pond.best_effort logger (enable with POND_DEBUG=1).
        if zm_index is not None:
            best_effort(f"clear zone maps for {name}",
                        zm_index.clear_zone_maps, name)

        # Sort by key_col so row groups are contiguous key ranges
        sorted_table = table.sort_by(key_col)
        n_rows = sorted_table.num_rows
        if n_rows == 0:
            return base.commit(commit_message + ": empty table")

        key_array = sorted_table.column(key_col).to_pylist()

        # Stage each row group
        for start in range(0, n_rows, row_group_size):
            end = min(start + row_group_size, n_rows)
            group_table = sorted_table.slice(start, end - start)
            max_pk = key_array[end - 1]
            rg_key = f"{_RG_PREFIX}{max_pk}"

            # Storage-specific write — returns data blob hash + optional cczm
            data_blob_hash, cczm = write_one_rowgroup(group_table, rg_key)
            base.stage(rg_key, data_blob_hash)

            # Build zone map for this row group (best-effort — failures
            # are logged via pond.best_effort, NOT silently swallowed).
            # A failure here means the row group won't have a zone map,
            # so it won't be prunable. That's correct behavior (no false
            # negatives) but the user should know.
            if zm_index is not None:
                def _build_and_add_zm(_gt=group_table, _rk=rg_key,
                                       _cczm=cczm, _dbh=data_blob_hash,
                                       _zm_index=zm_index, _name=name):
                    zm = ZoneMap.build(_gt)
                    if _cczm is not None:
                        zm_dict = zm.to_dict()
                        zm_dict["column_chunks"] = _cczm.to_dict()
                        zm = ZoneMap.from_dict(zm_dict)
                    _zm_index.add_zone_map(_name, _rk, zm, _dbh)
                best_effort(f"build zone map for {name}.{rg_key}",
                            _build_and_add_zm)

        n_groups = (n_rows + row_group_size - 1) // row_group_size
        commit_hash = base.commit(f"{commit_message}: {n_rows} rows in {n_groups} row groups")

        if zm_index is not None:
            best_effort(f"commit zone maps for {name}",
                        zm_index.commit_zone_maps, name,
                        f"zone maps for {name}")

        self._cached_tables.pop(f"{name}:{commit_hash}", None)
        self._notify_indexers(name)
        return commit_hash

    def range_write(self, name: str, table: pa.Table, key_col: str,
                    row_group_size: int = DEFAULT_RANGE_ROW_GROUP_SIZE) -> str:
        """Write a table as a sequence of row groups in the ProllyTreeIndex.

        Each row group is encoded as a Parquet blob. The ProllyTreeIndex
        maps f"rg/{max_pk_in_group}" → parquet_blob_hash. The result is
        one snapshot commit on top of the existing HEAD.

        This is the OLTP-friendly write path: each row group can be
        updated independently (structural sharing), point lookups are
        O(log N) via the Prolly tree, and range scans are O(log N + K)
        where K is the number of row groups in the range.

        Args:
            name: collection name (must already exist or be new)
            table: the rows to write
            key_col: the column whose values become the row-group keys.
                     Must be sortable (string, int, or sortable bytes).
            row_group_size: rows per row group. Default 10_000.

        Returns:
            The new HEAD commit hash.
        """
        # Also compute column-chunk zone maps for finer pruning (best-effort
        # — falls back gracefully if the extension is not installed).
        def write_parquet_blob(group_table, rg_key):
            parquet_bytes = self._encode_table(group_table)
            parquet_hash = self.kernel.write(parquet_bytes)

            # Compute column-chunk zone maps (in-line stats; no per-chunk blobs).
            # HAVE_PRUNING is set once at module import time.
            cczm = None
            if HAVE_PRUNING:
                cczm = best_effort(
                    f"build column-chunk zone map for {rg_key}",
                    ColumnChunkZoneMap.build, group_table, rg_key,
                    chunk_size=DEFAULT_CHUNK_SIZE)

            return parquet_hash, cczm

        return self._range_write_generic(
            name, table, key_col, row_group_size,
            write_one_rowgroup=write_parquet_blob,
            commit_message="range_write",
        )

    def range_write_column_chunks(self, name: str, table: pa.Table,
                                   key_col: str,
                                   row_group_size: int = DEFAULT_RANGE_ROW_GROUP_SIZE,
                                   chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
        """Write a table with per-column-chunk storage.

        Like range_write, but each row group is further split into
        per-column-chunk Parquet blobs. Each blob is a single-column,
        single-chunk Parquet file stored as its own content-addressed
        blob in the kernel.

        This enables TRUE I/O savings on object storage:
          - Column-chunk pruning skips actual kernel.read_blob() calls
            (not just row_filter work)
          - Skip 4/5 chunks = skip 4/5 of bytes per column
          - Each chunk is independently addressable for parallel reads

        The data ProllyTreeIndex maps f"rg/{max_pk}" → manifest_blob_hash.
        The manifest is a small JSON blob listing all chunk blob hashes
        per column. This preserves read_table() compatibility.

        The zone map blob's column_chunks stats are augmented with
        blob_hash fields so PruningReader can fetch surviving chunk
        blobs directly without reading the manifest.

        Args:
            name: collection name
            table: the rows to write
            key_col: the column whose values become the row-group keys
            row_group_size: rows per row group (default 10_000)
            chunk_size: rows per column chunk (default 1000)

        Returns:
            The new HEAD commit hash.
        """
        if not HAVE_PRUNING:
            raise ImportError(
                "ColumnChunkStorage extension required for range_write_column_chunks. "
                "Install the pond-sdk physical_structures extension package."
            )

        storage = ColumnChunkStorage(self.kernel)

        # Format-agnostic encode_fn: (col_name, values: list) -> bytes.
        # LakehouseLens uses Parquet; other lenses would use their own encoder.
        def encode_parquet(col_name, values):
            chunk_table = pa.Table.from_arrays(
                [pa.array(values)], names=[col_name])
            return self._encode_table(chunk_table)

        def write_per_column_chunks(group_table, rg_key):
            manifest_hash, cczm = storage.write_row_group_column_chunks(
                group_table, rg_key, chunk_size=chunk_size,
                encode_fn=encode_parquet,
            )
            return manifest_hash, cczm

        return self._range_write_generic(
            name, table, key_col, row_group_size,
            write_one_rowgroup=write_per_column_chunks,
            commit_message=f"range_write_column_chunks (chunk_size={chunk_size})",
        )

    def range_write_encoded(self, name: str, table: pa.Table,
                             key_col: str,
                             row_group_size: int = DEFAULT_RANGE_ROW_GROUP_SIZE,
                             chunk_size: int = DEFAULT_CHUNK_SIZE,
                             encoding_hints: Optional[dict] = None) -> str:
        """Write a table with per-column-chunk encoded storage.

        Like range_write_column_chunks, but each chunk blob is encoded
        with a FastLanes-style structural encoding (RLE, Dict, Bitpack,
        or Raw). The encoding is chosen automatically based on data
        characteristics, or per-column via encoding_hints.

        This enables ENCODED predicate evaluation at read time:
          - For RLE/Dict: evaluate predicate on encoded form, skip
            decode for fully-pruned chunks
          - For Bitpack: prune via min/max in the encoding header
          - For Raw: fall back to decode + filter (no shortcut)

        Args:
            name: collection name
            table: the rows to write
            key_col: the column whose values become the row-group keys
            row_group_size: rows per row group (default 10_000)
            chunk_size: rows per column chunk (default 1000)
            encoding_hints: optional dict {column: "auto"|"rle"|"dict"|
                "bitpack"|"raw"}. If None, all columns use "auto".

        Returns:
            The new HEAD commit hash.
        """
        if not HAVE_PRUNING:
            raise ImportError(
                "EncodedChunkStorage extension required for range_write_encoded. "
                "Install the pond-sdk physical_structures extension package."
            )

        storage = EncodedChunkStorage(self.kernel)

        def write_encoded_chunks(group_table, rg_key):
            manifest_hash, cczm = storage.write_row_group_encoded(
                group_table, rg_key, chunk_size=chunk_size,
                encoding_hints=encoding_hints,
            )
            return manifest_hash, cczm

        return self._range_write_generic(
            name, table, key_col, row_group_size,
            write_one_rowgroup=write_encoded_chunks,
            commit_message=f"range_write_encoded (chunk_size={chunk_size}, encoded)",
        )

    def range_read(self, name: str,
                   start_key: Optional[str] = None,
                   end_key: Optional[str] = None) -> pa.Table:
        """Read a range of rows from a range_write collection.

        Row groups are keyed in the ProllyTreeIndex by their MAX primary
        key (f"rg/{max_pk}"). To find all row groups that may contain
        rows in [start_key, end_key], we use:

            k >= f"rg/{start_key}"   (a group's max_pk >= start_key
                                      means it may contain rows >= start_key)

        The upper bound (end_key) is NOT used to filter row group keys,
        because a row group with max_pk > end_key may still contain
        rows <= end_key. The caller must filter exact rows via DuckDB.

        Args:
            name: collection name (must have been written via range_write)
            start_key: inclusive lower bound on primary key (None = no
                       lower bound, scan from the beginning)
            end_key: inclusive upper bound on primary key (None = no
                     upper bound). Used only for documentation; the
                     actual row-level filtering is the caller's job.

        Returns:
            A PyArrow Table containing all rows from row groups whose
            max primary key >= start_key. The caller should filter to
            exact rows in [start_key, end_key] via DuckDB.

        Note: this is row-group-granular. If a row group spans the
        start boundary, the entire group is included (you may get rows
        slightly below start_key). Filter via DuckDB for exact rows.
        """
        if ProllyLensBase is None:
            raise ImportError("range_read requires prolly_tree.py")

        base = ProllyLensBase(self.kernel, name)
        state = base.read_all()

        # Filter to row-group keys (start with the magic prefix).
        # Apply lower bound on max_pk: k >= f"rg/{start_key}".
        # No upper-bound filter on keys (see docstring).
        rg_prefix = _RG_PREFIX
        if start_key is None:
            scan_start = rg_prefix  # lexicographically smallest possible rg key
        else:
            scan_start = f"{rg_prefix}{start_key}"

        matching_keys = sorted(
            k for k in state.keys()
            if k.startswith(rg_prefix) and k >= scan_start
        )

        if not matching_keys:
            return pa.table({})

        tables = []
        for k in matching_keys:
            parquet_hash = state[k]
            parquet_bytes = self.kernel.read_blob(parquet_hash)
            tables.append(self._decode_table(parquet_bytes))

        try:
            return pa.concat_tables(tables, promote_options="default")
        except TypeError:
            return pa.concat_tables(tables)

    def range_point_lookup(self, name: str, key: str) -> Optional[pa.Table]:
        """O(log N) point lookup: find the single row group containing
        `key`, then return just the rows where the key column equals `key`.

        This requires range_write to have stored rows sorted by key_col.
        We look up the smallest row-group key >= f"rg/{key}" — that's
        the row group containing `key` (because row groups are sorted
        by max_pk and the lookup key falls within the group whose
        max_pk is the smallest >= the target).

        Returns:
            A PyArrow Table containing the matching row(s), or None
            if no row group contains the key.

        Note: the caller must know the key column name. We can't
        recover it from the storage — it's a Lakehouse-level decision.
        Pass it as `key_col`.
        """
        # Without knowing the key_col, we can only return the entire
        # matching row group. The caller can filter further.
        # This is a known limitation — point lookups require schema
        # awareness, which lives at the Lakehouse level, not in the
        # base class.
        if ProllyLensBase is None:
            raise ImportError("range_point_lookup requires prolly_tree.py")

        base = ProllyLensBase(self.kernel, name)
        # The row group containing `key` has max_pk >= key. We want the
        # SMALLEST such key. ProllyLensBase doesn't expose a successor
        # lookup directly, but read_all + sort gives us one.
        state = base.read_all()
        rg_keys = sorted(k for k in state.keys() if k.startswith(_RG_PREFIX))
        target = f"{_RG_PREFIX}{key}"
        for rg_key in rg_keys:
            if rg_key >= target:
                # This row group's max_pk is >= key, so it may contain key.
                parquet_hash = state[rg_key]
                parquet_bytes = self.kernel.read_blob(parquet_hash)
                return self._decode_table(parquet_bytes)
        return None

    # ==================================================================
    # Projection pushdown — read only needed columns from Parquet row groups
    # ==================================================================

    def read_columns(self, name: str, columns: list[str],
                     commit_hash: Optional[str] = None) -> pa.Table:
        """Read only the specified columns from a table.

        PROJECTION PUSHDOWN: Instead of reading and decoding all columns
        from every Parquet row group, this method reads only the requested
        columns. For wide tables (50+ columns), this can reduce I/O by
        10-100x when only a few columns are needed.

        This is the Pond equivalent of Vortex's projection pushdown:
        the reader descends only the column branches it needs, never
        touching the other columns' data.

        Args:
            name: collection name
            columns: list of column names to read. Other columns are
                NOT read from disk (Parquet column-level access).
            commit_hash: optional commit hash for time travel.

        Returns:
            A PyArrow Table with only the requested columns.
        """
        if commit_hash is None:
            commit_hash = self.kernel.resolve(self._head_ref(name))
            if commit_hash is None:
                raise KeyError(f"Collection '{name}' not found")

        # Read the commit to find row group blob hashes
        raw = self.kernel.read_blob(commit_hash)
        from binary_encoding import BinaryProllyTree as _BPT

        if len(raw) > 0 and raw[0] == 3:
            commit = _BPT.decode_commit(raw)
            snapshot_root = commit.get("snapshot")
            if snapshot_root is None:
                base = ProllyLensBase(self.kernel, name)
                state = base.read_state_at_commit(commit_hash)
            else:
                state = ProllyTree.read_all(self.kernel, snapshot_root)
        else:
            # Legacy JSON commit
            commit = json.loads(raw)
            if "parquet" in commit:
                parquet_bytes = self.kernel.read(commit["parquet"])
                full_table = self._decode_table(parquet_bytes)
                # Project columns
                available = [c for c in columns if c in full_table.column_names]
                return full_table.select(available) if available else pa.table({})
            raise ValueError(f"Cannot decode commit {commit_hash} for '{name}'")

        # Read row groups with column projection
        rg_keys = sorted(k for k in state.keys() if k.startswith(_RG_PREFIX))
        if not rg_keys:
            return pa.table({})

        tables = []
        for k in rg_keys:
            parquet_hash = state[k]
            parquet_bytes = self.kernel.read_blob(parquet_hash)
            # PyArrow Parquet reader supports column-level access:
            # pq.read_table(reader, columns=["col1", "col2"]) only reads
            # the specified column chunks from the Parquet file.
            reader = pa.BufferReader(parquet_bytes)
            try:
                table = pq.read_table(reader, columns=columns)
                tables.append(table)
            except (KeyError, ValueError, pa.ArrowInvalid) as exc:
                # Column projection failed (e.g., column not found, schema
                # mismatch). Fall back to full read + project afterwards.
                # Logged at DEBUG so users can diagnose slow reads.
                warn_best_effort(
                    f"column projection for {columns} failed — falling back to full read",
                    exc)
                full = self._decode_table(parquet_bytes)
                available = [c for c in columns if c in full.column_names]
                if available:
                    tables.append(full.select(available))

        if not tables:
            return pa.table({})

        try:
            return pa.concat_tables(tables, promote_options="default")
        except TypeError:
            return pa.concat_tables(tables)

    # ==================================================================
    # Pruning-accelerated read (Vortex-style predicate pushdown)
    # ==================================================================

    def _read_with_pruning_generic(self, name: str,
                                     predicates: Optional[list],
                                     row_filter: Optional[Callable],
                                     columns: Optional[list[str]],
                                     chunk_size: int,
                                     read_surviving_rowgroup: Callable
                                     ) -> pa.Table:
        """Shared scaffold for read_with_column_chunk_pruning and
        read_with_encoded_pruning.

        Handles the boilerplate that is identical across both:
          - Build PruningPredicate from (column, op, value) tuples
          - Build cc_predicates lookup (column → (op, value))
          - Infer columns if not provided
          - Walk surviving row groups via verbose scan
          - For each row group, call read_surviving_rowgroup(rg_key,
            manifest_hash, zm_dict, cc_predicates, columns) → pa.Table
            or None (skip this row group)
          - When row_filter is None (common case — SQL pushdown where
            DuckDB does the filter): concat tables via pa.concat_tables
            (no Python object allocation — Arrow-native fast path)
          - When row_filter is provided: convert to list[dict], apply
            filter, reconstruct via from_pylist (slower but supports
            arbitrary Python lambdas)

        Falls back to read_table if pruning extensions are missing or no
        zone maps exist for the collection.

        Args:
            name: collection name
            predicates: list of (column, op, value) tuples
            row_filter: optional function(row_dict) -> bool
            columns: list of columns to read (None = all)
            chunk_size: rows per column chunk (must match write time)
            read_surviving_rowgroup: callable(rg_key, manifest_hash,
                zm_dict, cc_predicates, columns) → pa.Table or None.
                Returns the surviving rows as a PyArrow Table, or None
                to skip this row group.

        Returns:
            A PyArrow Table containing surviving rows.
        """
        if not HAVE_PRUNING:
            return self.read_table(name)

        meta = CollectionMetadata(self.kernel)
        zm_index = meta.zm_index

        if zm_index is None or not zm_index.has_zone_maps(name):
            return self.read_table(name)

        # Build pruning predicate
        predicate = None
        if predicates:
            col_preds = [ColumnPredicate(column=c, op=o, value=v)
                         for c, o, v in predicates]
            predicate = PruningPredicate(col_preds, combine="and")

        # Build column-chunk predicate lookup (op, value) per column
        cc_predicates: dict[str, tuple[str, Any]] = {}
        if predicate:
            for pred in predicate.predicates:
                cc_predicates[pred.column] = (pred.op, pred.value)

        # Determine which columns to read
        if columns is None:
            columns = self._infer_columns(name, zm_index)

        # Walk surviving row groups via the verbose pruning scan.
        # Collect pa.Tables (Arrow-native fast path) when row_filter is None;
        # collect list[dict] when row_filter is provided (Python filter path).
        tables: list[pa.Table] = []
        all_rows: list[dict] = [] if row_filter is not None else None

        for rg_key, manifest_hash, zm_dict in zm_index.scan_with_pruning(
                name, predicate, verbose=True):

            # Delegate to the storage-specific reader → pa.Table or None
            table = read_surviving_rowgroup(
                rg_key, manifest_hash, zm_dict, cc_predicates, columns)

            if table is None or table.num_rows == 0:
                continue  # skip empty row groups

            if row_filter is None:
                # Arrow-native fast path — no Python object allocation
                tables.append(table)
            else:
                # Python filter path — convert to list[dict], filter, accumulate
                for row in table.to_pylist():
                    if row_filter(row):
                        all_rows.append(row)

        # Construct the result Table
        if row_filter is None:
            if not tables:
                return pa.table({})
            try:
                return pa.concat_tables(tables, promote_options="default")
            except TypeError:
                return pa.concat_tables(tables)
        else:
            if not all_rows:
                return pa.table({})
            return pa.Table.from_pylist(all_rows)

    @staticmethod
    def _compute_surviving_chunks(cczm, cc_predicates: dict
                                   ) -> Optional[set[int]]:
        """Compute the set of chunk indices that survive all predicates.

        Takes the INTERSECTION across predicate columns (predicates are
        ANDed). Returns None if no predicates have stats (caller should
        read all chunks). Returns the empty set if all chunks pruned
        (caller should skip the row group).

        Args:
            cczm: ColumnChunkZoneMap for the row group
            cc_predicates: dict of column_name → (op, value)

        Returns:
            Optional[set[int]] of surviving chunk indices.
        """
        if not cc_predicates:
            return None  # no predicates — read all chunks

        surviving_chunks: Optional[set[int]] = None
        for col, (op, val) in cc_predicates.items():
            chunks = cczm.prune_column_chunks(col, op, val)
            # None means no stats for this column — caller must fall back
            # to reading all chunks. Skip the column (don't include in
            # the intersection) so we don't silently drop rows.
            if chunks is None:
                continue
            chunk_set = set(chunks)
            if surviving_chunks is None:
                surviving_chunks = chunk_set
            else:
                surviving_chunks &= chunk_set
        return surviving_chunks

    def read_with_pruning(self, name: str,
                          predicates: Optional[list] = None,
                          row_filter: Optional[Callable] = None,
                          columns: Optional[list[str]] = None,
                          chunk_size: int = DEFAULT_CHUNK_SIZE) -> pa.Table:
        """Read a table with Vortex-style predicate pushdown.

        Reads zone maps first (small, cheap), evaluates the pruning
        predicate, and only fetches + decodes data blobs that MIGHT match.
        Skips row groups whose zone maps prove they can't match — WITHOUT
        reading or decoding the data blob.

        Args:
            name: collection name (must have zone maps, built at write time)
            predicates: list of (column, op, value) tuples for pruning.
                Example: [("age", ">", 30), ("region", "=", "US")]
                All predicates are ANDed together.
                If None, no pruning (reads all row groups).
            row_filter: optional function(row_dict) -> bool for exact
                row-level filtering after pruning. This catches false
                positives from zone-map pruning.
            columns: optional list of column names to enable column-chunk
                pruning. When provided, the reader uses per-column-chunk
                zone maps to skip individual column chunks within
                surviving row groups. Must be a subset of the predicate
                columns to have any effect.
            chunk_size: rows per column chunk (must match the chunk_size
                used at write time when building ColumnChunkZoneMap).
                Default 1000.

        Returns:
            A PyArrow Table containing rows from non-pruned row groups
            (optionally filtered by row_filter and column-chunk pruning).
        """
        try:
            from collection_metadata import CollectionMetadata
            from pruning import PruningPredicate, ColumnPredicate
            from pruning_reader import PruningReader
            meta = CollectionMetadata(self.kernel)
            zm_index = meta.zm_index
        except ImportError:
            # No pruning extension — fall back to full read
            return self.read_table(name)

        if zm_index is None:
            return self.read_table(name)

        # Check if zone maps exist for this collection
        if not zm_index.has_zone_maps(name):
            return self.read_table(name)  # No zone maps — no pruning

        # Build pruning predicate from the list of (column, op, value) tuples
        predicate = None
        if predicates:
            col_preds = [ColumnPredicate(column=c, op=o, value=v)
                         for c, o, v in predicates]
            predicate = PruningPredicate(col_preds, combine="and")

        reader = PruningReader(self.kernel, zm_index, name, predicate)

        # Decode function: Parquet bytes → list of row dicts
        def decode_parquet(data_bytes):
            table = self._decode_table(data_bytes)
            return table.to_pylist()

        # Scan with pruning (column-chunk pruning active if columns is set)
        rows = list(reader.scan(decode_fn=decode_parquet,
                                row_filter=row_filter,
                                columns=columns,
                                chunk_size=chunk_size))

        if not rows:
            return pa.table({})

        # Convert list of dicts back to PyArrow Table
        return pa.Table.from_pylist(rows)

    def read_with_column_chunk_pruning(self, name: str,
                                        predicates: Optional[list] = None,
                                        row_filter: Optional[Callable] = None,
                                        columns: Optional[list[str]] = None,
                                        chunk_size: int = DEFAULT_CHUNK_SIZE) -> pa.Table:
        """Read with per-column-chunk storage (TRUE I/O savings).

        Like read_with_pruning, but uses per-column-chunk storage
        (written by range_write_column_chunks). For each surviving row
        group, only the surviving chunk BLOBS are fetched from the kernel
        — not the whole row group blob.

        This delivers real I/O savings on object storage:
          - Skip 4/5 chunks = skip 4/5 of kernel.read_blob() calls
          - Skip 4/5 of bytes per column
          - Each chunk is a separate content-addressed blob

        Falls back to read_with_pruning if column-chunk storage is not
        available for this collection (e.g., written via range_write
        without the column_chunks mode).

        Args:
            name: collection name (must have zone maps)
            predicates: list of (column, op, value) tuples for pruning
            row_filter: optional function(row_dict) -> bool for exact
                row-level filtering after pruning
            columns: list of columns to read. If None, reads ALL columns
                from surviving chunks. If specified, only those columns
                are read (projection pushdown + column-chunk pruning).
            chunk_size: rows per column chunk (must match the chunk_size
                used at write time)

        Returns:
            A PyArrow Table containing rows from surviving chunks.
        """
        if not HAVE_PRUNING:
            return self.read_with_pruning(name, predicates=predicates,
                                          row_filter=row_filter,
                                          columns=columns,
                                          chunk_size=chunk_size)

        storage = ColumnChunkStorage(self.kernel)

        def read_surviving_rowgroup(rg_key, manifest_hash, zm_dict,
                                      cc_predicates, columns):
            # If column-chunk storage is not active for this row group,
            # fall back to reading the whole blob.
            if not ColumnChunkStorage.has_column_chunk_storage(zm_dict):
                actual_data_hash = zm_dict.get("blob_hash", manifest_hash)
                data_bytes = self.kernel.read_blob(actual_data_hash)
                decoded = best_effort(
                    f"decode whole blob for {rg_key} (column-chunk fallback)",
                    self._decode_table, data_bytes)
                if decoded is None:
                    return None
                return decoded

            # Column-chunk storage active — read only surviving chunks.
            # decode_fn returns list (format-agnostic); we wrap in pa.array.
            cczm = ColumnChunkZoneMap.from_dict(zm_dict["column_chunks"])
            surviving_chunks = self._compute_surviving_chunks(cczm, cc_predicates)
            if surviving_chunks is not None and not surviving_chunks:
                return None  # all chunks pruned — skip row group

            def decode_parquet(chunk_bytes):
                table = self._decode_table(chunk_bytes)
                return table.column(0).to_pylist()

            col_data = storage.read_column_chunks(
                cczm, columns, surviving_chunks,
                decode_fn=decode_parquet,
            )
            if not col_data:
                return None

            # Reassemble: concatenate value-lists per column, build pa.Table.
            arrays = []
            col_names_out = []
            for col_name in columns:
                if col_name in col_data and col_data[col_name]:
                    all_vals = []
                    for vals in col_data[col_name]:
                        all_vals.extend(vals)
                    arrays.append(pa.array(all_vals))
                    col_names_out.append(col_name)
            if not arrays:
                return None

            return pa.Table.from_arrays(arrays, names=col_names_out)

        return self._read_with_pruning_generic(
            name, predicates, row_filter, columns, chunk_size,
            read_surviving_rowgroup=read_surviving_rowgroup,
        )

    def read_with_encoded_pruning(self, name: str,
                                    predicates: Optional[list] = None,
                                    row_filter: Optional[Callable] = None,
                                    columns: Optional[list[str]] = None,
                                    chunk_size: int = DEFAULT_CHUNK_SIZE) -> pa.Table:
        """Read with FastLanes-style encoded predicate evaluation.

        Like read_with_column_chunk_pruning, but uses encoded chunk
        storage (written by range_write_encoded). For each surviving
        chunk blob, evaluates the predicate on the ENCODED form first:
          - RLE/Dict: evaluate predicate on encoded form, decode only
            surviving row ranges
          - Bitpack: prune via min/max in the encoding header
          - Raw: fall back to decode + filter

        This skips the decode step for pruned chunks, providing
        additional speedup on top of column-chunk pruning.

        Falls back to read_with_column_chunk_pruning if encoded
        storage is not available for this collection.

        Args:
            name: collection name (must have zone maps)
            predicates: list of (column, op, value) tuples
            row_filter: optional function(row_dict) -> bool
            columns: list of columns to read (projection). If None,
                reads all columns from surviving chunks.
            chunk_size: rows per column chunk (must match write time)

        Returns:
            A PyArrow Table containing rows that survived all pruning.
        """
        if not HAVE_PRUNING:
            return self.read_with_column_chunk_pruning(
                name, predicates=predicates, row_filter=row_filter,
                columns=columns, chunk_size=chunk_size)

        storage = EncodedChunkStorage(self.kernel)
        plain_storage = ColumnChunkStorage(self.kernel)

        def read_surviving_rowgroup(rg_key, manifest_hash, zm_dict,
                                      cc_predicates, columns):
            # If encoded storage is not active, fall back to:
            #   - column-chunk storage (read manifest chunks)
            #   - plain Parquet blob (decode directly)
            if not EncodedChunkStorage.has_encoded_storage(zm_dict):
                if ColumnChunkStorage.has_column_chunk_storage(zm_dict):
                    # Column-chunk storage: read all chunks for this row group
                    cczm = ColumnChunkZoneMap.from_dict(zm_dict["column_chunks"])
                    def decode_parquet_fallback(chunk_bytes):
                        table = self._decode_table(chunk_bytes)
                        return table.column(0).to_pylist()
                    col_data = plain_storage.read_column_chunks(
                        cczm, columns, None,  # None = read all chunks
                        decode_fn=decode_parquet_fallback,
                    )
                    if not col_data:
                        return None
                    arrays = []
                    col_names_out = []
                    for col_name in columns:
                        if col_name in col_data and col_data[col_name]:
                            all_vals = []
                            for vals in col_data[col_name]:
                                all_vals.extend(vals)
                            arrays.append(pa.array(all_vals))
                            col_names_out.append(col_name)
                    if not arrays:
                        return None
                    return pa.Table.from_arrays(arrays, names=col_names_out)
                else:
                    # Plain Parquet blob (range_write / _write_via_prolly)
                    actual_data_hash = zm_dict.get("blob_hash", manifest_hash)
                    data_bytes = self.kernel.read_blob(actual_data_hash)
                    decoded = best_effort(
                        f"decode plain Parquet blob for {rg_key}",
                        self._decode_table, data_bytes)
                    if decoded is None:
                        return None  # undecodable blob — skip
                    return decoded

            # Encoded storage active — read surviving chunks with encoded
            # predicate eval where possible.
            cczm = ColumnChunkZoneMap.from_dict(zm_dict["column_chunks"])
            surviving_chunks = self._compute_surviving_chunks(cczm, cc_predicates)
            if surviving_chunks is not None and not surviving_chunks:
                return None  # all chunks pruned — skip row group

            # read_column_chunks_encoded evaluates predicates on the
            # encoded form and decodes only surviving ranges.
            col_data = storage.read_column_chunks_encoded(
                cczm, columns, surviving_chunks,
                predicates=predicates,  # pass-through for encoded eval
            )
            if not col_data:
                return None

            # Reassemble rows: each column has a list of (chunk_index, values).
            # All columns have the SAME number of values per chunk (the
            # surviving rows) because read_column_chunks_encoded evaluates
            # the predicate on the predicate column and reads ALL columns
            # at the same surviving positions. So we can build pa.Table
            # directly from column arrays — Arrow-native, no list[dict]
            # round-trip.
            col_arrays_out: dict[str, list] = {}
            for col_name, chunk_list in col_data.items():
                for _ci, values in chunk_list:
                    if col_name not in col_arrays_out:
                        col_arrays_out[col_name] = []
                    col_arrays_out[col_name].extend(values)

            if not col_arrays_out:
                return None

            arrays = []
            col_names_out = []
            for col_name in columns:
                if col_name in col_arrays_out and col_arrays_out[col_name]:
                    arrays.append(pa.array(col_arrays_out[col_name]))
                    col_names_out.append(col_name)
            if not arrays:
                return None
            return pa.Table.from_arrays(arrays, names=col_names_out)

        return self._read_with_pruning_generic(
            name, predicates, row_filter, columns, chunk_size,
            read_surviving_rowgroup=read_surviving_rowgroup,
        )

    def _infer_columns(self, name: str, zm_index) -> list[str]:
        """Infer column names from the first zone map of a collection.

        Uses the public iter_zone_maps() API instead of reaching into
        zm_index._get_base(name). Best-effort — returns [] if the zone
        maps can't be read (e.g., extension not installed, kernel error).
        """
        try:
            for _rg_key, zm_dict in zm_index.iter_zone_maps(name):
                if "column_chunks" in zm_dict:
                    return list(zm_dict["column_chunks"].get(
                        "column_chunks", {}).keys())
                return list(zm_dict.get("min", {}).keys())
        except (KeyError, ValueError, AttributeError, ImportError) as exc:
            warn_best_effort(f"infer columns for {name}", exc)
        return []

    # ==================================================================
    # Internal helpers: row group storage via ProllyTreeIndex.
    #
    # ALL writes (create_table, insert, commit_to_branch, merge_branch,
    # range_write) go through these helpers. There is no separate
    # "whole-table Parquet blob" path — ProllyTreeIndex is THE backend.
    # ==================================================================

    def _write_via_prolly(self, name: str, table: pa.Table,
                          key_col: Optional[str],
                          row_group_size: int,
                          message: str = "",
                          build_zone_maps: bool = True) -> str:
        """Write a table as row groups in ProllyTreeIndex, commit to HEAD.

        This is the unified write path. Splits `table` into row groups of
        `row_group_size` rows each. Each group is encoded as a Parquet blob
        and staged in the ProllyTreeIndex under key f"rg/{max_pk_in_group}".

        REPLACES the existing row groups: old row group keys are deleted
        so the new commit contains ONLY the new row groups. This makes
        `insert` work correctly (the new table replaces the old, rather
        than accumulating).

        If build_zone_maps is True (default), also computes and stores
        zone maps (min/max/null_count per row group) in a separate
        ProllyTreeIndex. These enable Vortex-style predicate pushdown
        via PruningReader — skipping row groups without decoding.

        If key_col is None, uses row index (0..N-1) as the primary key.
        """
        if ProllyLensBase is None:
            raise ImportError("LakehouseLens requires prolly_tree.py")

        base = ProllyLensBase(self.kernel, name)
        n_rows = table.num_rows

        # Delete existing row group keys so the new commit replaces (not
        # accumulates) the old data. Without this, insert() would double
        # the row count because old row groups would persist in the tree.
        existing_state = base.read_all()
        for k in existing_state.keys():
            if k.startswith(_RG_PREFIX):
                base.stage_delete(k)

        if n_rows == 0:
            return base.commit(message or "write: empty table")

        # Determine the key for each row
        if key_col is not None:
            if key_col not in table.column_names:
                raise KeyError(f"key column '{key_col}' not in table columns {table.column_names}")
            # Sort by key_col so row groups are contiguous key ranges
            sorted_table = table.sort_by(key_col)
            key_array = sorted_table.column(key_col).to_pylist()
        else:
            # Use row index as the key — no sorting needed (already in order)
            sorted_table = table
            key_array = list(range(n_rows))

        # Optionally set up zone maps via CollectionMetadata (data-side)
        zm_index = None
        if build_zone_maps:
            try:
                from collection_metadata import CollectionMetadata
                from pruning import ZoneMap
                zm_index = CollectionMetadata(self.kernel).zm_index
                if zm_index is not None:
                    # Clear old zone maps for this collection (public API)
                    zm_index.clear_zone_maps(name)
            except ImportError:
                zm_index = None

        # Stage each row group
        for start in range(0, n_rows, row_group_size):
            end = min(start + row_group_size, n_rows)
            group_table = sorted_table.slice(start, end - start)
            parquet_bytes = self._encode_table(group_table)
            parquet_hash = self.kernel.write(parquet_bytes)
            max_pk = key_array[end - 1]
            rg_key = f"{_RG_PREFIX}{max_pk}"
            base.stage(rg_key, parquet_hash)

            # Compute and store zone map for this row group (best-effort).
            # Failures are logged via pond.best_effort, NOT silently swallowed.
            if zm_index is not None:
                def _build_zm(_gt=group_table, _rk=rg_key, _ph=parquet_hash,
                               _zm_index=zm_index, _name=name):
                    zm = ZoneMap.build(_gt)
                    # Also compute column-chunk zone maps for finer pruning.
                    # HAVE_PRUNING is set once at module import time.
                    if HAVE_PRUNING:
                        cczm = best_effort(
                            f"build column-chunk zone map for {_name}.{_rk}",
                            ColumnChunkZoneMap.build, _gt, _rk,
                            chunk_size=DEFAULT_CHUNK_SIZE)
                        if cczm is not None:
                            # Merge column-chunk stats into the zone map dict
                            # so PruningReader can use them
                            zm_dict = zm.to_dict()
                            zm_dict["column_chunks"] = cczm.to_dict()
                            zm = ZoneMap.from_dict(zm_dict)
                    _zm_index.add_zone_map(_name, _rk, zm, _ph)
                best_effort(f"build zone map for {name}.{rg_key}", _build_zm)

        n_groups = (n_rows + row_group_size - 1) // row_group_size
        commit_hash = base.commit(message or f"write: {n_rows} rows in {n_groups} row groups")
        self._cached_tables.pop(f"{name}:{commit_hash}", None)

        # Commit zone maps (if built) — best-effort
        if zm_index is not None:
            best_effort(f"commit zone maps for {name}",
                        zm_index.commit_zone_maps, name,
                        f"zone maps for {name}")

        self._notify_indexers(name)
        return commit_hash

    def _write_via_prolly_to_branch(self, name: str, branch_name: str,
                                     table: pa.Table,
                                     key_col: Optional[str],
                                     row_group_size: int,
                                     message: str = "") -> str:
        """Write row groups to ProllyTreeIndex and commit to a branch ref.

        Same as _write_via_prolly but updates the branch ref instead of HEAD.
        The ProllyLensBase.commit() updates HEAD by default, so we manually
        rebind the branch ref afterwards.
        """
        if ProllyLensBase is None:
            raise ImportError("LakehouseLens requires prolly_tree.py")

        ref = self._branch_ref(name, branch_name)
        parent = self.kernel.resolve(ref)
        if parent is None:
            raise KeyError(f"Branch '{branch_name}' not found in '{name}'")

        # Temporarily point HEAD at the branch's commit so ProllyLensBase
        # builds on top of the branch, not the main HEAD.
        # (ProllyLensBase.commit always updates collections/{name}/HEAD.)
        # Strategy: write row groups via ProllyLensBase (which updates HEAD),
        # then move the result to the branch ref and restore HEAD.
        original_head = self.kernel.resolve(self._head_ref(name))
        # Point HEAD at the branch
        self.kernel.reference(self._head_ref(name), parent)
        try:
            commit_hash = self._write_via_prolly(name, table, key_col,
                                                  row_group_size, message)
        finally:
            # Restore HEAD to original
            if original_head is not None:
                self.kernel.reference(self._head_ref(name), original_head)
        # Move the new commit to the branch ref
        self.kernel.reference(ref, commit_hash)
        self._notify_indexers(name)
        return commit_hash

    def _write_merge_via_prolly(self, name: str, table: pa.Table,
                                 first_parent: str, second_parent: str,
                                 row_group_size: int,
                                 message: str) -> str:
        """Write merged data as row groups and create a 2-parent merge commit.

        Uses ProllyLensBase to stage row groups, then writes a custom merge
        commit with both parents. REPLACES existing row groups (same as
        _write_via_prolly) so the merged table is the new state.
        """
        if ProllyLensBase is None:
            raise ImportError("LakehouseLens requires prolly_tree.py")

        base = ProllyLensBase(self.kernel, name)
        n_rows = table.num_rows

        # Delete existing row group keys (same pattern as _write_via_prolly)
        existing_state = base.read_all()
        for k in existing_state.keys():
            if k.startswith(_RG_PREFIX):
                base.stage_delete(k)

        if n_rows > 0:
            key_array = list(range(n_rows))  # use row index as key
            for start in range(0, n_rows, row_group_size):
                end = min(start + row_group_size, n_rows)
                group_table = table.slice(start, end - start)
                parquet_bytes = self._encode_table(group_table)
                parquet_hash = self.kernel.write(parquet_bytes)
                max_pk = key_array[end - 1]
                rg_key = f"{_RG_PREFIX}{max_pk}"
                base.stage(rg_key, parquet_hash)

        # Create a 2-parent merge commit with the staged row groups.
        # Uses the public create_merge_commit() API instead of reaching
        # into _compute_full_state, _staged_add, _staged_del, _commit_index.
        commit_hash = base.create_merge_commit(
            parent=first_parent,
            second_parent=second_parent,
            message=message,
        )
        self._cached_tables.pop(f"{name}:{commit_hash}", None)
        self._notify_indexers(name)
        return commit_hash

    def _read_all_row_groups(self, name: str,
                              commit_hash: str) -> pa.Table:
        """Read ALL row groups from the ProllyTreeIndex at commit_hash.

        Walks the Prolly tree at the given commit, reads each row group
        Parquet blob, and concatenates them into one PyArrow Table.
        """
        if ProllyLensBase is None:
            raise ImportError("LakehouseLens requires prolly_tree.py")
        from binary_encoding import BinaryProllyTree as _BPT

        # Read the commit to find the snapshot tree root
        raw = self.kernel.read_blob(commit_hash)
        # Lakehouse commits are stored as binary ProllyLensBase commits
        # (type byte 3). Decode to get the snapshot tree root.
        try:
            commit = _BPT.decode_commit(raw)
            snapshot_root = commit.get("snapshot")
        except (ValueError, IndexError):
            # Fallback: try JSON (for old-style commits)
            try:
                commit = json.loads(raw)
                if "parquet" in commit:
                    # Old-style whole-table commit — read directly
                    parquet_bytes = self.kernel.read(commit["parquet"])
                    return self._decode_table(parquet_bytes)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
            raise ValueError(f"Cannot decode commit {commit_hash} for '{name}'")

        if snapshot_root is None:
            # Delta-only commit — walk to find the snapshot
            base = ProllyLensBase(self.kernel, name)
            state = base.read_state_at_commit(commit_hash)
        else:
            state = ProllyTree.read_all(self.kernel, snapshot_root)

        # Filter to row-group keys (start with the magic prefix)
        rg_keys = sorted(k for k in state.keys() if k.startswith(_RG_PREFIX))
        if not rg_keys:
            return pa.table({})

        tables = []
        for k in rg_keys:
            blob_hash = state[k]
            blob_bytes = self.kernel.read_blob(blob_hash)
            # Detect manifest blobs (from range_write_column_chunks /
            # range_write_encoded) by their JSON structure. A manifest
            # has keys "row_group_key", "row_count", "chunk_size",
            # "column_chunks". A Parquet blob starts with b"PAR1".
            table_from_blob = self._decode_blob_to_table(blob_bytes, name, k)
            if table_from_blob is not None:
                tables.append(table_from_blob)

        try:
            return pa.concat_tables(tables, promote_options="default")
        except TypeError:
            return pa.concat_tables(tables)

    def _decode_blob_to_table(self, blob_bytes: bytes,
                               collection: str,
                               rg_key: str) -> Optional[pa.Table]:
        """Decode a row-group blob to a PyArrow Table.

        Handles three storage modes:
          1. Plain Parquet blob (from range_write / _write_via_prolly)
          2. Manifest blob (from range_write_column_chunks) — JSON listing
             per-column-chunk blob hashes
          3. Encoded manifest blob (from range_write_encoded) — same JSON
             structure but chunk blobs are FastLanes-encoded

        Returns None if the blob cannot be decoded (e.g., undecodable).
        """
        # Try Parquet first (cheap probe: magic bytes)
        if blob_bytes[:4] == b"PAR1":
            decoded = best_effort(
                f"decode Parquet blob for {collection}.{rg_key}",
                self._decode_table, blob_bytes)
            if decoded is not None:
                return decoded
            return None

        # Try JSON manifest
        try:
            manifest = json.loads(blob_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None  # not Parquet, not JSON — give up

        if not isinstance(manifest, dict) or "column_chunks" not in manifest:
            return None  # JSON but not a manifest — give up

        # Manifest: read all chunk blobs for all columns, reassemble.
        # Detect encoded storage by checking the first chunk blob's header
        # (magic bytes b"PND1" = encoded, b"PAR1" = Parquet).
        if not HAVE_PRUNING:
            return None  # cannot decode manifest without pruning extensions

        # Peek at the first chunk blob to detect encoding.
        # The blob may be compressed (starts with compression byte, not PND1).
        # Decompress first, then check for PND1 magic.
        first_col = next(iter(manifest["column_chunks"]))
        first_chunk_hash = manifest["column_chunks"][first_col][0]
        first_chunk_raw = self.kernel.read_blob(first_chunk_hash)
        try:
            from compression import decompress_blob
            first_chunk_bytes = decompress_blob(first_chunk_raw)
        except ImportError:
            first_chunk_bytes = first_chunk_raw
        is_encoded = first_chunk_bytes[:4] == b"PND1"

        # Build a synthetic cczm from the manifest
        cczm = ColumnChunkZoneMap(row_group_key=rg_key)
        for col_name, chunk_hashes in manifest["column_chunks"].items():
            chunk_stats = []
            for i, h in enumerate(chunk_hashes):
                chunk_stats.append(ColumnChunkStats(
                    chunk_index=i,
                    row_count=0,  # not used for full read
                    blob_hash=h,
                ))
            cczm.column_chunks[col_name] = chunk_stats

        col_names = list(manifest["column_chunks"].keys())

        if is_encoded:
            # Use EncodedChunkStorage to read encoded chunk blobs
            try:
                from encoded_chunk_storage import EncodedChunkStorage
                from encoding import decode_column
                enc_storage = EncodedChunkStorage(self.kernel)
                col_data = enc_storage.read_column_chunks_encoded(
                    cczm, col_names, None,  # None = read all chunks
                    predicates=None,  # no predicate eval — full read
                )
            except ImportError:
                return None

            arrays = []
            col_names_out = []
            for col_name in col_names:
                if col_name in col_data and col_data[col_name]:
                    # col_data[col_name] is list of (chunk_index, values)
                    all_values = []
                    for ci, vals in col_data[col_name]:
                        all_values.extend(vals)
                    if all_values:
                        arrays.append(pa.array(all_values))
                        col_names_out.append(col_name)
            if not arrays:
                return None
            return pa.Table.from_arrays(arrays, names=col_names_out)
        else:
            # Plain Parquet column-chunk storage
            storage = ColumnChunkStorage(self.kernel)
            def decode_parquet_manifest(chunk_bytes):
                table = self._decode_table(chunk_bytes)
                return table.column(0).to_pylist()
            col_data = storage.read_column_chunks(
                cczm, col_names, None,
                decode_fn=decode_parquet_manifest,
            )
            if not col_data:
                return None

            arrays = []
            col_names_out = []
            for col_name in col_names:
                if col_name in col_data and col_data[col_name]:
                    all_vals = []
                    for vals in col_data[col_name]:
                        all_vals.extend(vals)
                    arrays.append(pa.array(all_vals))
                    col_names_out.append(col_name)
            if not arrays:
                return None
            return pa.Table.from_arrays(arrays, names=col_names_out)

    @staticmethod
    def _encode_table(table: pa.Table) -> bytes:
        """Encode a PyArrow Table as Parquet bytes."""
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        return sink.getvalue().to_pybytes()

    @staticmethod
    def _decode_table(parquet_bytes: bytes) -> pa.Table:
        """Decode Parquet bytes into a PyArrow Table."""
        reader = pa.BufferReader(parquet_bytes)
        return pq.read_table(reader)

    def compact_zone_maps(self, collection: str) -> int:
        """Remove stale zone maps for a collection.

        After insert/merge, old data blobs are replaced by new ones.
        The old zone maps become stale. This method removes them.

        This is NOT called automatically (to avoid overhead on every
        insert/merge). Call it explicitly when you want to clean up:

            lens.create_table("users", data)
            lens.insert("users", more_data)
            lens.compact_zone_maps("users")  # clean up stale zone maps

        Returns the number of stale zone maps removed.
        """
        try:
            from collection_metadata import CollectionMetadata
            meta = CollectionMetadata(self.kernel)
            if meta.has_zone_maps(collection):
                return meta.compact_zone_maps(collection)
            return 0
        except (ImportError, KeyError, AttributeError, ValueError) as exc:
            warn_best_effort(f"compact zone maps for {collection}", exc)
            return 0

    def _notify_indexers(self, collection: str) -> None:
        """Notify attached indexer that a write has occurred.

        For EAGER indexes: refreshes immediately.
        For LAZY indexes: staleness accumulates (refreshed on next lookup).
        For MANUAL indexes: no-op.

        Called automatically after every commit. Best-effort — failures
        are logged via pond.best_effort, NOT silently swallowed.
        """
        if self._attached_indexer is not None:
            best_effort(f"notify indexer for {collection}",
                        self._attached_indexer.notify_write, collection)

# ---------------------------------------------------------------------------
# Backward-compat re-export — PondLakehouse now lives in pond_lakehouse.py
# ---------------------------------------------------------------------------
# Earlier versions of this module contained the PondLakehouse class, the SQL
# parser, and the self-test/benchmark. They have been extracted to:
#   - pond_lakehouse.py  (PondLakehouse DuckDB façade + self-test + benchmark)
#   - sql_pushdown.py    (regex SQL parser for predicate + projection extraction)
#
# LakehouseLens itself does NOT require DuckDB. Users who want SQL queries
# should use PondLakehouse (from pond_lakehouse import PondLakehouse).
#
# The re-export below keeps existing imports (from lakehouse_lens import
# PondLakehouse) working without modification.

try:
    from pond_lakehouse import PondLakehouse  # noqa: E402
except ImportError:
    PondLakehouse = None  # DuckDB not installed


if __name__ == "__main__":
    # Run the self-test from pond_lakehouse.py for backward compat.
    if PondLakehouse is not None:
        from pond_lakehouse import _self_test, _benchmark
        _self_test()
        _benchmark()
    else:
        print("DuckDB is not installed — cannot run PondLakehouse self-test.")
        print("Install with: pip install duckdb pyarrow")
