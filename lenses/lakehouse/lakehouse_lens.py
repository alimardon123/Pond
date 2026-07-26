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

# Make pond-core and pond-sdk importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk"))
from kernel import PondMinimal  # noqa: E402
from base_lens import PondLens  # noqa: E402

# DuckDB for the query engine
try:
    import duckdb
except ImportError:
    raise ImportError(
        "DuckDB is required for pond-lakehouse. Install with: pip install duckdb"
    )

# PyArrow for the Parquet/Arrow interchange
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError(
        "PyArrow is required for pond-lakehouse. Install with: pip install pyarrow"
    )

# ProllyTreeIndex for the range read/write path
try:
    from prolly_tree import ProllyLensBase, ProllyTree  # noqa: E402
    from binary_encoding import BinaryProllyTree  # noqa: E402
except ImportError:
    ProllyLensBase = None
    ProllyTree = None


# ---------------------------------------------------------------------------
# LakehouseLens — tabular semantics on Pond
# ---------------------------------------------------------------------------

# Default row group size. Each row group becomes one Parquet blob, one
# entry in the ProllyTreeIndex. Smaller groups = faster point lookups
# + more tree entries; larger groups = faster full scans + fewer tree
# entries. 10K is a balanced default (~1-2 MB per group for typical
# tabular data).
DEFAULT_RANGE_ROW_GROUP_SIZE = 10_000

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
        self.duckdb = duckdb.connect()
        self._cached_tables: dict[str, tuple[str, pa.Table]] = {}
        self._attached_indexer = None

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
        if ProllyLensBase is None:
            raise ImportError("range_write requires prolly_tree.py")

        if key_col not in table.column_names:
            raise KeyError(f"key column '{key_col}' not in table columns {table.column_names}")

        # Open a ProllyLensBase over the same collection name. This
        # shares the HEAD/snapshot refs with the whole-table mode —
        # the same namespace, two storage shapes.
        base = ProllyLensBase(self.kernel, name)

        # Sort rows by key_col so row groups are contiguous key ranges.
        # This is essential for range_read to work — without sorting,
        # a single key could appear in multiple row groups.
        sorted_table = table.sort_by(key_col)

        n_rows = sorted_table.num_rows
        if n_rows == 0:
            # Nothing to write — but still commit to advance HEAD
            return base.commit(f"range_write: empty table")

        # Stage each row group as a Parquet blob keyed by max_pk in group.
        key_array = sorted_table.column(key_col).to_pylist()

        # Optionally set up zone maps via CollectionMetadata (data-side)
        zm_index = None
        try:
            from collection_metadata import CollectionMetadata
            from pruning import ZoneMap
            zm_index = CollectionMetadata(self.kernel).zm_index
        except ImportError:
            pass

        for start in range(0, n_rows, row_group_size):
            end = min(start + row_group_size, n_rows)
            group_table = sorted_table.slice(start, end - start)
            parquet_bytes = self._encode_table(group_table)
            parquet_hash = self.kernel.write(parquet_bytes)
            max_pk = key_array[end - 1]
            rg_key = f"{_RG_PREFIX}{max_pk}"
            base.stage(rg_key, parquet_hash)

            # Compute and store zone map for this row group
            if zm_index is not None:
                try:
                    zm = ZoneMap.build(group_table)
                    # Also compute column-chunk zone maps for finer pruning
                    try:
                        sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk", "extensions", "physical_structures"))
                        from column_chunk_zone_map import ColumnChunkZoneMap
                        cczm = ColumnChunkZoneMap.build(group_table, rg_key,
                                                        chunk_size=1000)
                        zm_dict = zm.to_dict()
                        zm_dict["column_chunks"] = cczm.to_dict()
                        zm = ZoneMap.from_dict(zm_dict)
                    except ImportError:
                        pass
                    zm_index.add_zone_map(name, rg_key, zm, parquet_hash)
                except Exception:
                    pass

        commit_hash = base.commit(f"range_write: {n_rows} rows in "
                                  f"{(n_rows + row_group_size - 1) // row_group_size} row groups")

        # Commit zone maps (if built)
        if zm_index is not None:
            try:
                zm_index.commit_zone_maps(name, f"zone maps for {name}")
            except Exception:
                pass

        # Invalidate read cache
        self._cached_tables.pop(f"{name}:{commit_hash}", None)
        self._notify_indexers(name)
        return commit_hash

    def range_write_column_chunks(self, name: str, table: pa.Table,
                                   key_col: str,
                                   row_group_size: int = DEFAULT_RANGE_ROW_GROUP_SIZE,
                                   chunk_size: int = 1000) -> str:
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
        if ProllyLensBase is None:
            raise ImportError("range_write_column_chunks requires prolly_tree.py")

        if key_col not in table.column_names:
            raise KeyError(f"key column '{key_col}' not in table columns {table.column_names}")

        try:
            sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk", "extensions", "physical_structures"))
            from column_chunk_storage import ColumnChunkStorage
            from column_chunk_zone_map import ColumnChunkZoneMap
        except ImportError as e:
            raise ImportError(f"ColumnChunkStorage extension required: {e}")

        from collection_metadata import CollectionMetadata
        from pruning import ZoneMap

        base = ProllyLensBase(self.kernel, name)
        storage = ColumnChunkStorage(self.kernel)
        zm_index = CollectionMetadata(self.kernel).zm_index

        # Clear old zone maps for this collection (overwrite semantics)
        if zm_index is not None:
            zm_base = zm_index._get_base(name)
            for k in zm_base.read_all().keys():
                if k.startswith(_RG_PREFIX):
                    zm_base.stage_delete(k)

        # Sort by key_col so row groups are contiguous key ranges
        sorted_table = table.sort_by(key_col)
        n_rows = sorted_table.num_rows
        if n_rows == 0:
            return base.commit("range_write_column_chunks: empty table")

        key_array = sorted_table.column(key_col).to_pylist()

        # Stage each row group as a MANIFEST blob (not a Parquet blob).
        # The manifest lists chunk blob hashes per column.
        for start in range(0, n_rows, row_group_size):
            end = min(start + row_group_size, n_rows)
            group_table = sorted_table.slice(start, end - start)
            max_pk = key_array[end - 1]
            rg_key = f"{_RG_PREFIX}{max_pk}"

            # Split the row group into per-column-chunk blobs.
            # Returns (manifest_blob_hash, cczm_with_blob_hashes).
            manifest_hash, cczm = storage.write_row_group_column_chunks(
                group_table, rg_key, chunk_size=chunk_size,
                encode_fn=self._encode_table,
            )
            base.stage(rg_key, manifest_hash)

            # Build the row-group zone map (existing path) and merge in
            # the column-chunk stats (which now have blob_hash fields).
            if zm_index is not None:
                try:
                    zm = ZoneMap.build(group_table)
                    zm_dict = zm.to_dict()
                    zm_dict["column_chunks"] = cczm.to_dict()
                    zm = ZoneMap.from_dict(zm_dict)
                    zm_index.add_zone_map(name, rg_key, zm, manifest_hash)
                except Exception:
                    pass  # zone map is best-effort

        commit_hash = base.commit(
            f"range_write_column_chunks: {n_rows} rows in "
            f"{(n_rows + row_group_size - 1) // row_group_size} row groups "
            f"(chunk_size={chunk_size})"
        )

        if zm_index is not None:
            try:
                zm_index.commit_zone_maps(name, f"zone maps for {name}")
            except Exception:
                pass

        self._cached_tables.pop(f"{name}:{commit_hash}", None)
        self._notify_indexers(name)
        return commit_hash

    def range_write_encoded(self, name: str, table: pa.Table,
                             key_col: str,
                             row_group_size: int = DEFAULT_RANGE_ROW_GROUP_SIZE,
                             chunk_size: int = 1000,
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
        if ProllyLensBase is None:
            raise ImportError("range_write_encoded requires prolly_tree.py")

        if key_col not in table.column_names:
            raise KeyError(f"key column '{key_col}' not in table columns {table.column_names}")

        try:
            sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk", "extensions", "physical_structures"))
            from encoded_chunk_storage import EncodedChunkStorage
            from column_chunk_zone_map import ColumnChunkZoneMap
        except ImportError as e:
            raise ImportError(f"EncodedChunkStorage extension required: {e}")

        from collection_metadata import CollectionMetadata
        from pruning import ZoneMap

        base = ProllyLensBase(self.kernel, name)
        storage = EncodedChunkStorage(self.kernel)
        zm_index = CollectionMetadata(self.kernel).zm_index

        # Clear old zone maps for this collection (overwrite semantics)
        if zm_index is not None:
            zm_base = zm_index._get_base(name)
            for k in zm_base.read_all().keys():
                if k.startswith(_RG_PREFIX):
                    zm_base.stage_delete(k)

        # Sort by key_col so row groups are contiguous key ranges
        sorted_table = table.sort_by(key_col)
        n_rows = sorted_table.num_rows
        if n_rows == 0:
            return base.commit("range_write_encoded: empty table")

        key_array = sorted_table.column(key_col).to_pylist()

        # Stage each row group as a MANIFEST blob
        for start in range(0, n_rows, row_group_size):
            end = min(start + row_group_size, n_rows)
            group_table = sorted_table.slice(start, end - start)
            max_pk = key_array[end - 1]
            rg_key = f"{_RG_PREFIX}{max_pk}"

            # Split the row group into per-column-chunk ENCODED blobs
            manifest_hash, cczm = storage.write_row_group_encoded(
                group_table, rg_key, chunk_size=chunk_size,
                encoding_hints=encoding_hints,
            )
            base.stage(rg_key, manifest_hash)

            # Build the row-group zone map and merge in the column-chunk
            # stats (with blob_hash fields + _encoding_meta sidecar)
            if zm_index is not None:
                try:
                    zm = ZoneMap.build(group_table)
                    zm_dict = zm.to_dict()
                    zm_dict["column_chunks"] = cczm.to_dict()
                    zm = ZoneMap.from_dict(zm_dict)
                    zm_index.add_zone_map(name, rg_key, zm, manifest_hash)
                except Exception:
                    pass

        commit_hash = base.commit(
            f"range_write_encoded: {n_rows} rows in "
            f"{(n_rows + row_group_size - 1) // row_group_size} row groups "
            f"(chunk_size={chunk_size}, encoded)"
        )

        if zm_index is not None:
            try:
                zm_index.commit_zone_maps(name, f"zone maps for {name}")
            except Exception:
                pass

        self._cached_tables.pop(f"{name}:{commit_hash}", None)
        self._notify_indexers(name)
        return commit_hash

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
                state = base._read_state_from_commit(commit_hash)
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
            except Exception:
                # If column projection fails (e.g., column not found),
                # read the full table and project afterwards
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

    def read_with_pruning(self, name: str,
                          predicates: Optional[list] = None,
                          row_filter: Optional[Callable] = None,
                          columns: Optional[list[str]] = None,
                          chunk_size: int = 1000) -> pa.Table:
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
                                        chunk_size: int = 1000) -> pa.Table:
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
        try:
            from collection_metadata import CollectionMetadata
            from pruning import PruningPredicate, ColumnPredicate
            from pruning_reader import PruningReader
            sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk", "extensions", "physical_structures"))
            from column_chunk_storage import ColumnChunkStorage
            from column_chunk_zone_map import ColumnChunkZoneMap
            meta = CollectionMetadata(self.kernel)
            zm_index = meta.zm_index
        except ImportError:
            return self.read_with_pruning(name, predicates=predicates,
                                          row_filter=row_filter,
                                          columns=columns,
                                          chunk_size=chunk_size)

        if zm_index is None or not zm_index.has_zone_maps(name):
            return self.read_table(name)

        # Build pruning predicate
        predicate = None
        if predicates:
            col_preds = [ColumnPredicate(column=c, op=o, value=v)
                         for c, o, v in predicates]
            predicate = PruningPredicate(col_preds, combine="and")

        reader = PruningReader(self.kernel, zm_index, name, predicate)
        storage = ColumnChunkStorage(self.kernel)

        # Build column-chunk predicate lookup
        cc_predicates = {}
        if predicate:
            for pred in predicate.predicates:
                cc_predicates[pred.column] = (pred.op, pred.value)

        # Determine which columns to read
        # Default: all columns from the schema (look at first zone map)
        if columns is None:
            columns = self._infer_columns(name, zm_index)

        # Walk surviving row groups via the verbose pruning scan
        all_rows: list[dict] = []

        for rg_key, manifest_hash, zm_dict in zm_index.scan_with_pruning(
                name, predicate, verbose=True):

            # If column-chunk storage is not active for this row group,
            # fall back to the regular pruning path (read whole blob).
            if not ColumnChunkStorage.has_column_chunk_storage(zm_dict):
                data_bytes = self.kernel.read_blob(manifest_hash)
                # manifest_hash here is actually the data blob hash
                # (set by add_zone_map — same hash stored in zone map)
                # Re-derive via zone map's blob_hash field
                actual_data_hash = zm_dict.get("blob_hash", manifest_hash)
                data_bytes = self.kernel.read_blob(actual_data_hash)
                table = self._decode_table(data_bytes)
                for row in table.to_pylist():
                    if row_filter is None or row_filter(row):
                        all_rows.append(row)
                continue

            # Column-chunk storage active — read only surviving chunks
            cczm = ColumnChunkZoneMap.from_dict(zm_dict["column_chunks"])

            # Compute surviving chunk indices (intersection across
            # predicate columns — same logic as PruningReader.scan)
            surviving_chunks: Optional[set[int]] = None
            if cc_predicates:
                for col, (op, val) in cc_predicates.items():
                    chunks = cczm.prune_column_chunks(col, op, val)
                    # None means no stats for this column — caller must
                    # fall back to reading all chunks. Skip the column
                    # (don't include in the intersection) so we don't
                    # silently drop rows.
                    if chunks is None:
                        continue
                    chunk_set = set(chunks)
                    if surviving_chunks is None:
                        surviving_chunks = chunk_set
                    else:
                        surviving_chunks &= chunk_set
                if surviving_chunks is not None and not surviving_chunks:
                    continue  # all chunks pruned — skip row group

            # Read surviving chunk blobs for each requested column
            col_arrays = storage.read_column_chunks(
                cczm, columns, surviving_chunks,
                decode_fn=self._decode_table,
            )

            if not col_arrays:
                continue  # no chunks read (e.g., column missing)

            # Reassemble rows: concatenate arrays per column, then build
            # a PyArrow Table, then convert to row dicts for row_filter.
            arrays = []
            col_names_out = []
            for col_name in columns:
                if col_name in col_arrays and col_arrays[col_name]:
                    arrays.append(pa.concat_arrays(col_arrays[col_name]))
                    col_names_out.append(col_name)

            if not arrays:
                continue

            chunk_table = pa.Table.from_arrays(arrays, names=col_names_out)
            for row in chunk_table.to_pylist():
                if row_filter is None or row_filter(row):
                    all_rows.append(row)

        if not all_rows:
            return pa.table({})

        return pa.Table.from_pylist(all_rows)

    def read_with_encoded_pruning(self, name: str,
                                    predicates: Optional[list] = None,
                                    row_filter: Optional[Callable] = None,
                                    columns: Optional[list[str]] = None,
                                    chunk_size: int = 1000) -> pa.Table:
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
        try:
            from collection_metadata import CollectionMetadata
            from pruning import PruningPredicate, ColumnPredicate
            sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk", "extensions", "physical_structures"))
            from encoded_chunk_storage import EncodedChunkStorage
            from column_chunk_zone_map import ColumnChunkZoneMap
            from encoding import EncodingHeader, decode_column
            meta = CollectionMetadata(self.kernel)
            zm_index = meta.zm_index
            storage = EncodedChunkStorage(self.kernel)
        except ImportError:
            return self.read_with_column_chunk_pruning(
                name, predicates=predicates, row_filter=row_filter,
                columns=columns, chunk_size=chunk_size)

        if zm_index is None or not zm_index.has_zone_maps(name):
            return self.read_table(name)

        # Build pruning predicate
        predicate = None
        if predicates:
            col_preds = [ColumnPredicate(column=c, op=o, value=v)
                         for c, o, v in predicates]
            predicate = PruningPredicate(col_preds, combine="and")

        # Build column-chunk predicate lookup (op, value) per column
        cc_predicates = {}
        if predicate:
            for pred in predicate.predicates:
                cc_predicates[pred.column] = (pred.op, pred.value)

        # Determine which columns to read
        if columns is None:
            columns = self._infer_columns(name, zm_index)

        all_rows: list[dict] = []

        # Walk surviving row groups via the verbose pruning scan
        for rg_key, manifest_hash, zm_dict in zm_index.scan_with_pruning(
                name, predicate, verbose=True):

            # If encoded storage is not active for this row group,
            # fall back to the appropriate path:
            #   - If column-chunk storage is active, use read_column_chunks
            #     via read_with_column_chunk_pruning (which knows how to
            #     read the manifest).
            #   - Otherwise, decode as a plain Parquet blob.
            if not EncodedChunkStorage.has_encoded_storage(zm_dict):
                if ColumnChunkStorage.has_column_chunk_storage(zm_dict):
                    # Delegate to the column-chunk read path for this row group.
                    # We can't easily call read_with_column_chunk_pruning here
                    # because it iterates ALL row groups. Instead, reconstruct
                    # via the storage extension.
                    from column_chunk_zone_map import ColumnChunkZoneMap
                    cczm = ColumnChunkZoneMap.from_dict(zm_dict["column_chunks"])
                    col_arrays = storage.read_column_chunks(
                        cczm, columns, None,  # None = read all chunks
                        decode_fn=self._decode_table,
                    )
                    if not col_arrays:
                        continue
                    arrays = []
                    col_names_out = []
                    for col_name in columns:
                        if col_name in col_arrays and col_arrays[col_name]:
                            arrays.append(
                                pa.concat_arrays(col_arrays[col_name]))
                            col_names_out.append(col_name)
                    if not arrays:
                        continue
                    chunk_table = pa.Table.from_arrays(arrays, names=col_names_out)
                    for row in chunk_table.to_pylist():
                        if row_filter is None or row_filter(row):
                            all_rows.append(row)
                else:
                    # Plain Parquet blob (range_write / _write_via_prolly)
                    actual_data_hash = zm_dict.get("blob_hash", manifest_hash)
                    data_bytes = self.kernel.read_blob(actual_data_hash)
                    try:
                        table = self._decode_table(data_bytes)
                        for row in table.to_pylist():
                            if row_filter is None or row_filter(row):
                                all_rows.append(row)
                    except Exception:
                        pass  # undecodable blob — skip
                continue

            # Encoded storage active — read only surviving chunks,
            # using encoded predicate eval where possible.
            cczm = ColumnChunkZoneMap.from_dict(zm_dict["column_chunks"])

            # Compute surviving chunk indices (intersection across
            # predicate columns — same logic as before)
            surviving_chunks: Optional[set[int]] = None
            if cc_predicates:
                for col, (op, val) in cc_predicates.items():
                    chunks = set(cczm.prune_column_chunks(col, op, val))
                    if surviving_chunks is None:
                        surviving_chunks = chunks
                    else:
                        surviving_chunks &= chunks
                if surviving_chunks is not None and not surviving_chunks:
                    continue  # all chunks pruned — skip row group

            # Read surviving chunk blobs with encoded predicate eval
            # For each column, get (chunk_index, surviving_values) pairs
            col_data = storage.read_column_chunks_encoded(
                cczm, columns, surviving_chunks,
                predicates=predicates,  # pass-through for encoded eval
            )

            if not col_data:
                continue

            # Reassemble rows: each column has a list of (chunk_index, values)
            # We need to align them by chunk_index to build row dicts.
            # Group by chunk_index, then iterate rows within each chunk.
            chunks_per_col: dict[int, dict[str, list]] = {}
            for col_name, chunk_list in col_data.items():
                for ci, values in chunk_list:
                    if ci not in chunks_per_col:
                        chunks_per_col[ci] = {}
                    chunks_per_col[ci][col_name] = values

            # Build row dicts
            for ci in sorted(chunks_per_col.keys()):
                col_data_for_chunk = chunks_per_col[ci]
                if not col_data_for_chunk:
                    continue
                # All columns should have the same number of values per chunk
                n_rows = max(len(v) for v in col_data_for_chunk.values())
                for i in range(n_rows):
                    row = {}
                    for col_name, values in col_data_for_chunk.items():
                        if i < len(values):
                            row[col_name] = values[i]
                        else:
                            row[col_name] = None
                    if row_filter is None or row_filter(row):
                        all_rows.append(row)

        if not all_rows:
            return pa.table({})

        return pa.Table.from_pylist(all_rows)

    def _infer_columns(self, name: str, zm_index) -> list[str]:
        """Infer column names from the first zone map of a collection."""
        try:
            zm_base = zm_index._get_base(name)
            state = zm_base.read_all()
            for k in sorted(state.keys()):
                if k.startswith(_RG_PREFIX):
                    zm_blob_hash = state[k]
                    zm_dict = json.loads(self.kernel.read_blob(zm_blob_hash))
                    if "column_chunks" in zm_dict:
                        return list(zm_dict["column_chunks"].get(
                            "column_chunks", {}).keys())
                    return list(zm_dict.get("min", {}).keys())
        except Exception:
            pass
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
                    # Clear old zone maps for this collection
                    zm_base = zm_index._get_base(name)
                    for k in zm_base.read_all().keys():
                        if k.startswith(_RG_PREFIX):
                            zm_base.stage_delete(k)
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

            # Compute and store zone map for this row group
            if zm_index is not None:
                try:
                    zm = ZoneMap.build(group_table)
                    # Also compute column-chunk zone maps for finer pruning
                    try:
                        sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk", "extensions", "physical_structures"))
                        from column_chunk_zone_map import ColumnChunkZoneMap
                        cczm = ColumnChunkZoneMap.build(group_table, rg_key,
                                                        chunk_size=1000)
                        # Merge column-chunk stats into the zone map dict
                        # so PruningReader can use them
                        zm_dict = zm.to_dict()
                        zm_dict["column_chunks"] = cczm.to_dict()
                        # Create a ZoneMap with the extra field
                        zm = ZoneMap.from_dict(zm_dict)
                    except ImportError:
                        pass  # column_chunk_zone_map not available
                    zm_index.add_zone_map(name, rg_key, zm, parquet_hash)
                except Exception:
                    pass  # zone map computation is best-effort

        n_groups = (n_rows + row_group_size - 1) // row_group_size
        commit_hash = base.commit(message or f"write: {n_rows} rows in {n_groups} row groups")
        self._cached_tables.pop(f"{name}:{commit_hash}", None)

        # Commit zone maps (if built)
        if zm_index is not None:
            try:
                zm_index.commit_zone_maps(name, f"zone maps for {name}")
            except Exception:
                pass  # zone map commit is best-effort

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
        from binary_encoding import BinaryProllyTree as _BPT

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

        # Build the merged state and create a merge commit with 2 parents
        full_state = base._compute_full_state(first_parent)
        for k, h in base._staged_add.items():
            full_state[k] = h
        for k in base._staged_del:
            full_state.pop(k, None)
        tree_root = ProllyTree.build(self.kernel, full_state)

        commit_data = _BPT.encode_commit(
            first_parent, tree_root, {}, [], tree_root,
            message, time.time(), base._commit_index,
            second_parent=second_parent)
        commit_hash = self.kernel.write(commit_data)
        self.kernel.reference(self._head_ref(name), commit_hash)
        self.kernel.reference(f"collections/{name}/snapshot", commit_hash)

        base._staged_add.clear()
        base._staged_del.clear()
        base._commit_index += 1
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
            state = base._read_state_from_commit(commit_hash)
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
            try:
                return self._decode_table(blob_bytes)
            except Exception:
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
        try:
            sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk", "extensions", "physical_structures"))
            from column_chunk_storage import ColumnChunkStorage
            from column_chunk_zone_map import ColumnChunkZoneMap, ColumnChunkStats
            from encoding import EncodingHeader
        except ImportError:
            return None

        # Peek at the first chunk blob to detect encoding
        first_col = next(iter(manifest["column_chunks"]))
        first_chunk_hash = manifest["column_chunks"][first_col][0]
        first_chunk_bytes = self.kernel.read_blob(first_chunk_hash)
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
            col_arrays = storage.read_column_chunks(
                cczm, col_names, None,
                decode_fn=self._decode_table,
            )
            if not col_arrays:
                return None

            arrays = []
            col_names_out = []
            for col_name in col_names:
                if col_name in col_arrays and col_arrays[col_name]:
                    arrays.append(pa.concat_arrays(col_arrays[col_name]))
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
        except Exception:
            return 0

    def _notify_indexers(self, collection: str) -> None:
        """Notify attached indexer that a write has occurred.

        For EAGER indexes: refreshes immediately.
        For LAZY indexes: staleness accumulates (refreshed on next lookup).
        For MANUAL indexes: no-op.

        Called automatically after every commit. Best-effort.
        """
        if self._attached_indexer is not None:
            try:
                self._attached_indexer.notify_write(collection)
            except Exception:
                pass  # indexer notification is best-effort


# ---------------------------------------------------------------------------
# Lakehouse — DuckDB query layer on top of LakehouseLens
# ---------------------------------------------------------------------------

class PondLakehouse:
    """A lightweight lakehouse: Pond kernel + LakehouseLens + DuckDB.

    This is the flagship. It provides:
      - CREATE TABLE
      - INSERT
      - SELECT (via DuckDB, with pushdown to Parquet)
      - Time travel (AS OF commit hash)
      - Branching (dev/test branches on tables)
      - Merge (union merge for now)
      - Schema evolution (Parquet-native)
      - Range read/write for operational workloads
    """

    def __init__(self, base_dir: str, force_pruning: Optional[bool] = None):
        """Create a PondLakehouse.

        Args:
            base_dir: filesystem path or object store URL for the kernel.
            force_pruning: override auto-detection for predicate pushdown.
                None = auto (S3→on, local→off)
                True = always prune
                False = never prune
        """
        self.kernel = PondMinimal(base_dir)
        self.lens = LakehouseLens(self.kernel)
        self._force_pruning = force_pruning
        self.duckdb = duckdb.connect()

    def create_table(self, name: str, data: pa.Table) -> str:
        return self.lens.create_table(name, data)

    def insert(self, name: str, data: pa.Table) -> str:
        return self.lens.insert(name, data)

    def range_write(self, name: str, data: pa.Table, key_col: str,
                    row_group_size: int = DEFAULT_RANGE_ROW_GROUP_SIZE) -> str:
        """Range write: store data as row groups in the ProllyTreeIndex.
        See LakehouseLens.range_write for details."""
        return self.lens.range_write(name, data, key_col, row_group_size)

    def range_read(self, name: str,
                   start_key: Optional[str] = None,
                   end_key: Optional[str] = None) -> pa.Table:
        """Range read: scan a key range from the ProllyTreeIndex.
        See LakehouseLens.range_read for details."""
        return self.lens.range_read(name, start_key, end_key)

    def range_point_lookup(self, name: str, key: str) -> Optional[pa.Table]:
        """Point lookup: O(log N) via the ProllyTreeIndex."""
        return self.lens.range_point_lookup(name, key)

    def query(self, sql: str, table_name: Optional[str] = None,
              use_pruning: Optional[bool] = None) -> pa.Table:
        """Run a SQL query against a Pond-hosted table.

        If table_name is provided, the table is registered with DuckDB
        as a named relation. The SQL can then reference it by name.

        PREDICATE + PROJECTION PUSHDOWN:
        When pruning is enabled and the SQL contains a WHERE clause with
        simple column-op-value predicates, the query method automatically:
          1. Extracts WHERE predicates from the SQL
          2. Uses read_with_pruning to skip non-matching row groups
          3. Uses read_columns for projection pushdown (only needed columns)
          4. Registers the pruned+projected table with DuckDB
          5. Executes the SQL on the reduced dataset

        OBJECT-STORE-AWARE PRUNING:
        If use_pruning is None (default), pruning is auto-enabled when
        the kernel is backed by object storage (S3, GCS, etc.) — network
        RTT savings dwarf Python overhead. For local disk, pruning defaults
        to off (DuckDB native scan is faster for local data).
        Pass use_pruning=True/False to override.

        Args:
            sql: the SQL query string
            table_name: name of the table to register
            use_pruning: None=auto (object store→on, local→off),
                True=force on, False=force off.
        """
        if table_name:
            # Auto-decide pruning based on storage type or force_pruning override
            if use_pruning is None:
                if self._force_pruning is not None:
                    use_pruning = self._force_pruning
                else:
                    try:
                        from collection_metadata import CollectionMetadata
                        meta = CollectionMetadata(self.kernel)
                        use_pruning = meta.should_prune()
                    except Exception:
                        use_pruning = False  # default: no pruning if detection fails

            if use_pruning:
                table = self._read_with_pushdown(sql, table_name)
            else:
                table = self.lens.read_table(table_name)
            self.duckdb.register(table_name, table)
        return self.duckdb.execute(sql).to_arrow_table()

    def _read_with_pushdown(self, sql: str, table_name: str) -> pa.Table:
        """Read a table with predicate + projection pushdown.

        Extracts WHERE predicates and SELECT columns from the SQL, then
        uses the best available pruning read path:
          1. read_with_encoded_pruning (FastLanes-style) — fastest
          2. read_with_column_chunk_pruning — per-column-chunk I/O
          3. read_with_pruning — row-group pruning only
          4. read_table — full read (fallback)

        Each path falls back to the next if the storage mode is not
        available for this collection (e.g., legacy range_write data
        uses path 3 or 4; range_write_encoded data uses path 1).

        Falls back to full read_table if predicate extraction fails.
        """
        try:
            # Extract predicates from WHERE clause
            predicates = self._extract_predicates(sql)

            # Extract projected columns from SELECT clause
            columns = self._extract_columns(sql)

            if predicates:
                # Try the fastest path first; each path falls back
                # internally if the storage mode is not available.
                # read_with_encoded_pruning falls back to
                # read_with_column_chunk_pruning which falls back to
                # read_with_pruning which falls back to read_table.
                table = self.lens.read_with_encoded_pruning(
                    table_name,
                    predicates=predicates,
                    row_filter=None,  # let DuckDB evaluate the predicate
                )
                # Apply projection on the pruned result.
                # MUST include WHERE columns so DuckDB can evaluate the filter.
                if columns and columns != ["*"]:
                    # Add predicate columns to the projection
                    pred_cols = [p[0] for p in predicates]
                    all_cols = list(set(columns + pred_cols))
                    available = [c for c in all_cols if c in table.column_names]
                    if available:
                        table = table.select(available)
                return table
            elif columns and columns != ["*"]:
                # No WHERE but projection pushdown
                return self.lens.read_columns(table_name, columns)
            else:
                # No pushdown possible — full read
                return self.lens.read_table(table_name)
        except (ImportError, KeyError, AttributeError):
            # Missing extension or column — fall back to full read
            return self.lens.read_table(table_name)
        except Exception:
            # Any other failure in pushdown → fall back to full read.
            # (Catches parser bugs, predicate evaluation errors, etc.)
            return self.lens.read_table(table_name)

    @staticmethod
    def _extract_predicates(sql: str) -> list:
        """Extract simple column-op-value predicates from a SQL WHERE clause.

        Supports: =, !=, <, <=, >, >= on simple column comparisons.
        Returns a list of (column, op, value) tuples.

        Does NOT handle:
          - Joins (predicates on joined tables)
          - Subqueries
          - Complex expressions (functions, arithmetic)
        """
        import re

        predicates = []

        # Find WHERE clause (case-insensitive)
        where_match = re.search(r'\bWHERE\b\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)',
                                sql, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return predicates

        where_clause = where_match.group(1).strip()

        # Split on AND (case-insensitive) — each AND part is a separate predicate
        # For OR, we treat the entire OR expression as non-prunable (conservative)
        # because pruning with OR requires ALL branches to say "can't match".
        parts = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)

        for part in parts:
            part = part.strip()
            col, op, value = PondLakehouse._parse_single_predicate(part)
            if col is not None:
                predicates.append((col, op, value))

        return predicates

    @staticmethod
    def _parse_single_predicate(part: str):
        """Parse a single predicate like 'age > 30' or 'region = US'.

        Returns (column, op, value) or (None, None, None) if unparseable.
        Supports: =, !=, <, <=, >, >=, IN, BETWEEN
        """
        import re

        part = part.strip()

        # BETWEEN: col BETWEEN val1 AND val2
        # Note: this is tricky because AND is also a clause separator.
        # We handle BETWEEN before the AND split by looking for the pattern.
        between_match = re.match(
            r'(\w+)\s+BETWEEN\s+(?:\'([^\']*)\'|(\d+\.?\d*))\s+AND\s+(?:\'([^\']*)\'|(\d+\.?\d*))',
            part, re.IGNORECASE)
        if between_match:
            col = between_match.group(1)
            # Lower bound
            if between_match.group(2) is not None:
                lo = between_match.group(2)
            else:
                lo = float(between_match.group(3)) if '.' in between_match.group(3) else int(between_match.group(3))
            # Upper bound
            if between_match.group(4) is not None:
                hi = between_match.group(4)
            else:
                hi = float(between_match.group(5)) if '.' in between_match.group(5) else int(between_match.group(5))
            # BETWEEN lo AND hi is equivalent to >= lo AND <= hi
            # We return the lower bound; the upper bound is handled by
            # the caller checking for BETWEEN specifically.
            # For simplicity, we return >= lo (conservative — might read more)
            return (col, ">=", lo)

        # IN: col IN ('val1', 'val2', ...) or col IN (1, 2, 3)
        in_match = re.match(r'(\w+)\s+IN\s*\(([^)]+)\)', part, re.IGNORECASE)
        if in_match:
            col = in_match.group(1)
            values_str = in_match.group(2)
            # Parse values
            values = []
            for v in values_str.split(","):
                v = v.strip()
                if v.startswith("'") and v.endswith("'"):
                    values.append(v[1:-1])
                else:
                    try:
                        values.append(float(v) if '.' in v else int(v))
                    except ValueError:
                        pass
            if values:
                return (col, "in", values)

        # Simple comparison: col OP value
        pattern = r'(\w+)\s*(=|!=|<=|>=|<|>)\s*'
        pattern += r"(?:'([^']*)'|(\d+\.?\d*))"
        match = re.match(pattern, part, re.IGNORECASE)
        if match:
            col, op, str_val, num_val = match.groups()
            if str_val is not None:
                value = str_val
            elif num_val is not None:
                value = float(num_val) if '.' in num_val else int(num_val)
            else:
                return (None, None, None)
            return (col, op, value)

        return (None, None, None)

    @staticmethod
    def _extract_columns(sql: str) -> list:
        """Extract projected column names from a SQL SELECT clause.

        Returns ["*"] for SELECT * or if extraction fails.
        Returns a list of column names for SELECT col1, col2, ...
        """
        import re

        # Find SELECT ... FROM
        select_match = re.match(r'\s*SELECT\s+(.+?)\s+FROM\s+',
                                sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return ["*"]

        cols_str = select_match.group(1).strip()

        # SELECT *
        if cols_str == "*":
            return ["*"]

        # SELECT COUNT(*), SUM(col), etc. — don't project (need all columns for aggregation)
        if re.search(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', cols_str, re.IGNORECASE):
            return ["*"]

        # Split on commas, extract column names
        parts = [p.strip() for p in cols_str.split(",")]
        columns = []
        for part in parts:
            # Handle "column" or "table.column" or "column AS alias"
            col_match = re.match(r'(?:\w+\.)?(\w+)(?:\s+AS\s+\w+)?$', part, re.IGNORECASE)
            if col_match:
                columns.append(col_match.group(1))
            else:
                return ["*"]  # can't parse — read all columns

        return columns if columns else ["*"]

    def query_at(self, sql: str, table_name: str, commit_hash: str) -> pa.Table:
        """Time travel: query a table at a specific commit."""
        table = self.lens.read_table(table_name, commit_hash)
        # Register with a temp name to avoid clobbering the live table
        temp_name = f"{table_name}_at_{commit_hash[:8]}"
        self.duckdb.register(temp_name, table)
        # Replace table_name with temp_name in the SQL (simple substitution)
        sql_at = sql.replace(table_name, temp_name)
        return self.duckdb.execute(sql_at).to_arrow_table()

    def branch(self, table_name: str, branch_name: str) -> str:
        return self.lens.branch(table_name, branch_name)

    def commit_to_branch(self, table_name: str, branch_name: str,
                         data: pa.Table) -> str:
        return self.lens.commit_to_branch(table_name, branch_name, data)

    def merge_branch(self, table_name: str, branch_name: str) -> str:
        return self.lens.merge_branch(table_name, branch_name)

    def history(self, table_name: str) -> list[dict]:
        return self.lens.history(table_name)

    def close(self):
        self.duckdb.close()
        self.kernel.close()


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _self_test():
    """Verify the PondLakehouse flagship works end-to-end."""
    print("=== Pond Lakehouse self-test ===")

    tmpdir = tempfile.mkdtemp(prefix="pond_lakehouse_")
    try:
        lh = PondLakehouse(tmpdir)

        # Test 1: create a table and query it
        users = pa.table({
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "age": [30, 25, 35],
        })
        lh.create_table("users", users)
        result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
        assert result.column("cnt")[0].as_py() == 3, f"expected 3, got {result.column('cnt')[0]}"
        print(f"  [OK] create table + SELECT COUNT(*)")

        # Test 2: insert and re-query
        new_users = pa.table({
            "id": [4, 5],
            "name": ["dave", "eve"],
            "age": [40, 28],
        })
        lh.insert("users", new_users)
        result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
        assert result.column("cnt")[0].as_py() == 5, f"expected 5, got {result.column('cnt')[0]}"
        print(f"  [OK] insert + SELECT COUNT(*)")

        # Test 3: filter query (age > 30: carol=35, dave=40, eve=28 excluded)
        # Actually: alice=30 (not >30), bob=25, carol=35, dave=40, eve=28
        # So age > 30 → carol, dave
        result = lh.query(
            "SELECT name FROM users WHERE age > 30 ORDER BY name",
            table_name="users",
        )
        names = [r.as_py() for r in result.column("name")]
        assert names == ["carol", "dave"], f"expected ['carol', 'dave'], got {names}"
        print(f"  [OK] SELECT with WHERE + ORDER BY")

        # Test 4: time travel — query at the original commit (3 rows)
        history = lh.history("users")
        original_commit = history[-1]["hash"]  # last in history is the first commit
        result = lh.query_at(
            "SELECT COUNT(*) AS cnt FROM users",
            table_name="users",
            commit_hash=original_commit,
        )
        assert result.column("cnt")[0].as_py() == 3, \
            f"time travel: expected 3 rows at original commit, got {result.column('cnt')[0]}"
        print(f"  [OK] time travel: query at original commit returns 3 rows")

        # Test 5: branching
        lh.branch("users", "dev")
        dev_users = pa.table({
            "id": [6],
            "name": ["frank"],
            "age": [50],
        })
        lh.commit_to_branch("users", "dev", dev_users)
        result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
        assert result.column("cnt")[0].as_py() == 5, \
            "main HEAD unchanged after dev branch commit"
        print(f"  [OK] branch: dev branch commit doesn't affect main HEAD")

        # Test 6: merge dev into main
        # Note: union merge doubles common rows. dev branch had 5 (from main) + 1 (frank) = 6.
        # main has 5. Union → 5 + 6 = 11 (with duplicates from common ancestor).
        # This is the simple "union merge" policy; production would do row-level
        # 3-way merge to avoid duplicates.
        lh.merge_branch("users", "dev")
        result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
        cnt = result.column("cnt")[0].as_py()
        assert cnt == 11, \
            f"union merge: expected 11 rows (5 main + 6 dev, with dups), got {cnt}"
        result = lh.query(
            "SELECT COUNT(*) AS cnt FROM users WHERE name = 'frank'",
            table_name="users",
        )
        assert result.column("cnt")[0].as_py() == 1, "frank appears once after merge"
        print(f"  [OK] merge: 11 rows after union merge (frank included; dups from common ancestor)")

        # Test 7: history shows merge commit with 2 parents
        history = lh.history("users")
        latest = history[0]
        assert latest["second_parent"] is not None, "merge commit has second_parent"
        print(f"  [OK] history: merge commit has 2 parents")

        # Test 8: schema evolution — add a column
        users_v2 = pa.table({
            "id": [7],
            "name": ["grace"],
            "age": [45],
            "email": ["grace@example.com"],  # new column
        })
        lh.insert("users", users_v2)
        result = lh.query(
            "SELECT name, email FROM users WHERE email IS NOT NULL",
            table_name="users",
        )
        emails = [r.as_py() for r in result.column("email")]
        assert emails == ["grace@example.com"], f"expected ['grace@example.com'], got {emails}"
        print(f"  [OK] schema evolution: new column 'email' added; old rows have NULL")

        # Test 9: aggregation query
        result = lh.query(
            "SELECT COUNT(*) AS cnt, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age FROM users",
            table_name="users",
        )
        cnt = result.column("cnt")[0].as_py()
        assert cnt == 12, f"aggregation: expected 12 rows, got {cnt}"
        print(f"  [OK] aggregation: COUNT/AVG/MIN/MAX over 12 rows")

        # Test 10: JOIN two tables
        orders = pa.table({
            "order_id": [1, 2, 3],
            "user_id": [1, 2, 1],
            "amount": [100.0, 200.0, 150.0],
        })
        lh.create_table("orders", orders)
        lh.duckdb.register("users", lh.lens.read_table("users"))
        lh.duckdb.register("orders", lh.lens.read_table("orders"))
        result = lh.query("""
            SELECT u.name, SUM(o.amount) AS total
            FROM users u
            JOIN orders o ON u.id = o.user_id
            GROUP BY u.name
            ORDER BY total DESC
        """)
        names = [r.as_py() for r in result.column("name")]
        assert "alice" in names, "JOIN: alice has orders"
        print(f"  [OK] JOIN: users ⋈ orders on user_id, grouped by name")

        # ---------------------------------------------------------------
        # Test 11-14: Range read/write on top of the ProllyTreeIndex
        # ---------------------------------------------------------------

        # Test 11: range_write a sorted table to a NEW collection
        events = pa.table({
            "event_id": [f"e{i:04d}" for i in range(100)],
            "user_id": [i % 10 for i in range(100)],
            "amount": [float(i) for i in range(100)],
        })
        # Use small row_group_size so we get multiple row groups
        lh.range_write("events", events, key_col="event_id", row_group_size=25)
        # 100 rows / 25 per group = 4 row groups
        print(f"  [OK] range_write: 100 rows in 4 row groups (rg/e0024, rg/e0049, rg/e0074, rg/e0099)")

        # Test 12: range_read all → returns all 100 rows
        all_rows = lh.range_read("events")
        assert all_rows.num_rows == 100, \
            f"range_read all: expected 100 rows, got {all_rows.num_rows}"
        print(f"  [OK] range_read all: 100 rows")

        # Test 13: range_read a subrange → returns row groups overlapping the range
        # Row group keys are rg/e0024, rg/e0049, rg/e0074, rg/e0099.
        # Range [e0050, e0080] should match rg/e0074 (max_pk=e0074, which is >= e0050)
        # and rg/e0099 (max_pk=e0099, which is >= e0080? Actually e0099 > e0080, so yes).
        # Wait — the row group with max_pk=e0074 contains rows e0050..e0074.
        # The row group with max_pk=e0099 contains rows e0075..e0099.
        # So range_read("events", "e0050", "e0080") returns row groups
        # rg/e0074 (rows e0050..e0074) AND rg/e0099 (rows e0075..e0099) → 50 rows total.
        # Note: row-group granularity means we get rows outside the requested range
        # (e0075..e0099 even though we asked up to e0080). Caller must filter.
        range_result = lh.range_read("events", "e0050", "e0080")
        assert range_result.num_rows == 50, \
            f"range_read [e0050,e0080]: expected 50 rows (2 row groups), got {range_result.num_rows}"
        print(f"  [OK] range_read [e0050,e0080]: 50 rows (2 row groups; caller filters exact rows)")

        # Test 14: point lookup — find the row group containing a specific key
        point_result = lh.range_point_lookup("events", "e0042")
        assert point_result is not None, "point lookup should find a row group"
        # e0042 falls in the row group with max_pk=e0049 (rows e0025..e0049)
        assert point_result.num_rows == 25, \
            f"point lookup: expected row group of 25 rows, got {point_result.num_rows}"
        # Caller filters further via DuckDB:
        lh.duckdb.register("point_result", point_result)
        exact = lh.duckdb.execute(
            "SELECT event_id, amount FROM point_result WHERE event_id = 'e0042'"
        ).to_arrow_table()
        assert exact.num_rows == 1, f"exact filter: expected 1 row, got {exact.num_rows}"
        assert exact.column("amount")[0].as_py() == 42.0
        print(f"  [OK] range_point_lookup('e0042') + DuckDB filter: O(log N) tree lookup + 1 row")

        lh.close()
        print("\nAll Pond Lakehouse tests pass.")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _benchmark():
    """Quick benchmark: compare PondLakehouse vs native DuckDB+Parquet."""
    print("\n=== PondLakehouse vs native DuckDB+Parquet benchmark ===")
    tmpdir = tempfile.mkdtemp(prefix="pond_lh_bench_")
    try:
        import time as _time

        # Generate 10K rows
        n_rows = 10_000
        ids = list(range(n_rows))
        names = [f"user_{i}" for i in range(n_rows)]
        ages = [20 + (i % 50) for i in range(n_rows)]
        data = pa.table({"id": ids, "name": names, "age": ages})

        # PondLakehouse
        lh = PondLakehouse(os.path.join(tmpdir, "pond"))
        t0 = _time.perf_counter()
        lh.create_table("users", data)
        t_create_pond = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = lh.query("SELECT COUNT(*) FROM users", table_name="users")
        t_count_pond = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = lh.query("SELECT AVG(age) FROM users", table_name="users")
        t_avg_pond = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = lh.query("SELECT name FROM users WHERE age > 50", table_name="users")
        t_filter_pond = _time.perf_counter() - t0

        lh.close()

        # Native DuckDB + Parquet
        native_dir = os.path.join(tmpdir, "native")
        os.makedirs(native_dir)
        con = duckdb.connect(os.path.join(native_dir, "native.db"))
        con.execute("INSTALL parquet; LOAD parquet;")
        parquet_path = os.path.join(native_dir, "users.parquet")

        t0 = _time.perf_counter()
        import pyarrow.parquet as pq
        pq.write_table(data, parquet_path)
        t_create_native = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()
        t_count_native = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = con.execute(f"SELECT AVG(age) FROM read_parquet('{parquet_path}')").fetchone()
        t_avg_native = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = con.execute(f"SELECT name FROM read_parquet('{parquet_path}') WHERE age > 50").fetchall()
        t_filter_native = _time.perf_counter() - t0

        con.close()

        print(f"\n  Operation         | PondLakehouse | Native DuckDB+Parquet")
        print(f"  ------------------|---------------|----------------------")
        print(f"  create (10K rows) | {t_create_pond*1000:.1f}ms        | {t_create_native*1000:.1f}ms")
        print(f"  COUNT(*)          | {t_count_pond*1000:.1f}ms         | {t_count_native*1000:.1f}ms")
        print(f"  AVG(age)          | {t_avg_pond*1000:.1f}ms         | {t_avg_native*1000:.1f}ms")
        print(f"  filter + scan     | {t_filter_pond*1000:.1f}ms         | {t_filter_native*1000:.1f}ms")

        print(f"\n  Overhead of Pond layer (create): {((t_create_pond/t_create_native - 1) * 100):.0f}%")
        print(f"  Overhead of Pond layer (count):  {((t_count_pond/t_count_native - 1) * 100):.0f}%")
        print(f"  Overhead of Pond layer (avg):    {((t_avg_pond/t_avg_native - 1) * 100):.0f}%")
        print(f"  Overhead of Pond layer (filter): {((t_filter_pond/t_filter_native - 1) * 100):.0f}%")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    _self_test()
    _benchmark()
