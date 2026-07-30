"""
PondStorage — the ONE unified storage SDK.

This is the single entry point for all storage operations in Pond.
It unifies what was previously three separate classes:
  - PondLens (namespace ops: list_collections, set_definition, etc.)
  - ProllyLensBase (commit/branch/merge/history)
  - UnifiedStorage (PND2 write/read/point_lookup)

Into ONE class with three clear sections:

  ┌─────────────────────────────────────────────────────────┐
  │  PondStorage                                             │
  │  ┌─────────────┐  ┌───────────────┐  ┌────────────────┐ │
  │  │ Namespace   │  │ Commit/Branch │  │ Data I/O       │ │
  │  │ (list, def) │  │ (history)     │  │ (write/read)   │ │
  │  └─────────────┘  └───────────────┘  └────────────────┘ │
  └─────────────────────────────────────────────────────────┘

ARCHITECTURE:
  Lenses (Lakehouse, KV, Vector) compose PondStorage.
  PondStorage delegates to UnifiedStorage (PND2 + CollectionManifest)
  and ProllyLensBase (commit/branch/history).
  The kernel (PondMinimal or ObjectStoreNativeKernel) is FROZEN.

USAGE:
    from pond_sdk import PondStorage, PondMinimal

    storage = PondStorage(PondMinimal("/path/to/.pond"))

    # Write any workload — same API
    storage.write("users", [{"id": 1, "name": "alice"}], key_col="id")

    # Read any workload — same API
    rows = storage.read("users", predicates=[("id", "=", 1)])
    row = storage.point_lookup("users", key="1")

    # Version control — same API
    storage.branch("users", "dev")
    storage.merge("users", "dev")
    storage.history("users")

This class is a thin orchestrator — it delegates to the existing
UnifiedStorage + ProllyLensBase internally. No behavior change, just
a unified API surface so lens authors see ONE class instead of three.

MIGRATION: Existing lenses that use PondLens/ProllyLensBase/UnifiedStorage
directly still work. PondStorage is the recommended new API. Over time,
lenses will migrate to compose PondStorage instead of the individual classes.
"""

from __future__ import annotations

import os
import sys
import json
from typing import Optional, Any, Iterator, Callable

# Make pond-core and pond-sdk importable
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "pond-core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "extensions", "physical_structures"))

from kernel import PondMinimal  # noqa: E402
from base_lens import PondLens  # noqa: E402

# Import the unified storage layer (PND2 + CollectionManifest)
try:
    from unified_storage import UnifiedStorage, PND2  # noqa: E402
    from collection_manifest import CollectionManifest  # noqa: E402
    _HAVE_UNIFIED = True
except ImportError:
    _HAVE_UNIFIED = False

# Import ProllyLensBase for commit/branch/history
try:
    from prolly_tree import ProllyLensBase, ProllyTree  # noqa: E402
    from binary_encoding import BinaryProllyTree  # noqa: E402
    _HAVE_PROLLY = True
except ImportError:
    _HAVE_PROLLY = False


class PondStorage:
    """The ONE unified storage SDK for Pond.

    Three sections:
      1. Namespace: list_collections, collection_exists, set_definition,
         get_definition
      2. Commit/Branch: commit, branch, checkout, list_branches, merge,
         undo, history, diff
      3. Data I/O: write, append, read, read_as_columns, point_lookup,
         scan_with_pruning

    Lenses compose this class. They don't inherit from it. The lens
    provides workload-specific APIs (SQL, k-NN, JSON encode/decode) on
    top of the unified storage operations.

    Example:
        storage = PondStorage(kernel)
        storage.write("users", [{"id": 1, "name": "alice"}], key_col="id")
        row = storage.point_lookup("users", key="1")
        storage.branch("users", "dev")
        storage.merge("users", "dev")
    """

    def __init__(self, kernel: PondMinimal):
        """Create a PondStorage instance.

        Args:
            kernel: the PondMinimal or ObjectStoreNativeKernel instance
        """
        self.kernel = kernel
        # The namespace base (for list_collections, set_definition, etc.)
        self._lens = PondLens(kernel)
        # The unified storage layer (PND2 + CollectionManifest)
        self._unified: Optional[UnifiedStorage] = None
        if _HAVE_UNIFIED:
            self._unified = UnifiedStorage(kernel)

    # ==================================================================
    # Section 1: Namespace operations (was PondLens)
    # ==================================================================

    def list_collections(self) -> list[str]:
        """List all collections (any lens, any format)."""
        return self._lens.list_collections()

    def collection_exists(self, name: str) -> bool:
        """Check if a collection has a HEAD ref."""
        return self._lens.collection_exists(name)

    def set_definition(self, name: str, definition: dict) -> str:
        """Store lens-specific metadata for a collection."""
        return self._lens.set_definition(name, definition)

    def get_definition(self, name: str) -> Optional[dict]:
        """Read lens-specific metadata for a collection."""
        return self._lens.get_definition(name)

    def resolve_ref(self, name: str) -> Optional[str]:
        """Resolve a ref name to its current hash."""
        return self.kernel.resolve(name)

    # ==================================================================
    # Section 2: Commit / branch / history (was ProllyLensBase)
    # ==================================================================

    def _get_base(self, name: str) -> Optional["ProllyLensBase"]:
        """Get or create the ProllyLensBase for a collection."""
        if not _HAVE_PROLLY:
            return None
        return ProllyLensBase(self.kernel, name)

    def commit(self, name: str, message: str = "") -> str:
        """Commit staged changes for a collection.

        For the unified storage path, commit is handled by write()/append().
        For the legacy ProllyTreeIndex path, this delegates to ProllyLensBase.
        """
        if not _HAVE_PROLLY:
            raise RuntimeError("ProllyLensBase not available")
        base = self._get_base(name)
        return base.commit(message or f"{name} commit")

    def branch(self, name: str, branch_name: str) -> str:
        """Create a branch on a collection."""
        if not _HAVE_PROLLY:
            raise RuntimeError("ProllyLensBase not available")
        base = self._get_base(name)
        return base.branch(branch_name)

    def checkout(self, name: str, branch_name: str) -> None:
        """Checkout a branch."""
        if not _HAVE_PROLLY:
            raise RuntimeError("ProllyLensBase not available")
        base = self._get_base(name)
        base.checkout(branch_name)

    def list_branches(self, name: str) -> list[str]:
        """List all branches for a collection."""
        if not _HAVE_PROLLY:
            return []
        base = self._get_base(name)
        return base.list_branches()

    def merge(self, name: str, branch_name: str, message: str = "") -> str:
        """Merge a branch into HEAD."""
        if not _HAVE_PROLLY:
            raise RuntimeError("ProllyLensBase not available")
        base = self._get_base(name)
        return base.merge(branch_name, message)

    def undo(self, name: str, steps: int = 1) -> str:
        """Undo the last N commits."""
        if not _HAVE_PROLLY:
            raise RuntimeError("ProllyLensBase not available")
        base = self._get_base(name)
        return base.undo(steps)

    def history(self, name: str, limit: int = 100) -> list[dict]:
        """Walk the commit history for a collection."""
        # Try unified history first (format-agnostic)
        return self._lens.history(name, limit)

    def diff(self, name: str, commit_a: str, commit_b: str) -> dict:
        """Compute the diff between two commits."""
        if not _HAVE_PROLLY:
            raise RuntimeError("ProllyLensBase not available")
        base = self._get_base(name)
        return base.diff(commit_a, commit_b)

    # ==================================================================
    # Section 3: Data I/O (was UnifiedStorage)
    # ==================================================================

    def write(self, collection: str, rows,
              key_col: Optional[str] = None,
              row_group_size: int = 10_000,
              encoding_hints: Optional[dict[str, str]] = None,
              message: str = "") -> str:
        """Write rows to a collection as PND2 blobs.

        ONE write path for ALL workloads. Splits rows into row groups,
        encodes each as a PND2 blob (auto-selects encoding per column),
        builds a CollectionManifest, and commits atomically.

        Args:
            collection: collection name
            rows: a ColumnSource, PyArrow Table, or list[dict]
            key_col: column to use as the sort key (None = row index)
            row_group_size: rows per row group (default 10_000)
            encoding_hints: optional dict {col_name: "auto"|"rle"|...}
            message: commit message

        Returns:
            The new HEAD commit hash.

        Round trips: N + 3 S3 PUTs (N data blobs + manifest + root ref + root pointer)
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available — install the physical_structures extension")
        commit_hash = self._unified.write(collection, rows, key_col=key_col,
                                     row_group_size=row_group_size,
                                     encoding_hints=encoding_hints,
                                     message=message)
        # Save commit→manifest mapping for time-travel reads
        self._save_commit_manifest(collection, commit_hash)
        return commit_hash

    def append(self, collection: str, rows,
               key_col: Optional[str] = None,
               row_group_size: int = 10_000,
               encoding_hints: Optional[dict[str, str]] = None,
               message: str = "") -> str:
        """Append rows to an existing collection WITHOUT rewriting it.

        Non-destructive: reads the existing manifest (1 GET), keeps all
        existing row group entries, adds new row groups, writes a new
        manifest + commit.

        Args:
            collection: collection name (must already exist)
            rows: new rows to append
            key_col: sort key column (should match existing)
            row_group_size: rows per new row group
            encoding_hints: optional encoding hints
            message: commit message

        Returns:
            The new HEAD commit hash.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        commit_hash = self._unified.append(collection, rows, key_col=key_col,
                                      row_group_size=row_group_size,
                                      encoding_hints=encoding_hints,
                                      message=message)
        # Save commit→manifest mapping for time-travel reads
        self._save_commit_manifest(collection, commit_hash)
        return commit_hash

    def read(self, collection: str,
             predicates: Optional[list[tuple[str, str, Any]]] = None,
             columns: Optional[list[str]] = None,
             row_filter: Optional[Callable[[dict], bool]] = None,
             start_key: Optional[str] = None,
             end_key: Optional[str] = None,
             commit_hash: Optional[str] = None) -> list[dict]:
        """Read rows from a collection.

        ONE read path for ALL workloads. Reads the manifest (1 GET),
        evaluates predicates IN MEMORY against inline stats, fetches
        only surviving row groups.

        Args:
            collection: collection name
            predicates: list of (column, op, value) tuples. All ANDed.
            columns: projection pushdown (None = all columns)
            row_filter: exact row-level filter
            start_key: range scan lower bound
            end_key: range scan upper bound
            commit_hash: time-travel — resolves to the manifest at this commit.
                Fix (Round 11 Issue #4): now properly resolves to manifest_hash.

        Returns:
            List of row dicts.

        Round trips: 3 + K S3 GETs cold (root pointer + root ref + manifest + K data blobs)
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        manifest_hash = self._resolve_commit_manifest(collection, commit_hash) if commit_hash else None
        return self._unified.read(collection, predicates=predicates,
                                    columns=columns, row_filter=row_filter,
                                    start_key=start_key, end_key=end_key,
                                    manifest_hash=manifest_hash)

    def read_as_columns(self, collection: str,
                         predicates: Optional[list[tuple[str, str, Any]]] = None,
                         columns: Optional[list[str]] = None,
                         commit_hash: Optional[str] = None
                         ) -> dict[str, list]:
        """Read rows as column-oriented data (faster for columnar callers).

        Uses PARALLEL blob fetch for surviving row groups — K blobs fetched
        in ~1 RTT wall-clock instead of K × RTT.

        Fix (Round 12 Issue #2): resolves commit_hash to manifest_hash.
        Fix (Round 12 Issue #1): applies multi-predicate filter.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        manifest_hash = self._resolve_commit_manifest(collection, commit_hash) if commit_hash else None
        return self._unified.read_as_columns(collection, predicates=predicates,
                                               columns=columns,
                                               manifest_hash=manifest_hash)

    def read_as_arrow(self, collection: str,
                       predicates: Optional[list[tuple[str, str, Any]]] = None,
                       columns: Optional[list[str]] = None,
                       commit_hash: Optional[str] = None) -> "pa.Table":
        """Read rows as a PyArrow Table — FASTEST read path for tabular.

        1. Manifest pruning (in-memory, 0 GETs)
        2. Parallel blob fetch (K GETs in ~1 RTT wall-clock)
        3. Zero-copy Arrow construction from column data

        Fix (Round 12 Issue #2): resolves commit_hash to manifest_hash.
        Fix (Round 12 Issue #1): applies multi-predicate filter.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        manifest_hash = self._resolve_commit_manifest(collection, commit_hash) if commit_hash else None
        # read_as_arrow delegates to read_as_columns, so pass manifest_hash
        col_data = self._unified.read_as_columns(collection, predicates=predicates,
                                                   columns=columns,
                                                   manifest_hash=manifest_hash)
        if not col_data:
            import pyarrow as pa
            return pa.table({})
        import pyarrow as pa
        arrays = []
        names = []
        for col_name, values in col_data.items():
            arrays.append(pa.array(values))
            names.append(col_name)
        return pa.Table.from_arrays(arrays, names=names)

    def point_lookup(self, collection: str, key: str,
                      columns: Optional[list[str]] = None) -> Optional[dict]:
        """Point lookup — O(1) regardless of collection scale.

        Round trips: 4 S3 GETs cold (root pointer + root ref + manifest + 1 data blob)
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.point_lookup(collection, key=key, columns=columns)

    def scan_with_pruning(self, collection: str,
                           predicates: Optional[list[tuple[str, str, Any]]] = None
                           ) -> Iterator[tuple[str, str, dict]]:
        """Low-level scan — yields (rg_key, blob_hash, stats_dict) for surviving row groups."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        yield from self._unified.scan_with_pruning(collection, predicates=predicates)

    def iter_rows(self, collection: str,
                  predicates: Optional[list[tuple[str, str, Any]]] = None,
                  columns: Optional[list[str]] = None,
                  batch_size: int = 1000) -> Iterator[list[dict]]:
        """Streaming read — yields rows in batches without loading all into memory.

        Memory-safe for 1B+ row collections. O(batch_size) memory per yield.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        yield from self._unified.iter_rows(collection, predicates=predicates,
                                             columns=columns, batch_size=batch_size)

    # ==================================================================
    # Diagnostics
    # ==================================================================

    def _resolve_commit_manifest(self, collection: str,
                                  commit_hash: str) -> Optional[str]:
        """Resolve a commit hash to its manifest hash for time-travel reads.

        Fix (Round 11 Issue #4): PondStorage.write() and .append() save
        the commit→manifest mapping via _save_commit_manifest. This method
        looks it up.
        """
        return self.kernel.resolve(
            f"collections/{collection}/commits/{commit_hash}__manifest")

    def _save_commit_manifest(self, name: str, commit_hash: str) -> None:
        """Save the current manifest hash keyed by commit hash for time-travel."""
        manifest_hash = self.kernel.resolve(f"collections/{name}/manifest")
        if manifest_hash is not None:
            self.kernel.reference(
                f"collections/{name}/commits/{commit_hash}__manifest",
                manifest_hash)

    def count(self, collection: str,
              predicates: Optional[list] = None) -> int:
        """Count rows in a collection WITHOUT fetching data blobs.

        Sums n_rows from surviving row groups in the manifest.
        O(1) S3 GETs if manifest is cached, O(1) GET otherwise.
        """
        manifest = self._unified._load_manifest(collection) if self._unified else None
        if manifest is None:
            return 0
        return sum(rg.n_rows for rg in manifest.scan_with_pruning(predicates))

    def delete_collection(self, collection: str) -> bool:
        """Delete a collection by tombstoning its HEAD and manifest refs.

        Fix (Round 12 Issue #3): previously a no-op that returned True.
        Now uses the RFC-0008 tombstone pattern from maintenance.py to
        actually rebind HEAD to TOMBSTONE_HASH, making the collection
        unreadable. Underlying blobs are NOT deleted (content-addressed,
        may be shared). Use vacuum() for blob cleanup (not yet implemented).
        """
        deleted = False
        head_ref = f"collections/{collection}/HEAD"
        manifest_ref = f"collections/{collection}/manifest"

        try:
            from maintenance import drop_name, TOMBSTONE_HASH
            # Tombstone HEAD (makes collection_exists return False)
            if self.kernel.resolve(head_ref) is not None:
                drop_name(self.kernel, head_ref)
                deleted = True
            # Tombstone manifest (makes _load_manifest return None)
            if self.kernel.resolve(manifest_ref) is not None:
                drop_name(self.kernel, manifest_ref)
                deleted = True
        except ImportError:
            # maintenance.py not available — manual tombstone
            # Write a zero-length blob and point HEAD at it
            if self.kernel.resolve(head_ref) is not None:
                empty_blob = self.kernel.write(b"")
                self.kernel.reference(head_ref, empty_blob)
                deleted = True
            if self.kernel.resolve(manifest_ref) is not None:
                empty_blob = self.kernel.write(b"")
                self.kernel.reference(manifest_ref, empty_blob)
                deleted = True

        if self._unified:
            self._unified._invalidate_manifest_cache(collection)
        return deleted

    def compact(self, collection: str) -> Optional[str]:
        """Compact a delta-manifest chain into a single flat manifest.

        Call after many appends to prevent read amplification from deep
        parent chains. Auto-triggered after 8 appends, but can be called
        manually for finer control.
        """
        if self._unified is None:
            return None
        return self._unified.compact_manifest(collection)

    def get_round_trip_count(self, collection: str,
                              predicates: Optional[list] = None) -> dict:
        """Estimate S3 round trips for a read (without performing it)."""
        if self._unified is None:
            return {"error": "UnifiedStorage not available"}
        manifest = self._unified._load_manifest(collection)
        if manifest is None:
            return {"error": "no manifest for collection"}
        total = len(manifest.row_groups)
        surviving = list(manifest.scan_with_pruning(predicates))
        k = len(surviving)
        return {
            "manifest_fetches": 1,
            "data_blob_fetches": k,
            "total_fetches": 1 + k,
            "total_row_groups": total,
            "pruned_row_groups": total - k,
            "selectivity": k / total if total else 0.0,
        }
