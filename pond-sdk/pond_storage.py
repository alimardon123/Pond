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

    def stamp_collection_metadata(self, name: str, **kwargs) -> str:
        """Stamp cross-lens metadata on a collection. See base_lens.PondLens."""
        return self._lens.stamp_collection_metadata(name, **kwargs)

    def get_collection_metadata(self, name: str) -> dict:
        """Read cross-lens metadata for a collection. See base_lens.PondLens."""
        return self._lens.get_collection_metadata(name)

    def list_collections_with_metadata(self) -> list[dict]:
        """List ALL collections with their cross-lens metadata.

        Returns a list of {"name", "lens_type", "key_col", "schema_hint",
        "created_at"} for every collection in the pond, regardless of
        which lens created it. Any lens can call this to see the entire
        pond.
        """
        return self._lens.list_collections_with_metadata()

    def resolve_ref(self, name: str) -> Optional[str]:
        """Resolve a ref name to its current hash."""
        return self.kernel.resolve(name)

    # ==================================================================
    # Section 2: Commit / branch / history (manifest-based — no ProllyTree)
    #
    # All version control operations delegate to UnifiedStorage, which
    # uses a simple JSON commit blob format:
    #   {parent, second_parent, manifest, message, timestamp, index}
    #
    # The commit chain is: HEAD ref → commit blob → manifest blob → data blobs
    # Branches are ref copies. Merges create two-parent commits.
    # History walks parent pointers. No ProllyTree involved.
    # ==================================================================

    def commit(self, name: str, message: str = "") -> str:
        """Commit staged changes for a collection.

        With the unified manifest-based architecture, commits are
        created automatically by write()/append(). This method is kept
        for API compatibility — it's a no-op that returns the current HEAD.
        """
        head = self.kernel.resolve(f"collections/{name}/HEAD")
        return head or ""

    def branch(self, name: str, branch_name: str) -> str:
        """Create a branch on a collection — O(1) ref copy."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.branch(name, branch_name)

    def checkout(self, name: str, branch_name: str) -> None:
        """Checkout a branch — point HEAD at the branch's commit."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        self._unified.checkout(name, branch_name)

    def list_branches(self, name: str) -> list[str]:
        """List all branches for a collection."""
        if self._unified is None:
            return []
        return self._unified.list_branches(name)

    def merge(self, name: str, branch_name: str, message: str = "") -> str:
        """Merge a branch into HEAD — creates a two-parent merge commit."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.merge(name, branch_name, message)

    def undo(self, name: str, steps: int = 1) -> str:
        """Undo the last N commits — walk parent pointers."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.undo(name, steps)

    def history(self, name: str, limit: int = 100) -> list[dict]:
        """Walk the commit history for a collection."""
        if self._unified is not None:
            return self._unified.history(name, limit)
        return self._lens.history(name, limit)

    def diff(self, name: str, commit_a: str, commit_b: str) -> dict:
        """Compute the diff between two commits."""
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.diff(name, commit_a, commit_b)

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
        # Round 26: no need to save commit→manifest mapping separately.
        # The commit blob stores the manifest hash directly, and
        # _resolve_commit_manifest reads it from there (1 GET).
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
        # Round 26: no need to save commit→manifest mapping separately.
        # The commit blob stores the manifest hash directly.
        return commit_hash

    def append_concurrent(self, collection: str, rows,
                           key_col: Optional[str] = None,
                           row_group_size: int = 10_000,
                           encoding_hints: Optional[dict[str, str]] = None,
                           message: str = "",
                           max_retries: int = 5) -> str:
        """Concurrent-safe append — for multi-user/multi-engine scenarios.

        Uses optimistic concurrency (CAS on HEAD ref):
        - Multiple writers can append simultaneously
        - Losers re-read HEAD and retry (up to max_retries)
        - No in-memory cache dependency — a new connection works seamlessly

        Use this when:
          - Multiple processes/engines write to the same collection
          - Streaming writers + OLTP engines access the same storage
          - You want correctness without cache tuning

        Use append() instead when:
          - Single-writer scenario (same process)
          - You want O(1) warm writes via in-memory caching
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.append_concurrent(
            collection, rows, key_col=key_col,
            row_group_size=row_group_size,
            encoding_hints=encoding_hints,
            message=message, max_retries=max_retries)

    def append_shard(self, collection: str, rows,
                      key_col: Optional[str] = None,
                      row_group_size: int = 10_000,
                      encoding_hints: Optional[dict[str, str]] = None,
                      message: str = "") -> str:
        """Concurrent-safe append — NO CAS, NO retry, NO coordination.

        This is the BEAUTIFUL concurrency model. Each writer writes its
        own shard to a unique path. Readers merge all shards (CRDT union).

        Better than CAS because:
        - No retry storms — writes always succeed
        - No object-store-specific conditional PUTs — works on local FS too
        - No boilerplate — just write your shard, you're done
        - No coordination — writers don't know about each other

        Works on ANY storage that supports listing (local FS, S3, GCS).

        After writing shards, call compact_shards() periodically to merge
        them into HEAD (bounds read amplification).
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.append_shard(
            collection, rows, key_col=key_col,
            row_group_size=row_group_size,
            encoding_hints=encoding_hints, message=message)

    def read_with_shards(self, collection: str,
                          predicates: Optional[list[tuple[str, str, Any]]] = None,
                          columns: Optional[list[str]] = None,
                          row_filter: Optional[Callable[[dict], bool]] = None,
                          start_key: Optional[str] = None,
                          end_key: Optional[str] = None) -> list[dict]:
        """Read rows merging HEAD + all shards (CRDT union).

        Use this instead of read() when shards exist (after append_shard).
        Falls back to plain read() if no shards exist.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        # If no shards, fall back to plain read (faster)
        if self._unified.shard_count(collection) == 0:
            return self.read(collection, predicates=predicates,
                              columns=columns, row_filter=row_filter,
                              start_key=start_key, end_key=end_key)
        return self._unified.read_with_shards(
            collection, predicates=predicates, columns=columns,
            row_filter=row_filter, start_key=start_key, end_key=end_key)

    def compact_shards(self, collection: str) -> Optional[str]:
        """Merge all shards into HEAD, then clear the shards.

        Idempotent — multiple compactors produce the same result.
        Should be called periodically (e.g., after every N shards) to
        bound read amplification.
        """
        if self._unified is None:
            raise RuntimeError("UnifiedStorage not available")
        return self._unified.compact_shards(collection)

    def shard_count(self, collection: str) -> int:
        """Return the number of unmerged shards for a collection."""
        if self._unified is None:
            return 0
        return self._unified.shard_count(collection)

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

        Round 26: the manifest hash is stored directly IN the commit blob
        (JSON format). Read it from there (1 GET). Falls back to the
        legacy ref-based lookup for old collections.
        """
        if self._unified is not None:
            return self._unified._resolve_commit_manifest(collection, commit_hash)
        # Legacy fallback
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
