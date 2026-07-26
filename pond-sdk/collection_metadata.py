"""
CollectionMetadata — data-side metadata manager for Pond collections.

DESIGN: Metadata (zone maps, indexes, statistics, bloom filters) belongs
to COLLECTIONS (data-side), not to LENSES. A collection's metadata is
independent of what lens is reading it. Any lens can use any collection's
metadata.

This class is the single entry point for managing collection-level
metadata. Lenses call it at write time (to build metadata) and read
time (to use metadata for pruning/lookup). The metadata itself is stored
in the kernel via ProllyTreeIndex, separate from the data.

What this class manages:
  - Zone maps (min/max/null_count per data blob) — for predicate pushdown
  - Collection indexes (index_key → _rowid) — for secondary lookups
  - (Future: bloom filters, column statistics, materialized views)

Usage (write side — lens calls this after writing data):
    meta = CollectionMetadata(kernel)
    meta.build_zone_maps("users", scan_fn=lambda: lens.iterate("users"))
    meta.build_index("users", "by_name", extractor=lambda r: r["name"],
                     scan_fn=lambda: lens.iterate("users"))

Usage (read side — lens or query engine calls this):
    meta = CollectionMetadata(kernel)
    # Pruning: get data blob hashes that might match a predicate
    for blob_hash in meta.scan_with_pruning("users", predicate):
        data = kernel.read_blob(blob_hash)
        rows = lens.decode(data)

    # Index lookup: get _rowid by index key
    rowid = meta.lookup_index("users", "by_name", "alice")
    row = lens.get("users", rowid)

GENERICITY:
  - Works with ANY lens (KV, tabular, vector, custom)
  - The lens provides a scan_fn callback for building metadata
  - The metadata format is lens-agnostic (JSON zone maps, ProllyTree indexes)
  - Binary data (video, music) is handled gracefully (zone maps skipped)
  - No lens dependency — operates on kernel + collection name only
"""

from __future__ import annotations

import json
import os
import sys
from typing import Optional, Any, Callable, Iterator, Union

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# Add extensions/physical_structures to path for pruning imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "extensions", "indexing"))

from kernel import PondMinimal
from prolly_tree import ProllyTree, ProllyLensBase
from maintenance import drop_name, is_dropped, resolve_active, TOMBSTONE_HASH

# Import pruning components from the physical_structures extension
try:
    from pruning import ZoneMap, PruningPredicate, ColumnPredicate
    from zone_map_index import ZoneMapIndex
    from pruning_reader import PruningReader
    from collection_index import CollectionIndexer
    _PRUNING_AVAILABLE = True
except ImportError:
    _PRUNING_AVAILABLE = False


class CollectionMetadata:
    """Data-side metadata manager for Pond collections.

    Manages zone maps, indexes, and (future) other derived metadata for
    a collection. This is the CORRECT architecture per the design principles:
      - Metadata belongs to collections (data-side), not lenses
      - Any lens can use any collection's metadata
      - The metadata layer is independent of the lens layer

    This class does NOT know what format the data is in (JSON, Parquet,
    binary). It works through callbacks (scan_fn, decode_fn) provided
    by the lens.

    OBJECT-STORE-AWARE PRUNING:
    When the kernel's base_dir indicates object storage (S3, GCS, Azure
    Blob, or any network-backed path), pruning is automatically enabled
    because the network RTT savings dwarf Python overhead. When the
    kernel is local-disk-backed, pruning defaults to off (DuckDB native
    scan is faster for local data).

    Detection: checks kernel.base_dir for S3/network patterns.
    Override: pass use_pruning=True/False explicitly to read_with_pruning.
    """

    # Patterns that indicate object storage (not local disk)
    _OBJECT_STORE_PATTERNS = ["s3://", "gs://", "azure://", "abfs://",
                               "wasb://", "http://", "https://",
                               "nfs://", "/mnt/nfs/", "/mnt/shared/",
                               "/net/"]

    def __init__(self, kernel: PondMinimal, force_pruning: Optional[bool] = None):
        """Create a CollectionMetadata manager.

        Args:
            kernel: the PondMinimal kernel instance
            force_pruning: override auto-detection.
                None = auto (object store → on, local → off)
                True = always prune
                False = never prune
        """
        self.kernel = kernel
        self._is_object_store = self._detect_object_store()
        self._force_pruning = force_pruning

        # Lazy-init sub-managers (only if extensions are available)
        self._zm_index: Optional[ZoneMapIndex] = None
        self._indexer: Optional[CollectionIndexer] = None

    def _detect_object_store(self) -> bool:
        """Detect if the kernel is backed by object storage (S3, etc.).

        Returns True if the base_dir looks like a network/object store path.
        Returns False for local filesystem paths.
        """
        base_dir = getattr(self.kernel, 'base_dir', '')
        if not base_dir:
            return False
        base_lower = base_dir.lower()
        return any(base_lower.startswith(p) for p in self._OBJECT_STORE_PATTERNS)

    @property
    def is_object_store(self) -> bool:
        """True if the kernel is backed by object storage (S3, GCS, etc.).

        When True, pruning is auto-enabled (network RTT savings dwarf
        Python overhead). When False (local disk), pruning defaults to off.
        """
        return self._is_object_store

    def should_prune(self, explicit: Optional[bool] = None) -> bool:
        """Decide whether to use pruning for a read operation.

        Priority (highest first):
          1. explicit argument (caller's per-call override)
          2. force_pruning (set at construction time)
          3. auto-detection (object store → True, local → False)

        Args:
            explicit: caller's explicit choice for this call.

        Returns:
            True if pruning should be used, False otherwise.
        """
        if explicit is not None:
            return explicit
        if self._force_pruning is not None:
            return self._force_pruning
        return self._is_object_store

    @property
    def zm_index(self) -> Optional[ZoneMapIndex]:
        if self._zm_index is None and _PRUNING_AVAILABLE:
            self._zm_index = ZoneMapIndex(self.kernel)
        return self._zm_index

    @property
    def indexer(self) -> Optional[CollectionIndexer]:
        if self._indexer is None and _PRUNING_AVAILABLE:
            self._indexer = CollectionIndexer(self.kernel)
        return self._indexer

    # ==================================================================
    # Zone maps — predicate pushdown (skip data blobs without decoding)
    # ==================================================================

    def build_zone_maps(self, collection: str,
                        scan_fn: Callable[[], Iterator[tuple[str, Any, int]]],
                        columns: Optional[list[str]] = None) -> str:
        """Build zone maps for a collection by scanning its data.

        Args:
            collection: collection name
            scan_fn: generator yielding (row_group_key, data_bytes, row_count)
                tuples. The lens provides this — it knows how to iterate
                its own data blobs.
            columns: columns to compute stats for. If None, infers from data.

        Returns:
            The zone-map tree root hash.

        This is GENERIC: works with any data format. The scan_fn provides
        raw bytes; this method tries Parquet, then JSON, then skips.
        """
        if not _PRUNING_AVAILABLE or self.zm_index is None:
            return ""

        # Clear old zone maps for this collection
        base = self.zm_index._get_base(collection)
        for k in base.read_all().keys():
            if not k.startswith("_"):
                base.stage_delete(k)

        for row_group_key, data_bytes, row_count in scan_fn():
            zm = self._compute_zone_map(data_bytes, row_count, columns)
            from kernel import hash_bytes
            data_blob_hash = hash_bytes(data_bytes)
            self.zm_index.add_zone_map(collection, row_group_key, zm, data_blob_hash)

        return self.zm_index.commit_zone_maps(collection, f"zone maps for {collection}")

    def _compute_zone_map(self, data_bytes: bytes, row_count: int,
                          columns: Optional[list[str]] = None) -> "ZoneMap":
        """Compute a ZoneMap from raw data bytes.

        Tries Parquet first, then JSON, then returns empty (binary data).
        This is FORMAT-AGNOSTIC: works with any data type.
        """
        if not _PRUNING_AVAILABLE:
            return ZoneMap(row_count=row_count)

        # Try Parquet (LakehouseLens data)
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pq.read_table(pa.BufferReader(data_bytes))
            return ZoneMap.build(table, columns)
        except Exception:
            pass

        # Try JSON (KeyValueLens data — single row or list of rows)
        try:
            decoded = json.loads(data_bytes)
            if isinstance(decoded, dict):
                decoded = [decoded]
            if isinstance(decoded, list) and decoded:
                zm = ZoneMap(row_count=len(decoded))
                for row in decoded:
                    if not isinstance(row, dict):
                        continue
                    for col, val in row.items():
                        if columns and col not in columns:
                            continue
                        if val is None:
                            zm.null_count[col] = zm.null_count.get(col, 0) + 1
                        elif isinstance(val, (int, float, str)):
                            if col not in zm.min:
                                zm.min[col] = val
                                zm.max[col] = val
                            else:
                                zm.min[col] = min(zm.min[col], val)
                                zm.max[col] = max(zm.max[col], val)
                            zm.null_count[col] = zm.null_count.get(col, 0)
                return zm
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        # Binary data (video, music, blobs) — no zone map stats
        return ZoneMap(row_count=row_count)

    def has_zone_maps(self, collection: str) -> bool:
        """Check if a collection has zone maps."""
        if not _PRUNING_AVAILABLE or self.zm_index is None:
            return False
        return self.zm_index.has_zone_maps(collection)

    def scan_with_pruning(self, collection: str,
                          predicate: Optional["PruningPredicate"] = None,
                          start_key: Optional[str] = None,
                          end_key: Optional[str] = None) -> Iterator[str]:
        """Scan data blob hashes, skipping non-matching row groups.

        Yields data_blob_hash strings for row groups that MIGHT match.
        The caller reads + decodes only these blobs.
        """
        if not _PRUNING_AVAILABLE or self.zm_index is None:
            return
        yield from self.zm_index.scan_with_pruning(collection, predicate, start_key, end_key)

    def read_with_pruning(self, collection: str,
                          predicates: Optional[list] = None,
                          decode_fn: Optional[Callable[[bytes], Any]] = None,
                          row_filter: Optional[Callable[[Any], bool]] = None) -> Iterator[Any]:
        """Read rows with Vortex-style predicate pushdown.

        Args:
            collection: collection name
            predicates: list of (column, op, value) tuples for pruning
            decode_fn: function(bytes) → row or list of rows. If None,
                tries JSON decode.
            row_filter: optional function(row) → bool for exact filtering

        Yields:
            Individual rows from non-pruned data blobs.
        """
        if not _PRUNING_AVAILABLE or self.zm_index is None:
            return
        if not self.has_zone_maps(collection):
            return

        # Build pruning predicate
        predicate = None
        if predicates:
            col_preds = [ColumnPredicate(column=c, op=o, value=v)
                         for c, o, v in predicates]
            predicate = PruningPredicate(col_preds, combine="and")

        # Default decode function: JSON
        if decode_fn is None:
            decode_fn = lambda b: json.loads(b)

        reader = PruningReader(self.kernel, self.zm_index, collection, predicate)
        yield from reader.scan(decode_fn=decode_fn, row_filter=row_filter)

    def drop_zone_maps(self, collection: str) -> bool:
        """Drop all zone maps for a collection."""
        if not _PRUNING_AVAILABLE or self.zm_index is None:
            return False
        return self.zm_index.drop_zone_maps(collection)

    # ==================================================================
    # Indexes — secondary lookups (index_key → _rowid)
    # ==================================================================

    def build_index(self, collection: str, index_name: str,
                    extractor: Callable[[Any], Union[str, list[str]]],
                    scan_fn: Callable[[], Iterator[tuple[str, Any]]]) -> str:
        """Build a secondary index on a collection.

        Args:
            collection: collection name
            index_name: name for this index
            extractor: function(row_dict) → str | list[str]
            scan_fn: generator yielding (rowid, row_dict) pairs

        Returns:
            The index ProllyTree root hash.
        """
        if not _PRUNING_AVAILABLE or self.indexer is None:
            return ""
        return self.indexer.build_index(collection, index_name, extractor, scan_fn)

    def lookup_index(self, collection: str, index_name: str,
                     index_key: str) -> Optional[str]:
        """Look up a _rowid by index key.

        Returns the _rowid string, or None if not found.
        """
        if not _PRUNING_AVAILABLE or self.indexer is None:
            return None
        return self.indexer.lookup(collection, index_name, index_key)

    def list_indexes(self, collection: str) -> list[str]:
        """List all active indexes on a collection."""
        if not _PRUNING_AVAILABLE or self.indexer is None:
            return []
        return self.indexer.list_indexes(collection)

    def drop_index(self, collection: str, index_name: str) -> bool:
        """Drop an index from a collection."""
        if not _PRUNING_AVAILABLE or self.indexer is None:
            return False
        return self.indexer.drop_index(collection, index_name)

    def refresh_index(self, collection: str, index_name: str,
                      extractor, scan_fn=None) -> str:
        """Refresh an index incrementally (only update changed entries)."""
        if not _PRUNING_AVAILABLE or self.indexer is None:
            return ""
        return self.indexer.refresh_index(collection, index_name, extractor, scan_fn)

    def refresh_index_incremental(self, collection: str, index_name: str,
                                  extractor, old_commit: str, new_commit: str,
                                  decode_fn=None) -> str:
        """Refresh an index using commit-diff — O(changed) not O(N)."""
        if not _PRUNING_AVAILABLE or self.indexer is None:
            return ""
        return self.indexer.refresh_index_incremental(
            collection, index_name, extractor, old_commit, new_commit, decode_fn)

    def is_index_stale(self, collection: str, index_name: str,
                       scan_fn=None, extractor=None) -> bool:
        """Check if an index is stale (doesn't match current data)."""
        if not _PRUNING_AVAILABLE or self.indexer is None:
            return True
        return self.indexer.is_index_stale(collection, index_name, scan_fn, extractor)

    def register_lazy_index(self, collection: str, index_name: str,
                            extractor, scan_fn, staleness_budget: int = 5) -> None:
        """Register an index for LAZY auto-refresh."""
        if not _PRUNING_AVAILABLE or self.indexer is None:
            return
        self.indexer.register_lazy_index(collection, index_name, extractor, scan_fn, staleness_budget)

    def register_eager_index(self, collection: str, index_name: str,
                             extractor, scan_fn) -> None:
        """Register an index for EAGER auto-refresh."""
        if not _PRUNING_AVAILABLE or self.indexer is None:
            return
        self.indexer.register_eager_index(collection, index_name, extractor, scan_fn)

    def notify_write(self, collection: str) -> None:
        """Notify the indexer that a write has occurred on a collection.

        For EAGER indexes: refreshes immediately.
        For LAZY indexes: staleness accumulates (refreshed on next lookup).
        For MANUAL indexes: no-op.

        The lens should call this after each commit.
        """
        if not _PRUNING_AVAILABLE or self.indexer is None:
            return
        self.indexer.notify_write(collection)

    # ==================================================================
    # Compaction — clean up stale metadata after collection rewrites
    # ==================================================================

    def compact_zone_maps(self, collection: str,
                          current_scan_fn: Optional[Callable] = None) -> int:
        """Remove zone maps for data blobs that no longer exist in the collection.

        After an insert/merge, old data blobs are replaced by new ones.
        The old zone maps become stale (they point to unreachable blobs).
        This method removes them.

        Args:
            collection: collection name
            current_scan_fn: optional scan function to identify current
                data blob hashes. If None, reads the ProllyTreeIndex state.

        Returns:
            Number of stale zone maps removed.
        """
        if not _PRUNING_AVAILABLE or self.zm_index is None:
            return 0
        if not self.has_zone_maps(collection):
            return 0

        # Get current data blob hashes from the collection's ProllyTreeIndex
        base = ProllyLensBase(self.kernel, collection)
        current_state = base.read_all()
        current_blobs = set(h for k, h in current_state.items() if not k.startswith("_"))

        # Walk zone maps and find stale ones
        zm_base = self.zm_index._get_base(collection)
        zm_state = zm_base.read_all()
        stale_count = 0

        for zm_key, zm_blob_hash in list(zm_state.items()):
            if zm_key.startswith("_"):
                continue
            zm_dict = json.loads(self.kernel.read_blob(zm_blob_hash))
            data_blob_hash = zm_dict.get("blob_hash")
            if data_blob_hash and data_blob_hash not in current_blobs:
                zm_base.stage_delete(zm_key)
                stale_count += 1

        if stale_count > 0:
            zm_base.commit(f"compacted {stale_count} stale zone maps for {collection}")

        return stale_count
