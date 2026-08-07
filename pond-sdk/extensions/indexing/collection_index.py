"""
CollectionIndexer — collection-level indexing, independent of any lens.

MIGRATED from ProllyTree to UnifiedStorage. The index is now stored as
a JSON blob (index_key → rowid mapping) instead of a ProllyTree. This
removes the last dependency on prolly_tree.py.

The index is content-addressed (stored as kernel blobs) and referenced
from collections/{name}/indexes/{index_name}.

Usage:
    from collection_index import CollectionIndexer

    indexer = CollectionIndexer(kernel)
    indexer.build_index("users", "by_name",
                        extractor=lambda row: row.get("name", ""))
    rowid = indexer.lookup("users", "by_name", "alice")
"""
from __future__ import annotations

import json
import os
import sys
from typing import Optional, Any, Callable, Union, Iterator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "physical_structures"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal
from maintenance import drop_name, is_dropped, resolve_active, TOMBSTONE_HASH
from uuid7 import uuidv7

try:
    from .base import CollectionIndexerInterface
except ImportError:
    from base import CollectionIndexerInterface


class CollectionIndexer(CollectionIndexerInterface):
    """Collection-level indexer. Operates on any collection via the kernel.

    Indexes are stored as JSON blobs (index_key → rowid_hash mappings).
    This is simpler than ProllyTree and works with the unified architecture.

    INDEX MODES:
      - "manual" (default): caller explicitly calls build_index/refresh_index.
      - "lazy": index refreshed on lookup if stale.
      - "eager": index refreshed on every notify_write() call.
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel
        self._registered: dict[tuple[str, str], dict] = {}

    def register_lazy_index(self, collection: str, index_name: str,
                            extractor: Callable[[Any], Union[str, list[str]]],
                            scan_fn: Callable[[], Any],
                            staleness_budget: int = 5) -> None:
        self._registered[(collection, index_name)] = {
            "extractor": extractor, "scan_fn": scan_fn,
            "mode": "lazy", "staleness_budget": staleness_budget,
            "last_commit_index": self._get_commit_index(collection),
        }

    def register_eager_index(self, collection: str, index_name: str,
                             extractor: Callable[[Any], Union[str, list[str]]],
                             scan_fn: Callable[[], Any]) -> None:
        self._registered[(collection, index_name)] = {
            "extractor": extractor, "scan_fn": scan_fn,
            "mode": "eager", "staleness_budget": 0,
            "last_commit_index": self._get_commit_index(collection),
        }

    def notify_write(self, collection: str) -> None:
        for (coll, idx_name), config in self._registered.items():
            if coll != collection:
                continue
            if config["mode"] == "eager":
                self.refresh_index(coll, idx_name,
                                   config["extractor"], config["scan_fn"])
                config["last_commit_index"] = self._get_commit_index(collection)

    def _get_commit_index(self, collection: str) -> int:
        """Get the current commit index from the JSON commit blob."""
        try:
            head = self.kernel.resolve(
                f"collections/{collection}/_branches/main/commit")
            if head is None:
                return 0
            commit = json.loads(self.kernel.read_blob(head))
            return commit.get("index", 0)
        except (json.JSONDecodeError, UnicodeDecodeError, KeyError):
            return 0

    @staticmethod
    def _index_ref(collection: str, index_name: str) -> str:
        return f"collections/{collection}/indexes/{index_name}"

    def build_index(self, collection: str, index_name: str,
                    extractor: Callable[[Any], Union[str, list[str]]],
                    scan_rows: Callable[[], Any] = None) -> str:
        """Build an index on a collection.

        The index is stored as a single JSON blob: {index_key: rowid_string}.
        
        PREVIOUSLY (broken): wrote one kernel blob per rowid (N PUTs for N rows).
        At 100M rows this was ~5 days to build + $2,000/month in S3 storage.
        
        NOW (fixed): stores rowid strings directly in the index JSON.
        O(1) PUT for the entire index (one blob). O(1) GET on lookup
        (just read the JSON, no second blob read needed).
        """
        if scan_rows is None:
            scan_rows = self._default_scan_rows(collection)

        index_entries = {}
        for rowid, row_data in scan_rows():
            idx_keys = _extract_keys(extractor, row_data)
            for idx_key in idx_keys:
                index_entries[idx_key] = str(rowid)  # store directly, no separate blob

        # Store as a JSON blob (simple, debuggable, content-addressed)
        index_bytes = json.dumps(index_entries, sort_keys=True).encode()
        index_hash = self.kernel.write(index_bytes)
        self.kernel.reference(self._index_ref(collection, index_name), index_hash)
        return index_hash

    def drop_index(self, collection: str, index_name: str) -> bool:
        ref = self._index_ref(collection, index_name)
        current = self.kernel.resolve(ref)
        if not current or current == TOMBSTONE_HASH:
            return False
        drop_name(self.kernel, ref)
        return True

    def rebuild_index(self, collection: str, index_name: str,
                      extractor: Callable[[Any], Union[str, list[str]]],
                      scan_rows: Callable[[], Any] = None) -> str:
        return self.build_index(collection, index_name, extractor, scan_rows)

    def refresh_index(self, collection: str, index_name: str,
                      extractor: Callable[[Any], Union[str, list[str]]],
                      scan_rows: Callable[[], Any] = None) -> str:
        """Refresh an index — full rebuild (simple, correct)."""
        return self.build_index(collection, index_name, extractor, scan_rows)

    def is_index_stale(self, collection: str, index_name: str,
                       scan_rows: Callable[[], Any] = None,
                       extractor: Callable[[Any], Union[str, list[str]]] = None) -> bool:
        if scan_rows is None or extractor is None:
            return True
        ref = self._index_ref(collection, index_name)
        existing_root = resolve_active(self.kernel, ref)
        if existing_root is None or existing_root == TOMBSTONE_HASH:
            return True
        return self._get_commit_index(collection) > 0  # simplified check

    def lookup(self, collection: str, index_name: str,
               index_key: str) -> Optional[str]:
        """Look up a single _rowid by index key.
        
        O(1) GET: reads the index JSON and returns the rowid directly.
        (Previously: O(2) GETs — read index JSON, then read a separate
        rowid blob. Now: O(1) GET — rowid is stored in the index JSON.)
        """
        key = (collection, index_name)
        if key in self._registered:
            config = self._registered[key]
            if config["mode"] == "lazy":
                current_commit = self._get_commit_index(collection)
                staleness = current_commit - config["last_commit_index"]
                if staleness > config["staleness_budget"]:
                    self.refresh_index(collection, index_name,
                                       config["extractor"], config["scan_fn"])
                    config["last_commit_index"] = current_commit

        ref = self._index_ref(collection, index_name)
        index_hash = resolve_active(self.kernel, ref)
        if not index_hash:
            return None

        try:
            index_data = json.loads(self.kernel.read_blob(index_hash))
            return index_data.get(index_key)  # direct — no second blob read
        except (json.JSONDecodeError, KeyError):
            pass
        return None

    def lookup_all(self, collection: str, index_name: str,
                   index_key: str) -> list[str]:
        rowid = self.lookup(collection, index_name, index_key)
        return [rowid] if rowid else []

    def list_indexes(self, collection: str) -> list[str]:
        prefix = f"collections/{collection}/indexes/"
        return [n[len(prefix):] for n in self.kernel.list_names()
                if n.startswith(prefix) and not is_dropped(self.kernel, n)]

    def list_all_indexes(self, collection: str) -> list[str]:
        prefix = f"collections/{collection}/indexes/"
        return [n[len(prefix):] for n in self.kernel.list_names()
                if n.startswith(prefix)]

    def _default_scan_rows(self, collection: str):
        """Default scan using UnifiedStorage (replaces ProllyLensBase)."""
        try:
            from unified_storage import UnifiedStorage
            storage = UnifiedStorage(self.kernel)
            rows = storage.read_with_shards(collection)
            for row in rows:
                key_col = "_key"
                # Try to find a key column
                for col in ["_key", "id", "key", "path"]:
                    if col in row:
                        key_col = col
                        break
                rowid = row.get(key_col)
                if rowid is not None:
                    yield str(rowid), row
        except ImportError:
            pass  # UnifiedStorage not available


def _extract_keys(extractor, data) -> list[str]:
    result = extractor(data)
    if result is None:
        return []
    if isinstance(result, str):
        return [result]
    if isinstance(result, list):
        return result
    return []
