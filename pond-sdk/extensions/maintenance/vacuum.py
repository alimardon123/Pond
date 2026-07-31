"""
Garbage Collection + Vacuum — reclaim space from unreachable blobs.

PROBLEM:
  Content-addressed storage is immutable — blobs are never modified.
  But when HEAD moves (new commits), old manifests, commit blobs, and
  data blobs become unreachable. Shards create even more garbage.

SOLUTION:
  1. GC (read-only): walk reachability from all live refs, build the
     "live set" of blob hashes. Return the "dead set" (all - live).
  2. Vacuum (maintenance): delete dead blobs from the store.

  The kernel stays FROZEN — deletion is a storage-backend concern.
"""
from __future__ import annotations

import os
import sys
import json
from typing import Optional, Set

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "physical_structures"))


class GarbageCollector:
    """Garbage collector for Pond's content-addressed storage.

    GC is READ-ONLY — it walks reachability and returns the dead set.
    Vacuum is the MAINTENANCE operation that actually deletes dead blobs.
    """

    def __init__(self, kernel):
        self.kernel = kernel

    def collect(self, collection: Optional[str] = None) -> dict:
        """Analyze reachability and return GC stats.

        Args:
            collection: if None, analyze ALL collections. If specified,
                analyze only that collection (faster for targeted GC).

        Returns:
            {"live": int, "dead": int, "dead_hashes": list, "dead_size_bytes": int}
        """
        live_set = self._build_live_set(collection)
        all_blobs = set(self._list_all_blob_hashes())
        dead_hashes = all_blobs - live_set

        dead_size = 0
        for h in dead_hashes:
            try:
                data = self.kernel.read_blob(h)
                dead_size += len(data)
            except Exception:
                pass

        return {
            "live": len(live_set),
            "dead": len(dead_hashes),
            "dead_hashes": sorted(dead_hashes),
            "dead_size_bytes": dead_size,
        }

    def vacuum(self, collection: Optional[str] = None,
               dry_run: bool = False) -> dict:
        """Delete unreachable blobs.

        Args:
            collection: if None, vacuum ALL collections. If specified,
                vacuum only that collection.
            dry_run: if True, report what would be deleted without deleting.

        Returns:
            {"deleted": int, "freed_bytes": int, "dry_run": bool}
        """
        stats = self.collect(collection)
        deleted = 0
        freed = 0

        for h in stats["dead_hashes"]:
            if dry_run:
                deleted += 1
            else:
                try:
                    data = self.kernel.read_blob(h)
                    freed += len(data)
                    if hasattr(self.kernel, 'delete_blob'):
                        self.kernel.delete_blob(h)
                        deleted += 1
                except Exception:
                    pass

        return {
            "deleted": deleted,
            "freed_bytes": freed,
            "dry_run": dry_run,
        }

    def vacuum_all(self, dry_run: bool = False) -> dict:
        """Vacuum ALL collections (same as vacuum(collection=None))."""
        return self.vacuum(collection=None, dry_run=dry_run)

    # ------------------------------------------------------------------
    # Reachability walk
    # ------------------------------------------------------------------

    def _build_live_set(self, collection: Optional[str] = None) -> Set[str]:
        """Walk all live refs and build the set of reachable blob hashes."""
        live: Set[str] = set()
        names = self.kernel.list_names()

        if collection:
            prefix = f"collections/{collection}/"
            names = [n for n in names if n.startswith(prefix)]

        for name in names:
            h = self.kernel.resolve(name)
            if h is not None and h not in live:
                self._walk_reachable(h, live)

        return live

    def _walk_reachable(self, hash_val: str, live: Set[str]) -> None:
        """Recursively walk all blobs reachable from hash_val."""
        if hash_val in live:
            return
        live.add(hash_val)

        try:
            data = self.kernel.read_blob(hash_val)
        except Exception:
            return

        # Try as JSON commit blob or shard index
        try:
            obj = json.loads(data)
            if isinstance(obj, dict) and "manifest" in obj:
                # It's a commit — walk parent, second_parent, manifest
                for field in ("parent", "second_parent", "manifest"):
                    child = obj.get(field)
                    if child and child not in live:
                        self._walk_reachable(child, live)
                return
            if isinstance(obj, list):
                # It's a shard index — list of shard manifest hashes
                for shard_hash in obj:
                    if shard_hash not in live:
                        self._walk_reachable(shard_hash, live)
                return
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        # Try as CollectionManifest (binary format)
        try:
            from collection_manifest import CollectionManifest
            manifest = CollectionManifest.load(self.kernel, hash_val)
            for rg in manifest.scan_with_pruning():
                if rg.blob_hash and rg.blob_hash not in live:
                    live.add(rg.blob_hash)
            if manifest.parent_manifest_hash and manifest.parent_manifest_hash not in live:
                self._walk_reachable(manifest.parent_manifest_hash, live)
            if manifest.stats_tree_root and manifest.stats_tree_root not in live:
                self._walk_reachable(manifest.stats_tree_root, live)
            return
        except (ValueError, KeyError, ImportError):
            pass

        # Leaf node (data blob, stats tree node, etc.)
        return

    def _list_all_blob_hashes(self) -> list[str]:
        """List all blob hashes in the store."""
        if hasattr(self.kernel, 'list_all_blob_hashes'):
            return self.kernel.list_all_blob_hashes()
        return []
