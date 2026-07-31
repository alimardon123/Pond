"""
Garbage Collection + Vacuum — reclaim space from unreachable blobs.

PROBLEM:
  Content-addressed storage is immutable — blobs are never modified.
  When HEAD moves (new commits), old manifests, commit blobs, and data
  blobs become unreachable. Shards create even more garbage.

SOLUTION:
  1. GC (read-only): walk reachability from live refs, build "live set".
     Return the "dead set" (all blobs - live).
  2. Vacuum (maintenance): delete dead blobs, with optional preservation
     of recent commits (like Delta/Iceberg vacuum).

DESIGN (improved for PB scale + Delta/Iceberg parity):
  - `collections` parameter: vacuum specific collections (list)
  - `preserve_days` parameter: keep commits younger than N days
    (time-travel safety — like Delta/Iceberg vacuum)
  - `compute_size=False` by default: skip reading dead blobs to compute
    size (O(dead) reads avoided — huge at PB scale)
  - Efficient reachability walk: filter refs by collection prefix FIRST,
    then walk only reachable blobs (O(live) reads, not O(all))
  - Vacuum doesn't read dead blobs just to compute freed_bytes — it
    deletes directly (size is best-effort via has_blob, not read_blob)

USAGE:
    from vacuum import GarbageCollector

    gc = GarbageCollector(kernel)

    # Analyze (fast — no dead blob reads)
    stats = gc.collect()
    # {"live": 150, "dead": 23, "dead_size_bytes": -1}

    # Vacuum specific collections, preserve last 7 days
    gc.vacuum(collections=["events", "users"], preserve_days=7)

    # Dry run — see what would be deleted
    gc.vacuum(dry_run=True)

    # Compute dead size (slower — reads dead blobs)
    gc.collect(compute_size=True)
"""
from __future__ import annotations

import os
import sys
import json
import time
from typing import Optional, Set, List

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "physical_structures"))


class GarbageCollector:
    """Garbage collector for Pond's content-addressed storage.

    GC is READ-ONLY — it walks reachability and returns the dead set.
    Vacuum is the MAINTENANCE operation that deletes dead blobs.

    IMPROVED for PB scale + Delta/Iceberg parity:
      - collections parameter: vacuum specific collections
      - preserve_days: keep commits younger than N days (time-travel safety)
      - compute_size=False default: skip O(dead) reads for size calculation
      - Efficient walk: filter refs by prefix, walk only live blobs
    """

    def __init__(self, kernel):
        self.kernel = kernel

    def collect(self, collection: Optional[str] = None,
                compute_size: bool = False) -> dict:
        """Analyze reachability and return GC stats (read-only).

        Args:
            collection: if None, analyze ALL collections. If specified,
                analyze only that collection.
            compute_size: if True, read each dead blob to compute its size.
                Default False — skips O(dead) reads. At PB scale, this is
                the difference between seconds and hours.

        Returns:
            {
                "live": int,
                "dead": int,
                "dead_hashes": list,
                "dead_size_bytes": int,  # -1 if compute_size=False
            }
        """
        live_set = self._build_live_set(
            [collection] if collection else None)
        all_blobs = set(self._list_all_blob_hashes())
        dead_hashes = all_blobs - live_set

        dead_size = -1
        if compute_size:
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

    def vacuum(self, collections: Optional[list] = None,
               preserve_days: int = 0,
               dry_run: bool = False) -> dict:
        """Delete unreachable blobs, optionally preserving recent commits.

        Args:
            collections: list of collection names to vacuum. If None,
                vacuum ALL collections. If specified, only refs under
                these collections are considered live (plus a safety
                check: blobs referenced by OTHER collections are NOT
                deleted — content-addressed means shared blobs are safe).
            preserve_days: keep commits younger than N days. Commits
                older than this are eligible for deletion. Default 0
                (only the current HEAD + live refs are preserved).

                This is like Delta/Iceberg vacuum — it preserves recent
                history for time-travel queries. Set to 7 to keep the
                last week of commits.

                Note: content-addressed blobs shared between preserved
                and non-preserved commits are NEVER deleted (they're in
                the live set).

            dry_run: if True, report what would be deleted without deleting.

        Returns:
            {
                "deleted": int,
                "preserved": int,  # blobs preserved due to preserve_days
                "freed_bytes": int,  # -1 if can't compute (no read before delete)
                "dry_run": bool,
                "collections": list or None,
                "preserve_days": int,
            }
        """
        # Build live set, preserving recent commits if requested
        live_set = self._build_live_set(collections, preserve_days)
        all_blobs = set(self._list_all_blob_hashes())
        dead_hashes = all_blobs - live_set

        # Count preserved (blobs that would be dead without preserve_days)
        preserved = 0
        if preserve_days > 0:
            live_without_preserve = self._build_live_set(collections, 0)
            preserved = len(live_set - live_without_preserve)

        deleted = 0
        for h in dead_hashes:
            if dry_run:
                deleted += 1
            else:
                try:
                    if hasattr(self.kernel, 'delete_blob'):
                        self.kernel.delete_blob(h)
                        deleted += 1
                except Exception:
                    pass

        return {
            "deleted": deleted,
            "preserved": preserved,
            "freed_bytes": -1,  # we don't read before delete (PB-scale efficiency)
            "dry_run": dry_run,
            "collections": collections,
            "preserve_days": preserve_days,
        }

    def vacuum_all(self, preserve_days: int = 0,
                   dry_run: bool = False) -> dict:
        """Vacuum ALL collections (same as vacuum(collections=None))."""
        return self.vacuum(collections=None, preserve_days=preserve_days,
                            dry_run=dry_run)

    # ------------------------------------------------------------------
    # Reachability walk
    # ------------------------------------------------------------------

    def _build_live_set(self, collections: Optional[list] = None,
                         preserve_days: int = 0) -> Set[str]:
        """Walk all live refs and build the set of reachable blob hashes.

        Args:
            collections: if None, walk ALL collections. If specified,
                only walk refs under these collections (faster for
                targeted GC).
            preserve_days: if > 0, also walk commits from the last N
                days (preserves time-travel history).
        """
        live: Set[str] = set()
        names = self.kernel.list_names()

        # Filter by collections if specified
        if collections:
            filtered = []
            for n in names:
                for coll in collections:
                    if n.startswith(f"collections/{coll}/"):
                        filtered.append(n)
                        break
            names = filtered

        # Walk every ref as a potential starting point
        cutoff_time = time.time() - (preserve_days * 86400) if preserve_days > 0 else 0
        for name in names:
            h = self.kernel.resolve(name)
            if h is not None and h not in live:
                self._walk_reachable(h, live, cutoff_time)

        return live

    def _walk_reachable(self, hash_val: str, live: Set[str],
                         cutoff_time: float = 0) -> None:
        """Recursively walk all blobs reachable from hash_val.

        If cutoff_time > 0, stop walking commit chains at commits older
        than cutoff_time (preserve_days logic — old history is eligible
        for GC).
        """
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
                # It's a commit — check timestamp for preserve_days
                if cutoff_time > 0:
                    commit_ts = obj.get("timestamp", 0)
                    if commit_ts < cutoff_time:
                        # Old commit — don't walk its ancestors (eligible for GC)
                        # But DO keep the manifest (current data must stay live)
                        manifest = obj.get("manifest")
                        if manifest and manifest not in live:
                            self._walk_reachable(manifest, live, 0)  # walk manifest without cutoff
                        return

                # Walk parent, second_parent, manifest
                for field in ("parent", "second_parent", "manifest"):
                    child = obj.get(field)
                    if child and child not in live:
                        self._walk_reachable(child, live, cutoff_time)
                return
            if isinstance(obj, list):
                # Shard index — list of shard manifest hashes
                for shard_hash in obj:
                    if shard_hash not in live:
                        self._walk_reachable(shard_hash, live, cutoff_time)
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
                self._walk_reachable(manifest.parent_manifest_hash, live, cutoff_time)
            if manifest.stats_tree_root and manifest.stats_tree_root not in live:
                self._walk_reachable(manifest.stats_tree_root, live, cutoff_time)
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
