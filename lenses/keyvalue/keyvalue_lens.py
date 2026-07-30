"""
KeyValueLens — the app-facing KEY-VALUE lens.

This is one of three peer app-facing lenses in Pond:
  - KeyValueLens  (this file)        — per-row key→blob storage over ProllyTreeIndex
  - LakehouseLens (lenses/lakehouse)  — whole-table Parquet I/O + range read/write
  - FeatureStoreLens (pond-labs)      — versioned ML feature store on Parquet

All three extend PondLens (base_lens.py), the thin shared-namespace base.
PondLens provides only ref-namespace operations (branch, list_collections,
set_definition, get_definition, history) — no format awareness. Each
app-facing lens owns its OWN read/write API.

COLLECTION-AGNOSTIC: KeyValueLens is a STATELESS read/write engine. It does
NOT bind to a single collection in __init__. You pass the collection name to
each operation:

    lens = KeyValueLens(kernel)
    lens.put("users", "user:1", {"name": "alice"})
    lens.get("users", "user:1")
    lens.commit("users", "msg")

This matches LakehouseLens's API (create_table(name, data), read_table(name)).
The same lens instance can operate on ANY collection.

KeyValueLens stores each row as a single blob in the ProllyTreeIndex,
keyed by a user-supplied primary key (or auto-generated UUIDv7 for
KeylessLens). This makes it suitable for:
  - OLTP workloads (point lookups via Prolly tree, O(log N))
  - Streaming/event logs (KeylessLens variant with auto-UUIDv7 keys)
  - Document storage (each blob is a JSON document)
  - Cross-lens blob sharing (CrossLens helpers below)

Backward-compat: the old API `KeyValueLens(kernel, name)` still works via
a compatibility wrapper that binds to a single collection. New code should
use the collection-agnostic API.

Backward-compat aliases (defined at the END of this file):
  Lens = KeyValueLens  (old name, kept for compat)
  View = KeyValueLens  (older name, kept for compat)
"""

from __future__ import annotations

import json
import time
import sys
import os
import hashlib
import uuid
from typing import Optional, Any, Callable, Union

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "pond-sdk"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kernel import PondMinimal
from prolly_tree import ProllyLensBase, ProllyTree
from binary_encoding import BinaryProllyTree
from maintenance import (drop_name, is_dropped, resolve_active,
                         TOMBSTONE_HASH)
from row_query import LensQuery
from base_lens import PondLens
from uuid7 import uuidv7


# ===========================================================================
# KeyValueLens — the app-facing key-value lens (collection-agnostic)
# ===========================================================================

class KeyValueLens(PondLens):
    """App-facing KEY-VALUE lens with ProllyTreeIndex backing.

    COLLECTION-AGNOSTIC: This lens is a stateless read/write engine. It
    does NOT bind to a single collection. Pass the collection name to
    each operation:

        lens = KeyValueLens(kernel)
        lens.put("users", "user:1", {"name": "alice"})
        lens.commit("users", "insert alice")
        lens.get("users", "user:1")  # → {"name": "alice"}

    The same lens instance can operate on ANY collection. This matches
    LakehouseLens's API design.

    Key-value operations (all take collection as first arg):
      - put(collection, key, data): stage a key→blob mapping
      - put_auto(collection, data): stage with auto-generated UUIDv7 key
      - get(collection, key): read a single value by key (O(log N))
      - get_raw(collection, key): read raw bytes (no decode)
      - delete(collection, key): stage a deletion
      - commit(collection, message): atomically commit all staged changes
      - keys(collection), count(collection), exists(collection, key)
      - get_all(collection)

    Lazy query API:
      - where(collection, predicate=None, **kwargs)
      - select(collection, *fields)
      - map(collection, fn)
      - join(collection, other, on='field')

    Version control (delegated to ProllyLensBase):
      - branch(collection, branch_name), checkout(collection, branch_name)
      - list_branches(collection)
      - merge(collection, branch_name) [union merge with 2-parent commit]
      - undo(collection, steps), history(collection, limit)
      - diff(collection, a, b)
    """

    def __init__(self, kernel: PondMinimal, name: Optional[str] = None,
                 use_unified_storage: bool = False):
        """Create a KeyValueLens.

        Args:
            kernel: the PondMinimal kernel instance
            name: OPTIONAL. If provided, enables backward-compatible
                  single-collection API.
            use_unified_storage: if True, use UnifiedStorage (PND2 format)
                  as the storage backend instead of the legacy ProllyTreeIndex
                  + per-key JSON blobs. This gives:
                    - 4 GETs cold point lookup (vs O(log N))
                    - Non-destructive append() (vs rewrite-on-commit)
                    - Predicate pushdown via manifest stats
                    - Range scans via start_key/end_key
                  The lens API is IDENTICAL — only the storage backend changes.
                  Default: False (legacy path, for backward compat).
        """
        super().__init__(kernel)
        self._bases: dict[str, ProllyLensBase] = {}
        self._default_collection = name
        if name is not None:
            self.name = name
        # Attached indexer for auto-notify on commit (set via attach_indexer)
        self._attached_indexer = None

        # Unified storage backend (optional)
        self._unified_storage = None
        if use_unified_storage:
            try:
                from unified_storage import UnifiedStorage
                self._unified_storage = UnifiedStorage(kernel)
            except ImportError:
                # UnifiedStorage not available — fall back to legacy
                pass
        # Unified storage write buffer: collection → {key → value}
        self._unified_buffer: dict[str, dict[str, Any]] = {}

    def _resolve_collection(self, *args) -> tuple:
        """Resolve the collection name from args or default.

        If _default_collection is set (backward compat mode), the first
        arg is NOT the collection — it's the key. We prepend the default.
        Otherwise, the first arg IS the collection.

        Returns (collection, remaining_args).
        """
        if self._default_collection is not None:
            return self._default_collection, args
        else:
            if not args:
                raise TypeError("Collection name required (lens is not bound to a default collection)")
            return args[0], args[1:]

    def _get_base(self, collection: str) -> ProllyLensBase:
        """Get or create the ProllyLensBase for a collection.

        ProllyLensBase holds per-collection staging state (_staged_add,
        _staged_del). We cache one instance per collection so staged
        changes persist across calls until commit.
        """
        if collection not in self._bases:
            self._bases[collection] = ProllyLensBase(self.kernel, collection)
        return self._bases[collection]

    @property
    def base(self) -> ProllyLensBase:
        """Backward compat: return the ProllyLensBase for the default collection.

        New code should use _get_base(collection) instead.
        """
        if self._default_collection is None:
            raise TypeError("lens is not bound to a default collection; use _get_base(collection)")
        return self._get_base(self._default_collection)

    # --- Write path ---

    def put(self, *args) -> str:
        """Stage a key→blob mapping.

        Collection-agnostic API: put(collection, key, data)
        Backward compat API:    put(key, data)  [requires name in __init__]
        """
        collection, rest = self._resolve_collection(*args)
        key, data = rest[0], rest[1]

        # Unified storage path: buffer the put, commit later
        if self._unified_storage is not None:
            if collection not in self._unified_buffer:
                self._unified_buffer[collection] = {}
            self._unified_buffer[collection][key] = data
            return key  # placeholder — real hash assigned at commit

        # Legacy path: write blob immediately, stage in ProllyTreeIndex
        blob_hash = self.kernel.write(self.encode(data))
        self._get_base(collection).stage(key, blob_hash)
        return blob_hash

    def put_auto(self, *args) -> str:
        """Stage data with an auto-generated UUIDv7 key. Returns the key.

        Collection-agnostic API: put_auto(collection, data)
        Backward compat API:    put_auto(data)  [requires name in __init__]
        """
        collection, rest = self._resolve_collection(*args)
        data = rest[0]
        key = uuidv7()
        blob_hash = self.kernel.write(self.encode(data))
        self._get_base(collection).stage(key, blob_hash)
        return key

    def put_raw(self, *args) -> None:
        """Stage a pre-existing blob hash under the given key.

        Collection-agnostic API: put_raw(collection, key, blob_hash)
        Backward compat API:    put_raw(key, blob_hash)  [requires name in __init__]
        """
        collection, rest = self._resolve_collection(*args)
        key, blob_hash = rest[0], rest[1]
        self._get_base(collection).stage(key, blob_hash)

    def delete(self, *args) -> None:
        """Stage a deletion for the given key.

        Collection-agnostic API: delete(collection, key)
        Backward compat API:    delete(key)  [requires name in __init__]

        Fix (Round 16 Issue #2): in unified mode, stage the delete by
        marking the key in the buffer with a tombstone sentinel.
        commit() then skips tombstoned keys when writing.
        """
        collection, rest = self._resolve_collection(*args)
        key = rest[0]

        # Unified storage path: mark key for deletion in the buffer
        if self._unified_storage is not None:
            if collection not in self._unified_buffer:
                self._unified_buffer[collection] = {}
            # Tombstone: set value to None (commit skips None values)
            self._unified_buffer[collection][key] = None
            return

        # Legacy path: stage delete on ProllyLensBase
        self._get_base(collection).stage_delete(key)

    def commit(self, *args) -> str:
        """Atomically commit all staged changes for the collection.

        Collection-agnostic API: commit(collection, message="")
        Backward compat API:    commit(message="")  [requires name in __init__]

        After committing, if any indexers are registered (via
        CollectionMetadata.register_eager_index), they are notified
        via notify_write(). This enables EAGER index auto-refresh
        without coupling the lens to the indexer.
        """
        collection, rest = self._resolve_collection(*args)
        message = rest[0] if rest else ""

        # Unified storage path: flush buffer via UnifiedStorage
        if self._unified_storage is not None:
            if collection not in self._unified_buffer:
                raise ValueError(f"No staged data for collection '{collection}'")
            buffer = self._unified_buffer[collection]

            # Fix (Round 17 Issue #1): deletes must actually remove data.
            # UnifiedStorage.append() only adds — it can't delete old keys.
            # If there are tombstones (value=None), we must do a full rewrite:
            # read all existing data, remove deleted keys, add new puts,
            # write the result via write() (overwrite).
            has_deletes = any(v is None for v in buffer.values())
            puts_only = {k: v for k, v in buffer.items() if v is not None}

            if has_deletes:
                # Full rewrite: read existing data, apply deletes + puts
                existing_rows = self._unified_storage.read(collection,
                                                             columns=["_key", "value"])
                deleted_keys = {k for k, v in buffer.items() if v is None}
                # Keep existing rows that aren't deleted and aren't being overwritten
                result_rows = []
                for row in existing_rows:
                    if row["_key"] not in deleted_keys and row["_key"] not in puts_only:
                        result_rows.append(row)
                # Add new puts
                for k, v in puts_only.items():
                    result_rows.append({"_key": k, "value": self.encode(v)})
                result_rows.sort(key=lambda r: r["_key"])
                commit_hash = self._unified_storage.write(
                    collection, result_rows, key_col="_key",
                    row_group_size=10_000,
                    message=message or f"{collection} unified commit (with deletes)")
            elif puts_only:
                # No deletes — just append new puts
                rows = [{"_key": k, "value": self.encode(v)}
                         for k, v in puts_only.items()]
                rows.sort(key=lambda r: r["_key"])
                commit_hash = self._unified_storage.append(
                    collection, rows, key_col="_key",
                    row_group_size=10_000,
                    message=message or f"{collection} unified commit")
            else:
                commit_hash = ""
            del self._unified_buffer[collection]
            return commit_hash

        # Legacy path: ProllyTreeIndex commit
        commit_hash = self._get_base(collection).commit(message or f"{collection} commit")

        # Notify attached indexer (EAGER mode auto-refresh)
        # This is a no-op if no indexer is attached.
        if self._attached_indexer is not None:
            try:
                self._attached_indexer.notify_write(collection)
            except Exception:
                pass  # indexer notification is best-effort

        return commit_hash

    def attach_indexer(self, indexer) -> None:
        """Attach a CollectionMetadata or CollectionIndexer for auto-notify.

        After attaching, every commit() call will automatically notify
        the indexer (triggering EAGER refresh or LAZY staleness increment).

        Usage:
            meta = CollectionMetadata(kernel)
            meta.register_eager_index('users', 'by_name', extractor, scan_fn)
            lens.attach_indexer(meta)
            # Now every lens.commit('users', ...) auto-refreshes EAGER indexes
        """
        self._attached_indexer = indexer

    def build_zone_maps(self, *args) -> None:
        """Build zone maps for a KV collection (explicit, not auto).

        Collection-agnostic API: build_zone_maps(collection)
        Backward compat API:    build_zone_maps()  [uses default collection]

        Zone maps are NOT built automatically for KV commits because KV
        entries are individual blobs (1 zone map per blob = 2x writes).
        Instead, call this method explicitly when you want pruning support.

        For LakehouseLens, zone maps ARE auto-built (1 zone map per row
        group of 10K rows = negligible overhead).
        """
        collection, rest = self._resolve_collection(*args)
        try:
            from collection_metadata import CollectionMetadata
            from pruning import ZoneMap
        except ImportError:
            return  # pruning extension not available

        meta = CollectionMetadata(self.kernel)
        zm_index = meta.zm_index
        if zm_index is None:
            return
        base = self._get_base(collection)
        state = base.read_all()
        had_changes = False

        for key, blob_hash in state.items():
            if key.startswith("_"):
                continue
            rg_key = f"kv/{key}"
            existing_zm = zm_index.get_zone_map(collection, rg_key)
            if existing_zm is not None:
                continue
            try:
                raw = self.kernel.read_blob(blob_hash)
                row = json.loads(raw)
                if not isinstance(row, dict):
                    continue
                zm = ZoneMap(row_count=1)
                for col, val in row.items():
                    if val is None:
                        zm.null_count[col] = 1
                    elif isinstance(val, (int, float, str)):
                        zm.min[col] = val
                        zm.max[col] = val
                        zm.null_count[col] = 0
                if zm.min:
                    zm_index.add_zone_map(collection, rg_key, zm, blob_hash)
                    had_changes = True
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

        if had_changes:
            zm_index.commit_zone_maps(collection, f"zone maps for {collection}")

    # --- Read path ---

    def get(self, *args) -> Optional[Any]:
        """Read a single value by key. O(log N) via ProllyTreeIndex.

        Collection-agnostic API: get(collection, key)
        Backward compat API:    get(key)  [requires name in __init__]

        Unified storage path (use_unified_storage=True): 4 GETs cold
        point lookup via manifest + encoded predicate eval.
        """
        collection, rest = self._resolve_collection(*args)
        key = rest[0]

        # Unified storage path: point_lookup via manifest
        if self._unified_storage is not None:
            row = self._unified_storage.point_lookup(
                collection, key=key, columns=["_key", "value"])
            if row is None:
                return None
            return self.decode(row["value"])

        # Legacy path: ProllyTreeIndex lookup
        h = self._get_base(collection).lookup(key)
        return self.decode(self.kernel.read_blob(h)) if h else None

    def get_raw(self, *args) -> Optional[bytes]:
        """Read raw bytes by key (no decode)."""
        collection, rest = self._resolve_collection(*args)
        key = rest[0]
        h = self._get_base(collection).lookup(key)
        return self.kernel.read_blob(h) if h else None

    def get_all(self, *args) -> dict[str, Any]:
        """Read all key→value pairs from the collection."""
        collection, rest = self._resolve_collection(*args)

        # Unified storage path
        if self._unified_storage is not None:
            rows = self._unified_storage.read(collection,
                                                columns=["_key", "value"])
            return {r["_key"]: self.decode(r["value"])
                    for r in rows if r.get("_key")}

        # Legacy path
        state = self._get_base(collection).read_all()
        return {k: self.decode(self.kernel.read_blob(h))
                for k, h in state.items() if not k.startswith("_")}

    def keys(self, *args) -> list[str]:
        """List all user keys in the collection (excludes internal _ keys)."""
        collection, rest = self._resolve_collection(*args)

        # Unified storage path
        if self._unified_storage is not None:
            rows = self._unified_storage.read(collection, columns=["_key"])
            return [r["_key"] for r in rows if r.get("_key")]

        # Legacy path
        return [k for k in self._get_base(collection).read_all() if not k.startswith("_")]

    def exists(self, *args) -> bool:
        """Check if a key exists in the collection."""
        collection, rest = self._resolve_collection(*args)
        key = rest[0]

        # Unified storage path
        if self._unified_storage is not None:
            row = self._unified_storage.point_lookup(
                collection, key=key, columns=["_key"])
            return row is not None and row.get("_key") is not None

        # Legacy path
        return self._get_base(collection).lookup(key) is not None

    def count(self, *args) -> int:
        """Count user keys in the collection."""
        collection, rest = self._resolve_collection(*args)

        # Unified storage path
        if self._unified_storage is not None:
            return len(self.keys(collection))

        # Legacy path
        return sum(1 for k in self._get_base(collection).read_all() if not k.startswith("_"))

    # ------------------------------------------------------------------
    # Collection-like API — make a collection feel like an iterable of rows.
    # Uses the LensQuery lazy query API (row_query.py).
    # ------------------------------------------------------------------

    def iterate(self, *args):
        """Iterate over decoded rows in the collection.

        Collection-agnostic: iterate(collection)
        Backward compat:    iterate()  [uses default collection]
        """
        collection, rest = self._resolve_collection(*args)

        # Unified storage path: read all rows via manifest
        if self._unified_storage is not None:
            rows = self._unified_storage.read(collection,
                                                columns=["_key", "value"])
            for row in rows:
                yield self.decode(row["value"])
            return

        # Legacy path: ProllyTreeIndex
        # Fix (Round 22): call _get_base directly to avoid _resolve_collection
        # misinterpreting the key as the collection in bound mode.
        base = self._get_base(collection)
        for key in self.keys(collection):
            h = base.lookup(key)
            if h:
                row = self.decode(self.kernel.read_blob(h))
                if row is not None:
                    yield row

    def __iter__(self):
        """Backward compat: iterate over default collection."""
        if self._default_collection is None:
            raise TypeError("lens is not bound to a default collection; use iterate(collection)")
        # Fix (Round 22): pass NO args — _resolve_collection will use
        # _default_collection automatically. Passing it explicitly causes
        # it to be treated as a key by _resolve_collection.
        return self.iterate()

    def __len__(self):
        """Backward compat: len(lens) == lens.count()."""
        if self._default_collection is None:
            raise TypeError("lens is not bound to a default collection; use count(collection)")
        return self.count()

    def __contains__(self, key: str):
        """Backward compat: key in lens == lens.exists(key)."""
        if self._default_collection is None:
            raise TypeError("lens is not bound to a default collection; use exists(collection, key)")
        # Fix (Round 22): pass only the key — _resolve_collection will
        # use _default_collection automatically.
        return self.exists(key)

    def where(self, *args, **kwargs) -> LensQuery:
        """Start a lazy query that filters rows.

        Collection-agnostic: where(collection, predicate=None, **kwargs)
        Backward compat:    where(predicate=None, **kwargs)  [uses default]
        """
        collection, rest = self._resolve_collection(*args)
        adapter = _CollectionAdapter(self, collection)
        # rest may contain the predicate if in compat mode
        if rest and callable(rest[0]):
            return LensQuery(adapter).where(rest[0], **kwargs)
        elif rest and isinstance(rest[0], dict):
            return LensQuery(adapter).where(rest[0], **kwargs)
        else:
            return LensQuery(adapter).where(**kwargs)

    def select(self, *args) -> LensQuery:
        """Start a lazy query that projects rows to only these fields."""
        collection, rest = self._resolve_collection(*args)
        adapter = _CollectionAdapter(self, collection)
        return LensQuery(adapter).select(*rest)

    def map(self, *args) -> LensQuery:
        """Start a lazy query that transforms each row via fn(row)."""
        collection, rest = self._resolve_collection(*args)
        adapter = _CollectionAdapter(self, collection)
        return LensQuery(adapter).map(rest[0])

    def join(self, *args):
        """JOIN this collection with another collection or query."""
        collection, rest = self._resolve_collection(*args)
        adapter = _CollectionAdapter(self, collection)
        return LensQuery(adapter).join(rest[0], rest[1])

    # --- Pruning-accelerated read (Vortex-style predicate pushdown) ---

    def read_with_pruning(self, *args, **kwargs):
        """Scan a collection with Vortex-style predicate pushdown.

        Collection-agnostic API: read_with_pruning(collection, predicates=None, row_filter=None)
        Backward compat API:    read_with_pruning(predicates=None, row_filter=None)

        Reads zone maps first (small, cheap), evaluates the pruning
        predicate, and only fetches + decodes data blobs that MIGHT match.
        Skips blobs whose zone maps prove they can't match — WITHOUT
        reading or decoding the data blob.

        Args:
            collection: collection name (or omitted if using default)
            predicates: list of (column, op, value) tuples for pruning.
                Example: [("age", ">", 30), ("region", "=", "US")]
                All predicates are ANDed together.
                If None, no pruning (reads all blobs).
            row_filter: optional function(row_dict) -> bool for exact
                row-level filtering after pruning.

        Yields:
            Rows (dicts) from non-pruned blobs (optionally filtered).
        """
        collection, rest = self._resolve_collection(*args)
        predicates = rest[0] if len(rest) > 0 else kwargs.get("predicates")
        row_filter = rest[1] if len(rest) > 1 else kwargs.get("row_filter")

        try:
            from collection_metadata import CollectionMetadata
            from pruning import PruningPredicate, ColumnPredicate
            from pruning_reader import PruningReader
            meta = CollectionMetadata(self.kernel)
            zm_index = meta.zm_index
        except ImportError:
            zm_index = None

        if zm_index is None or not zm_index.has_zone_maps(collection):
            # No pruning extension or no zone maps — fall back to full scan
            for row in self.iterate(collection):
                if row_filter is None or row_filter(row):
                    yield row
            return

        # Build pruning predicate
        predicate = None
        if predicates:
            col_preds = [ColumnPredicate(column=c, op=o, value=v)
                         for c, o, v in predicates]
            predicate = PruningPredicate(col_preds, combine="and")

        reader = PruningReader(self.kernel, zm_index, collection, predicate)

        # Decode function: JSON bytes → row dict
        for row in reader.scan(decode_fn=self.decode, row_filter=row_filter):
            yield row

    # --- Version control ---

    def branch(self, *args) -> str:
        """Create a branch on the collection. O(1) — just a ref copy."""
        collection, rest = self._resolve_collection(*args)
        return self._get_base(collection).branch(rest[0])

    def checkout(self, *args) -> None:
        """Checkout a branch on the collection."""
        collection, rest = self._resolve_collection(*args)
        self._get_base(collection).checkout(rest[0])

    def list_branches(self, *args) -> list[str]:
        """List all branches on the collection."""
        collection, rest = self._resolve_collection(*args)
        return self._get_base(collection).list_branches()

    def merge(self, *args) -> str:
        """Merge a branch into the collection's HEAD. Union merge with 2-parent commit."""
        collection, rest = self._resolve_collection(*args)
        return self._get_base(collection).merge(rest[0])

    def undo(self, *args) -> str:
        """Undo the last N commits on the collection."""
        collection, rest = self._resolve_collection(*args)
        steps = rest[0] if rest else 1
        return self._get_base(collection).undo(steps)

    def history(self, *args) -> list[dict]:
        """Walk the commit chain for the collection."""
        collection, rest = self._resolve_collection(*args)
        limit = rest[0] if rest else 100
        return self._get_base(collection).history(limit)

    def diff(self, *args) -> dict:
        """Diff two commits on the collection."""
        collection, rest = self._resolve_collection(*args)
        return self._get_base(collection).diff(rest[0], rest[1])

    # --- Serialization (override in subclass) ---

    def encode(self, data: Any) -> bytes:
        # Fix (Round 24 Issue #4): handle raw bytes natively for git blobs,
        # notebook attachments, video segments, etc.
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        return json.dumps(data, sort_keys=True).encode()

    def decode(self, data: bytes) -> Any:
        # Fix (Round 24 Issue #4): return raw bytes if not JSON
        try:
            return json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            return bytes(data)


# ---------------------------------------------------------------------------
# _CollectionAdapter — adapts a (KeyValueLens, collection) pair to the
# LensQuery interface (keys()/get()). This lets LensQuery work with the
# collection-agnostic lens API.
# ---------------------------------------------------------------------------

class _CollectionAdapter:
    """Adapter that exposes keys()/get() for a specific collection.

    LensQuery uses duck-typing (hasattr source 'keys' and 'get'). This
    adapter wraps a (lens, collection) pair to provide those methods.
    """

    def __init__(self, lens: KeyValueLens, collection: str):
        self._lens = lens
        self._collection = collection

    def keys(self) -> list[str]:
        return self._lens.keys(self._collection)

    def get(self, key: str):
        return self._lens.get(self._collection, key)


# ===========================================================================
# KeylessLens — KeyValueLens variant that auto-generates UUIDv7 keys.
#
# The "auto-key" pattern only makes sense for KV-style storage (per-row
# keyed). KeylessLens stays in this file as a thin subclass.
# ===========================================================================

class KeylessLens(KeyValueLens):
    """KeyValueLens variant that auto-generates UUIDv7 primary keys.

    Use this when your data does not have a natural primary key:
    event logs, time-series, metrics, append-only streams, audit
    trails. The lens generates a UUIDv7 for each row; the caller
    receives the key from put() and can use it for later retrieval.

    UUIDv7 is time-ordered, making it suitable for distributed generation
    and range scans via ProllyTreeIndex.

    COLLECTION-AGNOSTIC: Like KeyValueLens, KeylessLens is a stateless
    engine. Pass the collection name to each operation:

        lens = KeylessLens(kernel)
        key = lens.put("events", {"event": "click", "user": "u1"})
        lens.commit("events", "log click")
    """

    def put(self, *args) -> str:
        """Stage data with an auto-generated UUIDv7 key. Returns the key.

        Collection-agnostic API: put(collection, key=None, data)
        Backward compat API:    put(key=None, data)  [requires name in __init__]

        The key MUST be None for KeylessLens. If you want to supply your
        own keys, use the regular KeyValueLens class.
        """
        collection, rest = self._resolve_collection(*args)
        if len(rest) == 2:
            # put(collection, key, data) or put(key, data) in compat mode
            key, data = rest[0], rest[1]
            if key is not None:
                raise TypeError(
                    "KeylessLens.put() does not accept a key. "
                    "Pass key=None, or use the regular KeyValueLens class."
                )
        elif len(rest) == 1:
            # put(data) — key omitted
            data = rest[0]
        else:
            raise TypeError(f"put() expects 1-3 args, got {len(rest)}")
        return self.put_auto(collection, data)

    def put_many(self, *args) -> list[str]:
        """Stage multiple rows, each with an auto-generated key.

        Collection-agnostic: put_many(collection, rows)
        Backward compat:    put_many(rows)  [requires name in __init__]
        """
        collection, rest = self._resolve_collection(*args)
        rows = rest[0]
        return [self.put_auto(collection, row) for row in rows]


# ===========================================================================
# CrossLens — cross-collection read/write operations between KeyValueLenses
# ===========================================================================

class CrossLens:
    """Cross-collection read/write operations.

    These helpers operate on KeyValueLens instances. They do NOT work on
    LakehouseLens or FeatureStoreLens because those lenses don't expose
    per-key get/put (they use whole-table Parquet I/O).

    Semantics:
    - Source = HEAD commit of the source collection.
    - Tombstoned indexes are skipped.
    - Zero-copy sharing: share_blob copies the blob HASH, not CONTENT.
    - No cross-collection atomicity. Use a coordinator for multi-collection commits.
    - Pipe is non-transactional. The target is NOT committed; caller must commit.
    """
    @staticmethod
    def read_from(lens: KeyValueLens, collection: str, key: str) -> Optional[Any]:
        """Read a single key from the collection's current HEAD."""
        return lens.get(collection, key)

    @staticmethod
    def read_all_from(lens: KeyValueLens, collection: str) -> dict[str, Any]:
        """Read all non-internal keys from the collection's current HEAD."""
        return lens.get_all(collection)

    @staticmethod
    def write_to(lens: KeyValueLens, collection: str, key: str, data: Any) -> str:
        """Stage a write on the target collection. Does NOT commit."""
        return lens.put(collection, key, data)

    @staticmethod
    def share_blob(from_lens: KeyValueLens, from_collection: str, from_key: str,
                    to_lens: KeyValueLens, to_collection: str, to_key: str) -> bool:
        """Zero-copy: share a blob's HASH from one collection to another.

        Returns True if the source key existed and the share succeeded,
        False if the source key was not found.
        """
        h = from_lens._get_base(from_collection).lookup(from_key)
        if h is None:
            return False
        to_lens.put_raw(to_collection, to_key, h)
        return True

    @staticmethod
    def pipe(from_lens: KeyValueLens, from_collection: str,
             to_lens: KeyValueLens, to_collection: str,
             transformer: Optional[Callable] = None) -> int:
        """Copy all non-internal keys from source to target collection.

        If transformer is None: zero-copy share (each blob hash is staged
        directly via put_raw, no re-encoding).
        If transformer is provided: re-encode path. The transformer
        receives (key, decoded_data) and returns (new_key, new_data).

        Returns the number of keys copied. Target is NOT committed.
        """
        state = from_lens._get_base(from_collection).read_all()
        count = 0
        for key, h in state.items():
            if key.startswith("_"):
                continue
            if transformer:
                data = from_lens.decode(from_lens.kernel.read_blob(h))
                to_key, to_data = transformer(key, data)
                to_lens.put(to_collection, to_key, to_data)
            else:
                to_lens.put_raw(to_collection, key, h)
            count += 1
        return count


# ===========================================================================
# Backward-compatible aliases.
#
# This file was previously called `lens_sdk.py` and the class was called
# `Lens` (and earlier, `View`). The class was renamed to `KeyValueLens`
# to make its role explicit.
#
# New code should use `KeyValueLens`:
#   from keyvalue_lens import KeyValueLens, KeylessLens, CrossLens
#
# Old code that imports `Lens` or `View` continues to work via the
# aliases below.
# ===========================================================================

Lens = KeyValueLens  # backward-compatible alias (old class name)
View = KeyValueLens  # backward-compatible alias (older class name)
KeylessView = KeylessLens  # backward-compatible alias
CrossView = CrossLens  # backward-compatible alias


# SemanticLens/OssieAdapter are in extensions/semantic/. CollectionIndexer is in
# extensions/indexing/. Both are imported lazily on attribute access to
# avoid circular imports.
def __getattr__(name):
    if name in ("SemanticLens", "SemanticView", "OssieLens", "OssieSemanticLens",
                "OssieAdapter", "SemanticModelAdapter"):
        try:
            from extensions.semantic.ossie import SemanticLens, OssieAdapter
            from extensions.semantic.base import SemanticModelAdapter
            if name == "SemanticLens" or name == "SemanticView":
                return SemanticLens
            if name in ("OssieLens", "OssieSemanticLens"):
                return SemanticLens
            if name == "OssieAdapter":
                return OssieAdapter
            if name == "SemanticModelAdapter":
                return SemanticModelAdapter
        except ImportError:
            pass
    if name in ("IndexedLens", "IndexedView"):
        # DEPRECATED: IndexedLens has been removed. Use CollectionMetadata.
        import warnings
        warnings.warn(
            "IndexedLens has been removed. Use CollectionMetadata instead: "
            "from collection_metadata import CollectionMetadata",
            DeprecationWarning, stacklevel=2
        )
        raise AttributeError(
            "IndexedLens has been removed. Use CollectionMetadata: "
            "from collection_metadata import CollectionMetadata"
        )
    raise AttributeError(f"module 'keyvalue_lens' has no attribute '{name}'")
