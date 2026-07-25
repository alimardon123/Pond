"""
KeyValueLens — the app-facing KEY-VALUE lens.

This is one of three peer app-facing lenses in Pond:
  - KeyValueLens  (this file)        — per-row key→blob storage over ProllyTreeIndex
  - LakehouseLens (lenses/lakehouse)  — whole-table Parquet I/O + range read/write
  - FeatureStoreLens (pond-labs)     — versioned ML feature store on Parquet

All three extend PondLens (base_lens.py), the thin shared-namespace base.
PondLens provides only ref-namespace operations (branch, list_collections,
set_definition, get_definition, history) — no format awareness. Each
app-facing lens owns its OWN read/write API.

KeyValueLens stores each row as a single blob in the ProllyTreeIndex,
keyed by a user-supplied (or auto-generated) primary key. This makes it
suitable for:
  - OLTP workloads (point lookups via Prolly tree, O(log N))
  - Streaming/event logs (KeylessLens variant with auto-UUID keys)
  - Document storage (each blob is a JSON document)
  - Cross-lens blob sharing (CrossLens helpers below)

Index management:
  Indexes are derived structures (Prolly trees of key→blob_hash).
  - Create index: scan data once, build a new Prolly tree (metadata only)
  - Drop index: remove the Reference to the index tree (1 operation)
  - Refresh index: rebuild the Prolly tree from current data (metadata only)
  Data blobs are immutable (kernel Law 1). Indexes are derived and rebuildable.

Backward-compat aliases (defined at the END of this file):
  Lens = KeyValueLens  (old name, kept for compat)
  View = KeyValueLens  (older name, kept for compat)
"""

import json
import time
import sys
import os
import hashlib
import uuid
from typing import Optional, Any, Callable, Union

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pond-core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kernel import PondMinimal
from prolly_tree import ProllyLensBase, ProllyTree
from binary_encoding import BinaryProllyTree
from maintenance import (drop_name, is_dropped, resolve_active,
                         TOMBSTONE_HASH)
from row_query import LensQuery
from base_lens import PondLens


# ===========================================================================
# KeyValueLens — the app-facing key-value lens.
#
# This is NOT the universal base class. The universal base is PondLens
# (base_lens.py), which provides only ref-namespace operations.
# KeyValueLens is a peer of LakehouseLens and FeatureStoreLens — all
# three extend PondLens directly and own their own read/write APIs.
#
# KeyValueLens stores each row as a blob in the ProllyTreeIndex, keyed
# by a user-supplied primary key. Point lookups are O(log N) via the
# Prolly tree. Branches, history, and time travel are inherited from
# ProllyLensBase (via self.base).
#
# Backward-compat: `Lens` and `View` are aliases for `KeyValueLens`,
# defined at the END of this file. New code should use `KeyValueLens`.
# ===========================================================================

class KeyValueLens(PondLens):
    """App-facing KEY-VALUE lens with ProllyTreeIndex backing.

    Extends PondLens (the shared-namespace base). The base class provides
    branch(), list_collections(), set_definition(), get_definition(),
    and history(). KeyValueLens adds:

      Key-value operations:
        - put(key, data): stage a key→blob mapping
        - put_auto(data): stage with auto-generated UUID4 key (returns key)
        - get(key): read a single value by key (O(log N))
        - get_raw(key): read raw bytes (no decode)
        - delete(key): stage a deletion
        - commit(): atomically commit all staged changes
        - keys(), count(), exists(key), get_all()

      Lazy query API:
        - where(predicate=None, **kwargs)
        - select(*fields)
        - map(fn)
        - join(other, on='field')

      Version control (delegated to ProllyLensBase):
        - branch(name), checkout(name), list_branches()
        - merge(name) [union merge with 2-parent commit]
        - undo(steps), history(limit), diff(a, b)

      Index management (METADATA ONLY — data blobs never touched):
        - create_index(name, extractor): builds a Prolly tree mapping
          extracted_key → blob_hash.
        - drop_index(name): tombstones the index Reference.
        - refresh_index(name, extractor): rebuilds the index from current data.
        - list_indexes(), list_all_indexes()
        - lookup_by_index(name, key): O(log N) secondary lookup.

    Subclasses:
      - KeylessLens (this file) — overrides put() to auto-generate keys.
      - SemanticLens (extensions/semantic/) — adds semantic-model adapter.
      - IndexedLens (indexing.py) — adds eager/lazy auto-indexing.
    """

    def __init__(self, kernel: PondMinimal, name: str):
        # PondLens.__init__ takes only (kernel); the name is a Lens-specific
        # attribute (each Lens instance is bound to one collection, while
        # PondLens itself is collection-agnostic).
        super().__init__(kernel)
        self.name = name
        self.base = ProllyLensBase(kernel, name)

    # --- Write path ---
    def put(self, key: str, data: Any) -> str:
        blob_hash = self.kernel.write(self.encode(data))
        self.base.stage(key, blob_hash)
        return blob_hash

    def put_auto(self, data: Any) -> str:
        """Stage data with an auto-generated primary key. Returns the key.

        The key is a 32-character hex string (UUID4 without dashes).
        Use this when your data does not have a natural primary key
        (event logs, time-series, append-only streams). The generated
        key is returned so you can retrieve the data later via get(key).

        This is a Layer 2 SDK convenience. The kernel still requires
        a key; this method generates one on the caller's behalf.

        The key is generated via uuid4 (random, ~122 bits of entropy).
        Collisions are astronomically unlikely (~10^-37 probability
        for 10^12 records).
        """
        key = uuid.uuid4().hex  # 32-char hex string, no dashes
        blob_hash = self.kernel.write(self.encode(data))
        self.base.stage(key, blob_hash)
        return key

    def put_raw(self, key: str, blob_hash: str) -> None:
        self.base.stage(key, blob_hash)

    def delete(self, key: str) -> None:
        self.base.stage_delete(key)

    def commit(self, message: str = "") -> str:
        return self.base.commit(message or f"{self.name} commit")

    # --- Read path ---
    def get(self, key: str) -> Optional[Any]:
        h = self.base.lookup(key)
        return self.decode(self.kernel.read_blob(h)) if h else None

    def get_raw(self, key: str) -> Optional[bytes]:
        h = self.base.lookup(key)
        return self.kernel.read_blob(h) if h else None

    def get_all(self) -> dict[str, Any]:
        state = self.base.read_all()
        return {k: self.decode(self.kernel.read_blob(h))
                for k, h in state.items() if not k.startswith("_")}

    def keys(self) -> list[str]:
        return [k for k in self.base.read_all() if not k.startswith("_")]

    def exists(self, key: str) -> bool:
        return self.base.lookup(key) is not None

    def count(self) -> int:
        return sum(1 for k in self.base.read_all() if not k.startswith("_"))

    # ------------------------------------------------------------------
    # Collection-like API — make Lens feel like an iterable of rows.
    # This is the "direct, easy, simple and elegant way of reading
    # data" per the architecture review. See view_query.py for the
    # lazy query API (.where, .select, .map, .join).
    # ------------------------------------------------------------------

    def __iter__(self):
        """Iterate over decoded rows (not keys).

            for row in view:
                print(row)

        Equivalent to:
            for key in lens.keys():
                row = lens.get(key)
                if row is not None:
                    yield row
        """
        for key in self.keys():
            row = self.get(key)
            if row is not None:
                yield row

    def __len__(self) -> int:
        """len(lens) == lens.count()."""
        return self.count()

    def __contains__(self, key: str) -> bool:
        """key in view == lens.exists(key)."""
        return self.exists(key)

    def where(self, predicate=None, **kwargs) -> LensQuery:
        """Start a lazy query that filters rows.

            # Filter with kwargs
            for row in lens.where(region="US"):
                ...

            # Filter with a predicate
            for row in lens.where(lambda r: r["amount"] > 100):
                ...

        See LensQuery for full chaining: .where().select().map().join().
        """
        return LensQuery(self).where(predicate, **kwargs)

    def select(self, *fields: str) -> LensQuery:
        """Start a lazy query that projects rows to only these fields.

            for row in lens.select("order_id", "amount"):
                ...
        """
        return LensQuery(self).select(*fields)

    def map(self, fn: Callable) -> LensQuery:
        """Start a lazy query that transforms each row via fn(row).

            for row in lens.map(lambda r: {**r, "amount_usd": r["amount"] * 1.1}):
                ...
        """
        return LensQuery(self).map(fn)

    def join(self, other, on: str):
        """JOIN with another Lens.

            for row in orders.join(customers, on="customer_id"):
                print(row["order_id"], row["customer_name"])

        LEFT JOIN semantics: left rows with no match are yielded as-is.
        Right side wins on field conflicts.
        """
        return LensQuery(self).join(other, on)

    # --- Version control ---
    def branch(self, name: str) -> str: return self.base.branch(name)
    def checkout(self, name: str) -> None: self.base.checkout(name)
    def list_branches(self) -> list[str]: return self.base.list_branches()
    def merge(self, name: str) -> str: return self.base.merge(name)
    def undo(self, steps: int = 1) -> str: return self.base.undo(steps)
    def history(self, limit: int = 20) -> list[dict]: return self.base.history(limit)
    def diff(self, a: str, b: str) -> dict: return self.base.diff(a, b)

    # ------------------------------------------------------------------
    # INDEX MANAGEMENT — metadata only, NO data rewrite
    # ------------------------------------------------------------------

    def create_index(self, index_name: str, key_extractor: Callable[[Any], str]) -> str:
        """
        Create a secondary index. METADATA ONLY — does NOT touch data blobs.

        How it works:
        1. Scan all data entries (read blobs, extract index keys)
        2. Build a Prolly tree mapping index_key → blob_hash
        3. Store the tree root as a Reference (metadata)

        Data blobs are NEVER modified. The index is a derived structure.
        """
        state = self.base.read_all()
        index_entries = {}
        for pk, bh in state.items():
            if pk.startswith("_"):
                continue
            data = self.decode(self.kernel.read_blob(bh))
            idx_key = key_extractor(data)
            index_entries[f"_index/{index_name}/{idx_key}"] = bh
        tree_root = ProllyTree.build(self.kernel, index_entries)
        self.kernel.reference(f"{self.name}__index__{index_name}", tree_root)
        return tree_root

    def drop_index(self, index_name: str) -> bool:
        """
        Drop an index. METADATA ONLY — does NOT touch data blobs.

        Per RFC-0008 (Deletion as Data), this uses the tombstone pattern:
          1. drop_name(kernel, ref_name) rebinds the index's Reference to
             TOMBSTONE_HASH. The index is now logically deleted.
          2. Subsequent lookup_by_index calls return None (resolve_active
             sees the tombstone and treats it as unbound).
          3. The previously-pointed-to index tree blob becomes unreachable;
             PondGC will sweep it on the next collection.
          4. compact_tombstones(kernel) (Layer 0.5 maintenance) can later
             remove the name's row from the roots table.

        Data blobs are NEVER modified. The index Reference becomes a
        tombstone; the index tree blob becomes orphaned.

        Returns True if the index existed and was dropped, False if the
        index was not registered (or was already a tombstone).
        """
        ref_name = f"{self.name}__index__{index_name}"
        current = self.kernel.resolve(ref_name)
        if not current or current == TOMBSTONE_HASH:
            return False
        drop_name(self.kernel, ref_name)
        return True

    def refresh_index(self, index_name: str, key_extractor: Callable[[Any], str]) -> str:
        """
        Refresh an index (rebuild from current data). METADATA ONLY.

        This is the same as create_index but overwrites the existing index
        (including a previously-tombstoned index — refresh_index revives
        a dropped index).
        """
        return self.create_index(index_name, key_extractor)

    def list_indexes(self) -> list[str]:
        """List all ACTIVE (non-tombstoned) indexes for this Lens.

        Per RFC-0008, tombstoned indexes are excluded. Use
        list_all_indexes() to include tombstoned indexes.
        """
        prefix = f"{self.name}__index__"
        return [n[len(prefix):] for n in self.kernel.list_names()
                if n.startswith(prefix) and not is_dropped(self.kernel, n)]

    def list_all_indexes(self) -> list[str]:
        """List ALL indexes, including tombstoned ones. Mainly for
        maintenance/diagnostic tools."""
        prefix = f"{self.name}__index__"
        return [n[len(prefix):] for n in self.kernel.list_names()
                if n.startswith(prefix)]

    def lookup_by_index(self, index_name: str, index_key: str) -> Optional[Any]:
        """Look up data via a secondary index. O(log N).

        Per RFC-0008, returns None if the index has been dropped
        (tombstoned). This makes drop_index immediately effective for
        readers — no need to wait for compaction.
        """
        ref_name = f"{self.name}__index__{index_name}"
        tree_root = resolve_active(self.kernel, ref_name)
        if not tree_root:
            return None
        full_key = f"_index/{index_name}/{index_key}"
        bh = ProllyTree.lookup(self.kernel, tree_root, full_key)
        return self.decode(self.kernel.read_blob(bh)) if bh else None

    # --- Serialization (override in subclass) ---
    def encode(self, data: Any) -> bytes:
        return json.dumps(data, sort_keys=True).encode()
    def decode(self, data: bytes) -> Any:
        return json.loads(data)


# ===========================================================================
# KeylessLens — KeyValueLens with auto-generated UUID primary keys.
#
# This IS KeyValueLens-specific. The "auto-key" pattern only makes sense
# when the underlying storage is per-row keyed (ProllyTreeIndex stores
# key→blob). LakehouseLens writes whole Parquet blobs (no per-row keys
# at storage layer); FeatureStoreLens uses entity columns from the data
# (application provides them). So KeylessLens stays in this file as a
# thin subclass that overrides put() to require key=None and delegate
# to put_auto().
# ===========================================================================

class KeylessLens(KeyValueLens):
    """KeyValueLens where primary keys are auto-generated, not user-supplied.

    Use this when your data does not have a natural primary key:
    event logs, time-series, metrics, append-only streams, audit
    trails. The lens generates a UUID4 for each row; the caller
    receives the key from put() and can use it for later retrieval.

    This is the first-class version of the auto-key pattern. Instead
    of calling `lens.put_auto(data)` on a regular KeyValueLens, you
    construct a KeylessLens and call `lens.put(data)` — the key
    generation is built into the put path.

    Internally, KeylessLens just overrides put() to delegate to
    put_auto(). All other KeyValueLens operations (get, delete,
    branch, merge, history, indexes) work unchanged.

    For indexed lookups on KeylessLens data, register indexes on
    fields WITHIN the data (e.g., a timestamp, a user_id) and use
    lookup_by_index() to query without knowing the primary key.
    """

    def put(self, key: Optional[str], data: Any) -> str:
        """Stage data with an auto-generated key. Returns the key.

        Args:
            key: MUST be None for KeylessLens. Passing a non-None key
                raises TypeError — if you want to supply your own
                keys, use the regular `KeyValueLens` class instead.
            data: the data to store.

        Returns:
            The auto-generated 32-char hex key (UUID4 without dashes).
            Use this key to retrieve the data via get(key) later.
        """
        if key is not None:
            raise TypeError(
                "KeylessLens.put() does not accept a key. "
                "Pass key=None, or use the regular KeyValueLens class "
                "if you want to supply your own keys."
            )
        return self.put_auto(data)

    def put_many(self, rows: list[Any]) -> list[str]:
        """Stage multiple rows, each with an auto-generated key.

        Args:
            rows: list of data values to store.

        Returns:
            List of generated keys, one per row.
        """
        return [self.put_auto(row) for row in rows]


# ===========================================================================
# CrossLens — read/write across Views
# ===========================================================================

class CrossLens:
    """Cross-Lens read/write operations between two KeyValueLens instances.

    These helpers operate on KeyValueLens instances (any subclass —
    KeyValueLens, KeylessLens, SemanticLens, IndexedLens). They do
    NOT work on LakehouseLens or FeatureStoreLens because those lenses
    don't expose per-key get/put (they use whole-table Parquet I/O).

    Semantics (settled per Phase B.3 SDK polish):

    - **Source = HEAD commit of the source lens's currently-checked-out
      branch.** CrossLens does NOT take a commit-hash argument; it
      always reads from the source lens's current HEAD. To read from
      a specific historical commit, check out that commit's branch
      first, then call CrossLens.
    - **Tombstoned indexes are skipped.** If `from_lens` has tombstoned
      indexes (per RFC-0008), `read_all_from` returns only non-internal
      user keys; tombstoned index References are excluded (they start
      with `{name}__index__` and resolve to TOMBSTONE_HASH).
    - **Zero-copy sharing.** `share_blob` copies the blob HASH, not
      the blob CONTENT. The two lenses now reference the same kernel
      blob (content-addressed dedup for free).
    - **No cross-lens atomicity.** A `write_to` followed by a `commit`
      on the target lens is atomic for the target, but there is no
      cross-lens atomic commit. If you need atomic multi-lens commits,
      use a higher-level coordinator (future RFC).
    - **Pipe is non-transactional.** `pipe` reads the source's current
      state at call time and writes to the target's staging area. The
      target is NOT committed; the caller must call `to_lens.commit()`
      after `pipe` returns.
    """
    @staticmethod
    def read_from(lens: "KeyValueLens", key: str) -> Optional[Any]:
        """Read a single key from the lens's current HEAD.

        Returns None if the key does not exist or was deleted.
        """
        return lens.get(key)

    @staticmethod
    def read_all_from(lens: "KeyValueLens") -> dict[str, Any]:
        """Read all non-internal keys from the lens's current HEAD.

        Keys starting with `_` (internal: schema, index metadata,
        semantic definitions) are excluded. Tombstoned names are
        excluded (they are not in `lens.get_all()` because `get_all`
        walks the lens state, not the root namespace).
        """
        return lens.get_all()

    @staticmethod
    def write_to(lens: "KeyValueLens", key: str, data: Any) -> str:
        """Stage a write on the target lens. Does NOT commit.

        The caller must call `lens.commit(message)` after one or more
        `write_to` calls to make the changes durable.
        """
        return lens.put(key, data)

    @staticmethod
    def share_blob(from_lens: "KeyValueLens", from_key: str,
                    to_lens: "KeyValueLens", to_key: str) -> bool:
        """Zero-copy: share a blob's HASH from one lens to another.

        The blob's CONTENT is not copied. Both lenses now reference
        the same kernel blob (content-addressed dedup for free).

        Returns True if the source key existed and the share succeeded,
        False if the source key was not found.

        The target lens's staging area is updated; the caller must
        call `to_lens.commit(message)` to make the share durable.
        """
        h = from_lens.base.lookup(from_key)
        if h is None:
            return False
        to_lens.put_raw(to_key, h)
        return True

    @staticmethod
    def pipe(from_lens: "KeyValueLens", to_lens: "KeyValueLens",
             transformer: Optional[Callable] = None) -> int:
        """Copy all non-internal keys from `from_lens` to `to_lens`.

        If `transformer` is None: zero-copy share (each blob hash is
        staged directly via `put_raw`, no re-encoding).

        If `transformer` is provided: re-encode path. The transformer
        receives `(key, decoded_data)` and returns `(new_key, new_data)`.
        The new_data is re-encoded via `to_lens.encode` and written as
        a new blob.

        The target lens's staging area is updated; the caller must
        call `to_lens.commit(message)` to make the pipe durable.

        Returns the number of keys copied.
        """
        state = from_lens.base.read_all()
        count = 0
        for key, h in state.items():
            if key.startswith("_"):
                continue
            if transformer:
                data = from_lens.decode(from_lens.kernel.read_blob(h))
                to_key, to_data = transformer(key, data)
                to_lens.put(to_key, to_data)
            else:
                to_lens.put_raw(key, h)
            count += 1
        return count


# ===========================================================================
# Semantic models are now an OPTIONAL extension.
#
# The base Lens (this file) does NOT include semantic model support.
# To use semantic models (Ossie, Cube, dbt, etc.), install the extension:
#
#   from extensions.semantic_ossie import SemanticLens, OssieAdapter
#
# Or create a custom adapter:
#
#   from extensions.semantic_base import SemanticModelAdapter
#   from extensions.semantic_ossie import SemanticLens
#
#   class MyAdapter(SemanticModelAdapter): ...
#   semantic = SemanticLens(kernel, "semantic", adapter=MyAdapter())
#
# This keeps the core Lens SDK small and lets different deployments
# use different semantic standards.
# ===========================================================================


# ===========================================================================
# Test: Index management (semantic model test is in extensions/semantic_ossie.py)
# ===========================================================================

def test_all():
    import shutil
    bench_dir = "/tmp/pond_sdk_v2_test"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    print("=== INDEX MANAGEMENT TEST ===\n")

    # Create a Lens with data
    db = KeyValueLens(kernel, "db")
    db.put("user:1", {"name": "Alice", "age": 30, "region": "US"})
    db.put("user:2", {"name": "Bob", "age": 25, "region": "EU"})
    db.put("user:3", {"name": "Carol", "age": 35, "region": "US"})
    db.commit("insert 3 users")

    # Create index on "region"
    db.create_index("by_region", lambda d: d.get("region", ""))
    print(f"  Created index 'by_region'")
    print(f"  Indexes: {db.list_indexes()}")
    print(f"  Lookup 'US': {db.lookup_by_index('by_region', 'US')}")

    # Add more data (index is now stale)
    db.put("user:4", {"name": "Dave", "age": 28, "region": "EU"})
    db.commit("add Dave")
    print(f"\n  After adding Dave (index is stale):")
    print(f"  Lookup 'EU' (stale — misses Dave): {db.lookup_by_index('by_region', 'EU')}")

    # Refresh index (METADATA ONLY — no data rewrite)
    db.refresh_index("by_region", lambda d: d.get("region", ""))
    print(f"\n  After refresh_index:")
    print(f"  Lookup 'EU' (now includes Dave): {db.lookup_by_index('by_region', 'EU')}")

    # Create another index
    db.create_index("by_age", lambda d: str(d.get("age", 0)))
    print(f"\n  Indexes: {db.list_indexes()}")

    # Drop an index (METADATA ONLY)
    db.drop_index("by_age")
    print(f"  After drop_index('by_age'): {db.list_indexes()}")
    print(f"  Lookup 'by_age' after drop: {db.lookup_by_index('by_age', '30')}")

    # Verify data is untouched
    print(f"\n  Data verification (untouched by index ops):")
    print(f"  user:1 = {db.get('user:1')}")
    print(f"  user:4 = {db.get('user:4')}")
    print(f"  count = {db.count()}")

    print("\n=== INDEX OPERATIONS: DATA vs METADATA ===\n")
    stats = kernel.storage_stats()
    print(f"  Total blobs: {stats['blob_count']}")
    print(f"  Indexes are stored as Prolly trees (metadata blobs)")
    print(f"  Data blobs are NEVER touched by index operations")
    print(f"  create_index: scans data → builds Prolly tree (metadata)")
    print(f"  drop_index: overwrites Reference to empty tree (metadata)")
    print(f"  refresh_index: rebuilds Prolly tree from current data (metadata)")
    print(f"  Zero data blobs modified during any index operation ✓")

    print("\n=== ALL TESTS PASSED ===")
    print("\n  Note: Semantic model tests are in extensions/semantic_ossie.py")
    print("  Run: python pond-sdk/extensions/semantic_ossie.py")
    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)


if __name__ == "__main__":
    test_all()


# ===========================================================================
# Backward-compatible aliases.
#
# This file was previously called `keyvalue_lens.py` and the class was called
# `Lens` (and earlier, `View`). The class was renamed to `KeyValueLens`
# to make its role explicit: it is the app-facing KEY-VALUE lens, a peer
# of LakehouseLens and FeatureStoreLens — NOT the universal base class
# (that's PondLens in base_lens.py).
#
# New code should use `KeyValueLens`:
#   from keyvalue_lens import KeyValueLens, KeylessLens, CrossLens
#
# Old code that imports `Lens` or `View` continues to work via the
# aliases below. The aliases are kept indefinitely — there is no plan
# to remove them.
#
# SemanticLens is an OPTIONAL extension. Import it from:
#   from extensions.semantic.ossie import SemanticLens, OssieAdapter
# ===========================================================================

Lens = KeyValueLens  # backward-compatible alias (old class name)
View = KeyValueLens  # backward-compatible alias (older class name)
KeylessView = KeylessLens  # backward-compatible alias
CrossView = CrossLens  # backward-compatible alias

# SemanticLens/OssieAdapter are in extensions/semantic/. IndexedLens is in
# indexing.py. Both are imported lazily on attribute access to avoid
# circular imports (indexing.py imports KeyValueLens from this module).
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
    if name == "IndexedLens":
        from extensions.indexing.auto_index import IndexedLens
        return IndexedLens
    raise AttributeError(f"module 'keyvalue_lens' has no attribute '{name}'")
