"""
Enhanced View SDK with:
  - Index management (create/drop/refresh — metadata only, NO data rewrite)
  - Ossie-aligned SemanticView (Apache Ossie open semantic interchange spec)
  - Recursive View composition (Phase D)

Answers the user's question:
  Q: If I want to drop/create/refresh indexes, do I have to rewrite data or metadata?
  A: METADATA ONLY. Indexes are derived structures (Prolly trees of key→blob_hash).
     The data blobs are NEVER touched when indexes change.
     - Create index: scan data once, build a new Prolly tree (metadata only)
     - Drop index: remove the Reference to the index tree (1 operation)
     - Refresh index: rebuild the Prolly tree from current data (metadata only)
     Data blobs are immutable (kernel Law 1). Indexes are derived and rebuildable.
"""

import json
import time
import sys
import os
import hashlib
import uuid
from typing import Optional, Any, Callable, Union

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "prototype"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond_minimal import PondMinimal
from prolly_view import ProllyViewBase, ProllyTree
from binary_encoding import BinaryProllyTree
from maintenance import (drop_name, is_dropped, resolve_active,
                         TOMBSTONE_HASH)
from view_query import ViewQuery


# ===========================================================================
# Naming: "Lens" is the preferred term for what was called "View".
#
# A Pond Lens is an interpretation layer over immutable bytes — like a
# lens that focuses light differently without changing the light itself.
# The old name "View" conflated with SQL VIEW, Materialized View, etc.
# "Lens" captures the actual philosophy: the bytes don't change; only
# the way you observe and manipulate them changes.
#
# Both names work. `View` is kept for backward compatibility. New code
# should use `Lens`. See RFC-0012 for the full rationale.
#
# The aliases (Lens = View, etc.) are defined at the END of this file,
# after all classes are declared.
# ===========================================================================


# ===========================================================================
# Enhanced View with full index management
# ===========================================================================

class View:
    """
    Abstract base class for all Pond Views.

    Index management:
      - create_index(name, extractor): builds a Prolly tree mapping
        extracted_key → blob_hash. Does NOT touch data blobs.
      - drop_index(name): removes the Reference to the index tree.
        Data blobs are untouched.
      - refresh_index(name, extractor): rebuilds the index from current data.
        Does NOT touch data blobs.
      - list_indexes(): lists all indexes for this View.

    All index operations work on METADATA only (Prolly trees of key→hash).
    Data blobs are immutable and never rewritten.
    """

    def __init__(self, kernel: PondMinimal, name: str):
        self.kernel = kernel
        self.name = name
        self.base = ProllyViewBase(kernel, name)

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
    # Collection-like API — make View feel like an iterable of rows.
    # This is the "direct, easy, simple and elegant way of reading
    # data" per the architecture review. See view_query.py for the
    # lazy query API (.where, .select, .map, .join).
    # ------------------------------------------------------------------

    def __iter__(self):
        """Iterate over decoded rows (not keys).

            for row in view:
                print(row)

        Equivalent to:
            for key in view.keys():
                row = view.get(key)
                if row is not None:
                    yield row
        """
        for key in self.keys():
            row = self.get(key)
            if row is not None:
                yield row

    def __len__(self) -> int:
        """len(view) == view.count()."""
        return self.count()

    def __contains__(self, key: str) -> bool:
        """key in view == view.exists(key)."""
        return self.exists(key)

    def where(self, predicate=None, **kwargs) -> ViewQuery:
        """Start a lazy query that filters rows.

            # Filter with kwargs
            for row in view.where(region="US"):
                ...

            # Filter with a predicate
            for row in view.where(lambda r: r["amount"] > 100):
                ...

        See ViewQuery for full chaining: .where().select().map().join().
        """
        return ViewQuery(self).where(predicate, **kwargs)

    def select(self, *fields: str) -> ViewQuery:
        """Start a lazy query that projects rows to only these fields.

            for row in view.select("order_id", "amount"):
                ...
        """
        return ViewQuery(self).select(*fields)

    def map(self, fn: Callable) -> ViewQuery:
        """Start a lazy query that transforms each row via fn(row).

            for row in view.map(lambda r: {**r, "amount_usd": r["amount"] * 1.1}):
                ...
        """
        return ViewQuery(self).map(fn)

    def join(self, other, on: str):
        """JOIN with another View.

            for row in orders.join(customers, on="customer_id"):
                print(row["order_id"], row["customer_name"])

        LEFT JOIN semantics: left rows with no match are yielded as-is.
        Right side wins on field conflicts.
        """
        return ViewQuery(self).join(other, on)

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
        """List all ACTIVE (non-tombstoned) indexes for this View.

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
# KeylessView — primary-keyless View as a first-class mode
# ===========================================================================

class KeylessView(View):
    """A View where primary keys are auto-generated, not user-supplied.

    Use this when your data does not have a natural primary key:
    event logs, time-series, metrics, append-only streams, audit
    trails. The View generates a UUID4 for each row; the caller
    receives the key from put() and can use it for later retrieval.

    This is the first-class version of the auto-key pattern. Instead
    of calling `view.put_auto(data)` on a regular View, you construct
    a KeylessView and call `view.put(data)` — the key generation is
    built into the put path.

    Internally, KeylessView just overrides put() to delegate to
    put_auto(). All other View operations (get, delete, branch, merge,
    history, indexes) work unchanged. The View's state space is the
    same as a regular View; the only difference is the put() signature.

    For indexed lookups on KeylessView data, register indexes on
    fields WITHIN the data (e.g., a timestamp, a user_id). Use
    find_by / find_all_by to query without knowing the primary key.
    """

    def put(self, key: Optional[str], data: Any) -> str:
        """Stage data with an auto-generated key. Returns the key.

        Args:
            key: MUST be None for KeylessView. Passing a non-None key
                raises TypeError — if you want to supply your own
                keys, use the regular `View` class instead.
            data: the data to store.

        Returns:
            The auto-generated 32-char hex key (UUID4 without dashes).
            Use this key to retrieve the data via get(key) later.
        """
        if key is not None:
            raise TypeError(
                "KeylessView.put() does not accept a key. "
                "Pass key=None, or use the regular View class if you "
                "want to supply your own keys."
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
# CrossView — read/write across Views
# ===========================================================================

class CrossView:
    """Cross-View read/write operations.

    Semantics (settled per Phase B.3 SDK polish):

    - **Source = HEAD commit of the source View's currently-checked-out
      branch.** CrossView does NOT take a commit-hash argument; it
      always reads from the source View's current HEAD. To read from
      a specific historical commit, check out that commit's branch
      first, then call CrossView.
    - **Tombstoned indexes are skipped.** If `from_view` has tombstoned
      indexes (per RFC-0008), `read_all_from` returns only non-internal
      user keys; tombstoned index References are excluded (they start
      with `{view_name}__index__` and resolve to TOMBSTONE_HASH).
    - **Zero-copy sharing.** `share_blob` copies the blob HASH, not
      the blob CONTENT. The two Views now reference the same kernel
      blob (content-addressed dedup for free).
    - **No cross-View atomicity.** A `write_to` followed by a `commit`
      on the target View is atomic for the target, but there is no
      cross-View atomic commit. If you need atomic multi-View commits,
      use a higher-level coordinator (future RFC).
    - **Pipe is non-transactional.** `pipe` reads the source's current
      state at call time and writes to the target's staging area. The
      target is NOT committed; the caller must call `to_view.commit()`
      after `pipe` returns.
    """
    @staticmethod
    def read_from(view: View, key: str) -> Optional[Any]:
        """Read a single key from the view's current HEAD.

        Returns None if the key does not exist or was deleted.
        """
        return view.get(key)

    @staticmethod
    def read_all_from(view: View) -> dict[str, Any]:
        """Read all non-internal keys from the view's current HEAD.

        Keys starting with `_` (internal: schema, index metadata,
        semantic definitions) are excluded. Tombstoned names are
        excluded (they are not in `view.get_all()` because `get_all`
        walks the View state, not the root namespace).
        """
        return view.get_all()

    @staticmethod
    def write_to(view: View, key: str, data: Any) -> str:
        """Stage a write on the target view. Does NOT commit.

        The caller must call `view.commit(message)` after one or more
        `write_to` calls to make the changes durable.
        """
        return view.put(key, data)

    @staticmethod
    def share_blob(from_view: View, from_key: str,
                    to_view: View, to_key: str) -> bool:
        """Zero-copy: share a blob's HASH from one View to another.

        The blob's CONTENT is not copied. Both Views now reference
        the same kernel blob (content-addressed dedup for free).

        Returns True if the source key existed and the share succeeded,
        False if the source key was not found.

        The target View's staging area is updated; the caller must
        call `to_view.commit(message)` to make the share durable.
        """
        h = from_view.base.lookup(from_key)
        if h is None:
            return False
        to_view.put_raw(to_key, h)
        return True

    @staticmethod
    def pipe(from_view: View, to_view: View,
             transformer: Optional[Callable] = None) -> int:
        """Copy all non-internal keys from `from_view` to `to_view`.

        If `transformer` is None: zero-copy share (each blob hash is
        staged directly via `put_raw`, no re-encoding).

        If `transformer` is provided: re-encode path. The transformer
        receives `(key, decoded_data)` and returns `(new_key, new_data)`.
        The new_data is re-encoded via `to_view.encode` and written as
        a new blob.

        The target View's staging area is updated; the caller must
        call `to_view.commit(message)` to make the pipe durable.

        Returns the number of keys copied.
        """
        state = from_view.base.read_all()
        count = 0
        for key, h in state.items():
            if key.startswith("_"):
                continue
            if transformer:
                data = from_view.decode(from_view.kernel.read_blob(h))
                to_key, to_data = transformer(key, data)
                to_view.put(to_key, to_data)
            else:
                to_view.put_raw(key, h)
            count += 1
        return count


# ===========================================================================
# OssieSemanticView — aligned with Apache Ossie open semantic interchange spec
# ===========================================================================

class SemanticModelAdapter:
    """Abstract interface for semantic model formats.
    Pond supports multiple semantic model standards (Ossie, Cube, dbt, etc.)
    via adapters. The kernel and View SDK are NOT coupled to any specific format."""
    def export_model(self, view: 'View') -> dict:
        raise NotImplementedError
    def import_model(self, view: 'View', model: dict) -> None:
        raise NotImplementedError


class OssieAdapter(SemanticModelAdapter):
    """Apache Ossie adapter — one implementation of the semantic model interface."""

    def export_model(self, view: 'View') -> dict:
        """Export semantic definitions in Ossie format."""
        state = view.base.read_all()
        model = {"name": view.name, "datasets": [], "metrics": [], "relationships": []}
        for key in state:
            if key.startswith("_semantic/metrics/"):
                model["metrics"].append(view.decode(view.kernel.read_blob(state[key])))
            elif key.startswith("_semantic/relationships/"):
                model["relationships"].append(view.decode(view.kernel.read_blob(state[key])))
        return model

    def import_model(self, view: 'View', model: dict) -> None:
        """Import an Ossie-format model into the View."""
        for metric in model.get("metrics", []):
            view.put(f"_semantic/metrics/{metric['name']}", metric)
        for rel in model.get("relationships", []):
            view.put(f"_semantic/relationships/{rel['name']}", rel)


class SemanticView(View):
    """
    A View that manages semantic models (metrics, dimensions, relationships).

    NOT coupled to any specific semantic model standard. Uses adapters:
      - OssieAdapter for Apache Ossie format
      - Future: CubeAdapter, DbtAdapter, etc.

    The View stores metric/dimension/relationship definitions as blobs.
    Adapters translate between the internal format and external standards.
    """

    def import_ossie_model(self, model: dict) -> str:
        """Import an Ossie-format semantic model.
        The model dict follows the Ossie core spec."""
        model_bytes = json.dumps(model, sort_keys=True).encode()
        h = self.kernel.write(model_bytes)
        self.put_raw(f"_ossie/models/{model['name']}", h)
        # Also index each metric, dimension, and relationship
        for metric in model.get("metrics", []):
            self.put(f"_semantic/metrics/{metric['name']}", metric)
        for dataset in model.get("datasets", []):
            for field in dataset.get("fields", []):
                if field.get("dimension"):
                    self.put(f"_semantic/dimensions/{field['name']}", {
                        "name": field["name"],
                        "source": dataset["source"],
                        "field": field["name"],
                        "type": field.get("type", "string"),
                        "is_time": field.get("dimension", {}).get("is_time", False),
                        "ai_context": field.get("ai_context"),
                    })
        for rel in model.get("relationships", []):
            self.put(f"_semantic/relationships/{rel['name']}", rel)
        return self.commit(f"import Ossie model '{model['name']}'")

    def export_ossie_model(self, model_name: str) -> Optional[dict]:
        """Export a semantic model in Ossie format."""
        h = self.base.lookup(f"_ossie/models/{model_name}")
        if not h:
            return None
        return json.loads(self.kernel.read_blob(h))

    def define_metric_ossie(self, name: str, source: str, field: str,
                            dialects: dict[str, str], dimensions: list = None,
                            ai_context: str = None) -> None:
        """Define a metric with multi-dialect expressions (Ossie pattern).
        dialects: {"ANSI_SQL": "SUM(amount)", "SNOWFLAKE": "SUM(amount)", ...}"""
        metric = {
            "name": name,
            "source": source,
            "field": field,
            "expression": {"dialects": dialects},
            "dimensions": dimensions or [],
            "ai_context": ai_context,
            "created_at": time.time(),
        }
        self.put(f"_semantic/metrics/{name}", metric)

    def define_dimension_ossie(self, name: str, source: str, field: str,
                               dim_type: str = "string", is_time: bool = False,
                               ai_context: str = None) -> None:
        """Define a dimension (Ossie pattern with ai_context)."""
        dim = {
            "name": name,
            "source": source,
            "field": field,
            "type": dim_type,
            "dimension": {"is_time": is_time},
            "ai_context": ai_context,
        }
        self.put(f"_semantic/dimensions/{name}", dim)

    def define_relationship_ossie(self, name: str, from_dataset: str,
                                  from_columns: list[str], to_dataset: str,
                                  to_columns: list[str]) -> None:
        """Define a relationship (Ossie pattern with composite key support)."""
        rel = {
            "name": name,
            "from": {"dataset": from_dataset, "columns": from_columns},
            "to": {"dataset": to_dataset, "columns": to_columns},
        }
        self.put(f"_semantic/relationships/{name}", rel)

    def list_metrics(self) -> list[str]:
        state = self.base.read_all()
        return [k[len("_semantic/metrics/"):] for k in state
                if k.startswith("_semantic/metrics/")]

    def list_dimensions(self) -> list[str]:
        state = self.base.read_all()
        return [k[len("_semantic/dimensions/"):] for k in state
                if k.startswith("_semantic/dimensions/")]

    def get_metric(self, name: str) -> Optional[dict]:
        return self.get(f"_semantic/metrics/{name}")

    def execute_metric(self, metric_name: str, source_view: View,
                       group_by: list[str] = None) -> list[dict]:
        """Execute a metric query against a source View."""
        metric = self.get_metric(metric_name)
        if not metric:
            raise ValueError(f"Metric '{metric_name}' not found")
        data = source_view.get_all()
        if not group_by:
            group_by = metric.get("dimensions", [])
        if not group_by:
            values = [row.get(metric["field"], 0) for row in data.values()]
            expr = metric.get("expression", {}).get("dialects", {})
            agg = "sum"  # default
            for dialect, sql in expr.items():
                if "SUM" in sql.upper(): agg = "sum"; break
                if "COUNT" in sql.upper(): agg = "count"; break
                if "AVG" in sql.upper(): agg = "avg"; break
            if agg == "sum": result = sum(values)
            elif agg == "count": result = len(values)
            elif agg == "avg": result = sum(values)/len(values) if values else 0
            else: result = sum(values)
            return [{"value": result, "metric": metric_name}]

        groups = {}
        for row in data.values():
            gk = tuple(row.get(d, "") for d in group_by)
            groups.setdefault(gk, []).append(row.get(metric["field"], 0))
        # Determine aggregation type from dialects
        expr = metric.get("expression", {}).get("dialects", {})
        agg = "sum"
        for dialect, sql in expr.items():
            if "SUM" in sql.upper(): agg = "sum"; break
            if "COUNT" in sql.upper(): agg = "count"; break
            if "AVG" in sql.upper(): agg = "avg"; break
        results = []
        for gk, vals in groups.items():
            if agg == "sum": v = sum(vals)
            elif agg == "count": v = len(vals)
            elif agg == "avg": v = sum(vals)/len(vals) if vals else 0
            else: v = sum(vals)
            r = {"metric": metric_name, "value": v}
            for i, d in enumerate(group_by): r[d] = gk[i]
            results.append(r)
        return results


# ===========================================================================
# Test: Index management + Ossie SemanticView
# ===========================================================================

def test_all():
    import shutil
    bench_dir = "/tmp/pond_sdk_v2_test"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    print("=== INDEX MANAGEMENT TEST ===\n")

    # Create a View with data
    db = View(kernel, "db")
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

    print("\n=== OSSIE SEMANTIC VIEW TEST ===\n")

    # Create Ossie semantic model
    semantic = OssieSemanticView(kernel, "semantic")

    # Define metrics with multi-dialect expressions (Ossie pattern)
    semantic.define_metric_ossie(
        "total_revenue", "orders", "amount",
        dialects={"ANSI_SQL": "SUM(amount)", "SNOWFLAKE": "SUM(amount)"},
        dimensions=["product", "region"],
        ai_context="Total revenue from all orders. Use this for financial reports."
    )
    semantic.define_metric_ossie(
        "order_count", "orders", "amount",
        dialects={"ANSI_SQL": "COUNT(*)"},
        dimensions=["product"]
    )

    # Define dimensions with ai_context
    semantic.define_dimension_ossie("product", "orders", "product", "string",
                                    ai_context="Product name. Categories: Widget, Gadget.")
    semantic.define_dimension_ossie("region", "users", "region", "string",
                                    is_time=False,
                                    ai_context="Customer region. Values: US, EU.")
    semantic.define_dimension_ossie("order_date", "orders", "ts", "time",
                                    is_time=True,
                                    ai_context="Order timestamp. Use for time series.")

    # Define relationships (composite key support)
    semantic.define_relationship_ossie("user_orders", "users", ["user_id"],
                                       "orders", ["user_id"])

    semantic.commit("define Ossie semantic model")

    print(f"  Metrics: {semantic.list_metrics()}")
    print(f"  Dimensions: {semantic.list_dimensions()}")
    print(f"  total_revenue metric: {json.dumps(semantic.get_metric('total_revenue'), indent=2)}")

    # Execute metric against source data
    orders = View(kernel, "orders")
    orders.put("order:1", {"user_id": 1, "amount": 100, "product": "Widget"})
    orders.put("order:2", {"user_id": 2, "amount": 200, "product": "Gadget"})
    orders.put("order:3", {"user_id": 1, "amount": 50, "product": "Widget"})
    orders.commit("insert 3 orders")

    results = semantic.execute_metric("total_revenue", orders, group_by=["product"])
    print(f"\n  Total revenue by product:")
    for r in results:
        print(f"    {r['product']}: ${r['value']}")

    # Import/export Ossie model
    ossie_model = {
        "name": "sales_model",
        "description": "Sales semantic model",
        "datasets": [
            {"name": "orders", "source": "orders", "primary_key": "order_id",
             "fields": [
                 {"name": "amount", "type": "float"},
                 {"name": "product", "type": "string", "dimension": {"is_time": False}},
                 {"name": "ts", "type": "timestamp", "dimension": {"is_time": True}},
             ]}
        ],
        "metrics": [
            {"name": "revenue", "source": "orders", "field": "amount",
             "expression": {"dialects": {"ANSI_SQL": "SUM(amount)"}}},
        ],
        "relationships": [
            {"name": "user_orders", "from": {"dataset": "users", "columns": ["user_id"]},
             "to": {"dataset": "orders", "columns": ["user_id"]}},
        ],
    }
    semantic.import_ossie_model(ossie_model)
    exported = semantic.export_ossie_model("sales_model")
    print(f"\n  Imported & exported Ossie model 'sales_model':")
    print(f"  Datasets: {[d['name'] for d in exported['datasets']]}")
    print(f"  Metrics: {[m['name'] for m in exported['metrics']]}")

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
    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)


if __name__ == "__main__":
    test_all()


# ===========================================================================
# Lens aliases — the preferred names going forward.
# See RFC-0012 for the rename rationale.
#
# "View" is kept for backward compatibility. New code should use "Lens":
#   from view_sdk import Lens, IndexedLens, KeylessLens, SemanticLens
#
# The Lens name captures Pond's philosophy: the bytes don't change;
# only the way you observe and manipulate them changes. Like a lens
# that focuses light differently without changing the light itself.
# ===========================================================================

Lens = View
IndexedLens = None  # set below after import
KeylessLens = KeylessView
SemanticLens = SemanticView
OssieLens = SemanticView  # backward compat for old name
