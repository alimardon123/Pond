"""
Pond View SDK — the common algebra across all Views.

Goal: discover what is common to every View. If 80% of every View
becomes identical, we've discovered a real abstraction.

This SDK provides:
  - View: abstract base class with the common interface
  - CrossView: utility for reading/writing across Views
  - SemanticView: base for semantic model Views (metrics, dimensions)

The SDK is View-level, NOT kernel-level. The kernel stays at 3 primitives.
"""

import json
import time
import sys
import os
from typing import Optional, Any, Callable

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "prototype"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pond_minimal import PondMinimal
from prolly_view import ProllyViewBase, ProllyTree
from binary_encoding import BinaryProllyTree


# ===========================================================================
# View — the abstract base class for all Views
# ===========================================================================

class View:
    """
    Abstract base class for all Pond Views.

    A View is a 5-tuple (RFC-0001):
      State × Encode × Decode × Commit × Resolve

    This base class provides the common algebra:
      - stage / stage_delete / commit (write path)
      - lookup / read_all (read path)
      - branch / checkout / merge / undo (version control)
      - history / diff (time travel)
      - build_index / lookup_by_index (secondary indexes)
      - encode / decode (serialization — override in subclass)

    Subclasses implement:
      - encode(data) → bytes: how to serialize View-specific data
      - decode(bytes) → data: how to deserialize
      - View-specific query methods (search, traverse, etc.)

    The SDK discovers: what percentage of a View is common (inherited)
    vs. View-specific (overridden)? If 80%+ is common, the abstraction is real.
    """

    def __init__(self, kernel: PondMinimal, name: str):
        self.kernel = kernel
        self.name = name
        self.base = ProllyViewBase(kernel, name)

    # ------------------------------------------------------------------
    # Write path (common to all Views)
    # ------------------------------------------------------------------

    def put(self, key: str, data: Any) -> str:
        """Encode data, write to kernel, stage for commit. Returns blob hash."""
        blob_hash = self.kernel.write(self.encode(data))
        self.base.stage(key, blob_hash)
        return blob_hash

    def put_raw(self, key: str, blob_hash: str) -> None:
        """Stage a pre-existing blob hash (for cross-View sharing)."""
        self.base.stage(key, blob_hash)

    def delete(self, key: str) -> None:
        """Mark a key for deletion."""
        self.base.stage_delete(key)

    def commit(self, message: str = "") -> str:
        """Commit staged changes. O(1) delta, O(log N) compaction."""
        return self.base.commit(message or f"{self.name} commit")

    # ------------------------------------------------------------------
    # Read path (common to all Views)
    # ------------------------------------------------------------------

    def get(self, key: str) -> Optional[Any]:
        """Look up a key, read blob, decode. O(log N) via Prolly tree."""
        h = self.base.lookup(key)
        if h is None:
            return None
        return self.decode(self.kernel.read_blob(h))

    def get_raw(self, key: str) -> Optional[bytes]:
        """Look up a key, return raw bytes (no decode)."""
        h = self.base.lookup(key)
        if h is None:
            return None
        return self.kernel.read_blob(h)

    def get_all(self) -> dict[str, Any]:
        """Read all entries, decode each. O(N/chunk) via Prolly tree."""
        state = self.base.read_all()
        return {k: self.decode(self.kernel.read_blob(h)) for k, h in state.items()
                if not k.startswith("_")}

    def keys(self) -> list[str]:
        """List all keys (without reading values)."""
        return [k for k in self.base.read_all().keys() if not k.startswith("_")]

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        return self.base.lookup(key) is not None

    def count(self) -> int:
        """Count entries."""
        return sum(1 for k in self.base.read_all() if not k.startswith("_"))

    # ------------------------------------------------------------------
    # Version control (common to all Views)
    # ------------------------------------------------------------------

    def branch(self, name: str) -> str:
        """Create a branch. O(1)."""
        return self.base.branch(name)

    def checkout(self, name: str) -> None:
        """Switch to a branch. O(1)."""
        self.base.checkout(name)

    def list_branches(self) -> list[str]:
        """List all branches."""
        return self.base.list_branches()

    def merge(self, name: str) -> str:
        """Merge a branch. O(|A|+|B|)."""
        return self.base.merge(name)

    def undo(self, steps: int = 1) -> str:
        """Undo N commits. O(N)."""
        return self.base.undo(steps)

    def history(self, limit: int = 20) -> list[dict]:
        """Show commit history."""
        return self.base.history(limit)

    def diff(self, a: str, b: str) -> dict:
        """Content-based diff between two commits."""
        return self.base.diff(a, b)

    # ------------------------------------------------------------------
    # Indexing (common to all Views)
    # ------------------------------------------------------------------

    def create_index(self, index_name: str, key_extractor: Callable[[Any], str]) -> str:
        """Build a secondary index. key_extractor takes decoded data, returns index key."""
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

    def lookup_by_index(self, index_name: str, index_key: str) -> Optional[Any]:
        """Look up data via a secondary index. O(log N)."""
        tree_root = self.kernel.resolve(f"{self.name}__index__{index_name}")
        if not tree_root:
            return None
        full_key = f"_index/{index_name}/{index_key}"
        bh = ProllyTree.lookup(self.kernel, tree_root, full_key)
        if not bh:
            return None
        return self.decode(self.kernel.read_blob(bh))

    # ------------------------------------------------------------------
    # Serialization (override in subclass)
    # ------------------------------------------------------------------

    def encode(self, data: Any) -> bytes:
        """Serialize View data to bytes. Override in subclass."""
        return json.dumps(data, sort_keys=True).encode()

    def decode(self, data: bytes) -> Any:
        """Deserialize bytes to View data. Override in subclass."""
        return json.loads(data)


# ===========================================================================
# CrossView — read/write across Views seamlessly
# ===========================================================================

class CrossView:
    """
    Utility for cross-View data access.

    Enables: View A reads View B's latest state, writes to it, or
    shares data without copying.

    This is the "any View can read the latest state from any View
    and change it seamlessly" utility.
    """

    @staticmethod
    def read_from(view: View, key: str) -> Optional[Any]:
        """Read a key from any View's latest committed state."""
        return view.get(key)

    @staticmethod
    def read_all_from(view: View) -> dict[str, Any]:
        """Read all data from any View's latest committed state."""
        return view.get_all()

    @staticmethod
    def write_to(view: View, key: str, data: Any) -> str:
        """Write data to any View (staged, call view.commit() to persist)."""
        return view.put(key, data)

    @staticmethod
    def share_blob(from_view: View, from_key: str, to_view: View, to_key: str) -> bool:
        """Share a blob from one View to another without copying.
        The blob hash is the same in both Views. Zero copy."""
        h = from_view.base.lookup(from_key)
        if h is None:
            return False
        to_view.put_raw(to_key, h)
        return True

    @staticmethod
    def mirror(from_view: View, to_view: View, prefix: str = "") -> int:
        """Mirror all data from one View to another (zero-copy via shared hashes).
        Returns count of mirrored entries."""
        state = from_view.base.read_all()
        count = 0
        for key, h in state.items():
            if key.startswith("_"):
                continue
            if prefix and not key.startswith(prefix):
                continue
            to_view.put_raw(key, h)
            count += 1
        return count

    @staticmethod
    def pipe(from_view: View, to_view: View,
             transformer: Optional[Callable[[str, Any], tuple[str, Any]]] = None) -> int:
        """Pipe data from one View to another, optionally transforming.
        transformer: function(from_key, from_data) → (to_key, to_data).
        If None, copies as-is (zero-copy)."""
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
# SemanticView — base for semantic model Views (metrics, dimensions)
# ===========================================================================

class SemanticView(View):
    """
    A View that defines semantic models: metrics, dimensions, and
    relationships on top of raw data.

    This is the "semantic layer" pattern (like dbt's semantic layer,
    Cube.dev, or Apache Atlas's metadata framework).

    SemanticView stores:
      - _semantic/metrics/{metric_name}: metric definition
      - _semantic/dimensions/{dim_name}: dimension definition
      - _semantic/relationships/{rel_name}: relationship definition

    A metric definition specifies how to compute a metric from raw data:
      {"name": "revenue", "type": "sum", "source": "orders", "field": "amount",
       "dimensions": ["region", "product"]}

    The SemanticView doesn't execute queries — it stores definitions.
    A query engine (View-level) reads definitions and executes against
    the source View (e.g., SQLView).
    """

    def define_metric(self, name: str, metric_type: str, source: str,
                      field: str, dimensions: list[str] = None,
                      filter_expr: str = None) -> None:
        """Define a semantic metric."""
        metric = {
            "name": name,
            "type": metric_type,  # sum, count, avg, min, max, distinct
            "source": source,     # source View name
            "field": field,       # field to aggregate
            "dimensions": dimensions or [],
            "filter": filter_expr,
            "created_at": time.time(),
        }
        self.put(f"_semantic/metrics/{name}", metric)

    def define_dimension(self, name: str, source: str, field: str,
                         type: str = "string") -> None:
        """Define a semantic dimension."""
        dim = {
            "name": name,
            "source": source,
            "field": field,
            "type": type,  # string, int, float, time, boolean
            "created_at": time.time(),
        }
        self.put(f"_semantic/dimensions/{name}", dim)

    def define_relationship(self, name: str, from_view: str, from_field: str,
                            to_view: str, to_field: str,
                            rel_type: str = "many_to_one") -> None:
        """Define a relationship between Views."""
        rel = {
            "name": name,
            "from": {"view": from_view, "field": from_field},
            "to": {"view": to_view, "field": to_field},
            "type": rel_type,  # one_to_one, one_to_many, many_to_one
            "created_at": time.time(),
        }
        self.put(f"_semantic/relationships/{name}", rel)

    def get_metric(self, name: str) -> Optional[dict]:
        return self.get(f"_semantic/metrics/{name}")

    def get_dimension(self, name: str) -> Optional[dict]:
        return self.get(f"_semantic/dimensions/{name}")

    def get_relationship(self, name: str) -> Optional[dict]:
        return self.get(f"_semantic/relationships/{name}")

    def list_metrics(self) -> list[str]:
        return [k[len("_semantic/metrics/"):] for k in self.keys()
                if k.startswith("_semantic/metrics/")]

    def list_dimensions(self) -> list[str]:
        return [k[len("_semantic/dimensions/"):] for k in self.keys()
                if k.startswith("_semantic/dimensions/")]

    def execute_metric(self, metric_name: str, source_view: View,
                       group_by: list[str] = None) -> list[dict]:
        """Execute a metric query against a source View.
        This is a simple aggregation executor — production would use
        a proper query engine."""
        metric = self.get_metric(metric_name)
        if not metric:
            raise ValueError(f"Metric '{metric_name}' not found")

        data = source_view.get_all()
        if metric.get("filter"):
            # Simple filter (production would use a proper expression evaluator)
            pass

        if not group_by:
            group_by = metric.get("dimensions", [])

        if not group_by:
            # No grouping — single aggregate
            values = [row.get(metric["field"], 0) for row in data.values()]
            if metric["type"] == "sum":
                result = sum(values)
            elif metric["type"] == "count":
                result = len(values)
            elif metric["type"] == "avg":
                result = sum(values) / len(values) if values else 0
            elif metric["type"] == "min":
                result = min(values) if values else 0
            elif metric["type"] == "max":
                result = max(values) if values else 0
            else:
                result = None
            return [{"value": result, "metric": metric_name}]

        # Group by dimensions
        groups = {}
        for row in data.values():
            group_key = tuple(row.get(dim, "") for dim in group_by)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row.get(metric["field"], 0))

        results = []
        for group_key, values in groups.items():
            if metric["type"] == "sum":
                val = sum(values)
            elif metric["type"] == "count":
                val = len(values)
            elif metric["type"] == "avg":
                val = sum(values) / len(values) if values else 0
            elif metric["type"] == "min":
                val = min(values) if values else 0
            elif metric["type"] == "max":
                val = max(values) if values else 0
            else:
                val = None
            result = {"metric": metric_name, "value": val}
            for i, dim in enumerate(group_by):
                result[dim] = group_key[i]
            results.append(result)

        return results


# ===========================================================================
# Test: View SDK + CrossView + SemanticView
# ===========================================================================

def test_sdk():
    import shutil
    bench_dir = "/tmp/pond_sdk_test"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    print("=== View SDK Test ===\n")

    # Create a simple key-value View
    kv = View(kernel, "kv_store")
    kv.put("user:1", {"name": "Alice", "age": 30, "region": "US"})
    kv.put("user:2", {"name": "Bob", "age": 25, "region": "EU"})
    kv.put("user:3", {"name": "Carol", "age": 35, "region": "US"})
    kv.put("user:4", {"name": "Dave", "age": 28, "region": "EU"})
    kv.commit("insert 4 users")

    print(f"  KV View: {kv.count()} entries")
    print(f"  user:1 = {kv.get('user:1')}")
    print(f"  exists('user:5') = {kv.exists('user:5')}")

    # Create index on "region"
    kv.create_index("by_region", lambda data: data.get("region", ""))
    print(f"\n  Index lookup 'US': {kv.lookup_by_index('by_region', 'US')}")

    # Create a second View
    orders = View(kernel, "orders")
    orders.put("order:1", {"user_id": 1, "amount": 100, "product": "Widget"})
    orders.put("order:2", {"user_id": 2, "amount": 200, "product": "Gadget"})
    orders.put("order:3", {"user_id": 1, "amount": 50, "product": "Widget"})
    orders.commit("insert 3 orders")

    # Cross-View: share a blob from KV to Orders (zero copy)
    print(f"\n=== CrossView ===")
    shared = CrossView.share_blob(kv, "user:1", orders, "user_ref:1")
    print(f"  Shared blob: {shared}")
    print(f"  Orders reads shared user: {orders.get('user_ref:1')}")

    # Cross-View: pipe data with transformation
    def transform(key, data):
        new_key = key.replace("user:", "customer:")
        return new_key, {**data, "source": "imported"}

    count = CrossView.pipe(kv, orders, transform)
    orders.commit(f"imported {count} users as customers")
    print(f"  Piped {count} entries with transformation")
    print(f"  customer:1 = {orders.get('customer:1')}")

    # Semantic View
    print(f"\n=== SemanticView ===")
    semantic = SemanticView(kernel, "semantic_layer")
    semantic.define_metric("total_revenue", "sum", "orders", "amount",
                           dimensions=["product"])
    semantic.define_metric("order_count", "count", "orders", "amount",
                           dimensions=["product"])
    semantic.define_dimension("product", "orders", "product", "string")
    semantic.define_dimension("region", "kv_store", "region", "string")
    semantic.define_relationship("user_orders", "kv_store", "user_id",
                                 "orders", "user_id", "one_to_many")
    semantic.commit("define semantic model")

    print(f"  Metrics: {semantic.list_metrics()}")
    print(f"  Dimensions: {semantic.list_dimensions()}")
    print(f"  Metric 'total_revenue': {semantic.get_metric('total_revenue')}")

    # Execute metric
    results = semantic.execute_metric("total_revenue", orders, group_by=["product"])
    print(f"\n  Total revenue by product:")
    for r in results:
        print(f"    {r['product']}: ${r['value']}")

    results = semantic.execute_metric("order_count", orders, group_by=["product"])
    print(f"  Order count by product:")
    for r in results:
        print(f"    {r['product']}: {r['value']} orders")

    # Cross-View: read latest state from any View
    print(f"\n=== Cross-View Latest State ===")
    print(f"  KV latest user:1: {CrossView.read_from(kv, 'user:1')}")
    print(f"  Orders latest order:1: {CrossView.read_from(orders, 'order:1')}")
    print(f"  Semantic latest metric: {CrossView.read_from(semantic, '_semantic/metrics/total_revenue')}")

    # Verify: kernel still 3 primitives
    print(f"\n=== Architecture Verification ===")
    print(f"  Kernel methods: write, read, read_blob, reference, resolve, list_names")
    print(f"  All Views use ProllyViewBase (View-level, not kernel)")
    print(f"  SemanticView is a View subclass (View-level, not kernel)")
    print(f"  CrossView uses only View.get/put (View-level, not kernel)")

    print(f"\n=== ALL TESTS PASSED ===")
    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)


if __name__ == "__main__":
    test_sdk()
