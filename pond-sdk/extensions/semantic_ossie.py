"""
Apache Ossie Semantic Adapter — one implementation of the semantic model interface.

This extension provides:
  - OssieAdapter: translates between Pond's internal semantic storage
    and the Apache Ossie open semantic interchange spec.
  - SemanticLens: a Lens subclass with semantic model management
    (metrics, dimensions, relationships) + Ossie import/export.

Usage:
    from extensions.semantic_ossie import SemanticLens, OssieAdapter

    semantic = SemanticLens(kernel, "semantic")
    semantic.define_metric("revenue", "orders", "amount",
                           dialects={"ANSI_SQL": "SUM(amount)"})

To use a DIFFERENT semantic standard (e.g., Cube.js), create a new
adapter module (semantic_cube.py) implementing SemanticModelAdapter,
and use it with the same SemanticLens:

    from extensions.semantic_base import SemanticModelAdapter
    from extensions.semantic_ossie import SemanticLens

    class CubeAdapter(SemanticModelAdapter): ...
    semantic = SemanticLens(kernel, "semantic", adapter=CubeAdapter())

The Lens SDK core (lens_sdk.py) does NOT import this module. It is
loaded only when the application needs Ossie semantic models.
"""

from __future__ import annotations

import json
import time
import os
import sys
from typing import Optional, Any

# Make lens_sdk importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lens_sdk import Lens  # noqa: E402
from extensions.semantic_base import SemanticModelAdapter  # noqa: E402
from extensions import register_extension  # noqa: E402


class OssieAdapter(SemanticModelAdapter):
    """Apache Ossie adapter — one implementation of the semantic model interface.

    Translates between Pond's internal semantic storage and the Ossie
    core spec format (datasets, metrics, relationships, dimensions).
    """

    def export_model(self, lens: "Lens") -> dict:
        """Export semantic definitions in Ossie format."""
        state = lens.base.read_all()
        model = {"name": lens.name, "datasets": [], "metrics": [], "relationships": []}
        for key in state:
            if key.startswith("_semantic/metrics/"):
                model["metrics"].append(lens.decode(lens.kernel.read_blob(state[key])))
            elif key.startswith("_semantic/relationships/"):
                model["relationships"].append(lens.decode(lens.kernel.read_blob(state[key])))
        return model

    def import_model(self, lens: "Lens", model: dict) -> None:
        """Import an Ossie-format model into the Lens."""
        for metric in model.get("metrics", []):
            lens.put(f"_semantic/metrics/{metric['name']}", metric)
        for rel in model.get("relationships", []):
            lens.put(f"_semantic/relationships/{rel['name']}", rel)

    def validate_model(self, model: dict) -> bool:
        """Validate that a model dict conforms to Ossie format."""
        required_keys = {"name", "metrics", "relationships"}
        return required_keys.issubset(model.keys())


class SemanticLens(Lens):
    """
    A Lens that manages semantic models (metrics, dimensions, relationships).

    NOT coupled to any specific semantic model standard. Uses pluggable
    adapters:
      - OssieAdapter for Apache Ossie format (default)
      - CubeAdapter for Cube.js format (future)
      - DbtAdapter for dbt metrics (future)
      - Custom adapters via SemanticModelAdapter interface

    The Lens stores metric/dimension/relationship definitions as blobs.
    Adapters translate between the internal format and external standards.

    Usage:
        # Default (Ossie adapter)
        semantic = SemanticLens(kernel, "semantic")

        # Custom adapter
        from extensions.semantic_base import SemanticModelAdapter
        class MyAdapter(SemanticModelAdapter): ...
        semantic = SemanticLens(kernel, "semantic", adapter=MyAdapter())
    """

    def __init__(self, kernel, name: str, adapter: SemanticModelAdapter = None):
        super().__init__(kernel, name)
        self.adapter = adapter or OssieAdapter()

    # --- Metric management (standard-agnostic) ---

    def define_metric(self, name: str, source: str, field: str,
                      dialects: dict = None, dimensions: list = None,
                      ai_context: str = None) -> None:
        """Define a metric with multi-dialect expressions.

        dialects: {"ANSI_SQL": "SUM(amount)", "SNOWFLAKE": "SUM(amount)", ...}
        This is standard-agnostic — the adapter translates to the external format.
        """
        metric = {
            "name": name,
            "source": source,
            "field": field,
            "expression": {"dialects": dialects or {}},
            "dimensions": dimensions or [],
            "ai_context": ai_context,
            "created_at": time.time(),
        }
        self.put(f"_semantic/metrics/{name}", metric)

    def define_dimension(self, name: str, source: str, field: str,
                         dim_type: str = "string", is_time: bool = False,
                         ai_context: str = None) -> None:
        """Define a dimension."""
        dim = {
            "name": name,
            "source": source,
            "field": field,
            "type": dim_type,
            "dimension": {"is_time": is_time},
            "ai_context": ai_context,
        }
        self.put(f"_semantic/dimensions/{name}", dim)

    def define_relationship(self, name: str, from_dataset: str,
                            from_columns: list, to_dataset: str,
                            to_columns: list) -> None:
        """Define a relationship between datasets."""
        rel = {
            "name": name,
            "from": {"dataset": from_dataset, "columns": from_columns},
            "to": {"dataset": to_dataset, "columns": to_columns},
        }
        self.put(f"_semantic/relationships/{name}", rel)

    # --- Querying ---

    def list_metrics(self) -> list:
        state = self.base.read_all()
        return [k[len("_semantic/metrics/"):] for k in state
                if k.startswith("_semantic/metrics/")]

    def list_dimensions(self) -> list:
        state = self.base.read_all()
        return [k[len("_semantic/dimensions/"):] for k in state
                if k.startswith("_semantic/dimensions/")]

    def get_metric(self, name: str) -> Optional[dict]:
        return self.get(f"_semantic/metrics/{name}")

    def execute_metric(self, metric_name: str, source_lens: "Lens",
                       group_by: list = None) -> list:
        """Execute a metric query against a source Lens."""
        metric = self.get_metric(metric_name)
        if not metric:
            raise ValueError(f"Metric '{metric_name}' not found")
        data = source_lens.get_all()
        if not group_by:
            group_by = metric.get("dimensions", [])
        if not group_by:
            values = [row.get(metric["field"], 0) for row in data.values()]
            expr = metric.get("expression", {}).get("dialects", {})
            agg = "sum"
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

    # --- Adapter-specific import/export ---

    def import_model(self, model: dict) -> str:
        """Import a semantic model using the configured adapter."""
        self.adapter.import_model(self, model)
        # Store the full model blob for round-trip export
        model_bytes = json.dumps(model, sort_keys=True).encode()
        h = self.kernel.write(model_bytes)
        self.put_raw(f"_semantic/models/{model['name']}", h)
        return self.commit(f"import model '{model['name']}'")

    def export_model(self, model_name: str = None) -> Optional[dict]:
        """Export a semantic model using the configured adapter."""
        if model_name:
            h = self.base.lookup(f"_semantic/models/{model_name}")
            if not h:
                return None
            return json.loads(self.kernel.read_blob(h))
        return self.adapter.export_model(self)


# Register this extension
register_extension(
    "semantic_ossie",
    "extensions.semantic_ossie",
    {"SemanticLens": SemanticLens, "OssieAdapter": OssieAdapter}
)


# --- Self-test ---

def _self_test():
    """Quick test that the extension works standalone."""
    import tempfile, shutil
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pond-core"))
    from pond_minimal import PondMinimal

    tmpdir = tempfile.mkdtemp(prefix="pond_ossie_")
    try:
        kernel = PondMinimal(tmpdir)
        semantic = SemanticLens(kernel, "semantic")

        # Define metrics
        semantic.define_metric("revenue", "orders", "amount",
                               dialects={"ANSI_SQL": "SUM(amount)"},
                               dimensions=["product"])
        semantic.define_dimension("product", "orders", "product", "string")
        semantic.define_relationship("user_orders", "users", ["user_id"],
                                     "orders", ["user_id"])
        semantic.commit("define semantic model")

        assert "revenue" in semantic.list_metrics()
        assert "product" in semantic.list_dimensions()

        # Import/export
        model = {
            "name": "test_model",
            "metrics": [{"name": "count", "source": "orders", "field": "amount",
                         "expression": {"dialects": {"ANSI_SQL": "COUNT(*)"}}}],
            "relationships": [],
        }
        semantic.import_model(model)
        exported = semantic.export_model("test_model")
        assert exported["name"] == "test_model"

        # Test with custom adapter
        from extensions.semantic_base import SemanticModelAdapter

        class CustomAdapter(SemanticModelAdapter):
            def export_model(self, lens): return {"custom": True}
            def import_model(self, lens, model): pass
            def validate_model(self, model): return True

        custom_semantic = SemanticLens(kernel, "custom_sem", adapter=CustomAdapter())
        result = custom_semantic.export_model()
        assert result == {"custom": True}

        print("[OK] Ossie extension: all tests pass")
        print(f"  Metrics: {semantic.list_metrics()}")
        print(f"  Dimensions: {semantic.list_dimensions()}")
        print(f"  Custom adapter works: {result}")

        kernel.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    _self_test()
