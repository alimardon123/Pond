// Ossie Semantic Adapter — one implementation of the semantic model interface.
//
// Port of Python bindings/python/sdk/extensions/semantic/ossie.py
//
// This extension provides:
//   - OssieAdapter: translates between Pond's internal semantic storage
//     and the Apache Ossie open semantic interchange spec.
//
// DESIGN: Semantic models are ORTHOGONAL to storage. A semantic model
// defines metrics/dimensions/relationships over data — the data itself
// can live in any collection. The adapter translates between Pond's
// internal SemanticDefinitions and the Ossie format.
//
// The Ossie format is:
//   {
//     "name": "model_name",
//     "datasets": [...],
//     "metrics": [
//       {"name": "revenue", "source": "orders", "field": "amount",
//        "expression": {"dialects": {"ANSI_SQL": "SUM(amount)"}},
//        "dimensions": ["product"]}
//     ],
//     "relationships": [
//       {"name": "user_orders", "from": {"dataset": "users", "columns": ["user_id"]},
//        "to": {"dataset": "orders", "columns": ["user_id"]}}
//     ]
//   }
//
// To add a DIFFERENT semantic standard (e.g., Cube.js), create a new
// adapter at extensions/semantic/cube/rust/ implementing SemanticModelAdapter.

use pond_semantic::{SemanticModelAdapter, SemanticDefinitions, Metric, Dimension, Relationship};
use serde_json::{Value, json};

/// Ossie adapter — translates between Pond's internal semantic definitions
/// and the Apache Ossie open semantic interchange format.
pub struct OssieAdapter;

impl OssieAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for OssieAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SemanticModelAdapter for OssieAdapter {
    fn name(&self) -> &str {
        "ossie"
    }

    /// Export semantic definitions in Ossie format.
    fn export_model(&self, defs: &SemanticDefinitions) -> Value {
        let metrics: Vec<Value> = defs.metrics.iter().map(|m| {
            json!({
                "name": m.name,
                "source": "",
                "field": m.expression,
                "expression": {"dialects": {"ANSI_SQL": m.expression}},
                "description": m.description,
                "format": m.format,
            })
        }).collect();

        let dimensions: Vec<Value> = defs.dimensions.iter().map(|d| {
            json!({
                "name": d.name,
                "source": "",
                "field": d.name,
                "type": d.data_type,
                "description": d.description,
            })
        }).collect();

        let relationships: Vec<Value> = defs.relationships.iter().map(|r| {
            json!({
                "name": r.name,
                "from": {"dataset": r.from_collection, "columns": []},
                "to": {"dataset": r.to_collection, "columns": []},
                "join_type": r.join_type,
                "condition": r.join_condition,
            })
        }).collect();

        json!({
            "name": "pond_model",
            "datasets": [],
            "metrics": metrics,
            "dimensions": dimensions,
            "relationships": relationships,
        })
    }

    /// Import an Ossie-format model into Pond definitions.
    fn import_model(&self, model: &Value) -> Result<SemanticDefinitions, String> {
        if !self.validate_model(model) {
            return Err("Invalid Ossie model: missing required keys (name, metrics, relationships)".to_string());
        }

        let mut defs = SemanticDefinitions::new();

        // Import metrics
        if let Some(metrics) = model.get("metrics").and_then(|m| m.as_array()) {
            for m in metrics {
                let name = m.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let expression = m.get("expression")
                    .and_then(|e| e.get("dialects"))
                    .and_then(|d| d.get("ANSI_SQL"))
                    .and_then(|v| v.as_str())
                    .or_else(|| m.get("field").and_then(|v| v.as_str()))
                    .unwrap_or("")
                    .to_string();
                let description = m.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let format = m.get("format").and_then(|v| v.as_str()).unwrap_or("number").to_string();

                defs.metrics.push(Metric { name: name.to_string(), description, expression, format });
            }
        }

        // Import dimensions
        if let Some(dimensions) = model.get("dimensions").and_then(|d| d.as_array()) {
            for d in dimensions {
                let name = d.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let data_type = d.get("type").and_then(|v| v.as_str()).unwrap_or("string").to_string();
                let description = d.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();

                defs.dimensions.push(Dimension { name: name.to_string(), description, data_type });
            }
        }

        // Import relationships
        if let Some(rels) = model.get("relationships").and_then(|r| r.as_array()) {
            for r in rels {
                let name = r.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let from = r.get("from").and_then(|f| f.get("dataset")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                let to = r.get("to").and_then(|t| t.get("dataset")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                let join_type = r.get("join_type").and_then(|v| v.as_str()).unwrap_or("inner").to_string();
                let join_condition = r.get("condition").and_then(|v| v.as_str()).unwrap_or("").to_string();

                defs.relationships.push(Relationship {
                    name, from_collection: from, to_collection: to, join_type, join_condition,
                });
            }
        }

        Ok(defs)
    }

    /// Validate that a model conforms to Ossie format.
    fn validate_model(&self, model: &Value) -> bool {
        let required_keys = ["name", "metrics", "relationships"];
        required_keys.iter().all(|k| model.get(k).is_some())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_model() {
        let mut defs = SemanticDefinitions::new();
        defs.add_metric("revenue", "SUM(amount)");
        defs.add_dimension("country", "string");

        let adapter = OssieAdapter::new();
        let model = adapter.export_model(&defs);

        assert_eq!(model["name"], "pond_model");
        assert!(model["metrics"].is_array());
        assert_eq!(model["metrics"].as_array().unwrap().len(), 1);
        assert_eq!(model["metrics"][0]["name"], "revenue");
        assert_eq!(model["metrics"][0]["expression"]["dialects"]["ANSI_SQL"], "SUM(amount)");
        assert_eq!(model["dimensions"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_import_model() {
        let model = json!({
            "name": "test_model",
            "datasets": [],
            "metrics": [
                {"name": "revenue", "source": "orders", "field": "amount",
                 "expression": {"dialects": {"ANSI_SQL": "SUM(amount)"}}},
            ],
            "relationships": [
                {"name": "user_orders", "from": {"dataset": "users", "columns": ["user_id"]},
                 "to": {"dataset": "orders", "columns": ["user_id"]}},
            ],
            "dimensions": [
                {"name": "product", "type": "string"},
            ],
        });

        let adapter = OssieAdapter::new();
        assert!(adapter.validate_model(&model));

        let defs = adapter.import_model(&model).unwrap();
        assert_eq!(defs.metrics.len(), 1);
        assert_eq!(defs.metrics[0].name, "revenue");
        assert_eq!(defs.metrics[0].expression, "SUM(amount)");
        assert_eq!(defs.dimensions.len(), 1);
        assert_eq!(defs.dimensions[0].name, "product");
        assert_eq!(defs.relationships.len(), 1);
        assert_eq!(defs.relationships[0].name, "user_orders");
    }

    #[test]
    fn test_roundtrip() {
        let mut defs = SemanticDefinitions::new();
        defs.add_metric("revenue", "SUM(amount)");
        defs.add_dimension("country", "string");

        let adapter = OssieAdapter::new();

        // Export → Import → verify
        let model = adapter.export_model(&defs);
        let restored = adapter.import_model(&model).unwrap();

        assert_eq!(restored.metrics.len(), 1);
        assert_eq!(restored.metrics[0].name, "revenue");
        assert_eq!(restored.dimensions.len(), 1);
        assert_eq!(restored.dimensions[0].name, "country");
    }

    #[test]
    fn test_validate_invalid_model() {
        let adapter = OssieAdapter::new();

        // Missing "metrics" key
        let bad = json!({"name": "test", "relationships": []});
        assert!(!adapter.validate_model(&bad));

        // Valid
        let good = json!({"name": "test", "metrics": [], "relationships": []});
        assert!(adapter.validate_model(&good));
    }

    #[test]
    fn test_adapter_name() {
        let adapter = OssieAdapter::new();
        assert_eq!(adapter.name(), "ossie");
    }

    #[test]
    fn test_import_invalid_returns_error() {
        let adapter = OssieAdapter::new();
        let bad = json!({"name": "test"});  // missing metrics, relationships
        let result = adapter.import_model(&bad);
        assert!(result.is_err());
    }
}
