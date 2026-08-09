// Semantic Model Adapter — translate between Pond and external semantic formats.
//
// Port of Python bindings/python/sdk/extensions/semantic/base.py
//
// A semantic model adapter translates between Pond's internal
// metric/dimension/relationship storage and an external semantic
// model standard (Cube, dbt, Malloy, etc.).
//
// The adapter does NOT own data — it translates. The Lens stores
// the definitions; the adapter converts them to/from external formats.
//
// TRAIT:
//   SemanticModelAdapter — defines the interface (export, import, validate)
//
// FUTURE ADAPTERS:
//   - CubeAdapter: Cube.js semantic model format
//   - DbtAdapter: dbt metrics format
//   - MalloyAdapter: Malloy model format
//
// Note: "Ossie" was a placeholder name in the Python code, not a real
// semantic model spec. It has been replaced with this clean trait.

use serde_json::Value;

/// Abstract interface for semantic model formats.
///
/// A semantic model adapter translates between Pond's internal
/// metric/dimension/relationship storage and an external semantic
/// model standard (Cube, dbt, Malloy, etc.).
///
/// The adapter does NOT own data — it translates. The Lens stores
/// the definitions; the adapter converts them to/from external formats.
pub trait SemanticModelAdapter: Send + Sync {
    /// Export the semantic definitions in this adapter's format.
    ///
    /// Returns a JSON value representing the model in the adapter's format.
    fn export_model(&self, definitions: &SemanticDefinitions) -> Value;

    /// Import an external-format semantic model into Pond definitions.
    ///
    /// Returns the parsed semantic definitions.
    fn import_model(&self, model: &Value) -> Result<SemanticDefinitions, String>;

    /// Validate that a model conforms to this adapter's format.
    fn validate_model(&self, model: &Value) -> bool;

    /// The name of this adapter (e.g., "cube", "dbt", "malloy").
    fn name(&self) -> &str;
}

/// Pond's internal semantic definitions.
///
/// Stored in a collection's metadata. Translated to/from external
/// formats by adapters.
#[derive(Debug, Clone, Default)]
pub struct SemanticDefinitions {
    /// Named metrics (e.g., "revenue", "active_users")
    pub metrics: Vec<Metric>,
    /// Named dimensions (e.g., "country", "date")
    pub dimensions: Vec<Dimension>,
    /// Named relationships between collections
    pub relationships: Vec<Relationship>,
}

/// A metric definition.
#[derive(Debug, Clone)]
pub struct Metric {
    pub name: String,
    pub description: String,
    pub expression: String,
    pub format: String,
}

/// A dimension definition.
#[derive(Debug, Clone)]
pub struct Dimension {
    pub name: String,
    pub description: String,
    pub data_type: String,
}

/// A relationship between collections.
#[derive(Debug, Clone)]
pub struct Relationship {
    pub name: String,
    pub from_collection: String,
    pub to_collection: String,
    pub join_type: String,
    pub join_condition: String,
}

impl SemanticDefinitions {
    /// Create empty semantic definitions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a metric.
    pub fn add_metric(&mut self, name: &str, expression: &str) -> &mut Self {
        self.metrics.push(Metric {
            name: name.to_string(),
            description: String::new(),
            expression: expression.to_string(),
            format: "number".to_string(),
        });
        self
    }

    /// Add a dimension.
    pub fn add_dimension(&mut self, name: &str, data_type: &str) -> &mut Self {
        self.dimensions.push(Dimension {
            name: name.to_string(),
            description: String::new(),
            data_type: data_type.to_string(),
        });
        self
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "metrics": self.metrics.iter().map(|m| serde_json::json!({
                "name": m.name,
                "description": m.description,
                "expression": m.expression,
                "format": m.format,
            })).collect::<Vec<_>>(),
            "dimensions": self.dimensions.iter().map(|d| serde_json::json!({
                "name": d.name,
                "description": d.description,
                "data_type": d.data_type,
            })).collect::<Vec<_>>(),
            "relationships": self.relationships.iter().map(|r| serde_json::json!({
                "name": r.name,
                "from": r.from_collection,
                "to": r.to_collection,
                "join_type": r.join_type,
                "condition": r.join_condition,
            })).collect::<Vec<_>>(),
        })
    }

    /// Deserialize from JSON.
    pub fn from_json(json: &Value) -> Self {
        let mut defs = Self::new();

        if let Some(metrics) = json.get("metrics").and_then(|m| m.as_array()) {
            for m in metrics {
                defs.metrics.push(Metric {
                    name: m.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    description: m.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    expression: m.get("expression").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    format: m.get("format").and_then(|v| v.as_str()).unwrap_or("number").to_string(),
                });
            }
        }

        if let Some(dimensions) = json.get("dimensions").and_then(|d| d.as_array()) {
            for d in dimensions {
                defs.dimensions.push(Dimension {
                    name: d.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    description: d.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    data_type: d.get("data_type").and_then(|v| v.as_str()).unwrap_or("string").to_string(),
                });
            }
        }

        if let Some(rels) = json.get("relationships").and_then(|r| r.as_array()) {
            for r in rels {
                defs.relationships.push(Relationship {
                    name: r.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    from_collection: r.get("from").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    to_collection: r.get("to").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    join_type: r.get("join_type").and_then(|v| v.as_str()).unwrap_or("inner").to_string(),
                    join_condition: r.get("condition").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                });
            }
        }

        defs
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_definitions() {
        let mut defs = SemanticDefinitions::new();
        defs.add_metric("revenue", "SUM(amount)")
            .add_dimension("country", "string");

        assert_eq!(defs.metrics.len(), 1);
        assert_eq!(defs.metrics[0].name, "revenue");
        assert_eq!(defs.dimensions.len(), 1);
        assert_eq!(defs.dimensions[0].name, "country");
    }

    #[test]
    fn test_json_roundtrip() {
        let mut defs = SemanticDefinitions::new();
        defs.add_metric("revenue", "SUM(amount)")
            .add_dimension("country", "string");

        let json = defs.to_json();
        let restored = SemanticDefinitions::from_json(&json);

        assert_eq!(restored.metrics.len(), 1);
        assert_eq!(restored.metrics[0].name, "revenue");
        assert_eq!(restored.dimensions.len(), 1);
        assert_eq!(restored.dimensions[0].name, "country");
    }

    #[test]
    fn test_empty_definitions() {
        let defs = SemanticDefinitions::new();
        let json = defs.to_json();
        let restored = SemanticDefinitions::from_json(&json);
        assert_eq!(restored.metrics.len(), 0);
        assert_eq!(restored.dimensions.len(), 0);
    }
}
