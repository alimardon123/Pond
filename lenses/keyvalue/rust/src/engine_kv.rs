// engine_kv.rs — a key-value collection as an index, not a rewritten map.
//
// # What was wrong
//
// The original lens keeps the whole map in one JSON object. Two consequences,
// and the second is the worse one:
//
//   * `commit` reads every pair, applies the staged changes, and writes every
//     pair back — so a commit costs the size of the collection however small
//     the change.
//   * **`get` reads every pair and filters.** A point lookup — the one
//     operation a key-value store exists to do — is a full scan.
//
// # What replaces it
//
// The key is the index key. That makes `get` a descent: two or three requests
// whatever the collection holds, which is the whole reason the index is shaped
// the way it is. A commit writes only the pairs that changed, and a delete is
// a tombstone rather than a rewrite of everything that survived.
//
// Values are stored as JSON so any `serde_json::Value` round-trips, including
// the scalars the original lens wrapped in `{"value": ...}`.

use pond_kernel::PondKernel;
use pond_record::Value as RecValue;
use pond_storage::definition::{self, Format};
use pond_storage::engine_path;
use serde_json::Value;

/// Field holding the JSON-encoded value.
const FIELD_VALUE: &str = "value";

/// Is this collection an engine-backed key-value store?
pub fn is_engine_kv(kernel: &PondKernel, collection: &str) -> bool {
    definition::format_of(kernel, collection) == Format::Engine
}

/// Create an engine-backed key-value collection.
pub fn create(kernel: &PondKernel, collection: &str) -> Result<(), String> {
    engine_path::create(kernel, collection)
}

/// Write key-value pairs. Only the named keys are touched.
pub fn put_many(
    kernel: &PondKernel,
    collection: &str,
    pairs: &[(String, Value)],
    writer_id: u64,
) -> Result<(), String> {
    let rows: Vec<engine_path::NamedRow> = pairs
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                vec![(FIELD_VALUE.to_string(), RecValue::Json(v.to_string()))],
            )
        })
        .collect();
    engine_path::put_string_keyed_rows(kernel, collection, &rows, writer_id)
}

/// Read one value. A descent, not a scan.
pub fn get(kernel: &PondKernel, collection: &str, key: &str) -> Result<Option<Value>, String> {
    let row = engine_path::get_string_keyed_row(kernel, collection, key)?;
    Ok(row.and_then(|fields| decode_value(fields.get(FIELD_VALUE))))
}

/// Delete keys, leaving tombstones. Keys that were not present are still
/// counted, because a delete is idempotent — asking twice is not an error.
pub fn delete_many(
    kernel: &PondKernel,
    collection: &str,
    keys: &[String],
    writer_id: u64,
) -> Result<usize, String> {
    engine_path::delete_string_keyed_rows(kernel, collection, keys, writer_id)
}

/// Every pair in the collection.
pub fn get_all(kernel: &PondKernel, collection: &str) -> Result<Vec<(String, Value)>, String> {
    Ok(engine_path::scan_string_keyed_rows(kernel, collection)?
        .into_iter()
        .filter_map(|(key, fields)| {
            let value = fields
                .iter()
                .find(|(n, _)| n == FIELD_VALUE)
                .map(|(_, v)| v.clone());
            decode_value(value.as_ref()).map(|v| (key, v))
        })
        .collect())
}

/// Every key.
pub fn keys(kernel: &PondKernel, collection: &str) -> Result<Vec<String>, String> {
    Ok(engine_path::scan_string_keyed_rows(kernel, collection)?
        .into_iter()
        .map(|(k, _)| k)
        .collect())
}

/// Does this key exist? A point lookup, like `get`.
pub fn exists(kernel: &PondKernel, collection: &str, key: &str) -> Result<bool, String> {
    Ok(engine_path::get_string_keyed_row(kernel, collection, key)?.is_some())
}

/// How many pairs the collection holds.
pub fn count(kernel: &PondKernel, collection: &str) -> Result<usize, String> {
    Ok(engine_path::scan_string_keyed_rows(kernel, collection)?.len())
}

/// Turn a stored field back into the JSON the caller put in.
///
/// Anything that is not valid JSON is surfaced as a string rather than
/// discarded — losing a value because it cannot be parsed would be worse than
/// returning it in a shape the caller can inspect.
fn decode_value(field: Option<&RecValue>) -> Option<Value> {
    match field? {
        RecValue::Json(s) | RecValue::Str(s) => {
            Some(serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone())))
        }
        RecValue::Int(i) => Some(Value::Number((*i).into())),
        RecValue::Bool(b) => Some(Value::Bool(*b)),
        RecValue::F64(f) => serde_json::Number::from_f64(*f).map(Value::Number),
        RecValue::Null => Some(Value::Null),
        _ => None,
    }
}
