// engine_oltp.rs — an OLTP table as an index, with the memtable kept.
//
// # What changes and what does not
//
// The memtable stays. Buffering writes in memory and flushing them in a batch
// is exactly right on object storage, where the cost is the request and not
// the byte — it is the reason a 1000-row batch costs 0.02 PUTs per row where a
// single row costs 5.
//
// What changes is what a flush does, and what a read costs. The original lens
// keeps the table in one JSON object, so:
//
//   * flushing reads every row, applies the memtable, and writes every row
//     back — the change costs the size of the table;
//   * **`get` reads every row and filters**, so a point lookup is a full scan
//     even when the memtable answers most of them.
//
// Keying rows by the index makes a flush write only the rows that changed and
// a miss cost the tree's depth rather than the table's size.

use std::collections::HashMap;

use pond_kernel::PondKernel;
use pond_record::Value as RecValue;
use pond_storage::definition::{self, Format};
use pond_storage::engine_path;
use serde_json::Value;

const FIELD_VALUE: &str = "value";

/// Is this collection an engine-backed table?
pub fn is_engine_table(kernel: &PondKernel, collection: &str) -> bool {
    definition::format_of(kernel, collection) == Format::Engine
}

/// Create an engine-backed table.
pub fn create(kernel: &PondKernel, collection: &str) -> Result<(), String> {
    engine_path::create(kernel, collection)
}

/// Apply a memtable: `Some` writes the row, `None` deletes it.
///
/// Writes and deletes go out as two batches rather than row by row, so a flush
/// of a thousand changes is two publishes and not a thousand.
pub fn flush(
    kernel: &PondKernel,
    collection: &str,
    memtable: &HashMap<String, Option<Value>>,
    writer_id: u64,
) -> Result<usize, String> {
    if memtable.is_empty() {
        return Ok(0);
    }

    let mut puts: Vec<engine_path::NamedRow> = Vec::new();
    let mut deletes: Vec<String> = Vec::new();
    for (key, value) in memtable {
        match value {
            Some(v) => puts.push((
                key.clone(),
                vec![(FIELD_VALUE.to_string(), RecValue::Json(v.to_string()))],
            )),
            None => deletes.push(key.clone()),
        }
    }

    let applied = puts.len() + deletes.len();
    engine_path::put_string_keyed_rows(kernel, collection, &puts, writer_id)?;
    engine_path::delete_string_keyed_rows(kernel, collection, &deletes, writer_id)?;
    Ok(applied)
}

/// Read one row from storage. The memtable is the caller's to check first —
/// it is the newer of the two and cheaper to consult.
pub fn get(kernel: &PondKernel, collection: &str, key: &str) -> Result<Option<Value>, String> {
    let row = engine_path::get_string_keyed_row(kernel, collection, key)?;
    Ok(row.and_then(|fields| match fields.get(FIELD_VALUE) {
        Some(RecValue::Json(s)) | Some(RecValue::Str(s)) => {
            Some(serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone())))
        }
        _ => None,
    }))
}

/// Every key held in storage.
pub fn keys(kernel: &PondKernel, collection: &str) -> Result<Vec<String>, String> {
    let mut names: Vec<String> = engine_path::scan_string_keyed_rows(kernel, collection)?
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    names.sort();
    Ok(names)
}
