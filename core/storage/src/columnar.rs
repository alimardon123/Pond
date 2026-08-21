// columnar.rs — the translation between columns and records.
//
// The engine stores records: a key, and a set of named values each carrying
// its own version. Every existing caller in this repo speaks columns. Neither
// representation is more correct than the other — the data is the same data —
// so this is a translation layer, not a conversion, and the property it must
// have is that going through it and back changes nothing.
//
// Two things make that non-trivial, and both are handled here rather than
// being discovered later:
//
//   1. **Types collapse.** A date, a timestamp and an integer are all `i64`
//      once they are values. The declared column type in the collection
//      definition is what distinguishes them on the way back, which is why the
//      definition carries a schema and not just a format tag.
//
//   2. **Rows need identity.** A column layout has no notion of *which* row is
//      which. Position is the only handle it offers, and position is not an
//      identity: the second write to a collection starts again at row 0, so
//      keying by ordinal makes every write overwrite the one before it. The
//      legacy path already generates `_rowid` (UUIDv7) per row for exactly
//      this reason, and this path does the same — a supplied `_rowid` names
//      the row, and its absence means "these are new rows", which is what an
//      insert is.

use std::collections::BTreeMap;

use pond_core::constants::{
    VT_BINARY, VT_BOOLEAN, VT_DATE, VT_FLOAT64, VT_INT64, VT_STRING, VT_TIMESTAMP, VT_VARIANT,
    VT_VECTOR,
};
use pond_core::encode::TypedColumn;
use pond_index::{str_, Key};
use pond_record::{Record, Value, Version};

use crate::definition::Definition;

/// Column that carries a row's identity, when the writer supplied one.
pub const ROWID: &str = "_rowid";
/// Column that carries a row's version, when the writer supplied one.
pub const VERSION: &str = "_version";

/// The PND2 type a `TypedColumn` declares.
pub fn vtype_of(col: &TypedColumn) -> u8 {
    col.vtype()
}

/// Turn columns into keyed records.
///
/// `writer_id` participates in every field version, so two writers that touch
/// the same field at the same instant still order deterministically rather
/// than by chance.
pub fn columns_to_records(
    columns: &[(&str, TypedColumn)],
    writer_id: u64,
    physical: u64,
) -> Vec<(Key, Record)> {
    columns_to_records_with_nulls(columns, &[], writer_id, physical)
}

/// As [`columns_to_records`], but honouring a per-column null mask.
///
/// A dense column cannot say "no value here", so without the mask a null and
/// the type's zero are the same bytes. They are not the same fact — a score of
/// zero and an unrecorded score are different things — so the mask is what
/// lets the record carry [`Value::Null`] instead of a fabricated zero.
pub fn columns_to_records_with_nulls(
    columns: &[(&str, TypedColumn)],
    nulls: &[Option<Vec<bool>>],
    writer_id: u64,
    physical: u64,
) -> Vec<(Key, Record)> {
    let n_rows = columns.first().map(|(_, c)| c.len()).unwrap_or(0);
    let mut out = Vec::with_capacity(n_rows);

    for row in 0..n_rows {
        let mut record = Record::new();
        let mut rowid: Option<String> = None;

        for (idx, (name, col)) in columns.iter().enumerate() {
            let is_null = nulls
                .get(idx)
                .and_then(|m| m.as_ref())
                .and_then(|m| m.get(row))
                .copied()
                .unwrap_or(false);
            let value = if is_null {
                Value::Null
            } else {
                match value_at(col, row) {
                    Some(v) => v,
                    None => continue,
                }
            };
            if *name == ROWID {
                if let Value::Str(s) = &value {
                    rowid = Some(s.clone());
                }
            }
            // The logical counter is the row index, so two rows written in the
            // same batch never collide on version while still ordering after
            // anything from an earlier batch.
            record = record.with_field(name, value, Version::new(physical, row as u64, writer_id));
        }

        // A supplied `_rowid` is the row's identity, across writers and across
        // rewrites — write the same id again and you have updated that row.
        //
        // Without one, the row is new. Generating an id rather than falling
        // back to the ordinal is not a detail: ordinals restart at zero on
        // every write, so keying by them would make each write silently
        // replace the previous one row for row. The generated id is stored on
        // the record, so it survives the round trip and a later update can
        // name the row it means.
        let key = match rowid {
            Some(id) => Key::new(vec![str_(id)]),
            None => {
                let id = pond_kernel::crdt::uuidv7();
                record = record.with_field(
                    ROWID,
                    Value::Str(id.clone()),
                    Version::new(physical, row as u64, writer_id),
                );
                Key::new(vec![str_(id)])
            }
        };
        out.push((key, record));
    }
    out
}

/// Turn records back into columns, using the definition to restore the
/// declared type of each column.
///
/// Column order follows the definition, so a round trip preserves it. Columns
/// the definition does not know about are appended in name order — a lens that
/// wrote a column the schema has not caught up with must still see it come
/// back, not silently lose it.
pub fn records_to_columns(
    records: &[(Key, Record)],
    def: &Definition,
) -> Vec<(String, TypedColumn)> {
    records_to_columns_with_nulls(records, def).0
}

/// As [`records_to_columns`], and also the null mask each column needs.
///
/// A field holding [`Value::Null`], and a field a record does not carry at
/// all, are both null to a reader: the column is dense, so something has to
/// occupy the slot, and the mask is what says the occupant is a placeholder.
#[allow(clippy::type_complexity)]
pub fn records_to_columns_with_nulls(
    records: &[(Key, Record)],
    def: &Definition,
) -> (Vec<(String, TypedColumn)>, Vec<Option<Vec<bool>>>) {
    // Every column name present anywhere, in a stable order.
    let mut names: Vec<String> = def.columns.iter().map(|(n, _)| n.clone()).collect();
    let mut extra: Vec<String> = Vec::new();
    for (_, rec) in records {
        for name in rec.fields.keys() {
            if !names.contains(name) && !extra.contains(name) {
                extra.push(name.clone());
            }
        }
    }
    extra.sort();
    names.extend(extra);

    let mut columns = Vec::new();
    let mut masks = Vec::new();
    for name in names {
        let values: Vec<Option<&Value>> = records.iter().map(|(_, r)| r.get(&name)).collect();
        // A column no record carries is not a column.
        if values.iter().all(|v| v.is_none()) {
            continue;
        }
        // Pick the declared type, or infer from the first value that is
        // neither absent nor null — a null carries no type information.
        let Some(declared) = def.column_type(&name).or_else(|| {
            values
                .iter()
                .flatten()
                .find(|v| !matches!(v, Value::Null))
                .map(|v| default_vtype(v))
        }) else {
            continue;
        };
        let Some(col) = column_from_values(&values, declared) else {
            continue;
        };
        let mask: Vec<bool> = values
            .iter()
            .map(|v| matches!(v, None | Some(Value::Null)))
            .collect();
        columns.push((name, col));
        masks.push(if mask.iter().any(|b| *b) {
            Some(mask)
        } else {
            None
        });
    }
    (columns, masks)
}

/// The value at one row of a column, or `None` if the row is past its end.
fn value_at(col: &TypedColumn, row: usize) -> Option<Value> {
    match col {
        TypedColumn::Int64(v) => v.get(row).map(|x| Value::Int(*x)),
        TypedColumn::Date(v) => v.get(row).map(|x| Value::Int(*x)),
        TypedColumn::Timestamp(v) => v.get(row).map(|x| Value::Int(*x)),
        TypedColumn::Float64(v) => v.get(row).map(|x| Value::F64(*x)),
        TypedColumn::String(v) => v.get(row).map(|x| Value::Str(x.clone())),
        TypedColumn::Variant(v) => v.get(row).map(|x| Value::Json(x.clone())),
        TypedColumn::Binary(v) => v.get(row).map(|x| Value::Bytes(x.clone())),
        TypedColumn::Boolean(v) => v.get(row).map(|x| Value::Bool(*x)),
        TypedColumn::Vector(v) => v.get(row).map(|x| Value::Vector(x.clone())),
    }
}

/// The column type a value maps to when nothing declared one.
fn default_vtype(v: &Value) -> u8 {
    match v {
        Value::Int(_) => VT_INT64,
        Value::F64(_) => VT_FLOAT64,
        Value::Str(_) => VT_STRING,
        Value::Json(_) => VT_VARIANT,
        Value::Bytes(_) => VT_BINARY,
        Value::Bool(_) => VT_BOOLEAN,
        Value::Vector(_) => VT_VECTOR,
        Value::Null => VT_STRING,
    }
}

/// Rebuild one column of the declared type from per-row values.
///
/// A row missing this column gets the type's zero value. PND2 columns are
/// dense, so there is no way to say "absent" other than by carrying a null
/// bitmap, which the encoder does not accept on this path — the zero is
/// therefore explicit and documented rather than silent.
fn column_from_values(values: &[Option<&Value>], vtype: u8) -> Option<TypedColumn> {
    match vtype {
        VT_INT64 | VT_DATE | VT_TIMESTAMP => {
            let v: Vec<i64> = values
                .iter()
                .map(|x| match x {
                    Some(Value::Int(i)) => *i,
                    Some(Value::F64(f)) => *f as i64,
                    Some(Value::Bool(b)) => *b as i64,
                    _ => 0,
                })
                .collect();
            Some(match vtype {
                VT_DATE => TypedColumn::Date(v),
                VT_TIMESTAMP => TypedColumn::Timestamp(v),
                _ => TypedColumn::Int64(v),
            })
        }
        VT_FLOAT64 => Some(TypedColumn::Float64(
            values
                .iter()
                .map(|x| match x {
                    Some(Value::F64(f)) => *f,
                    Some(Value::Int(i)) => *i as f64,
                    _ => 0.0,
                })
                .collect(),
        )),
        VT_STRING => Some(TypedColumn::String(
            values
                .iter()
                .map(|x| match x {
                    Some(Value::Str(s)) => s.clone(),
                    Some(Value::Json(s)) => s.clone(),
                    Some(other) => scalar_to_string(other),
                    None => String::new(),
                })
                .collect(),
        )),
        VT_VARIANT => Some(TypedColumn::Variant(
            values
                .iter()
                .map(|x| match x {
                    Some(Value::Json(s)) | Some(Value::Str(s)) => s.clone(),
                    Some(other) => scalar_to_string(other),
                    None => String::new(),
                })
                .collect(),
        )),
        VT_BINARY => Some(TypedColumn::Binary(
            values
                .iter()
                .map(|x| match x {
                    Some(Value::Bytes(b)) => b.clone(),
                    _ => Vec::new(),
                })
                .collect(),
        )),
        VT_BOOLEAN => Some(TypedColumn::Boolean(
            values
                .iter()
                .map(|x| matches!(x, Some(Value::Bool(true))))
                .collect(),
        )),
        VT_VECTOR => Some(TypedColumn::Vector(
            values
                .iter()
                .map(|x| match x {
                    Some(Value::Vector(v)) => v.clone(),
                    _ => Vec::new(),
                })
                .collect(),
        )),
        _ => None,
    }
}

fn scalar_to_string(v: &Value) -> String {
    match v {
        Value::Int(i) => i.to_string(),
        Value::F64(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Str(s) | Value::Json(s) => s.clone(),
        Value::Null => String::new(),
        Value::Bytes(_) | Value::Vector(_) => String::new(),
    }
}

/// The schema a set of columns declares.
pub fn schema_of(columns: &[(&str, TypedColumn)]) -> Vec<(String, u8)> {
    columns
        .iter()
        .map(|(n, c)| (n.to_string(), vtype_of(c)))
        .collect()
}

/// Column values as a map, for tests and for callers that want row shape.
pub fn record_to_map(record: &Record) -> BTreeMap<String, Value> {
    record
        .fields
        .iter()
        .map(|(k, f)| (k.clone(), f.value.clone()))
        .collect()
}
