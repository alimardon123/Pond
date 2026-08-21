// to_json.rs — the one conversion from decoded columns to JSON rows.
//
// This exists because there were three of them: one in the CLI, one in the SQL
// executor, one in the Python bindings. Three implementations of the same
// conversion drift, and they did — when per-value nulls were added to the
// format, the CLI learned to emit `null` and the other two kept returning the
// type's zero, so `SELECT` disagreed with `read-rows` about the same row.
//
// A single implementation makes that class of divergence impossible rather
// than merely fixed.

use serde_json::{Map, Value as JsonValue};

use crate::constants::{VT_BINARY, VT_BOOLEAN, VT_FLOAT64, VT_INT64, VT_STRING, VT_VARIANT};
use crate::decode::is_null_at;
use crate::types::PondColumn;

/// Column names that are storage bookkeeping rather than user data.
pub const INTERNAL_COLUMNS: [&str; 3] = ["_rowid", "_version", "_deleted"];

/// One column's value at one row, as JSON.
///
/// A null is reported as `JsonValue::Null` — the column is dense, so the slot
/// holds the type's zero, and the bitmap is the only thing that distinguishes
/// that placeholder from a value the caller wrote.
pub fn column_value_to_json(col: &PondColumn, row: usize) -> JsonValue {
    if col
        .null_bitmap
        .as_ref()
        .is_some_and(|b| is_null_at(b, row))
    {
        return JsonValue::Null;
    }

    match col.vtype {
        VT_INT64 => col
            .i64_data
            .get(row)
            .map(|v| JsonValue::Number((*v).into()))
            .unwrap_or(JsonValue::Null),
        VT_BOOLEAN => col
            .i64_data
            .get(row)
            .map(|v| JsonValue::Bool(*v != 0))
            .unwrap_or(JsonValue::Null),
        VT_FLOAT64 => col
            .f64_data
            .get(row)
            .and_then(|v| serde_json::Number::from_f64(*v))
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        VT_STRING => col
            .str_data
            .get(row)
            .map(|v| JsonValue::String(v.to_string_lossy().into_owned()))
            .unwrap_or(JsonValue::Null),
        VT_VARIANT => col
            .str_data
            .get(row)
            .and_then(|s| serde_json::from_str::<JsonValue>(&s.to_string_lossy()).ok())
            .unwrap_or(JsonValue::Null),
        VT_BINARY => col
            .bin_data
            .get(row)
            .map(|b| JsonValue::String(format!("__bin_b64__:{}", base64_encode(b))))
            .unwrap_or(JsonValue::Null),
        // A type this converter does not model yet reads as absent rather
        // than as a fabricated value.
        _ => JsonValue::Null,
    }
}

/// Decoded columns as JSON rows.
///
/// `skip_internal` drops the storage bookkeeping columns, which a user-facing
/// surface wants and an internal one does not.
pub fn columns_to_json_rows(cols: &[PondColumn], skip_internal: bool) -> Vec<JsonValue> {
    let n_rows = cols.first().map(|c| c.n_values).unwrap_or(0);
    let mut rows = Vec::with_capacity(n_rows);
    for row in 0..n_rows {
        let mut obj = Map::new();
        for col in cols {
            let name = col.name.to_string_lossy().into_owned();
            if skip_internal && INTERNAL_COLUMNS.contains(&name.as_str()) {
                continue;
            }
            obj.insert(name, column_value_to_json(col, row));
        }
        rows.push(JsonValue::Object(obj));
    }
    rows
}

/// Minimal base64, so binary values survive a JSON round trip.
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode::{pnd2_encode_multi_typed_with_nulls, TypedColumn};
    use crate::decode::pnd2_decode;

    /// The bug this module exists to prevent: a null and a zero must not
    /// produce the same JSON.
    #[test]
    fn a_null_is_not_a_zero() {
        let cols = vec![("score", TypedColumn::Int64(vec![0, 0]))];
        let blob = pnd2_encode_multi_typed_with_nulls(&cols, &[Some(vec![false, true])]);
        let rows = columns_to_json_rows(&pnd2_decode(&blob).unwrap(), true);

        assert_eq!(rows[0]["score"], JsonValue::Number(0.into()));
        assert_eq!(rows[1]["score"], JsonValue::Null);
    }

    #[test]
    fn internal_columns_are_dropped_only_when_asked() {
        let cols = vec![
            ("_rowid", TypedColumn::String(vec!["r1".into()])),
            ("id", TypedColumn::Int64(vec![1])),
        ];
        let decoded = pnd2_decode(&pnd2_encode_multi_typed_with_nulls(&cols, &[])).unwrap();

        let user_facing = columns_to_json_rows(&decoded, true);
        assert!(user_facing[0].get("_rowid").is_none());
        assert!(user_facing[0].get("id").is_some());

        let internal = columns_to_json_rows(&decoded, false);
        assert!(internal[0].get("_rowid").is_some());
    }

    #[test]
    fn base64_matches_known_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }
}
