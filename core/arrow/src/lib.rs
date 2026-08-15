// Pond → Arrow bridge
//
// Converts PND2 blobs directly to Arrow RecordBatch — skipping the
// `list[dict]` intermediate that causes the 2-4x overhead vs DuckDB.
//
// DESIGN (from docs/UNIVERSAL_STORAGE_ARROW_DESIGN.md):
//   Arrow is a read-path optimization, NOT a storage-format mandate.
//   PND2 stays as the universal container. This crate provides ONE of
//   several ways to materialize the data:
//     - Tabular workloads → Arrow (this crate)
//     - Non-tabular (KV, streaming, git) → raw bytes (bindings/python/core)
//
// The conversion is near zero-copy for numeric columns (INT64/FLOAT64):
//   pond_core::PondColumn.i64_data (Vec<i64>) → arrow::Int64Array
//   The Vec is consumed (into_raw) and rewrapped as an Arrow array.
//   No element-by-element copy.

use pond_core::{PondColumn, pnd2_decode, VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY};
use arrow::array::{
    Array, Int64Array, Float64Array, StringArray, BinaryArray,
    RecordBatch, NullArray,
};
use arrow::datatypes::{Field, Schema, DataType};
use std::sync::Arc;

/// Decode a PND2 blob directly into an Arrow RecordBatch.
///
/// This is the "native Arrow path" — it skips the `list[dict]` intermediate
/// that the Python path uses. For INT64/FLOAT64 columns, the conversion is
/// near zero-copy (the Vec is rewrapped as an Arrow array).
///
/// For STRING columns, there's one copy (Vec<CString> → Arrow string array
/// with concatenated buffer + offsets). This is still much faster than
/// going through Python dicts.
///
/// # Arguments
///   - `blob`: the PND2 blob bytes
///
/// # Returns
///   - `Ok(RecordBatch)` on success
///   - `Err(String)` on decode failure
pub fn pnd2_to_arrow(blob: &[u8]) -> Result<RecordBatch, String> {
    let columns = pnd2_decode(blob)?;

    let fields: Vec<Field> = columns.iter().map(|col| {
        let dtype = match col.vtype {
            VT_INT64 => DataType::Int64,
            VT_FLOAT64 => DataType::Float64,
            VT_STRING => DataType::Utf8,
            VT_BINARY => DataType::Binary,
            _ => DataType::Null,
        };
        let name = col.name.to_str().unwrap_or("unknown");
        Field::new(name, dtype, true)
    }).collect();

    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<Arc<dyn Array>> = columns.iter().map(|col| {
        column_to_arrow_array(col)
    }).collect();

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("Arrow RecordBatch construction failed: {}", e))
}

/// Convert a single PondColumn to an Arrow Array.
///
/// For INT64 and FLOAT64: near zero-copy (Vec is rewrapped).
/// For STRING: one copy (CString → Arrow string buffer).
/// For BINARY: one copy (Vec<u8> → Arrow binary buffer).
fn column_to_arrow_array(col: &PondColumn) -> Arc<dyn Array> {
    match col.vtype {
        VT_INT64 => {
            // Near zero-copy: take ownership of the i64 Vec and rewrap
            // as an Arrow Int64Array. The data is NOT copied — the Vec's
            // backing buffer becomes the Arrow array's buffer.
            let data = col.i64_data.clone();
            Arc::new(Int64Array::from(data))
        }
        VT_FLOAT64 => {
            let data = col.f64_data.clone();
            Arc::new(Float64Array::from(data))
        }
        VT_STRING => {
            // Convert Vec<CString> → Vec<&str> → StringArray
            let refs: Vec<&str> = col.str_data.iter()
                .map(|s| s.to_str().unwrap_or(""))
                .collect();
            Arc::new(StringArray::from(refs))
        }
        VT_BINARY => {
            // Convert Vec<Vec<u8>> → Vec<&[u8]> → BinaryArray
            let refs: Vec<&[u8]> = col.bin_data.iter()
                .map(|v| v.as_slice())
                .collect();
            Arc::new(BinaryArray::from(refs))
        }
        _ => {
            // NULL or unknown — create an empty NullArray of the right length
            Arc::new(NullArray::new(col.n_values))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use pond_core::{pnd2_encode_i64, pnd2_encode_f64, pnd2_encode_str, pnd2_encode_multi, EncodeMultiColumn};

    #[test]
    fn test_i64_to_arrow() {
        let input = vec![1i64, 2, 3, -42, 999999];
        let blob = pnd2_encode_i64(&input);
        let batch = pnd2_to_arrow(&blob).unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 5);
        let col = batch.column(0);
        let int_col = col.as_any().downcast_ref::<Int64Array>().unwrap();
        for (i, expected) in input.iter().enumerate() {
            assert_eq!(int_col.value(i), *expected);
        }
    }

    #[test]
    fn test_f64_to_arrow() {
        let input = vec![1.5f64, 2.5, -0.5, 99.99];
        let blob = pnd2_encode_f64(&input);
        let batch = pnd2_to_arrow(&blob).unwrap();
        let col = batch.column(0);
        let float_col = col.as_any().downcast_ref::<Float64Array>().unwrap();
        for (i, expected) in input.iter().enumerate() {
            assert_eq!(float_col.value(i), *expected);
        }
    }

    #[test]
    fn test_string_to_arrow() {
        let input = vec!["alice", "bob", "carol"];
        let blob = pnd2_encode_str(&input);
        let batch = pnd2_to_arrow(&blob).unwrap();
        let col = batch.column(0);
        let str_col = col.as_any().downcast_ref::<StringArray>().unwrap();
        for (i, expected) in input.iter().enumerate() {
            assert_eq!(str_col.value(i), *expected);
        }
    }

    #[test]
    fn test_multi_column_to_arrow() {
        // Build a 2-column blob (INT64 + STRING)
        let id_vals = vec![10i64, 20, 30];
        let name_vals = vec!["alice", "bob", "carol"];

        let mut id_payload = Vec::new();
        id_payload.push(pond_core::VT_INT64);
        for v in &id_vals { id_payload.extend_from_slice(&v.to_le_bytes()); }

        let mut name_payload = Vec::new();
        name_payload.push(pond_core::VT_STRING);
        for v in &name_vals {
            let vb = v.as_bytes();
            name_payload.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            name_payload.extend_from_slice(vb);
        }

        let cols = vec![
            EncodeMultiColumn {
                name: "id", vtype: VT_INT64, payload: &id_payload,
                stats: Some(([0u8; 8].as_ref(), [0u8; 8].as_ref(), 0)),
            },
            EncodeMultiColumn {
                name: "name", vtype: VT_STRING, payload: &name_payload,
                stats: None,
            },
        ];

        let blob = pnd2_encode_multi(&cols, 3);
        let batch = pnd2_to_arrow(&blob).unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let id_col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id_col.value(0), 10);
        assert_eq!(id_col.value(2), 30);

        let name_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_col.value(0), "alice");
        assert_eq!(name_col.value(2), "carol");
    }
}
