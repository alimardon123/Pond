use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::Bound;

fn skip_stat_value(vtype: u8, data: &[u8]) -> usize {
    match vtype {
        1 | 2 => 8,
        3 => { if data.len() >= 4 { 4 + u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize } else { 0 } }
        _ => 0,
    }
}

fn unpack_bitpack(data: &[u8], n_rows: usize) -> Vec<i64> {
    if data.len() < 25 { return Vec::new(); }
    let bitwidth = data[0] as usize;
    let offset = i64::from_le_bytes([data[1],data[2],data[3],data[4],data[5],data[6],data[7],data[8]]);
    let packed = &data[25..];
    if bitwidth == 0 || bitwidth > 64 { return Vec::new(); }
    let mut result = Vec::with_capacity(n_rows);
    let mut bit_pos = 0usize;
    for _ in 0..n_rows {
        let byte_pos = bit_pos / 8;
        if byte_pos >= packed.len() { break; }
        let mut val: u64 = 0;
        for b in 0..bitwidth {
            let bp = bit_pos + b;
            if bp / 8 >= packed.len() { break; }
            if packed[bp / 8] & (1 << (bp % 8)) != 0 { val |= 1 << b; }
        }
        result.push(val as i64 + offset);
        bit_pos += bitwidth;
    }
    result
}

fn filter_indices_i64(vals: &[i64], op: &str, target: i64) -> Vec<usize> {
    match op {
        "=" => vals.iter().enumerate().filter(|(_, &v)| v == target).map(|(i, _)| i).collect(),
        ">" => vals.iter().enumerate().filter(|(_, &v)| v > target).map(|(i, _)| i).collect(),
        "<" => vals.iter().enumerate().filter(|(_, &v)| v < target).map(|(i, _)| i).collect(),
        ">=" => vals.iter().enumerate().filter(|(_, &v)| v >= target).map(|(i, _)| i).collect(),
        "<=" => vals.iter().enumerate().filter(|(_, &v)| v <= target).map(|(i, _)| i).collect(),
        "!=" => vals.iter().enumerate().filter(|(_, &v)| v != target).map(|(i, _)| i).collect(),
        _ => (0..vals.len()).collect(),
    }
}

fn filter_indices_f64(vals: &[f64], op: &str, target: f64) -> Vec<usize> {
    match op {
        "=" => vals.iter().enumerate().filter(|(_, &v)| v == target).map(|(i, _)| i).collect(),
        ">" => vals.iter().enumerate().filter(|(_, &v)| v > target).map(|(i, _)| i).collect(),
        "<" => vals.iter().enumerate().filter(|(_, &v)| v < target).map(|(i, _)| i).collect(),
        ">=" => vals.iter().enumerate().filter(|(_, &v)| v >= target).map(|(i, _)| i).collect(),
        "<=" => vals.iter().enumerate().filter(|(_, &v)| v <= target).map(|(i, _)| i).collect(),
        "!=" => vals.iter().enumerate().filter(|(_, &v)| v != target).map(|(i, _)| i).collect(),
        _ => (0..vals.len()).collect(),
    }
}

fn filter_indices_str(vals: &[String], op: &str, target: &str) -> Vec<usize> {
    match op {
        "=" => vals.iter().enumerate().filter(|(_, v)| v.as_str() == target).map(|(i, _)| i).collect(),
        ">" => vals.iter().enumerate().filter(|(_, v)| v.as_str() > target).map(|(i, _)| i).collect(),
        "<" => vals.iter().enumerate().filter(|(_, v)| v.as_str() < target).map(|(i, _)| i).collect(),
        _ => (0..vals.len()).collect(),
    }
}

#[pyfunction]
#[pyo3(signature = (blob_bytes, columns=None, predicates=None))]
fn decode(py: Python, blob_bytes: &[u8], columns: Option<Vec<String>>, predicates: Option<Vec<(String, String, PyObject)>>) -> PyResult<PyObject> {
    if blob_bytes.len() < 13 || &blob_bytes[0..4] != b"PND2" {
        return Ok(py.None());
    }

    let flags = blob_bytes[5];
    let n_rows = u32::from_le_bytes([blob_bytes[6], blob_bytes[7], blob_bytes[8], blob_bytes[9]]) as usize;
    let n_columns = u16::from_le_bytes([blob_bytes[10], blob_bytes[11]]) as usize;
    let has_stats = (flags & 0x01) != 0;

    let inner = &blob_bytes[13..];
    let mut pos = 0;

    // Parse schema
    let mut schema: Vec<(String, u8, u8)> = Vec::with_capacity(n_columns);
    for _ in 0..n_columns {
        if pos + 1 > inner.len() { break; }
        let name_len = inner[pos] as usize; pos += 1;
        if pos + name_len > inner.len() { break; }
        let name = String::from_utf8_lossy(&inner[pos..pos+name_len]).to_string();
        pos += name_len;
        if pos + 2 > inner.len() { break; }
        let vtype = inner[pos]; pos += 1;
        let enc = inner[pos]; pos += 1;
        schema.push((name, vtype, enc));
    }

    // Skip stats
    if has_stats {
        for (_, vtype, _) in &schema {
            if pos >= inner.len() { break; }
            let has_mm = inner[pos]; pos += 1;
            if has_mm != 0 {
                pos += skip_stat_value(*vtype, &inner[pos..]);
                pos += skip_stat_value(*vtype, &inner[pos..]);
            }
            pos += 4;
        }
    }

    // Record payload positions for each column (we'll decode selectively)
    let mut payload_positions: Vec<(String, u8, u8, usize, usize)> = Vec::new(); // (name, vtype, enc, start, len)
    for (col_name, vtype, enc) in &schema {
        if pos + 4 > inner.len() { break; }
        let plen = u32::from_le_bytes([inner[pos], inner[pos+1], inner[pos+2], inner[pos+3]]) as usize;
        pos += 4;
        let pstart = pos;
        pos += plen;
        payload_positions.push((col_name.clone(), *vtype, *enc, pstart, plen));
    }

    // === VORTEX-STYLE PREDICATE PUSHDOWN ===
    // If predicates are provided, decode the predicate column first,
    // find surviving indices, then only decode matching rows from other columns.
    let mut surviving_indices: Option<Vec<usize>> = None;
    let mut pred_col_name: Option<String> = None;

    if let Some(ref preds) = predicates {
        for (col_name, op, val) in preds {
            // Find this column in the schema
            let pp = payload_positions.iter().find(|(n, _, _, _, _)| n == col_name);
            if let Some((_, vtype, enc, pstart, plen)) = pp {
                let payload = &inner[*pstart..*pstart + *plen];
                if payload.is_empty() { continue; }

                // Decode the predicate column
                let vals = match enc {
                    0 => { // RAW
                        let vt = payload[0];
                        let data = &payload[1..];
                        match vt {
                            1 => { // INT64
                                let n = data.len() / 8;
                                (0..n).map(|i| {
                                    let o = i * 8;
                                    i64::from_le_bytes([data[o],data[o+1],data[o+2],data[o+3],data[o+4],data[o+5],data[o+6],data[o+7]])
                                }).collect::<Vec<i64>>()
                            }
                            2 => { // FLOAT64 — need different handling
                                Vec::new() // skip for now
                            }
                            _ => Vec::new()
                        }
                    }
                    3 => unpack_bitpack(payload, n_rows), // BITPACK
                    _ => Vec::new(),
                };

                // Filter based on predicate
                if !vals.is_empty() {
                    // Try to extract the target value as i64
                    let target_i64: Option<i64> = val.extract::<i64>(py).ok();
                    if let Some(target) = target_i64 {
                        surviving_indices = Some(filter_indices_i64(&vals, op, target));
                        pred_col_name = Some(col_name.clone());
                        break; // Only use first predicate column for filtering
                    }
                }
            }
        }
    }

    // Decode columns (filtered if we have surviving indices)
    let result = PyDict::new_bound(py);

    for (col_name, vtype, enc, pstart, plen) in &payload_positions {
        // Check if caller wants this column
        if let Some(ref cols) = columns {
            if !cols.contains(col_name) && (pred_col_name.as_ref() != Some(col_name)) {
                continue;
            }
        }

        let payload = &inner[*pstart..*pstart + *plen];
        if payload.is_empty() { continue; }

        match enc {
            0 => { // RAW
                let vt = payload[0];
                let data = &payload[1..];
                match vt {
                    1 => { // INT64
                        let n = data.len() / 8;
                        let all_vals: Vec<i64> = (0..n).map(|i| {
                            let o = i * 8;
                            i64::from_le_bytes([data[o],data[o+1],data[o+2],data[o+3],data[o+4],data[o+5],data[o+6],data[o+7]])
                        }).collect();
                        let filtered = if let Some(ref indices) = surviving_indices {
                            if pred_col_name.as_ref() == Some(col_name) {
                                // This IS the predicate column — return only matching values
                                indices.iter().map(|&i| all_vals[i]).collect()
                            } else {
                                // Different column — filter by surviving indices
                                indices.iter().map(|&i| all_vals[i]).collect()
                            }
                        } else {
                            all_vals
                        };
                        result.set_item(col_name, filtered)?;
                    }
                    2 => { // FLOAT64
                        let n = data.len() / 8;
                        let all_vals: Vec<f64> = (0..n).map(|i| {
                            let o = i * 8;
                            f64::from_le_bytes([data[o],data[o+1],data[o+2],data[o+3],data[o+4],data[o+5],data[o+6],data[o+7]])
                        }).collect();
                        let filtered = if let Some(ref indices) = surviving_indices {
                            indices.iter().map(|&i| all_vals[i]).collect()
                        } else { all_vals };
                        result.set_item(col_name, filtered)?;
                    }
                    3 => { // STRING
                        let mut all_vals: Vec<String> = Vec::with_capacity(n_rows);
                        let mut off = 0;
                        while off + 4 <= data.len() {
                            let slen = u32::from_le_bytes([data[off], data[off+1], data[off+2], data[off+3]]) as usize;
                            off += 4;
                            if off + slen <= data.len() {
                                all_vals.push(String::from_utf8_lossy(&data[off..off+slen]).to_string());
                                off += slen;
                            } else { break; }
                        }
                        let filtered = if let Some(ref indices) = surviving_indices {
                            indices.iter().map(|&i| all_vals[i].clone()).collect()
                        } else { all_vals };
                        result.set_item(col_name, filtered)?;
                    }
                    _ => { result.set_item(col_name, Vec::<i64>::new())?; }
                }
            }
            3 => { // BITPACK
                let all_vals = unpack_bitpack(payload, n_rows);
                let filtered = if let Some(ref indices) = surviving_indices {
                    indices.iter().map(|&i| all_vals[i]).collect()
                } else { all_vals };
                result.set_item(col_name, filtered)?;
            }
            _ => { result.set_item(col_name, Vec::<i64>::new())?; }
        }
    }

    Ok(result.unbind().into())
}

#[pymodule]
fn pond_rust(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    Ok(())
}
