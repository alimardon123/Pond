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
    // Format: bitwidth(1B) + offset(8B, i64) + min(8B) + max(8B) + packed
    if data.len() < 25 { return Vec::new(); }
    let bitwidth = data[0] as usize;
    let offset = i64::from_le_bytes([data[1],data[2],data[3],data[4],data[5],data[6],data[7],data[8]]);
    let packed = &data[25..];

    if bitwidth == 0 || bitwidth > 64 { return Vec::new(); }

    let mut result = Vec::with_capacity(n_rows);
    let mut bit_pos = 0usize;

    for _ in 0..n_rows {
        let byte_pos = bit_pos / 8;
        let bit_off = bit_pos % 8;

        if byte_pos >= packed.len() { break; }

        // Read bitwidth bits starting at bit_pos (little-endian bit order)
        let mut val: u64 = 0;
        for b in 0..bitwidth {
            let bp = bit_pos + b;
            if bp / 8 >= packed.len() { break; }
            if packed[bp / 8] & (1 << (bp % 8)) != 0 {
                val |= 1 << b;
            }
        }
        result.push(val as i64 + offset);
        bit_pos += bitwidth;
    }
    result
}

#[pyfunction]
#[pyo3(signature = (blob_bytes, columns=None, predicates=None))]
fn decode(py: Python, blob_bytes: &[u8], columns: Option<Vec<String>>, predicates: Option<Vec<PyObject>>) -> PyResult<PyObject> {
    if blob_bytes.len() < 13 || &blob_bytes[0..4] != b"PND2" {
        return Ok(py.None());
    }

    let flags = blob_bytes[5];
    let n_rows = u32::from_le_bytes([blob_bytes[6], blob_bytes[7], blob_bytes[8], blob_bytes[9]]) as usize;
    let n_columns = u16::from_le_bytes([blob_bytes[10], blob_bytes[11]]) as usize;
    let has_stats = (flags & 0x01) != 0;

    let inner = &blob_bytes[13..];
    let mut pos = 0;

    // Parse schema: per col: 1B name_len + name + 1B vtype + 1B encoding
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

    let result = PyDict::new_bound(py);

    for (col_name, vtype, enc) in &schema {
        if let Some(ref cols) = columns {
            if !cols.contains(col_name) {
                if pos + 4 <= inner.len() {
                    let plen = u32::from_le_bytes([inner[pos], inner[pos+1], inner[pos+2], inner[pos+3]]) as usize;
                    pos += 4 + plen;
                }
                continue;
            }
        }

        if pos + 4 > inner.len() { break; }
        let payload_len = u32::from_le_bytes([inner[pos], inner[pos+1], inner[pos+2], inner[pos+3]]) as usize;
        pos += 4;
        if pos + payload_len > inner.len() { break; }
        let payload = &inner[pos..pos + payload_len];
        pos += payload_len;

        if payload.is_empty() { continue; }

        // Decode based on encoding type
        match enc {
            0 => {
                // RAW encoding
                let vt = payload[0];
                let data = &payload[1..];
                match vt {
                    1 => {
                        let n = data.len() / 8;
                        let vals: Vec<i64> = (0..n).map(|i| {
                            let o = i * 8;
                            i64::from_le_bytes([data[o],data[o+1],data[o+2],data[o+3],data[o+4],data[o+5],data[o+6],data[o+7]])
                        }).collect();
                        result.set_item(col_name, vals)?;
                    }
                    2 => {
                        let n = data.len() / 8;
                        let vals: Vec<f64> = (0..n).map(|i| {
                            let o = i * 8;
                            f64::from_le_bytes([data[o],data[o+1],data[o+2],data[o+3],data[o+4],data[o+5],data[o+6],data[o+7]])
                        }).collect();
                        result.set_item(col_name, vals)?;
                    }
                    3 => {
                        let mut vals: Vec<String> = Vec::with_capacity(n_rows);
                        let mut off = 0;
                        while off + 4 <= data.len() {
                            let slen = u32::from_le_bytes([data[off], data[off+1], data[off+2], data[off+3]]) as usize;
                            off += 4;
                            if off + slen <= data.len() {
                                vals.push(String::from_utf8_lossy(&data[off..off+slen]).to_string());
                                off += slen;
                            } else { break; }
                        }
                        result.set_item(col_name, vals)?;
                    }
                    _ => { result.set_item(col_name, Vec::<i64>::new())?; }
                }
            }
            3 => {
                // BITPACK encoding (for INT64)
                let vals = unpack_bitpack(payload, n_rows);
                result.set_item(col_name, vals)?;
            }
            _ => {
                // RLE, DICT — fall back to empty (add later)
                result.set_item(col_name, Vec::<i64>::new())?;
            }
        }
    }

    Ok(result.unbind().into())
}

#[pymodule]
fn pond_rust(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    Ok(())
}
