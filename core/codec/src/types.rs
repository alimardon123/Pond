// types.rs — `PondColumn` and the small `CString` helpers used by the decoder.
//
// `PondColumn` is the language-agnostic representation of a decoded PND2
// column: it owns its data (so it can outlive the input blob) and uses
// `CString` for the column name and STRING values so that `as_ptr()`
// returns a null-terminated `*const c_char` directly — required by the C
// ABI accessors `pond_result_column_name` and `pond_result_column_str`.
//
// `bytes_to_cstring` / `str_to_cstring` are `pub(crate)` helpers that strip
// interior null bytes (which are invalid in C strings) and wrap the result
// in a `CString`. They're used by both `types.rs` (in `PondColumn::empty`)
// and `decode.rs` (when materializing STRING values from RAW/DICT/RLE
// payloads).

use std::ffi::CString;

/// A decoded PND2 column. Owns its data so it can outlive the input blob.
///
/// Each column stores its values in the `*_data` vec matching its `vtype`:
///   - VT_INT64   → `i64_data`
///   - VT_FLOAT64 → `f64_data`
///   - VT_STRING  → `str_data`  (Vec<CString> — null-terminated for C ABI)
///   - VT_BINARY  → `bin_data`
///   - VT_NULL    → all vecs empty, `n_values` = row count
///
/// `name` and `str_data` use `CString` (not `String`) so that `as_ptr()`
/// returns a null-terminated `*const c_char` directly — required by the
/// C ABI accessors `pond_result_column_name` and `pond_result_column_str`.
///
/// `n_values` is the logical row count of the column (always set, even when
/// the data vecs are empty — e.g. for VT_NULL columns or unsupported encodings).
#[derive(Clone, Debug)]
pub struct PondColumn {
    pub name: CString,
    pub vtype: u8,
    pub i64_data: Vec<i64>,
    pub f64_data: Vec<f64>,
    pub str_data: Vec<CString>,
    pub bin_data: Vec<Vec<u8>>,
    pub n_values: usize,
    pub null_bitmap: Option<Vec<u8>>,
}

impl PondColumn {
    /// Create an empty column with the given name and vtype.
    /// `name_bytes` is the raw UTF-8 bytes of the column name (interior
    /// null bytes are stripped — they're invalid in C strings).
    pub fn empty(name_bytes: &[u8], vtype: u8) -> Self {
        Self {
            name: bytes_to_cstring(name_bytes),
            vtype,
            i64_data: vec![],
            f64_data: vec![],
            str_data: vec![],
            bin_data: vec![],
            n_values: 0,
            null_bitmap: None,
        }
    }

    /// Same as `empty` but takes a `&str` for ergonomic callers.
    pub fn empty_named(name: &str, vtype: u8) -> Self {
        Self::empty(name.as_bytes(), vtype)
    }

    /// Check if a row is null (using the null bitmap if present).
    pub fn is_null(&self, row: usize) -> bool {
        if row >= self.n_values { return true; }
        match &self.null_bitmap {
            Some(bitmap) => {
                let byte_idx = row / 8;
                let bit_idx = row % 8;
                if byte_idx < bitmap.len() { bitmap[byte_idx] & (1 << bit_idx) != 0 } else { false }
            }
            None => false,
        }
    }
}

/// Convert raw bytes to a `CString`, stripping any interior null bytes
/// (which are invalid in C strings). For invalid UTF-8 the bytes are
/// kept as-is — callers that need lossy UTF-8 conversion should do it
/// before calling this.
pub(crate) fn bytes_to_cstring(b: &[u8]) -> CString {
    let mut v: Vec<u8> = b.to_vec();
    v.retain(|&c| c != 0);
    // Safety: we just stripped all 0x00 bytes, so v contains no interior nulls.
    // CString::new would also work but does a redundant scan.
    CString::new(v).unwrap_or_else(|_| CString::new("").unwrap())
}

/// Convert a `&str` to a `CString` (lossy — interior nulls are stripped).
pub(crate) fn str_to_cstring(s: &str) -> CString {
    bytes_to_cstring(s.as_bytes())
}

/// The `dim_N` columns of a vector row group, in numeric order.
///
/// # Why this exists rather than a `sort_by_key` at each call site
///
/// A vector is stored one dimension per column — `dim_0`, `dim_1`, … — so
/// reading one back means collecting those columns and putting them in the
/// order the vector was written in. Four places did that independently: the
/// IVF build path, the IVF search path, the HNSW build path, and the vector
/// lens. Three sorted the column names as *strings*; the fourth did not sort
/// at all and took whatever order the decoder happened to return.
///
/// Both are wrong, and they are wrong in a way that hides.
///
/// Lexicographically, `"dim_10" < "dim_2"`, so from ten dimensions upward the
/// string order stops matching the numeric order. The stored vectors then come
/// back permuted while the *query* vector — which the caller supplies as a
/// plain list — does not, so every distance is computed between coordinates
/// that do not correspond. Under nine dimensions the two orders coincide and
/// everything works, which is why the small tests passed: measured recall was
/// 10/10 at eight dimensions and 0/10 at thirty-two, and an exact-match query
/// returned a distance of 25.9 instead of 0.
///
/// Sorting numerically fixes it everywhere at once, and having one function do
/// it is the point — the two paths that disagreed could only disagree because
/// each had its own copy.
///
/// Columns whose suffix is not a number sort last, by name, so a malformed
/// name cannot silently take position zero.
pub fn dim_columns_in_order(cols: &[PondColumn]) -> Vec<&PondColumn> {
    let mut dims: Vec<&PondColumn> = cols
        .iter()
        .filter(|c| c.name.to_string_lossy().starts_with("dim_"))
        .collect();
    dims.sort_by_key(|c| {
        let name = c.name.to_string_lossy().to_string();
        let index = name["dim_".len()..].parse::<u64>().ok();
        (index.is_none(), index.unwrap_or(0), name)
    });
    dims
}

#[cfg(test)]
mod dim_order_tests {
    use super::*;

    fn col(name: &str) -> PondColumn {
        let mut c = PondColumn::empty_named(name, 3);
        c.f64_data.push(0.0);
        c
    }

    /// The case the string sort gets wrong, and the only one that matters.
    #[test]
    fn dimensions_are_ordered_by_number_not_by_name() {
        let cols: Vec<PondColumn> = (0..12).map(|i| col(&format!("dim_{i}"))).collect();
        let ordered: Vec<String> = dim_columns_in_order(&cols)
            .iter()
            .map(|c| c.name.to_string_lossy().to_string())
            .collect();

        let expected: Vec<String> = (0..12).map(|i| format!("dim_{i}")).collect();
        assert_eq!(
            ordered, expected,
            "dim_10 must follow dim_9, not dim_1 — a string sort puts it second \
             and permutes every stored vector against the query"
        );
    }

    /// Order must not depend on the order the decoder returned them in.
    #[test]
    fn the_input_order_does_not_matter() {
        let mut cols: Vec<PondColumn> = (0..12).map(|i| col(&format!("dim_{i}"))).collect();
        cols.reverse();
        let ordered: Vec<String> = dim_columns_in_order(&cols)
            .iter()
            .map(|c| c.name.to_string_lossy().to_string())
            .collect();
        let expected: Vec<String> = (0..12).map(|i| format!("dim_{i}")).collect();
        assert_eq!(ordered, expected);
    }

    /// Non-dimension columns are excluded; malformed ones go last rather than
    /// taking position zero.
    #[test]
    fn only_well_formed_dimensions_lead() {
        let cols = vec![col("id"), col("dim_1"), col("dim_x"), col("dim_0"), col("metadata")];
        let ordered: Vec<String> = dim_columns_in_order(&cols)
            .iter()
            .map(|c| c.name.to_string_lossy().to_string())
            .collect();
        assert_eq!(ordered, vec!["dim_0", "dim_1", "dim_x"]);
    }
}

/// The `id` column as strings, whatever type it is stored in.
///
/// # Why this exists
///
/// An id column is `Int64` when every id parses as a number and `String`
/// otherwise — the writer picks per commit. A reader that looks at only one of
/// those fields does not get an error when it guesses wrong; it gets an empty
/// vector, and what happens next depends on how it uses it:
///
///   - The HNSW index read `i64_data` and mapped a miss to `unwrap_or_default`,
///     so every search result on a string-keyed collection came back with an
///     empty id. The distances were right and the answers were unusable.
///   - The IVF index *iterated* `i64_data`, so on the same collection the loop
///     body never ran and search returned no results at all — an empty answer
///     that looks exactly like "nothing matched".
///
/// Neither failed loudly, and both are the same mistake: assuming one
/// representation of something stored in two.
///
/// Rows with no id at all get their ordinal, so a caller always has as many
/// ids as it has rows and positional correspondence is never broken by a gap.
pub fn id_strings(cols: &[PondColumn], n_rows: usize) -> Vec<String> {
    let id_col = cols.iter().find(|c| c.name.to_string_lossy() == "id");
    (0..n_rows)
        .map(|i| match id_col {
            Some(c) if !c.str_data.is_empty() => c
                .str_data
                .get(i)
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| i.to_string()),
            Some(c) if !c.i64_data.is_empty() => c
                .i64_data
                .get(i)
                .map(|v| v.to_string())
                .unwrap_or_else(|| i.to_string()),
            _ => i.to_string(),
        })
        .collect()
}

#[cfg(test)]
mod id_string_tests {
    use super::*;

    fn int_ids(v: &[i64]) -> PondColumn {
        let mut c = PondColumn::empty_named("id", 1);
        c.i64_data = v.to_vec();
        c
    }

    fn str_ids(v: &[&str]) -> PondColumn {
        let mut c = PondColumn::empty_named("id", 4);
        c.str_data = v
            .iter()
            .map(|s| std::ffi::CString::new(*s).unwrap())
            .collect();
        c
    }

    #[test]
    fn integer_ids_read_back_as_their_digits() {
        assert_eq!(id_strings(&[int_ids(&[7, 8, 9])], 3), vec!["7", "8", "9"]);
    }

    /// The case both index extensions got wrong.
    #[test]
    fn string_ids_are_not_lost() {
        assert_eq!(
            id_strings(&[str_ids(&["a", "b"])], 2),
            vec!["a", "b"],
            "a string id column must not read as empty — HNSW returned blank \
             ids and IVF returned no rows at all"
        );
    }

    #[test]
    fn a_missing_id_column_falls_back_to_ordinals() {
        assert_eq!(id_strings(&[], 3), vec!["0", "1", "2"]);
    }

    /// Never fewer ids than rows: a caller zips these with vectors, so a
    /// short id column must not silently shorten the answer and shift every
    /// pairing after the gap.
    ///
    /// The filler is the row's ordinal, which can in principle collide with a
    /// real id — `[1]` over three rows yields `1, 1, 2`. That is accepted
    /// rather than solved: the column is malformed if it is shorter than the
    /// data, no filler is correct, and losing positional correspondence is
    /// worse than a duplicate. This asserts the property that matters, not
    /// the particular filler.
    #[test]
    fn short_id_columns_do_not_shorten_the_result() {
        let got = id_strings(&[int_ids(&[7])], 3);
        assert_eq!(got.len(), 3, "one id per row, always");
        assert_eq!(got[0], "7", "the ids that are present must be preserved");
    }
}
