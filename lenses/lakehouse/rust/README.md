# lenses/lakehouse/rust/

Rust implementation of LakehouseLens — tabular storage with PND2.

## Status

**Implemented (core API).** The following operations are ported:

| Operation | Status | Notes |
|---|---|---|
| `create_table(table, columns, key_col, message)` | ✅ | Write typed columns as PND2 |
| `insert(table, new_columns, message)` | ✅ | Append rows (read-merge-write) |
| `read_table(table)` | ✅ | Read all columns as typed data |
| `read_columns(table, columns)` | ✅ | Column projection |
| `point_lookup(table, key_col, key_val)` | ✅ | Find single row by key |
| SQL query (DuckDB) | ❌ | Not ported (use Python LakehouseLens for SQL) |

## Not Ported

DuckDB SQL query is NOT implemented in Rust because it requires the DuckDB
C API (heavy dependency). For SQL queries, use the Python LakehouseLens
which has DuckDB integration built in.

## Usage

```rust
use pond_lakehouse_lens::LakehouseLens;
use pond_storage::UnifiedStorage;
use pond_core::TypedColumn;

let storage = UnifiedStorage::new_local("/var/lib/pond").unwrap();
let lens = LakehouseLens::new(storage);

// Create table with mixed types
lens.create_table("users", &[
    ("id", TypedColumn::Int64(vec![1, 2, 3])),
    ("score", TypedColumn::Float64(vec![1.5, 2.5, 3.5])),
    ("name", TypedColumn::String(vec!["a".into(), "b".into(), "c".into()])),
], "id", "create users").unwrap();

// Read all columns
let cols = lens.read_table("users").unwrap();

// Read with projection
let proj = vec!["name".to_string()];
let cols = lens.read_columns("users", Some(&proj)).unwrap();

// Point lookup
let row = lens.point_lookup("users", "id", 2).unwrap();
```

## Tests

5 tests pass:
- `test_create_table_and_read`: PND2 roundtrip (INT64 + STRING)
- `test_insert_appends_rows`: Insert appends to existing data
- `test_read_columns_projection`: Read only specific columns
- `test_point_lookup`: Find single row by key
- `test_mixed_types_roundtrip`: INT64 + FLOAT64 + STRING roundtrip
