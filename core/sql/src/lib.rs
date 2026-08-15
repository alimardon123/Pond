//! Pond SQL engine — pure-Rust SQL parser and executor.
//!
//! This crate provides a SQL engine that runs entirely in Rust, with no
//! dependency on PyO3 or Python. It is the shared SQL layer used by:
//!
//!   - The Go SDK (via the C ABI)
//!   - The CLI (`pond sql "SELECT * FROM users"`)
//!   - The MCP server
//!   - The Python binding (which delegates to this crate to avoid
//!     code duplication)
//!
//! # Supported statements
//!
//! ```text
//! SELECT [items]  FROM collection [alias]
//!        [JOIN ... ON ...]
//!        [WHERE ...]
//!        [GROUP BY col1, ... [HAVING ...]]
//!        [ORDER BY col1 [ASC|DESC], ...]
//!        [LIMIT n [OFFSET m]]
//!
//! INSERT INTO collection (col1, col2) VALUES (v1, v2), (v3, v4)
//! UPDATE collection SET col1 = val1 [WHERE ...]
//! DELETE FROM collection [WHERE ...]
//! MERGE INTO target USING source_rows ON key = key
//!   WHEN MATCHED THEN UPDATE | DELETE | SKIP
//!   WHEN NOT MATCHED THEN INSERT | SKIP
//! ```
//!
//! # Quick example
//!
//! ```
//! use pond_sql::execute;
//! use pond_storage::UnifiedStorage;
//!
//! let dir = tempfile::tempdir().unwrap();
//! let storage = UnifiedStorage::new_local(dir.path()).unwrap();
//!
//! // Insert some rows.
//! execute(&storage, "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30), (2, 'bob', 25)").unwrap();
//!
//! // Query them back.
//! let result = execute(&storage, "SELECT name, age FROM users WHERE age > 20 ORDER BY age DESC").unwrap();
//! assert_eq!(result.rows.len(), 2);
//! ```

pub mod executor;
pub mod parser;
pub mod where_clause;

pub use executor::{execute, SqlResult};
pub use parser::{
    parse_sql, AggregateExpr, AggregateFunc, JoinClause, JoinType, MergeAction, OrderByItem,
    SelectItem, SqlStatement, TableRef,
};
pub use where_clause::{json_values_equal, parse_where, WhereExpr};
