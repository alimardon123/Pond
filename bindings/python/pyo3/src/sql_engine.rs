// SQL engine — full SELECT / UPDATE / DELETE / MERGE support.
//
// Parses and executes a subset of SQL against Pond collections.
// All execution happens in Rust — zero Python overhead.
//
// Supported statements:
//
//   SELECT * | col1, col2, ... FROM collection [WHERE ...]
//   UPDATE collection SET col1 = val1, col2 = val2 [WHERE ...]
//   DELETE FROM collection [WHERE ...]
//   INSERT INTO collection (col1, col2) VALUES (v1, v2), (v3, v4)
//   MERGE INTO target USING source ON key = key
//     WHEN MATCHED THEN UPDATE SET ...
//     WHEN MATCHED THEN DELETE
//     WHEN NOT MATCHED THEN INSERT (cols) VALUES (vals)
//
// All statements support full WHERE clauses (see sql_where.rs).

use crate::sql_where::{parse_where, WhereExpr};
use serde_json::{json, Value as JsonValue};

/// A parsed SQL statement.
#[derive(Debug, Clone)]
pub enum SqlStatement {
    Select {
        collection: String,
        columns: Vec<String>,    // empty = SELECT *
        r#where: WhereExpr,
    },
    Update {
        collection: String,
        sets: Vec<(String, JsonValue)>,
        r#where: WhereExpr,
    },
    Delete {
        collection: String,
        r#where: WhereExpr,
    },
    Insert {
        collection: String,
        columns: Vec<String>,
        rows: Vec<Vec<JsonValue>>,
    },
    Merge {
        target: String,
        source_rows: Vec<JsonValue>,
        match_keys: Vec<(String, String)>,  // (target_col, source_col) pairs
        when_matched: MergeAction,
        when_not_matched: MergeAction,
    },
}

#[derive(Debug, Clone)]
pub enum MergeAction {
    Update,
    Delete,
    Skip,
    Insert,
}

impl Default for MergeAction {
    fn default() -> Self {
        MergeAction::Skip
    }
}

/// Parse a SQL statement string.
pub fn parse_sql(sql: &str) -> Result<SqlStatement, String> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();

    if upper.starts_with("SELECT") {
        parse_select(sql)
    } else if upper.starts_with("UPDATE") {
        parse_update(sql)
    } else if upper.starts_with("DELETE") {
        parse_delete(sql)
    } else if upper.starts_with("INSERT") {
        parse_insert(sql)
    } else if upper.starts_with("MERGE") {
        parse_merge(sql)
    } else {
        Err(format!(
            "Unsupported SQL statement. Expected SELECT, UPDATE, DELETE, INSERT, or MERGE. Got: {}",
            sql.split_whitespace().next().unwrap_or("")
        ))
    }
}

// ---------------------------------------------------------------------------
// SELECT parser
// ---------------------------------------------------------------------------

fn parse_select(sql: &str) -> Result<SqlStatement, String> {
    // SELECT * FROM collection [WHERE ...]
    // SELECT col1, col2 FROM collection [WHERE ...]

    let after_select = strip_prefix_ci(sql, "SELECT")
        .ok_or_else(|| "Expected SELECT".to_string())?
        .trim();

    // Find FROM
    let from_pos = find_keyword(after_select, "FROM")
        .ok_or_else(|| "Expected FROM in SELECT".to_string())?;

    let cols_str = after_select[..from_pos].trim();
    let after_from = after_select[from_pos + 4..].trim();

    // Parse columns
    let columns: Vec<String> = if cols_str.trim() == "*" {
        vec![]
    } else {
        cols_str.split(',').map(|s| s.trim().to_string()).collect()
    };

    // Check for WHERE
    let (collection, where_expr) = if let Some(where_pos) = find_keyword(after_from, "WHERE") {
        let coll = after_from[..where_pos].trim().to_string();
        let where_str = after_from[where_pos + 5..].trim();
        (coll, parse_where(where_str)?)
    } else {
        (after_from.trim().to_string(), WhereExpr::True)
    };

    Ok(SqlStatement::Select {
        collection,
        columns,
        r#where: where_expr,
    })
}

// ---------------------------------------------------------------------------
// UPDATE parser
// ---------------------------------------------------------------------------

fn parse_update(sql: &str) -> Result<SqlStatement, String> {
    // UPDATE collection SET col1 = val1, col2 = val2 [WHERE ...]

    let after_update = strip_prefix_ci(sql, "UPDATE")
        .ok_or_else(|| "Expected UPDATE".to_string())?
        .trim();

    let set_pos = find_keyword(after_update, "SET")
        .ok_or_else(|| "Expected SET in UPDATE".to_string())?;

    let collection = after_update[..set_pos].trim().to_string();
    let after_set = after_update[set_pos + 3..].trim();

    // Find optional WHERE
    let (sets_str, where_str) = if let Some(where_pos) = find_keyword(after_set, "WHERE") {
        (after_set[..where_pos].trim(), after_set[where_pos + 5..].trim())
    } else {
        (after_set, "")
    };

    // Parse SET col = val, ...
    let sets = parse_set_clause(sets_str)?;
    let where_expr = if where_str.is_empty() {
        WhereExpr::True
    } else {
        parse_where(where_str)?
    };

    Ok(SqlStatement::Update {
        collection,
        sets,
        r#where: where_expr,
    })
}

// ---------------------------------------------------------------------------
// DELETE parser
// ---------------------------------------------------------------------------

fn parse_delete(sql: &str) -> Result<SqlStatement, String> {
    // DELETE FROM collection [WHERE ...]

    let after_delete = strip_prefix_ci(sql, "DELETE")
        .ok_or_else(|| "Expected DELETE".to_string())?
        .trim();

    let from_pos = find_keyword(after_delete, "FROM")
        .ok_or_else(|| "Expected FROM in DELETE".to_string())?;

    let after_from = after_delete[from_pos + 4..].trim();

    let (collection, where_expr) = if let Some(where_pos) = find_keyword(after_from, "WHERE") {
        let coll = after_from[..where_pos].trim().to_string();
        let where_str = after_from[where_pos + 5..].trim();
        (coll, parse_where(where_str)?)
    } else {
        (after_from.trim().to_string(), WhereExpr::True)
    };

    Ok(SqlStatement::Delete {
        collection,
        r#where: where_expr,
    })
}

// ---------------------------------------------------------------------------
// INSERT parser
// ---------------------------------------------------------------------------

fn parse_insert(sql: &str) -> Result<SqlStatement, String> {
    // INSERT INTO collection (col1, col2) VALUES (v1, v2), (v3, v4)

    let after_insert = strip_prefix_ci(sql, "INSERT")
        .ok_or_else(|| "Expected INSERT".to_string())?
        .trim();

    let into_pos = find_keyword(after_insert, "INTO")
        .ok_or_else(|| "Expected INTO in INSERT".to_string())?;

    let after_into = after_insert[into_pos + 4..].trim();

    // Find the opening paren for columns
    let paren_pos = after_into.find('(')
        .ok_or_else(|| "Expected ( after collection name in INSERT".to_string())?;

    let collection = after_into[..paren_pos].trim().to_string();
    let after_paren = after_into[paren_pos..].trim();

    // Parse column list
    let close_paren = after_paren.find(')')
        .ok_or_else(|| "Expected ) after column list".to_string())?;

    let cols_str = &after_paren[1..close_paren];
    let columns: Vec<String> = cols_str.split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let after_cols = after_paren[close_paren + 1..].trim();

    // Expect VALUES
    let values_pos = find_keyword(after_cols, "VALUES")
        .ok_or_else(|| "Expected VALUES in INSERT".to_string())?;

    let values_str = after_cols[values_pos + 6..].trim();

    // Parse value tuples: (v1, v2), (v3, v4)
    let rows = parse_value_tuples(values_str)?;

    Ok(SqlStatement::Insert {
        collection,
        columns,
        rows,
    })
}

// ---------------------------------------------------------------------------
// MERGE parser
// ---------------------------------------------------------------------------

fn parse_merge(sql: &str) -> Result<SqlStatement, String> {
    // MERGE INTO target USING source_rows ON key1 = key2 [AND key3 = key4]
    //   WHEN MATCHED THEN UPDATE | DELETE | SKIP
    //   WHEN NOT MATCHED THEN INSERT | SKIP

    let after_merge = strip_prefix_ci(sql, "MERGE")
        .ok_or_else(|| "Expected MERGE".to_string())?
        .trim();

    let into_pos = find_keyword(after_merge, "INTO")
        .ok_or_else(|| "Expected INTO in MERGE".to_string())?;

    let after_into = after_merge[into_pos + 4..].trim();

    let using_pos = find_keyword(after_into, "USING")
        .ok_or_else(|| "Expected USING in MERGE".to_string())?;

    let target = after_into[..using_pos].trim().to_string();
    let after_using = after_into[using_pos + 6..].trim();

    let on_pos = find_keyword(after_using, "ON")
        .ok_or_else(|| "Expected ON in MERGE".to_string())?;

    // Source is between USING and ON — for now, source must be a JSON array
    let source_str = after_using[..on_pos].trim();
    let source_rows: Vec<JsonValue> = if source_str.starts_with('[') {
        serde_json::from_str(source_str)
            .map_err(|e| format!("Invalid source JSON: {}", e))?
    } else {
        return Err("MERGE source must be a JSON array of row objects".to_string());
    };

    let after_on = after_using[on_pos + 2..].trim();

    // Parse match keys: key1 = key2 [AND key3 = key4]
    // Find WHEN keyword to delimit the ON clause
    let when_pos = find_keyword(after_on, "WHEN")
        .ok_or_else(|| "Expected WHEN MATCHED in MERGE".to_string())?;

    let on_str = after_on[..when_pos].trim();
    let match_keys = parse_on_clause(on_str)?;

    let after_when = after_on[when_pos..].trim();

    // Parse WHEN MATCHED and WHEN NOT MATCHED clauses
    let (when_matched, when_not_matched) = parse_when_clauses(after_when)?;

    Ok(SqlStatement::Merge {
        target,
        source_rows,
        match_keys,
        when_matched,
        when_not_matched,
    })
}

fn parse_on_clause(on_str: &str) -> Result<Vec<(String, String)>, String> {
    // Parse: target_col = source_col [AND target_col2 = source_col2]
    let parts: Vec<&str> = on_str.split_whitespace().collect();
    let mut keys = Vec::new();
    let mut i = 0;
    while i < parts.len() {
        let left = parts[i].trim_end_matches(',');
        i += 1;
        if i >= parts.len() || parts[i] != "=" {
            return Err(format!("Expected = in ON clause after '{}'", left));
        }
        i += 1;
        if i >= parts.len() {
            return Err("Expected source column after = in ON clause".to_string());
        }
        let right = parts[i].trim_end_matches(',');
        i += 1;
        keys.push((left.to_string(), right.to_string()));
        // Skip AND
        if i < parts.len() && parts[i].to_uppercase() == "AND" {
            i += 1;
        }
    }
    if keys.is_empty() {
        return Err("ON clause must have at least one key pair".to_string());
    }
    Ok(keys)
}

fn parse_when_clauses(s: &str) -> Result<(MergeAction, MergeAction), String> {
    let mut when_matched = MergeAction::Update;
    let mut when_not_matched = MergeAction::Insert;

    // Split by WHEN
    let clauses: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;
    while i < clauses.len() {
        if clauses[i].to_uppercase() == "WHEN" {
            i += 1;
            if i >= clauses.len() {
                return Err("Expected MATCHED or NOT after WHEN".to_string());
            }
            let is_not = clauses[i].to_uppercase() == "NOT";
            if is_not {
                i += 1;
            }
            if i >= clauses.len() || clauses[i].to_uppercase() != "MATCHED" {
                return Err("Expected MATCHED after WHEN".to_string());
            }
            i += 1;
            if i >= clauses.len() || clauses[i].to_uppercase() != "THEN" {
                return Err("Expected THEN after WHEN MATCHED".to_string());
            }
            i += 1;
            if i >= clauses.len() {
                return Err("Expected action after THEN".to_string());
            }
            let action = match clauses[i].to_uppercase().as_str() {
                "UPDATE" => MergeAction::Update,
                "DELETE" => MergeAction::Delete,
                "SKIP" => MergeAction::Skip,
                "INSERT" => MergeAction::Insert,
                other => return Err(format!("Unknown merge action: {}", other)),
            };
            if is_not {
                when_not_matched = action;
            } else {
                when_matched = action;
            }
            i += 1;
            // Skip any SET clause or other tokens until next WHEN
            while i < clauses.len() && clauses[i].to_uppercase() != "WHEN" {
                i += 1;
            }
        } else {
            i += 1;
        }
    }

    Ok((when_matched, when_not_matched))
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.to_uppercase().starts_with(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

fn find_keyword(s: &str, keyword: &str) -> Option<usize> {
    let upper = s.to_uppercase();
    let kw_upper = keyword.to_uppercase();
    // Find the keyword as a whole word
    let mut search_start = 0;
    while let Some(pos) = upper[search_start..].find(&kw_upper) {
        let abs_pos = search_start + pos;
        // Check it's a whole word
        let before_ok = abs_pos == 0 || !s[..abs_pos].chars().last().unwrap().is_alphanumeric();
        let after_pos = abs_pos + kw_upper.len();
        let after_ok = after_pos >= s.len() || !s[after_pos..].chars().next().unwrap().is_alphanumeric();
        if before_ok && after_ok {
            return Some(abs_pos);
        }
        search_start = abs_pos + 1;
    }
    None
}

fn parse_set_clause(s: &str) -> Result<Vec<(String, JsonValue)>, String> {
    // Parse: col1 = val1, col2 = val2
    let mut sets = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let eq_pos = part.find('=')
            .ok_or_else(|| format!("Expected = in SET clause: '{}'", part))?;
        let col = part[..eq_pos].trim().to_string();
        let val_str = part[eq_pos + 1..].trim();
        let val = parse_sql_value(val_str)?;
        sets.push((col, val));
    }
    Ok(sets)
}

fn parse_sql_value(s: &str) -> Result<JsonValue, String> {
    let s = s.trim();
    if s.starts_with('\'') && s.ends_with('\'') {
        return Ok(JsonValue::String(s[1..s.len()-1].to_string()));
    }
    if s.eq_ignore_ascii_case("true") {
        return Ok(JsonValue::Bool(true));
    }
    if s.eq_ignore_ascii_case("false") {
        return Ok(JsonValue::Bool(false));
    }
    if s.eq_ignore_ascii_case("null") {
        return Ok(JsonValue::Null);
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(JsonValue::Number(serde_json::Number::from(i)));
    }
    if let Ok(f) = s.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Ok(JsonValue::Number(n));
        }
    }
    // Unquoted string — treat as string literal
    Ok(JsonValue::String(s.to_string()))
}

fn parse_value_tuples(s: &str) -> Result<Vec<Vec<JsonValue>>, String> {
    // Parse: (v1, v2), (v3, v4)
    let mut rows = Vec::new();
    let mut depth = 0;
    let mut current = String::new();
    let mut in_tuple = false;

    for c in s.chars() {
        if c == '(' {
            depth += 1;
            in_tuple = true;
            current.clear();
        } else if c == ')' {
            depth -= 1;
            if depth == 0 && in_tuple {
                let values: Vec<JsonValue> = current.split(',')
                    .map(|v| parse_sql_value(v.trim()).unwrap_or(JsonValue::Null))
                    .collect();
                rows.push(values);
                in_tuple = false;
            }
        } else if in_tuple {
            current.push(c);
        }
    }

    if rows.is_empty() {
        return Err("No value tuples found".to_string());
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_star() {
        let stmt = parse_sql("SELECT * FROM users").unwrap();
        match stmt {
            SqlStatement::Select { collection, columns, .. } => {
                assert_eq!(collection, "users");
                assert!(columns.is_empty()); // SELECT *
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_cols_where() {
        let stmt = parse_sql("SELECT name, age FROM users WHERE age >= 18").unwrap();
        match stmt {
            SqlStatement::Select { collection, columns, r#where } => {
                assert_eq!(collection, "users");
                assert_eq!(columns, vec!["name", "age"]);
                assert!(matches!(r#where, WhereExpr::Compare { .. }));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse_sql("UPDATE users SET status = 'active' WHERE age >= 18").unwrap();
        match stmt {
            SqlStatement::Update { collection, sets, .. } => {
                assert_eq!(collection, "users");
                assert_eq!(sets.len(), 1);
                assert_eq!(sets[0].0, "status");
                assert_eq!(sets[0].1, JsonValue::String("active".to_string()));
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse_sql("DELETE FROM users WHERE status = 'inactive'").unwrap();
        match stmt {
            SqlStatement::Delete { collection, .. } => {
                assert_eq!(collection, "users");
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse_sql(
            "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')"
        ).unwrap();
        match stmt {
            SqlStatement::Insert { collection, columns, rows } => {
                assert_eq!(collection, "users");
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], JsonValue::Number(serde_json::Number::from(1)));
                assert_eq!(rows[0][1], JsonValue::String("alice".to_string()));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_merge() {
        let sql = "MERGE INTO users USING [{\"id\":1,\"name\":\"alice\"}] ON id = id \
                   WHEN MATCHED THEN UPDATE \
                   WHEN NOT MATCHED THEN INSERT";
        let stmt = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Merge { target, match_keys, when_matched, when_not_matched, .. } => {
                assert_eq!(target, "users");
                assert_eq!(match_keys, vec![("id".to_string(), "id".to_string())]);
                assert!(matches!(when_matched, MergeAction::Update));
                assert!(matches!(when_not_matched, MergeAction::Insert));
            }
            _ => panic!("Expected Merge"),
        }
    }
}
