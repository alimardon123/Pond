// Integration tests for the pond_sql crate.
//
// Each test exercises one of the SQL features listed in the task spec:
//   - SELECT * with WHERE
//   - SELECT with JOIN
//   - INSERT + SELECT round-trip
//   - UPDATE with WHERE
//   - DELETE with WHERE
//   - GROUP BY with COUNT/SUM/AVG
//   - ORDER BY ASC/DESC
//   - LIMIT/OFFSET
//   - Subqueries in WHERE

use pond_sql::execute;
use pond_storage::UnifiedStorage;
use serde_json::Value as JsonValue;

fn setup() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn open_storage(dir: &tempfile::TempDir) -> UnifiedStorage {
    UnifiedStorage::new_local(dir.path()).expect("open storage")
}

/// Helper: insert some rows into a fresh `users` collection.
fn seed_users(storage: &UnifiedStorage) {
    let sql = "INSERT INTO users (id, name, age, city) VALUES \
               (1, 'alice', 30, 'NYC'), \
               (2, 'bob', 25, 'LA'), \
               (3, 'carol', 35, 'NYC'), \
               (4, 'dave', 40, 'SF'), \
               (5, 'erin', 28, 'LA')";
    execute(storage, sql).expect("seed users");
}

fn row_col(row: &JsonValue, col: &str) -> JsonValue {
    if let Some(obj) = row.as_object() {
        if let Some(v) = obj.get(col) {
            return v.clone();
        }
        // Try matching by suffix (`alias.col` → `col`).
        for (k, v) in obj {
            if k.ends_with(&format!(".{}", col)) || k.rsplit('.').next() == Some(col) {
                return v.clone();
            }
        }
    }
    JsonValue::Null
}

#[test]
fn test_select_star_with_where() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(&storage, "SELECT * FROM users WHERE age >= 30")
        .expect("select * where");
    // alice (30), carol (35), dave (40) → 3 rows
    assert_eq!(result.rows.len(), 3);
    let names: Vec<String> = result.rows.iter()
        .map(|r| row_col(r, "name").as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"alice".to_string()));
    assert!(names.contains(&"carol".to_string()));
    assert!(names.contains(&"dave".to_string()));
}

#[test]
fn test_insert_select_roundtrip() {
    let dir = setup();
    let storage = open_storage(&dir);

    let insert = execute(
        &storage,
        "INSERT INTO products (id, name, price) VALUES (10, 'widget', 9.99), (20, 'gadget', 19.99)",
    )
    .expect("insert");
    // Insert returns a commit hash.
    assert!(insert.rows[0].get("commit").is_some());

    let select = execute(&storage, "SELECT id, name, price FROM products")
        .expect("select");
    assert_eq!(select.rows.len(), 2);
    // Both rows should be present.
    let ids: Vec<i64> = select.rows.iter()
        .filter_map(|r| row_col(r, "id").as_i64())
        .collect();
    assert!(ids.contains(&10));
    assert!(ids.contains(&20));
}

#[test]
fn test_select_with_join() {
    let dir = setup();
    let storage = open_storage(&dir);

    execute(&storage, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
        .expect("seed users");
    execute(
        &storage,
        "INSERT INTO orders (id, user_id, amount) VALUES \
         (100, 1, 50), (101, 1, 75), (102, 2, 30)",
    )
    .expect("seed orders");

    let result = execute(
        &storage,
        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1",
    )
    .expect("join");

    // alice has 2 orders → 2 joined rows.
    assert_eq!(result.rows.len(), 2);
    for row in &result.rows {
        assert_eq!(row_col(row, "name").as_str(), Some("alice"));
        assert!(row_col(row, "amount").as_i64().is_some());
    }
}

#[test]
fn test_select_left_join() {
    let dir = setup();
    let storage = open_storage(&dir);

    execute(&storage, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .expect("seed users");
    execute(
        &storage,
        "INSERT INTO orders (id, user_id, amount) VALUES (100, 1, 50)",
    )
    .expect("seed orders");

    let result = execute(
        &storage,
        "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id",
    )
    .expect("left join");

    // All 3 users should appear; bob + carol have NULL for order columns.
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_update_with_where() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(
        &storage,
        "UPDATE users SET city = 'Boston' WHERE age > 30",
    )
    .expect("update");
    // carol (35) + dave (40) → 2 updated.
    let count = result.rows[0].get("updated").and_then(|v| v.as_u64()).unwrap_or(0);
    assert_eq!(count, 2);

    // Verify via SELECT.
    let select = execute(&storage, "SELECT name, city FROM users WHERE city = 'Boston'")
        .expect("select after update");
    assert_eq!(select.rows.len(), 2);
}

#[test]
fn test_delete_with_where() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(&storage, "DELETE FROM users WHERE age < 30")
        .expect("delete");
    // bob (25) + erin (28) → 2 deleted.
    let count = result.rows[0].get("deleted").and_then(|v| v.as_u64()).unwrap_or(0);
    assert_eq!(count, 2);

    let select = execute(&storage, "SELECT name FROM users").expect("select after delete");
    assert_eq!(select.rows.len(), 3); // 5 - 2
}

#[test]
fn test_group_by_with_count() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(
        &storage,
        "SELECT city, COUNT(*) FROM users GROUP BY city",
    )
    .expect("group by count");

    // NYC: alice + carol = 2, LA: bob + erin = 2, SF: dave = 1 → 3 groups.
    assert_eq!(result.rows.len(), 3);

    let by_city: std::collections::HashMap<String, u64> = result.rows.iter()
        .map(|r| {
            let city = row_col(r, "city").as_str().unwrap().to_string();
            let count = row_col(r, "COUNT(*)").as_u64().unwrap_or(0);
            (city, count)
        })
        .collect();
    assert_eq!(by_city.get("NYC"), Some(&2));
    assert_eq!(by_city.get("LA"), Some(&2));
    assert_eq!(by_city.get("SF"), Some(&1));
}

#[test]
fn test_group_by_with_sum_and_avg() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(
        &storage,
        "SELECT city, SUM(age), AVG(age) FROM users GROUP BY city",
    )
    .expect("group by sum/avg");

    // Find the NYC group: alice (30) + carol (35) → sum=65, avg=32.5.
    let nyc = result.rows.iter()
        .find(|r| row_col(r, "city").as_str() == Some("NYC"))
        .expect("NYC group");
    let sum = nyc.get("SUM(age)").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let avg = nyc.get("AVG(age)").and_then(|v| v.as_f64()).unwrap_or(0.0);
    assert_eq!(sum, 65.0);
    assert!((avg - 32.5).abs() < 1e-6);
}

#[test]
fn test_order_by_asc_desc() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let asc = execute(&storage, "SELECT name, age FROM users ORDER BY age ASC")
        .expect("order asc");
    let ages_ascending: Vec<i64> = asc.rows.iter()
        .filter_map(|r| row_col(r, "age").as_i64())
        .collect();
    let mut expected = ages_ascending.clone();
    expected.sort();
    assert_eq!(ages_ascending, expected);

    let desc = execute(&storage, "SELECT name, age FROM users ORDER BY age DESC")
        .expect("order desc");
    let ages_descending: Vec<i64> = desc.rows.iter()
        .filter_map(|r| row_col(r, "age").as_i64())
        .collect();
    let mut expected_desc = ages_descending.clone();
    expected_desc.sort();
    expected_desc.reverse();
    assert_eq!(ages_descending, expected_desc);
}

#[test]
fn test_limit_and_offset() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let limit_only = execute(&storage, "SELECT name FROM users ORDER BY name ASC LIMIT 2")
        .expect("limit only");
    assert_eq!(limit_only.rows.len(), 2);

    let limit_offset = execute(
        &storage,
        "SELECT name FROM users ORDER BY name ASC LIMIT 2 OFFSET 1",
    )
    .expect("limit+offset");
    assert_eq!(limit_offset.rows.len(), 2);
    // Make sure offset actually skipped the first row.
    let first_name = row_col(&limit_offset.rows[0], "name").as_str().unwrap().to_string();
    let first_in_full = execute(&storage, "SELECT name FROM users ORDER BY name ASC")
        .expect("full select");
    let full_first_name = row_col(&first_in_full.rows[0], "name").as_str().unwrap().to_string();
    assert_ne!(first_name, full_first_name);
}

#[test]
fn test_subquery_in_where() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);
    // Add some orders referencing user ids.
    execute(
        &storage,
        "INSERT INTO orders (id, user_id, amount) VALUES \
         (1, 1, 100), (2, 3, 200), (3, 5, 50)",
    )
    .expect("seed orders");

    // Find users whose id appears in orders.user_id.
    let result = execute(
        &storage,
        "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)",
    )
    .expect("subquery");

    // user_ids in orders: 1, 3, 5 → alice, carol, erin.
    let names: Vec<String> = result.rows.iter()
        .filter_map(|r| row_col(r, "name").as_str().map(|s| s.to_string()))
        .collect();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"alice".to_string()));
    assert!(names.contains(&"carol".to_string()));
    assert!(names.contains(&"erin".to_string()));
}

#[test]
fn test_select_file_csv() {
    let dir = setup();
    let csv_path = dir.path().join("data.csv");
    std::fs::write(
        &csv_path,
        "id,name,age\n1,alice,30\n2,bob,25\n3,carol,35\n",
    )
    .expect("write csv");

    let storage = open_storage(&dir);
    let csv_str = csv_path.to_str().unwrap();
    let result = execute(
        &storage,
        &format!("SELECT name FROM '{}' WHERE age > 26", csv_str),
    )
    .expect("select from csv");

    // alice (30), carol (35) → 2 rows.
    assert_eq!(result.rows.len(), 2);
    let names: Vec<String> = result.rows.iter()
        .filter_map(|r| row_col(r, "name").as_str().map(|s| s.to_string()))
        .collect();
    assert!(names.contains(&"alice".to_string()));
    assert!(names.contains(&"carol".to_string()));
}

#[test]
fn test_select_file_json() {
    let dir = setup();
    let json_path = dir.path().join("data.json");
    std::fs::write(
        &json_path,
        r#"[
            {"id": 1, "name": "alice", "active": true},
            {"id": 2, "name": "bob", "active": false},
            {"id": 3, "name": "carol", "active": true}
        ]"#,
    )
    .expect("write json");

    let storage = open_storage(&dir);
    let json_str = json_path.to_str().unwrap();
    let result = execute(
        &storage,
        &format!("SELECT name FROM '{}' WHERE active = true", json_str),
    )
    .expect("select from json");

    // alice + carol → 2 rows.
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_merge_statement() {
    let dir = setup();
    let storage = open_storage(&dir);

    execute(
        &storage,
        "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30), (2, 'bob', 25)",
    )
    .expect("seed users");

    // Merge: update existing id=1, insert new id=3.
    let result = execute(
        &storage,
        "MERGE INTO users USING [{\"id\":1,\"age\":31},{\"id\":3,\"name\":\"carol\",\"age\":28}] \
         ON id = id \
         WHEN MATCHED THEN UPDATE \
         WHEN NOT MATCHED THEN INSERT",
    )
    .expect("merge");

    // matched: 1 (id=1), inserted: 1 (id=3).
    let matched = result.rows[0].get("matched").and_then(|v| v.as_u64()).unwrap_or(99);
    let inserted = result.rows[0].get("inserted").and_then(|v| v.as_u64()).unwrap_or(99);
    assert_eq!(matched, 1);
    assert_eq!(inserted, 1);

    // Verify via SELECT.
    let select = execute(&storage, "SELECT id, name, age FROM users ORDER BY id ASC")
        .expect("select after merge");
    // Should be 3 rows total: id=1 (updated), id=2 (unchanged), id=3 (inserted).
    assert_eq!(select.rows.len(), 3);
}
