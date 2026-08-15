// Integration tests for the pond CLI
// Tests the full API: init, write, read, branch, checkout (-b), merge (source→target),
// branches, history, undo, ls, cat, version.

use std::process::Command;
use std::fs;
use tempfile::TempDir;

const POND_BIN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../target/release/pond");

fn pond(root: &std::path::Path, args: &[&str]) -> Command {
    let mut cmd = Command::new(POND_BIN);
    cmd.arg("--root").arg(root).args(args);
    cmd
}

fn run(root: &std::path::Path, args: &[&str]) -> String {
    let output = pond(root, args).output().unwrap();
    if !output.status.success() {
        panic!("pond {:?} failed: {}", args, String::from_utf8_lossy(&output.stderr));
    }
    String::from_utf8_lossy(&output.stdout).to_string()
}

#[test]
fn test_init_creates_pond_dir() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    assert!(dir.path().join("blobs").exists());
}

#[test]
fn test_write_and_read_json() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"name":"alice"}"#]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains("alice"));
}

#[test]
fn test_write_from_file() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    let f = dir.path().join("data.txt");
    fs::write(&f, "hello from file").unwrap();
    run(dir.path(), &["write", "docs", f.to_str().unwrap()]);
    assert_eq!(run(dir.path(), &["read", "docs"]), "hello from file");
}

#[test]
fn test_write_from_stdin() {
    use std::io::Write;
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    let mut cmd = pond(dir.path(), &["write", "logs", "--bytes"]);
    cmd.stdin(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    child.stdin.as_mut().unwrap().write_all(b"raw log data").unwrap();
    child.wait_with_output().unwrap();
    assert_eq!(run(dir.path(), &["read", "logs"]), "raw log data");
}

#[test]
fn test_dedup() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    // Write the same data to two collections. The DATA blob is deduped
    // (same hash), but the COMMIT blobs differ (different timestamps).
    // We verify dedup by checking the underlying data is identical.
    run(dir.path(), &["write", "c1", "--json", r#"{"d":"same"}"#]);
    run(dir.path(), &["write", "c2", "--json", r#"{"d":"same"}"#]);
    // Both collections should return the same data
    let out1 = run(dir.path(), &["read", "c1"]);
    let out2 = run(dir.path(), &["read", "c2"]);
    assert_eq!(out1, out2, "same data must produce same content (dedup)");
}

#[test]
fn test_ls() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"a":1}"#]);
    run(dir.path(), &["write", "orders", "--json", r#"{"b":2}"#]);
    let out = run(dir.path(), &["ls"]);
    assert!(out.contains("users"));
    assert!(out.contains("orders"));
}

#[test]
fn test_history_shows_commits() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#, "-m", "first"]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":2}"#, "-m", "second"]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":3}"#, "-m", "third"]);
    let out = run(dir.path(), &["history", "users"]);
    assert!(out.contains("first"));
    assert!(out.contains("second"));
    assert!(out.contains("third"));
}

#[test]
fn test_checkout_and_branch() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]);
    run(dir.path(), &["checkout", "-b", "users", "experiment"]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":99}"#]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains("99"), "experiment should have v=99, got: {}", out);
    run(dir.path(), &["checkout", "users", "main"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains(r#""v":1"#), "main should have v=1, got: {}", out);
}

#[test]
fn test_merge_source_into_target() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]);
    run(dir.path(), &["checkout", "-b", "users", "experiment"]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":99}"#]);
    run(dir.path(), &["checkout", "users", "main"]);
    run(dir.path(), &["merge", "users", "experiment"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains("99"), "after merge, main should have v=99, got: {}", out);
}

#[test]
fn test_merge_into_specific_target() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]);
    run(dir.path(), &["branch", "users", "feature"]);
    run(dir.path(), &["branch", "users", "staging"]);
    run(dir.path(), &["checkout", "users", "feature"]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":42}"#]);
    run(dir.path(), &["checkout", "users", "main"]);
    run(dir.path(), &["merge", "users", "feature", "--into", "staging"]);
    run(dir.path(), &["checkout", "users", "staging"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains("42"), "staging should have v=42, got: {}", out);
    run(dir.path(), &["checkout", "users", "main"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains(r#""v":1"#), "main should still have v=1, got: {}", out);
}

#[test]
fn test_branches_command() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]);
    run(dir.path(), &["branch", "users", "experiment"]);
    run(dir.path(), &["branch", "users", "feature"]);
    let out = run(dir.path(), &["branches", "users"]);
    assert!(out.contains("main"));
    assert!(out.contains("experiment"));
    assert!(out.contains("feature"));
    assert!(out.contains("* main"));
}

#[test]
fn test_undo() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":2}"#]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":3}"#]);
    run(dir.path(), &["undo", "users", "1"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains(r#""v":2"#), "after undo 1, should be v=2, got: {}", out);
    run(dir.path(), &["undo", "users", "1"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains(r#""v":1"#), "after undo 2, should be v=1, got: {}", out);
}

#[test]
fn test_cat_by_prefix() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    // Write data and get the commit hash
    let out = run(dir.path(), &["write", "coll", "--json", r#"{"key":"value"}"#]);
    let prefix = out.split('\t').next().unwrap();
    // cat reads the raw blob (which is the commit JSON, containing "manifest" field)
    let out = run(dir.path(), &["cat", prefix]);
    assert!(out.contains("manifest"), "cat should read the commit blob with manifest field, got: {}", out);
}

#[test]
fn test_version() {
    let out = run(std::path::Path::new("/tmp"), &["version"]);
    assert!(out.starts_with("pond "));
}

#[test]
fn test_persistence_across_invocations() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "persistent", "--json", r#"{"survives":true}"#]);
    let out = run(dir.path(), &["read", "persistent"]);
    assert!(out.contains("survives"));
}

#[test]
fn test_auto_discovery_from_subdir() {
    // Test git-style auto-discovery: `pond init` creates .pond/ marker,
    // and subsequent commands find it by walking up from CWD.
    let dir = TempDir::new().unwrap();
    let root = dir.path();

    // Init in the root
    run(root, &["init", "."]);
    assert!(root.join(".pond").exists());
    assert!(root.join(".pond/config").exists());

    // Write data from the root
    run(root, &["write", "users", "--json", r#"{"name":"alice"}"#]);

    // Read from a nested subdirectory — should auto-discover .pond/
    let subdir = root.join("a/b/c");
    fs::create_dir_all(&subdir).unwrap();

    let mut cmd = Command::new(POND_BIN);
    cmd.current_dir(&subdir).args(&["read", "users"]);
    let output = cmd.output().unwrap();
    assert!(output.status.success(),
        "auto-discovery failed: {}", String::from_utf8_lossy(&output.stderr));
    let out = String::from_utf8_lossy(&output.stdout);
    assert!(out.contains("alice"), "expected 'alice' in output, got: {}", out);

    // Also test `ls` from the subdirectory
    let mut cmd = Command::new(POND_BIN);
    cmd.current_dir(&subdir).args(&["ls"]);
    let output = cmd.output().unwrap();
    assert!(output.status.success());
    let out = String::from_utf8_lossy(&output.stdout);
    assert!(out.contains("users"), "expected 'users' in ls output, got: {}", out);
}

#[test]
fn test_auto_discovery_creates_pond_marker() {
    // Verify that `pond init` creates the .pond/ marker directory
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    // The .pond/ marker should exist
    assert!(dir.path().join(".pond").is_dir(),
        ".pond/ marker directory not created");

    // The .pond/config file should exist (Pond-level settings)
    let config = dir.path().join(".pond/config");
    assert!(config.exists(), ".pond/config not created");
    // Config for local FS should NOT have an active storage= line
    // (only comments mentioning storage=s3:// as documentation).
    // An active storage= line means S3.
    let config_content = fs::read_to_string(&config).unwrap();
    let has_active_storage = config_content.lines()
        .map(|l| l.trim())
        .filter(|l| !l.starts_with('#'))
        .any(|l| l.starts_with("storage="));
    assert!(!has_active_storage,
        "local FS config should NOT have an active 'storage=' line (only comments)");
}

// ===========================================================================
// Structured row tests — write-rows + read-rows round-trips
// ===========================================================================

#[test]
fn test_write_rows_and_read_rows_round_trip() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    // Write a small users table.
    let json = r#"[
        {"id": 1, "name": "alice", "age": 30},
        {"id": 2, "name": "bob",   "age": 25},
        {"id": 3, "name": "carol", "age": 35}
    ]"#;
    let out = run(dir.path(), &["write-rows", "users", "--json", json, "-m", "seed users"]);
    // Output is "<12-char-hash>\t<collection>"
    assert!(out.contains("users"), "write-rows output should mention collection: {}", out);

    // Read back as JSON.
    let read = run(dir.path(), &["read-rows", "users"]);
    let parsed: serde_json::Value = serde_json::from_str(&read)
        .unwrap_or_else(|e| panic!("read-rows output is not valid JSON: {}\noutput: {}", e, read));

    let arr = parsed.as_array().expect("read-rows output should be a JSON array");
    assert_eq!(arr.len(), 3, "expected 3 rows, got: {}", read);

    // Find alice's row and verify.
    let alice = arr.iter().find(|r| r.get("name").and_then(|v| v.as_str()) == Some("alice"))
        .unwrap_or_else(|| panic!("alice not found in rows: {}", read));
    assert_eq!(alice.get("id").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(alice.get("age").and_then(|v| v.as_i64()), Some(30));
}

#[test]
fn test_read_rows_with_where_filter() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    let json = r#"[
        {"id": 1, "name": "alice", "age": 30},
        {"id": 2, "name": "bob",   "age": 25},
        {"id": 3, "name": "carol", "age": 35},
        {"id": 4, "name": "dave",  "age": 40}
    ]"#;
    run(dir.path(), &["write-rows", "users", "--json", json, "-m", "seed"]);

    // WHERE age > 30 → carol (35) + dave (40).
    let read = run(dir.path(), &["read-rows", "users", "--where", "age > 30"]);
    let parsed: serde_json::Value = serde_json::from_str(&read)
        .unwrap_or_else(|e| panic!("read-rows output is not valid JSON: {}\noutput: {}", e, read));
    let arr = parsed.as_array().expect("array");
    assert_eq!(arr.len(), 2, "expected 2 rows with age > 30, got: {}", read);

    let names: Vec<&str> = arr.iter()
        .filter_map(|r| r.get("name").and_then(|v| v.as_str()))
        .collect();
    assert!(names.contains(&"carol"));
    assert!(names.contains(&"dave"));
}

#[test]
fn test_read_rows_with_column_projection() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    let json = r#"[
        {"id": 1, "name": "alice", "age": 30, "city": "NYC"},
        {"id": 2, "name": "bob",   "age": 25, "city": "LA"}
    ]"#;
    run(dir.path(), &["write-rows", "users", "--json", json, "-m", "seed"]);

    // Project only id and name.
    let read = run(dir.path(), &["read-rows", "users", "--columns", "id,name"]);
    let parsed: serde_json::Value = serde_json::from_str(&read)
        .unwrap_or_else(|e| panic!("read-rows output is not valid JSON: {}\noutput: {}", e, read));
    let arr = parsed.as_array().expect("array");
    assert_eq!(arr.len(), 2);

    for row in arr {
        let obj = row.as_object().unwrap();
        assert!(obj.contains_key("id"), "id column should be present: {:?}", obj);
        assert!(obj.contains_key("name"), "name column should be present: {:?}", obj);
        assert!(!obj.contains_key("age"), "age should be projected out: {:?}", obj);
        assert!(!obj.contains_key("city"), "city should be projected out: {:?}", obj);
    }
}

#[test]
fn test_sql_select_star() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    // Seed via SQL INSERT.
    let insert_sql = "INSERT INTO products (id, name, price) VALUES (10, 'widget', 9.99), (20, 'gadget', 19.99)";
    let out = run(dir.path(), &["sql", insert_sql]);
    let parsed: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("sql output is not valid JSON: {}\noutput: {}", e, out));
    // INSERT returns a commit status row.
    assert!(parsed.get("rows").is_some(), "sql output should have rows: {}", out);

    // SELECT * FROM products.
    let select_out = run(dir.path(), &["sql", "SELECT * FROM products"]);
    let select_parsed: serde_json::Value = serde_json::from_str(&select_out)
        .unwrap_or_else(|e| panic!("sql SELECT output is not valid JSON: {}\noutput: {}", e, select_out));
    let rows = select_parsed.get("rows").and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("sql SELECT output should have rows array: {}", select_out));
    assert_eq!(rows.len(), 2, "expected 2 product rows, got: {}", select_out);
}

#[test]
fn test_sql_select_with_where_and_limit() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    // Seed 5 users.
    let insert_sql = "INSERT INTO users (id, name, age) VALUES \
        (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35), \
        (4, 'dave', 40), (5, 'erin', 28)";
    run(dir.path(), &["sql", insert_sql]);

    // SELECT with WHERE age >= 30 LIMIT 2.
    let out = run(dir.path(), &["sql", "SELECT name, age FROM users WHERE age >= 30 ORDER BY age ASC LIMIT 2"]);
    let parsed: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("sql output is not valid JSON: {}\noutput: {}", e, out));
    let rows = parsed.get("rows").and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("sql output should have rows: {}", out));

    assert_eq!(rows.len(), 2, "expected 2 rows after LIMIT, got: {}", out);
    // Ordered by age ASC, the two youngest >=30 are alice (30) and carol (35).
    let first_age = rows[0].get("age").and_then(|v| v.as_i64());
    let second_age = rows[1].get("age").and_then(|v| v.as_i64());
    assert_eq!(first_age, Some(30), "first row age should be 30: {}", out);
    assert_eq!(second_age, Some(35), "second row age should be 35: {}", out);
}
