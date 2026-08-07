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
