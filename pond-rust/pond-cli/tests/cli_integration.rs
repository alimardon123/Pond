// Integration tests for the pond CLI
//
// These tests invoke the `pond` binary as a subprocess and verify
// its behavior end-to-end. They test the full stack: CLI arg parsing
// → kernel → local FS → output.

use std::process::Command;
use std::fs;
use tempfile::TempDir;

/// Path to the built `pond` binary.
/// CARGO_MANIFEST_DIR is the pond-cli crate directory; the binary is
/// at ../target/release/pond (workspace target dir).
const POND_BIN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../target/release/pond");

fn pond(root: &std::path::Path, args: &[&str]) -> Command {
    let mut cmd = Command::new(POND_BIN);
    cmd.arg("--root").arg(root).args(args);
    cmd
}

#[test]
fn test_init_creates_pond_dir() {
    let dir = TempDir::new().unwrap();
    let output = pond(dir.path(), &["init", "."]).output().unwrap();
    assert!(output.status.success(),
            "stderr: {}", String::from_utf8_lossy(&output.stderr));
    assert!(dir.path().join(".pond").exists());
    assert!(dir.path().join(".pond").join("objects").exists());
}

#[test]
fn test_write_and_read_json() {
    let dir = TempDir::new().unwrap();
    pond(dir.path(), &["init", "."]).output().unwrap();

    let output = pond(dir.path(), &["write", "users", "--json",
                                    r#"{"name":"alice","age":30}"#])
        .output().unwrap();
    assert!(output.status.success(),
            "stderr: {}", String::from_utf8_lossy(&output.stderr));

    let output = pond(dir.path(), &["read", "users"]).output().unwrap();
    assert!(output.status.success());
    let data = String::from_utf8_lossy(&output.stdout);
    assert!(data.contains("alice"));
}

#[test]
fn test_write_from_file() {
    let dir = TempDir::new().unwrap();
    pond(dir.path(), &["init", "."]).output().unwrap();

    let data_file = dir.path().join("data.txt");
    fs::write(&data_file, "hello from file").unwrap();

    let output = pond(dir.path(), &["write", "docs",
                                    data_file.to_str().unwrap()])
        .output().unwrap();
    assert!(output.status.success());

    let output = pond(dir.path(), &["read", "docs"]).output().unwrap();
    assert_eq!(String::from_utf8_lossy(&output.stdout), "hello from file");
}

#[test]
fn test_write_from_stdin() {
    use std::io::Write;
    let dir = TempDir::new().unwrap();
    pond(dir.path(), &["init", "."]).output().unwrap();

    let mut cmd = pond(dir.path(), &["write", "logs", "--bytes"]);
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"raw binary log data").unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());

    let output = pond(dir.path(), &["read", "logs"]).output().unwrap();
    assert_eq!(String::from_utf8_lossy(&output.stdout), "raw binary log data");
}

#[test]
fn test_dedup() {
    let dir = TempDir::new().unwrap();
    pond(dir.path(), &["init", "."]).output().unwrap();

    let output1 = pond(dir.path(), &["write", "coll1", "--json",
                                     r#"{"data":"same"}"#]).output().unwrap();
    let output2 = pond(dir.path(), &["write", "coll2", "--json",
                                     r#"{"data":"same"}"#]).output().unwrap();

    let out1 = String::from_utf8_lossy(&output1.stdout).into_owned();
    let out2 = String::from_utf8_lossy(&output2.stdout).into_owned();
    let hash1 = out1.split('\t').next().unwrap();
    let hash2 = out2.split('\t').next().unwrap();
    assert_eq!(hash1, hash2, "same data must produce same hash (dedup)");
}

#[test]
fn test_ls() {
    let dir = TempDir::new().unwrap();
    pond(dir.path(), &["init", "."]).output().unwrap();

    pond(dir.path(), &["write", "users", "--json", r#"{"a":1}"#]).output().unwrap();
    pond(dir.path(), &["write", "orders", "--json", r#"{"b":2}"#]).output().unwrap();

    let output = pond(dir.path(), &["ls"]).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("users"));
    assert!(stdout.contains("orders"));
}

#[test]
fn test_branch_and_merge() {
    let dir = TempDir::new().unwrap();
    pond(dir.path(), &["init", "."]).output().unwrap();

    pond(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]).output().unwrap();

    let output = pond(dir.path(), &["branch", "users", "experiment"]).output().unwrap();
    assert!(output.status.success(),
            "stderr: {}", String::from_utf8_lossy(&output.stderr));

    pond(dir.path(), &["write", "users", "--json", r#"{"v":2}"#]).output().unwrap();

    let output = pond(dir.path(), &["merge", "users", "experiment"]).output().unwrap();
    assert!(output.status.success());

    let output = pond(dir.path(), &["read", "users"]).output().unwrap();
    let data = String::from_utf8_lossy(&output.stdout);
    assert!(data.contains(r#""v":1"#), "after merge, users should be v1 (branch HEAD)");
}

#[test]
fn test_cat_by_prefix() {
    let dir = TempDir::new().unwrap();
    pond(dir.path(), &["init", "."]).output().unwrap();

    let output = pond(dir.path(), &["write", "coll", "--json",
                                    r#"{"key":"value"}"#]).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let prefix = stdout.split('\t').next().unwrap();

    let output = pond(dir.path(), &["cat", prefix]).output().unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("value"));
}

#[test]
fn test_version() {
    let output = pond(std::path::Path::new("/tmp"), &["version"]).output().unwrap();
    assert!(output.status.success());
    let v = String::from_utf8_lossy(&output.stdout);
    assert!(v.starts_with("pond "));
}

#[test]
fn test_persistence_across_invocations() {
    let dir = TempDir::new().unwrap();
    pond(dir.path(), &["init", "."]).output().unwrap();

    pond(dir.path(), &["write", "persistent", "--json",
                      r#"{"survives":true}"#]).output().unwrap();

    let output = pond(dir.path(), &["read", "persistent"]).output().unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("survives"));
}
