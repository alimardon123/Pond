// read_errors.rs — a failed read must not come back as a short answer.
//
// `NodeStore::get` returns `Option<Vec<u8>>`, which cannot distinguish "this
// node is not there" from "asking for it failed". A transient 500, a dropped
// connection, an expired credential: every one arrives as `None`, and a
// traversal that reads `None` as "empty subtree" returns fewer rows and
// reports success.
//
// That is the worst thing a storage system can do. Losing rows is bad; losing
// rows while saying `Ok` means nothing downstream can even retry, because
// nothing downstream knows. A partial scan feeding a report, a migration, or a
// compaction propagates the loss into data that looks fine.
//
// The write path already refused this: `EngineStore::put` panics rather than
// let a failed write turn into a hash referencing nothing, and says so in a
// comment. The read path did the opposite, two lines below.
//
// These tests fail — by returning `Ok` with too few rows — without the failure
// counter that `EngineStore` now keeps.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use pond_engine::{Engine, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, ObjectStore};
use pond_record::{Record, Value, Version};

/// A store that starts failing `get_blob` once armed.
///
/// It fails reads and nothing else, because that is the failure being tested:
/// writes land, the tree on disk is complete and correct, and only the reading
/// of it goes wrong. Anything that comes back short is the reader's doing.
struct Flaky {
    inner: LocalFSObjectStore,
    armed: AtomicBool,
    /// How many reads to fail before letting them through again. `u64::MAX`
    /// means "all of them".
    budget: AtomicU64,
}

impl Flaky {
    fn new(inner: LocalFSObjectStore) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(false),
            budget: AtomicU64::new(u64::MAX),
        }
    }

    fn arm(&self, failures: u64) {
        self.budget.store(failures, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    fn should_fail(&self) -> bool {
        if !self.armed.load(Ordering::SeqCst) {
            return false;
        }
        let left = self.budget.load(Ordering::SeqCst);
        if left == 0 {
            return false;
        }
        if left != u64::MAX {
            self.budget.fetch_sub(1, Ordering::SeqCst);
        }
        true
    }
}

impl ObjectStore for Flaky {
    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        if self.should_fail() {
            return Err(io::Error::other("simulated backend failure"));
        }
        self.inner.get_blob(hash)
    }
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        self.inner.put_blob(data)
    }
    fn put_path(&self, path: &str, hash: &str) -> io::Result<()> {
        self.inner.put_path(path, hash)
    }
    fn get_path(&self, path: &str) -> Option<String> {
        self.inner.get_path(path)
    }
    fn put_object(&self, path: &str, bytes: &[u8]) -> io::Result<()> {
        self.inner.put_object(path, bytes)
    }
    fn get_object(&self, path: &str) -> Option<Vec<u8>> {
        self.inner.get_object(path)
    }
    fn delete_path(&self, path: &str) -> io::Result<bool> {
        self.inner.delete_path(path)
    }
    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list_paths(prefix)
    }
    fn blob_exists(&self, hash: &str) -> bool {
        self.inner.blob_exists(hash)
    }
    fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        self.inner.delete_blob(hash)
    }
}

const ROWS: usize = 20_000;

fn record(i: u64) -> Record {
    let mut r = Record::new();
    r.set("v", Value::Int(i as i64), Version::new(i, 0, 1));
    r
}

/// Build a complete collection, then hand back a store that can be made to
/// fail reads against it.
fn populated() -> (tempfile::TempDir, Arc<Flaky>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Flaky::new(LocalFSObjectStore::new(dir.path()).unwrap()));

    let mut engine = Engine::open(store.clone(), 1).unwrap();
    let rows: Vec<(Key, Record)> = (0..ROWS)
        .map(|i| (Key::new(vec![int(i as i64)]), record(i as u64)))
        .collect();
    for chunk in rows.chunks(5_000) {
        engine.write_records("t", chunk.to_vec()).unwrap();
    }
    engine.publish().unwrap();

    // The data really is all there when reads work.
    let mut reader = Reader::open(store.clone()).unwrap();
    assert_eq!(reader.scan("t").unwrap().len(), ROWS, "setup did not land");

    (dir, store)
}

/// A scan that hits a failed node read must return an error, not a short list.
#[test]
fn a_failed_node_read_does_not_become_a_short_scan() {
    let (_dir, store) = populated();

    let mut reader = Reader::open(store.clone()).unwrap();
    store.arm(1);

    match reader.scan("t") {
        Err(_) => {}
        Ok(rows) => panic!(
            "a failed read came back as success with {} of {} rows — the caller \
             cannot tell data loss from an empty range",
            rows.len(),
            ROWS
        ),
    }
}

/// The same for a range scan, which walks its own path through the tree.
#[test]
fn a_failed_node_read_does_not_become_a_short_range_scan() {
    let (_dir, store) = populated();

    let mut reader = Reader::open(store.clone()).unwrap();
    store.arm(1);

    let lo = Key::new(vec![int(0)]);
    let hi = Key::new(vec![int(ROWS as i64)]);
    assert!(
        reader.scan_range("t", &lo, &hi).is_err(),
        "a failed read came back as a successful range scan"
    );
}

/// And for a projected scan, which takes a third path.
#[test]
fn a_failed_node_read_does_not_become_a_short_projected_scan() {
    let (_dir, store) = populated();

    let mut reader = Reader::open(store.clone()).unwrap();
    store.arm(1);

    assert!(
        reader.scan_projected("t", &["v"]).is_err(),
        "a failed read came back as a successful projected scan"
    );
}

/// A point read that fails must say so rather than report the key absent.
///
/// "Not found" and "could not look" are different answers, and a caller that
/// treats the first as authoritative will happily insert a duplicate.
#[test]
fn a_failed_node_read_does_not_become_a_missing_key() {
    let (_dir, store) = populated();

    let mut reader = Reader::open(store.clone()).unwrap();
    store.arm(u64::MAX);

    match reader.get("t", &Key::new(vec![int(1234)])) {
        Err(_) => {}
        Ok(None) => panic!("a failed read reported the key as absent"),
        Ok(Some(_)) => panic!("a read that could not reach the store returned a value"),
    }
}

/// Reads that genuinely find nothing must still succeed — the fix must not
/// turn every empty result into an error.
#[test]
fn an_honestly_empty_result_is_still_success() {
    let (_dir, store) = populated();
    let mut reader = Reader::open(store.clone()).unwrap();

    let far = Key::new(vec![int(ROWS as i64 + 10_000)]);
    assert_eq!(reader.get("t", &far).unwrap(), None, "absent key must be Ok(None)");

    let lo = Key::new(vec![int(ROWS as i64 + 1_000)]);
    let hi = Key::new(vec![int(ROWS as i64 + 2_000)]);
    assert!(
        reader.scan_range("t", &lo, &hi).unwrap().is_empty(),
        "an empty range must be Ok(empty)"
    );

    assert_eq!(reader.scan("t").unwrap().len(), ROWS, "a healthy scan must still work");
}
