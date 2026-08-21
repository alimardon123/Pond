// valuesize.rs — what does storing values inline in the index actually cost?
//
// Records currently live as the index's values. That is right for small values
// and the reason the point-lookup numbers are good. The question this answers
// is where it stops being right, because that boundary is what a segment layer
// exists to move.
//
// Two costs grow with value size and neither is visible at 100 bytes:
//
//   1. An insert rewrites its leaf. A leaf holds `target` entries, so the
//      bytes rewritten per single-row insert are target x value_size — the
//      value is paid for `target` times over.
//   2. A scan that wants one field still transfers every field of every row,
//      because the row is one opaque value inside the node.
use pond_engine::{Engine, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, ObjectStore};
use pond_record::{Record, Value, Version};

fn main() {
    let rows = 2_000usize;

    println!("| value bytes | bytes rewritten by a 1-row update | amplification |");
    println!("|---|---|---|");

    for value_bytes in [100usize, 1_000, 10_000, 100_000] {
        let dir = tempfile::tempdir().unwrap();
        let payload = "x".repeat(value_bytes);

        let mut e = Engine::open(LocalFSObjectStore::new(dir.path()).unwrap(), 1).unwrap();
        e.write_records(
            "docs",
            (0..rows as i64)
                .map(|i| {
                    (
                        Key::new(vec![int(i)]),
                        Record::new().with_field(
                            "b",
                            Value::Str(payload.clone()),
                            Version::new(100, 0, 1),
                        ),
                    )
                })
                .collect(),
        )
        .unwrap();
        e.publish().unwrap();

        // Measure a single-row update against a fresh handle so nothing is
        // staged, and count only what that update writes.
        // `ObjectStore for Arc<T>` lets the counter stay readable after the
        // store is moved into the engine.
        let backend = std::sync::Arc::new(Counting::new(
            LocalFSObjectStore::new(dir.path()).unwrap(),
        ));
        let written = {
            let mut e = Engine::open(std::sync::Arc::clone(&backend), 1).unwrap();
            backend.reset();
            e.write_records(
                "docs",
                vec![(
                    Key::new(vec![int(rows as i64 / 2)]),
                    Record::new().with_field(
                        "b",
                        Value::Str("z".repeat(value_bytes)),
                        Version::new(200, 0, 1),
                    ),
                )],
            )
            .unwrap();
            e.publish().unwrap();
            backend.bytes()
        };

        println!(
            "| {} | {} KB | **{:.0}x** |",
            value_bytes,
            written / 1_000,
            written as f64 / value_bytes as f64
        );
        let _ = Reader::open(LocalFSObjectStore::new(dir.path()).unwrap());
    }
}

/// Counts bytes written to the backend.
struct Counting<S: ObjectStore> {
    inner: S,
    bytes: std::sync::atomic::AtomicU64,
}

impl<S: ObjectStore> Counting<S> {
    fn new(inner: S) -> Self {
        Self { inner, bytes: std::sync::atomic::AtomicU64::new(0) }
    }
    fn reset(&self) {
        self.bytes.store(0, std::sync::atomic::Ordering::Relaxed);
    }
    fn bytes(&self) -> u64 {
        self.bytes.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl<S: ObjectStore> ObjectStore for Counting<S> {
    fn put_blob(&self, data: &[u8]) -> std::io::Result<String> {
        self.bytes.fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.inner.put_blob(data)
    }
    fn get_blob(&self, h: &str) -> std::io::Result<Vec<u8>> { self.inner.get_blob(h) }
    fn put_path(&self, p: &str, h: &str) -> std::io::Result<()> { self.inner.put_path(p, h) }
    fn get_path(&self, p: &str) -> Option<String> { self.inner.get_path(p) }
    fn put_object(&self, p: &str, b: &[u8]) -> std::io::Result<()> {
        self.bytes.fetch_add(b.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.inner.put_object(p, b)
    }
    fn get_object(&self, p: &str) -> Option<Vec<u8>> { self.inner.get_object(p) }
    fn delete_path(&self, p: &str) -> std::io::Result<bool> { self.inner.delete_path(p) }
    fn list_paths(&self, p: &str) -> std::io::Result<Vec<String>> { self.inner.list_paths(p) }
    fn blob_exists(&self, h: &str) -> bool { self.inner.blob_exists(h) }
    fn delete_blob(&self, h: &str) -> std::io::Result<bool> { self.inner.delete_blob(h) }
}

