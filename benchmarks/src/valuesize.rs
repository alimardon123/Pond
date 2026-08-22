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
use pond_kernel::LocalFSObjectStore;
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
            backend.stats().bytes_written
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

/// Bytes reaching the store, counted by the kernel's `Metered` rather than by
/// a wrapper written here. The one this file used to define left the batch
/// methods on their sequential defaults, so it de-parallelised what it
/// measured.
type Counting<S> = pond_kernel::Metered<S>;


