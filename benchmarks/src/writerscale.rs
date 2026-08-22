// writerscale.rs — what does a reader pay as writers accumulate?
//
// The design's headline property is that any number of writers converge
// without coordination, because each publishes to its own head key and readers
// merge every head they find. Convergence is tested and holds.
//
// The cost of that convergence is not tested anywhere. A reader opens with one
// LIST plus one read per head, and folds every root it finds into a merged
// view. Heads are never removed and never folded, so both terms grow with the
// number of writers that have *ever* published — not the number currently
// active, and not the amount of data.
//
// For a system whose stated goal is writers "from any lens, any user
// worldwide", that is the number worth knowing. This measures it.
//
//   cargo run --release -p pond_bench --bin writerscale

use std::sync::Arc;
use std::time::{Duration, Instant};

use pond_engine::{Engine, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered, ObjectStore};
use pond_record::{Record, Value, Version};

/// Per-request delay used to model an object-storage round trip.
///
/// Request counts cannot show what this benchmark is about. A balanced merge
/// issues roughly as many requests as a left fold — what it changes is how
/// many of them have to *wait* for each other, and that only becomes visible
/// as time. Kept small so the whole table runs in seconds; the ratio is what
/// matters, and it scales with the real figure.
const RTT: Duration = Duration::from_micros(500);

/// Adds a fixed delay to every backend operation.
struct Latent<S: ObjectStore> {
    inner: S,
}

impl<S: ObjectStore> ObjectStore for Latent<S> {
    fn put_blob(&self, d: &[u8]) -> std::io::Result<String> {
        std::thread::sleep(RTT);
        self.inner.put_blob(d)
    }
    fn get_blob(&self, h: &str) -> std::io::Result<Vec<u8>> {
        std::thread::sleep(RTT);
        self.inner.get_blob(h)
    }
    fn put_blob_batch(&self, i: &[Vec<u8>]) -> std::io::Result<Vec<String>> {
        std::thread::sleep(RTT);
        self.inner.put_blob_batch(i)
    }
    fn get_blob_batch(&self, h: &[String]) -> std::io::Result<Vec<Vec<u8>>> {
        std::thread::sleep(RTT);
        self.inner.get_blob_batch(h)
    }
    fn get_object_batch(&self, p: &[String]) -> Vec<Option<Vec<u8>>> {
        std::thread::sleep(RTT);
        self.inner.get_object_batch(p)
    }
    fn delete_path_batch(&self, p: &[String]) -> std::io::Result<usize> {
        std::thread::sleep(RTT);
        self.inner.delete_path_batch(p)
    }
    fn delete_blob_batch(&self, h: &[String]) -> std::io::Result<usize> {
        std::thread::sleep(RTT);
        self.inner.delete_blob_batch(h)
    }
    fn put_path(&self, p: &str, h: &str) -> std::io::Result<()> {
        std::thread::sleep(RTT);
        self.inner.put_path(p, h)
    }
    fn get_path(&self, p: &str) -> Option<String> {
        std::thread::sleep(RTT);
        self.inner.get_path(p)
    }
    fn put_object(&self, p: &str, b: &[u8]) -> std::io::Result<()> {
        std::thread::sleep(RTT);
        self.inner.put_object(p, b)
    }
    fn get_object(&self, p: &str) -> Option<Vec<u8>> {
        std::thread::sleep(RTT);
        self.inner.get_object(p)
    }
    fn delete_path(&self, p: &str) -> std::io::Result<bool> {
        self.inner.delete_path(p)
    }
    fn list_paths(&self, p: &str) -> std::io::Result<Vec<String>> {
        std::thread::sleep(RTT);
        self.inner.list_paths(p)
    }
    fn blob_exists(&self, h: &str) -> bool {
        self.inner.blob_exists(h)
    }
    fn delete_blob(&self, h: &str) -> std::io::Result<bool> {
        self.inner.delete_blob(h)
    }
}

/// Rows each writer contributes. Kept small and constant so the only thing
/// varying across the table is the writer count.
const ROWS_PER_WRITER: i64 = 4;

struct Sample {
    writers: u64,
    open_requests: u64,
    open_round_trips: u64,
    scan_requests: u64,
    scan_round_trips: u64,
    /// Measured wall clock of the first scan, with every request delayed.
    scan_millis: f64,
    /// What that scan would have cost if every request waited for the one
    /// before it — the left fold this replaced.
    sequential_millis: f64,
    rows: usize,
}

fn measure(writers: u64) -> Sample {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Metered::new(Latent {
        inner: LocalFSObjectStore::new(dir.path()).unwrap(),
    }));

    for w in 1..=writers {
        let mut e = Engine::open(Arc::clone(&store), w).unwrap();
        let rows: Vec<(Key, Record)> = (0..ROWS_PER_WRITER)
            .map(|i| {
                let id = w as i64 * 1_000 + i;
                (
                    Key::new(vec![int(id)]),
                    Record::new().with_field("w", Value::Int(w as i64), Version::new(100, w, 1)),
                )
            })
            .collect();
        e.write_records("t", rows).unwrap();
        e.publish().unwrap();
    }

    // Opening a reader: one LIST, then the heads.
    store.reset();
    let mut r = Reader::open(Arc::clone(&store)).unwrap();
    let open = store.stats();

    // First read of a collection: folds every root into a merged view.
    store.reset();
    let started = Instant::now();
    let rows = r.scan("t").unwrap();
    let scan_millis = started.elapsed().as_secs_f64() * 1_000.0;
    let scan = store.stats();

    Sample {
        writers,
        open_requests: open.requests(),
        open_round_trips: open.round_trips,
        scan_requests: scan.requests(),
        scan_round_trips: scan.round_trips,
        scan_millis,
        sequential_millis: scan.round_trips as f64 * RTT.as_secs_f64() * 1_000.0,
        rows: rows.len(),
    }
}

fn main() {
    println!(
        "Reader cost against the number of writers that have ever published.\n\
         Each writer contributes {} rows, so data grows only linearly and any\n\
         super-linear term below belongs to the writer count, not the data.\n",
        ROWS_PER_WRITER
    );
    println!(
        "Every backend request is delayed by {:?} to model an object-storage \
         round trip.\n",
        RTT
    );
    println!(
        "| writers | rows | open reqs / trips | scan reqs / trips | scan wall clock | \
         if fully sequential | speedup |"
    );
    println!("|---|---|---|---|---|---|---|");

    for writers in [1u64, 2, 4, 8, 16, 32, 64, 128, 256] {
        let s = measure(writers);
        println!(
            "| {} | {} | {} / {} | {} / {} | {:.0} ms | {:.0} ms | **{:.1}x** |",
            s.writers,
            s.rows,
            s.open_requests,
            s.open_round_trips,
            s.scan_requests,
            s.scan_round_trips,
            s.scan_millis,
            s.sequential_millis,
            s.sequential_millis / s.scan_millis.max(0.001),
        );
    }

    println!(
        "\nRequest counts grow linearly with writers and cannot show what this\n\
         measures. The merge is a semilattice join — associative and\n\
         commutative — so the fold over writer roots can be re-associated into\n\
         a balanced reduction whose levels are independent. That leaves the\n\
         request count alone and collapses the *dependent* depth from W to\n\
         log2(W), which is the column on the right."
    );
}
