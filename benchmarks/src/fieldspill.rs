// fieldspill.rs — what does per-*record* spilling cost a row with one big
// field and several small ones?
//
// Spilling moves a large value out of the index leaf and leaves a pointer.
// It does that per *record*: a row whose encoding exceeds the threshold goes
// to its own blob, whole. So a row holding a 1 MiB attachment alongside a
// handful of small columns is one blob containing all of it, and that has two
// consequences worth measuring before deciding whether to change anything.
//
//   1. Reading any field means fetching every field. A scan that wants only
//      `id` still pulls the attachment across the network.
//   2. Writing any field means rewriting every field. Touching a status column
//      re-encodes the row, produces different bytes, and writes a whole new
//      blob — the attachment included, unchanged.
//
// The alternative is spilling per *field*: large fields become pointers, small
// ones stay inline in the record. That is a change to the record encoding, so
// it needs a reason bigger than "it seems tidier". This measures the reason.
//
//   cargo run --release -p pond_bench --bin fieldspill

use std::sync::Arc;

use pond_engine::{Engine, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered};
use pond_record::{Record, Value, Version};

const ROWS: i64 = 200;

fn row(i: i64, blob: &str, status: &str) -> (Key, Record) {
    (
        Key::new(vec![int(i)]),
        Record::new()
            .with_field("id", Value::Int(i), Version::new(100, 1, 1))
            .with_field("status", Value::Str(status.to_string()), Version::new(100, 1, 1))
            .with_field("attachment", Value::Str(blob.to_string()), Version::new(100, 1, 1)),
    )
}

struct Sample {
    attachment_kib: usize,
    update_bytes: u64,
    scan_bytes: u64,
    full_scan_bytes: u64,
    payload_bytes: u64,
}

fn measure(attachment_kib: usize) -> Sample {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let blob = "x".repeat(attachment_kib * 1024);

    let mut e = Engine::open(Arc::clone(&store), 1).unwrap();
    e.write_records("t", (0..ROWS).map(|i| row(i, &blob, "new")).collect())
        .unwrap();
    e.publish().unwrap();

    // Update one small field on one row. Nothing about the attachment changes.
    store.reset();
    let mut e = Engine::open(Arc::clone(&store), 1).unwrap();
    e.write_records(
        "t",
        vec![(
            Key::new(vec![int(ROWS / 2)]),
            Record::new().with_field(
                "status",
                Value::Str("done".to_string()),
                Version::new(200, 1, 1),
            ),
        )],
    )
    .unwrap();
    e.publish().unwrap();
    let update_bytes = store.stats().bytes_written;

    // Scan wanting only the small fields, asked for as such.
    store.reset();
    let mut r = Reader::open(Arc::clone(&store)).unwrap();
    let rows = r.scan_projected("t", &["id", "status"]).unwrap();
    let scan_bytes = store.stats().bytes_read;

    // The same scan without saying so, for comparison.
    store.reset();
    let mut r2 = Reader::open(Arc::clone(&store)).unwrap();
    let _ = r2.scan("t").unwrap();
    let full_scan_bytes = store.stats().bytes_read;

    // What those small fields actually weigh.
    let payload_bytes: u64 = rows
        .iter()
        .map(|(_, rec)| {
            let id = match rec.get("id") {
                Some(Value::Int(_)) => 8u64,
                _ => 0,
            };
            let status = match rec.get("status") {
                Some(Value::Str(s)) => s.len() as u64,
                _ => 0,
            };
            id + status
        })
        .sum();

    Sample {
        attachment_kib,
        update_bytes,
        scan_bytes,
        full_scan_bytes,
        payload_bytes,
    }
}

fn main() {
    println!(
        "{} rows, each with a small `id` and `status` and one large `attachment`.\n\
         `update` changes only `status`, on one row. `scan` wants only the small\n\
         fields. Both are measured as bytes crossing the store boundary.\n",
        ROWS
    );
    println!(
        "| attachment | update one small field | scan, asking for the small fields | scan, asking for everything | the small fields weigh |"
    );
    println!("|---|---|---|---|---|");

    for kib in [1usize, 4, 16, 64, 256] {
        let s = measure(kib);
        println!(
            "| {} KiB | {} | {} | {} | {} |",
            s.attachment_kib,
            human(s.update_bytes),
            human(s.scan_bytes),
            human(s.full_scan_bytes),
            human(s.payload_bytes),
        );
    }

    println!(
        "\nThe last column is the floor: what the requested data weighs.\n\
         Update cost no longer tracks the attachment — the pointer merges as\n\
         itself. A projected scan does not fetch what it was not asked for.\n\
         What remains above the floor is the leaf: records live in the index,\n\
         so every row's small fields cross the wire either way. Removing that\n\
         needs values laid out by column rather than by row."
    );
}

fn human(bytes: u64) -> String {
    if bytes >= 1 << 20 {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}
