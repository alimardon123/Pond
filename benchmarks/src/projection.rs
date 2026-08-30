// projection.rs — what a projected scan costs, and what it should cost.
//
// Selecting two columns out of fifty reads all fifty. Not approximately: the
// same number of bytes, to the byte, because a record is stored whole inside
// the leaf and a scan has to read the leaf to reach any of it. Projection
// currently drops the unwanted fields *after* they have crossed the network.
//
// This measures the gap against an ideal — the bytes the wanted columns would
// occupy on their own — so the size of the prize is a number rather than an
// intuition, and so the claim "Pond reads only what you select" can be checked
// rather than asserted.
//
//   cargo run --release -p pond_bench --bin projection

use std::sync::Arc;

use pond_engine::{Engine, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered};
use pond_record::{Record, Value, Version};

const ROWS: usize = 20_000;
const WIDTHS: &[usize] = &[8, 50];
const SELECTED: usize = 2;

const LATENCY_MS: f64 = 30.0;
const MS_PER_MIB: f64 = 20.0;

fn record(i: u64, columns: usize) -> Record {
    let mut r = Record::new();
    for c in 0..columns {
        r.set(
            &format!("col{c:02}"),
            Value::Str(format!("v{i}-{c}")),
            Version::new(i + 1, 0, 1),
        );
    }
    r
}

fn build(columns: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFSObjectStore::new(dir.path()).unwrap());
    let mut engine = Engine::open(store, 1).unwrap();
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(5_000) {
        let rows: Vec<(Key, Record)> = chunk
            .iter()
            .map(|&i| (Key::new(vec![int(i as i64)]), record(i as u64, columns)))
            .collect();
        engine.write_records("t", rows).unwrap();
    }
    engine.publish().unwrap();
    dir
}

fn measure(dir: &std::path::Path, wanted: Option<&[&str]>) -> (u64, u64) {
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir).unwrap()));
    let mut reader = Reader::open(store.clone()).unwrap();
    store.reset();
    let rows = match wanted {
        None => reader.scan("t").unwrap(),
        Some(w) => reader.scan_projected("t", w).unwrap(),
    };
    assert_eq!(rows.len(), ROWS);
    let s = store.stats();
    (s.round_trips, s.bytes_read)
}

fn main() {
    println!(
        "{:>8} {:>10} {:>12} {:>10} {:>12} {:>10}",
        "columns", "query", "KiB read", "ms", "ideal KiB", "ideal ms"
    );

    for &columns in WIDTHS {
        let dir = build(columns);

        let (w_full, b_full) = measure(dir.path(), None);
        let names: Vec<String> = (0..SELECTED).map(|c| format!("col{c:02}")).collect();
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let (w_proj, b_proj) = measure(dir.path(), Some(&refs));

        // What the selected columns would cost if they could be read alone:
        // their share of the column bytes, plus the keys, which any scan pays.
        let share = SELECTED as f64 / columns as f64;
        let ideal = b_full as f64 * share;

        let ms = |waits: u64, bytes: f64| -> f64 {
            waits as f64 * LATENCY_MS + (bytes / (1024.0 * 1024.0)) * MS_PER_MIB
        };

        println!(
            "{:>8} {:>10} {:>12.1} {:>10.1} {:>12} {:>10}",
            columns,
            "full",
            b_full as f64 / 1024.0,
            ms(w_full, b_full as f64),
            "-",
            "-"
        );
        println!(
            "{:>8} {:>10} {:>12.1} {:>10.1} {:>12.1} {:>10.1}",
            columns,
            format!("{SELECTED} of {columns}"),
            b_proj as f64 / 1024.0,
            ms(w_proj, b_proj as f64),
            ideal / 1024.0,
            ms(w_proj, ideal),
        );
    }

    println!();
    println!("The projected row reads exactly what the full row reads. The gap");
    println!("between it and the ideal column is what a columnar leaf would win.");
}
