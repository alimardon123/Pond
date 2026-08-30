// headscale.rs — what does a publish cost as collections accumulate?
//
// A head is one writer's whole view of the pond: every collection it has
// published, mapped to that collection's root, in one object. That is a
// deliberate and load-bearing choice — object stores give single-object write
// atomicity, so writing one head publishes every collection in it at once, and
// atomic multi-collection publish falls out with no transaction machinery at
// all. `core/record/src/head.rs` argues it well.
//
// The cost of that choice is the number this measures. The head is rewritten
// whole on every publish, so a single-row write into one collection moves
// bytes proportional to *every* collection the writer has ever published, and
// a reader pays the same on open. Round trips do not grow — this is invisible
// to a round-trip count, which is exactly why it needs its own measurement.
//
//   cargo run --release -p pond_bench --bin headscale

use std::sync::Arc;

use pond_engine::{Engine, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered};
use pond_record::{Record, Value, Version};

/// Collection counts to profile at.
const COUNTS: &[usize] = &[1, 10, 100, 1_000, 10_000];

/// Latency model, matching docs/ROUND_TRIP_AUDIT.md.
const LATENCY_MS: f64 = 30.0;
const MS_PER_MIB: f64 = 20.0;

fn row(seq: u64, v: i64) -> Record {
    let mut r = Record::new();
    r.set("v", Value::Int(v), Version::new(seq, 0, 1));
    r
}

fn main() {
    println!(
        "{:>12} {:>10} {:>14} {:>12} {:>10} {:>13} {:>12}",
        "collections", "pub waits", "pub bytes", "pub ms", "open waits", "open bytes", "open ms"
    );

    for &n in COUNTS {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
        let mut engine = Engine::open(store.clone(), 1).unwrap();

        for c in 0..n {
            engine
                .write_records(
                    &format!("c{c}"),
                    vec![(Key::new(vec![int(0)]), row(c as u64 + 1, c as i64))],
                )
                .unwrap();
        }
        engine.publish().unwrap();

        // One more single-row write, into a single collection.
        store.reset();
        engine
            .write_records("c0", vec![(Key::new(vec![int(1)]), row(9_999, -1))])
            .unwrap();
        engine.publish().unwrap();
        let w = store.stats();

        let probe = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
        let _ = Reader::open(probe.clone()).unwrap();
        let o = probe.stats();

        println!(
            "{:>12} {:>10} {:>14} {:>12.1} {:>10} {:>13} {:>12.1}",
            n,
            w.round_trips,
            w.bytes_written,
            w.modelled_millis(LATENCY_MS, MS_PER_MIB),
            o.round_trips,
            o.bytes_read,
            o.modelled_millis(LATENCY_MS, MS_PER_MIB),
        );
    }

    println!();
    println!("Round trips are flat. Bytes are not, and neither is the clock once");
    println!("the bytes have to cross a network.");
}
