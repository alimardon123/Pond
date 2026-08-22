// spillpoint.rs — where should a value stop living in the leaf?
//
// Spilling trades one extra GET on every read of that value against
// `target` x value_size on every write that touches its leaf. Which side wins
// depends on the read/write mix, so a threshold argued from leaf arithmetic
// alone is a guess.
//
// This measures the surface: for a range of value sizes and read/write mixes,
// what does a workload cost in requests and in bytes, with and without
// spilling? Requests are priced separately because a PUT is roughly 12x a GET
// on object storage, so a design that trades writes for reads wins even at
// equal counts.
//
//   cargo run --release -p pond_bench --bin spillpoint

use std::sync::Arc;

use pond_engine::{Engine, EngineConfig, Reader};
use pond_index::{int, Key};
use pond_kernel::{LocalFSObjectStore, Metered};
use pond_record::{Record, Value, Version};

/// S3 list prices: $5.00 per million PUT, $0.40 per million GET.
const PUT_PER_GET: f64 = 12.5;

/// Fixed per-request latency, and the per-byte term on top of it.
///
/// Requests alone are not the cost function. A leaf rewritten whole is *one*
/// PUT however large it is, so counting requests makes an inline 200 MB leaf
/// look cheaper than a spilled 200 KB one — which is exactly backwards once
/// the bytes have to cross a network. Both terms have to be priced or the
/// answer is wrong in a way that looks rigorous.
const REQUEST_MS: f64 = 30.0;
const MS_PER_MIB: f64 = 20.0;

/// Run a workload and return (weighted request cost, bytes).
fn run(value_bytes: usize, threshold: usize, reads_per_write: usize, warm: bool) -> Outcome {
    const SEED_ROWS: i64 = 2_000;
    const OPS: i64 = 40;

    let dir = tempfile::tempdir().unwrap();
    // `Metered` from the kernel. The counting wrapper this file used to define
    // did not forward the batch methods, so every batched read and write it
    // measured was unrolled into sequential calls by the trait's defaults —
    // the benchmark was measuring a slower system than the one it was pricing.
    let backend = Arc::new(Metered::new(
        LocalFSObjectStore::new(dir.path()).unwrap(),
    ));
    let config = EngineConfig::default().with_spill_threshold(threshold);
    let payload = "x".repeat(value_bytes);

    let record = |i: i64, tag: &str| {
        (
            Key::new(vec![int(i)]),
            Record::new().with_field(
                "b",
                Value::Str(format!("{}{}", tag, payload)),
                Version::new(100 + i as u64, 0, 1),
            ),
        )
    };

    // Seed.
    let mut e = Engine::open_with(
        Arc::clone(&backend),
        1,
        pond_cache::CacheConfig::default(),
        config,
    )
    .unwrap();
    e.write_records("t", (0..SEED_ROWS).map(|i| record(i, "s")).collect())
        .unwrap();
    e.publish().unwrap();

    // Measure the mixed workload only.
    backend.reset();

    for op in 0..OPS {
        let mut e = Engine::open_with(
            Arc::clone(&backend),
            1,
            pond_cache::CacheConfig::default(),
            config,
        )
        .unwrap();
        e.write_records("t", vec![record(op * 7 % SEED_ROWS, "u")])
            .unwrap();
        e.publish().unwrap();

        // Cold: a fresh reader per read, so nothing is cached. Warm: one
        // reader across the batch, which is what a long-lived process
        // actually does — and the case this design is built for, since
        // content-addressed entries can never go stale.
        if warm {
            let mut reader = Reader::open_with(
                Arc::clone(&backend),
                pond_cache::CacheConfig::default(),
                config,
            )
            .unwrap();
            for r in 0..reads_per_write {
                let key = Key::new(vec![int((op * 13 + r as i64 * 31) % SEED_ROWS)]);
                let _ = reader.get("t", &key).unwrap();
            }
        } else {
            for r in 0..reads_per_write {
                let mut reader = Reader::open_with(
                    Arc::clone(&backend),
                    pond_cache::CacheConfig::default(),
                    config,
                )
                .unwrap();
                let key = Key::new(vec![int((op * 13 + r as i64 * 31) % SEED_ROWS)]);
                let _ = reader.get("t", &key).unwrap();
            }
        }
    }

    let st = backend.stats();
    let (puts, gets) = (st.puts as f64, st.gets as f64);
    // Both directions. The wrapper this replaced counted only bytes written,
    // which understated the case for spilling: an inline point read drags the
    // whole leaf across the network, a spilled one fetches just the value.
    let bytes = st.bytes_written + st.bytes_read;

    // A PUT costs ~12.5x a GET, so requests are weighted rather than counted.
    let millis =
        (puts * PUT_PER_GET + gets) * REQUEST_MS + (bytes as f64 / (1024.0 * 1024.0)) * MS_PER_MIB;
    Outcome { millis, bytes }
}

struct Outcome {
    /// Modelled wall clock: a price-weighted per-request cost plus a per-byte
    /// term. Both terms matter — see [`REQUEST_MS`].
    millis: f64,
    /// Backend bytes moved. Reported alongside the verdict because it is the
    /// other half of the argument: spilling wins by moving less, and a reader
    /// should be able to see how much less rather than take the ratio on faith.
    bytes: u64,
}

fn main() {
    // A threshold at usize::MAX never spills, which is the "before" case.
    let never = usize::MAX;

    println!("40 writes + N reads over 2000 rows. Lower is better.");
    println!(
        "`inline` never spills; `spill` spills values of that row's size.\n\
         Modelled time is {} ms per request plus {} ms per MiB.\n\
         cold = a fresh reader per read; warm = one reader across the batch,\n\
         which is what a long-lived process does and what the cache is for.\n",
        REQUEST_MS, MS_PER_MIB
    );
    println!(
        "| value | reads/write | bytes inline -> spill | cold: inline -> spill \
         | warm: inline -> spill |"
    );
    println!("|---|---|---|---|---|");

    for value_bytes in [1024usize, 4096, 16384, 65536] {
        for reads_per_write in [1usize, 10, 100] {
            let verdict = |inline: &Outcome, spill: &Outcome| -> String {
                if spill.millis < inline.millis {
                    format!(
                        "{:.0} -> {:.0} ms **{:.1}x faster**",
                        inline.millis,
                        spill.millis,
                        inline.millis / spill.millis.max(0.001)
                    )
                } else {
                    format!(
                        "{:.0} -> {:.0} ms {:.1}x slower",
                        inline.millis,
                        spill.millis,
                        spill.millis / inline.millis.max(0.001)
                    )
                }
            };

            // Spill *at this size*, not at the crate's current threshold.
            // Pinning to the constant makes every row below it a control
            // comparing a setting against itself — which measures nothing and
            // cannot answer the question the benchmark exists for: should a
            // value of this size spill? Each row now answers that on its own,
            // so the constant can be re-derived from the table rather than
            // assumed by it.
            let cold_in = run(value_bytes, never, reads_per_write, false);
            let cold_sp = run(value_bytes, value_bytes, reads_per_write, false);
            let warm_in = run(value_bytes, never, reads_per_write, true);
            let warm_sp = run(value_bytes, value_bytes, reads_per_write, true);

            println!(
                "| {} | {} | {} -> {} | {} | {} |",
                human(value_bytes as u64),
                reads_per_write,
                human(cold_in.bytes),
                human(cold_sp.bytes),
                verdict(&cold_in, &cold_sp),
                verdict(&warm_in, &warm_sp),
            );
        }
    }
}

fn human(bytes: u64) -> String {
    if bytes >= 1 << 20 {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{} KiB", bytes / 1024)
    }
}
