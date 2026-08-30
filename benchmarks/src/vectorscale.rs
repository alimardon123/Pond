// vectorscale.rs — what a vector query costs, and which term is the problem.
//
// An HNSW search here reads every vector in the collection to compute
// distances, so its bytes are linear in N. That sounds damning and, on its
// own, is misleading: bytes are not what a query waits for until there are
// enough of them.
//
// This separates the two terms, because the fix depends entirely on which
// dominates:
//
//   latency  = round trips x per-request time. HNSW is pointer chasing — walk
//              a layer, decide where to go, walk the next — so its round trips
//              are sequential and largely independent of N.
//   transfer = bytes / throughput. Linear in N, and in the dimension.
//
// Below the crossover the query is waiting, not transferring, and making it
// read fewer bytes would buy almost nothing. Above it, transfer runs away.
//
// This matters for what NOT to do. The obvious fix for "reads everything" is
// to fetch each visited node's vector on demand — and that trades the term
// that does not dominate for the term that does, adding a round trip per hop
// to save bytes that were nearly free. It would make small collections much
// slower. The design that fixes both is blocks plus an ordering that puts
// graph neighbours in the same block, so a walk touches few blocks and they
// can be fetched in a few batched rounds.
//
//   cargo run --release -p pond_bench --bin vectorscale

use std::sync::Arc;

use pond_kernel::{LocalFSObjectStore, Metered};
use pond_storage::UnifiedStorage;
use pond_vector_lens::VectorLens;

/// Collection sizes to profile.
const SIZES: &[usize] = &[1_000, 4_000, 16_000, 64_000];

/// Vector width. 128 is an ordinary embedding size; the payload share rises
/// with it (24% of a query at 8 dimensions, 83% at 128, 93% at 384).
const DIMS: usize = 128;

const LATENCY_MS: f64 = 30.0;
const MS_PER_MIB: f64 = 20.0;

fn vector(i: usize) -> Vec<f64> {
    (0..DIMS).map(|d| ((i * 7 + d * 13) % 977) as f64).collect()
}

fn main() {
    // Measuring the store, so the cache must not stand in front of it. What
    // the cache is worth is a different question, answered in
    // lenses/vector/rust/tests/kernel_cache.rs.
    std::env::set_var("POND_CACHE_DIR", "off");

    println!(
        "{:>8} {:>7} {:>12} {:>11} {:>11} {:>10}",
        "vectors", "waits", "KiB read", "latency ms", "transfer ms", "total ms"
    );

    for &n in SIZES {
        let dir = tempfile::tempdir().unwrap();

        {
            let store = Arc::new(LocalFSObjectStore::new(dir.path()).unwrap());
            let lens = VectorLens::new(UnifiedStorage::new(pond_storage::cached_kernel(store)));
            for i in 0..n {
                lens.insert("v", &format!("id-{i}"), &vector(i), None);
            }
            lens.commit("v", "seed").unwrap();
            lens.build_hnsw_index("v", 16, 100, "l2").unwrap();
        }

        // A fresh reader: what a query costs when nothing is warm.
        let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
        let lens = VectorLens::new(UnifiedStorage::new(pond_storage::cached_kernel(
            store.clone(),
        )));
        store.reset();
        let q: Vec<f64> = (0..DIMS).map(|d| (d * 13) as f64).collect();
        let hits = lens.search("v", &q, 10, 10, 50).unwrap();
        assert_eq!(hits.len(), 10, "the query must return k results");

        let s = store.stats();
        let mib = s.bytes_read as f64 / (1024.0 * 1024.0);
        println!(
            "{:>8} {:>7} {:>12.1} {:>11.1} {:>11.1} {:>10.1}",
            n,
            s.round_trips,
            s.bytes_read as f64 / 1024.0,
            s.round_trips as f64 * LATENCY_MS,
            mib * MS_PER_MIB,
            s.modelled_millis(LATENCY_MS, MS_PER_MIB),
        );
    }

    println!();
    println!("Round trips barely move with N; bytes are linear. Whichever column");
    println!("is larger is the one worth attacking, and it is not the same one at");
    println!("both ends of this table.");
}
