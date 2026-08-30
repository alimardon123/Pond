// kernel_cache.rs — the blob cache has to sit under the kernel, not beside it.
//
// `PondKernel` had no cache. Every `read_blob` went to the object store, every
// time, and that is the read path for the legacy storage layer, both vector
// index extensions, the streaming and keyvalue lenses, and the CLI. The disk
// cache added earlier lived inside `engine_path`, so it covered the engine's
// own reads and nothing else — which is easy to miss, because the engine is
// where all the round-trip measurements were pointed.
//
// An HNSW query is the clearest case: it reads every vector in the collection
// to compute distances, so a 1000-vector 128-dimension index re-read 1.2 MB
// from the store on every single query, forever.
//
// Caching is safe here for the same reason it is safe anywhere in this design:
// `BlobCache` caches only hash-keyed blobs, whose name is a digest of their own
// bytes, so an entry cannot go stale. Refs, named objects and listings pass
// straight through.
//
// This does NOT make the query sublinear — it still reads every vector, just
// from local disk instead of across a network. That remains open in
// docs/CRITIQUE.md, and a cache is not a substitute for the layout.

use std::sync::Arc;

use pond_kernel::{LocalFSObjectStore, Metered};
use pond_storage::UnifiedStorage;
use pond_vector_lens::VectorLens;

const N: usize = 1000;
const DIMS: usize = 128;

fn vector(i: usize) -> Vec<f64> {
    (0..DIMS).map(|d| ((i * 7 + d * 13) % 977) as f64).collect()
}

/// Bytes a query pulls from the store, from a reader that has just started.
///
/// A fresh kernel per pass on purpose: reusing one would answer from its own
/// memory tier and report zero whether or not the disk cache works, which is
/// the flattering measurement rather than the useful one.
fn query_bytes(dir: &std::path::Path) -> (u64, usize) {
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir).unwrap()));
    let lens = VectorLens::new(UnifiedStorage::new(pond_storage::cached_kernel(
        store.clone(),
    )));
    store.reset();
    let q: Vec<f64> = (0..DIMS).map(|d| (d * 13) as f64).collect();
    let hits = lens.search("v", &q, 10, 10, 50).expect("search");
    (store.stats().bytes_read, hits.len())
}

fn build(dir: &std::path::Path) {
    let store = Arc::new(LocalFSObjectStore::new(dir).unwrap());
    let lens = VectorLens::new(UnifiedStorage::new(pond_storage::cached_kernel(store)));
    for i in 0..N {
        lens.insert("v", &format!("id-{i}"), &vector(i), None);
    }
    lens.commit("v", "seed").expect("commit");
    lens.build_hnsw_index("v", 16, 100, "l2").expect("build index");
}

/// One test, not two: `POND_CACHE_DIR` is process-global, so two tests setting
/// it would race and each would sometimes measure the other's setting.
#[test]
fn a_repeated_query_stops_re_reading_the_store() {
    // Without a disk cache: every query pays full price, forever.
    let cold_dir = tempfile::tempdir().unwrap();
    std::env::set_var("POND_CACHE_DIR", "off");
    build(cold_dir.path());
    let (cold_first, hits) = query_bytes(cold_dir.path());
    let (cold_again, _) = query_bytes(cold_dir.path());

    assert_eq!(hits, 10, "the query must still work");
    assert!(
        cold_first > 500_000,
        "expected this query to read the whole collection without a cache, \
         got {cold_first} bytes — the test is no longer measuring anything"
    );
    assert_eq!(
        cold_first, cold_again,
        "with no cache, a repeated query must cost exactly the same: {cold_first} \
         then {cold_again}"
    );

    // With one: the second reader serves it from local disk.
    let warm_dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    std::env::set_var("POND_CACHE_DIR", cache.path());
    build(warm_dir.path());
    let (warm, warm_hits) = query_bytes(warm_dir.path());

    assert_eq!(warm_hits, 10, "the cached query must return the same answer");
    assert!(
        warm * 10 < cold_first,
        "a warm query read {warm} bytes against {cold_first} cold — the kernel \
         is not caching blob reads"
    );
}
