// round_trip_cost.rs — what does one write actually cost the backend?
//
// Round trips are the unit this system is designed around, so the legacy write
// path's cost should be a measured number rather than an impression. This test
// records it. It is not a pass/fail quality bar — it is the baseline the
// engine cutover has to beat, and a tripwire if the legacy path gets more
// expensive while it is still the one every binding calls.

use std::sync::Arc;

use pond_kernel::{LocalFSObjectStore, Metered, PondKernel};

/// One metered store, shared with the kernel that writes through it.
///
/// [`Metered`] replaces the counting `ObjectStore` this file used to define
/// inline — fifty lines of delegation that had to be kept in step with the
/// trait by hand, and, like every other copy in the tree, silently dropped the
/// batch methods onto their sequential defaults.
fn metered_kernel() -> (tempfile::TempDir, Arc<Metered<LocalFSObjectStore>>, PondKernel) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Metered::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let kernel = PondKernel::new_with_store(Box::new(Arc::clone(&store)));
    (dir, store, kernel)
}

/// One legacy write, counted.
///
/// The number itself is the point. At an object-storage round trip of ~50 ms,
/// each one is ~50 ms of commit latency that no amount of caching removes,
/// because these are writes.
///
/// The bound is deliberately loose — this test exists to record the cost and
/// to catch it growing, not to freeze an implementation detail.
#[test]
fn legacy_write_round_trip_cost_is_recorded() {
    let (_dir, store, kernel) = metered_kernel();
    let storage = pond_storage::UnifiedStorage::new(kernel);

    // First write, then a second one so the parent-commit read is included —
    // the steady-state cost is what matters, not the empty-collection case.
    pond_storage::write::write(storage.kernel(), "users", "main", b"row one", "first").unwrap();

    store.reset();
    pond_storage::write::write(storage.kernel(), "users", "main", b"row two", "second").unwrap();

    let st = store.stats();
    let (puts, gets, heads, total) = (st.puts, st.gets, st.heads, st.requests());

    println!(
        "legacy write: {} requests in {} round trips ({} PUT, {} GET, {} HEAD)",
        total, st.round_trips, puts, gets, heads
    );
    // Every one of these is a separate, dependent call: nothing in the legacy
    // path batches, so its request count and its round-trip count are the same
    // number. That equality is the cost being recorded here.
    assert_eq!(
        st.round_trips, total,
        "the legacy path issues no batches, so every request is its own wait"
    );

    assert!(
        puts >= 6,
        "a legacy write issues several independent writes ({} seen)",
        puts
    );
    // `reference` used to HEAD the blob before binding a name to it, adding a
    // round trip per ref — three per commit — to verify something the caller
    // had just written. The kernel now remembers its own writes.
    assert_eq!(
        heads, 0,
        "binding a name to a blob this kernel just wrote must not cost a \
         round trip ({} seen)",
        heads
    );
    // The remaining cost is structural, not incidental: separate writes for
    // the data, the manifest, the commit, and three refs. Only the engine's
    // single-object publish removes those.
    assert!(
        total <= 9,
        "one legacy write should now cost ~8 round trips, saw {}",
        total
    );
}

/// The commit is not atomic, and the count shows why.
///
/// A legacy write ends by updating three separate refs in sequence. A crash
/// between them leaves the branch pointing at a new commit whose manifest ref
/// still names the old manifest. There is no way to make three writes atomic
/// on a store that only promises single-object atomicity — which is the whole
/// reason the engine publishes one object.
#[test]
fn legacy_commit_updates_several_refs_and_so_cannot_be_atomic() {
    let (_dir, _store, kernel) = metered_kernel();

    pond_storage::write::write(&kernel, "users", "main", b"row", "msg").unwrap();

    // branch ref, manifest ref, and the bare collection name.
    let refs = kernel.list_names();
    let touched: Vec<&String> = refs
        .iter()
        .filter(|r| r.starts_with("collections/users") || r.as_str() == "users")
        .collect();
    assert!(
        touched.len() >= 3,
        "a commit spans {} refs, so it cannot be a single atomic write: {:?}",
        touched.len(),
        touched
    );
}

/// The same row, through the engine path.
///
/// This is the number the cutover exists for. The legacy path spends its
/// round trips on structure — a manifest, a commit, three refs — none of which
/// the engine needs, because publishing is a single object write and the index
/// is reached from it directly.
#[test]
fn engine_write_costs_far_fewer_round_trips() {
    use pond_core::encode::TypedColumn;

    let (_dir, store, kernel) = metered_kernel();

    pond_storage::engine_path::create(&kernel, "users").unwrap();
    let columns: Vec<(&str, TypedColumn)> = vec![
        ("id", TypedColumn::Int64(vec![1])),
        ("name", TypedColumn::String(vec!["ada".into()])),
    ];

    // Warm: write once so the steady-state cost is what gets measured, the
    // same way the legacy comparison does.
    pond_storage::engine_path::write_rows(&kernel, "users", &columns, 1).unwrap();

    store.reset();
    pond_storage::engine_path::write_rows(&kernel, "users", &columns, 1).unwrap();
    let total = store.stats().requests();
    let heads = store.stats().heads;

    println!(
        "engine write: {} round trips ({} PUT, {} GET, {} HEAD, {} LIST)",
        total,
        store.stats().puts,
        store.stats().gets,
        heads,
        store.stats().lists,
    );

    assert_eq!(heads, 0, "the engine path must never probe for existence");
    assert!(
        total <= 6,
        "engine write cost {} round trips against the legacy path's 8",
        total
    );
    // The comparison that matters economically. On S3 a PUT costs roughly
    // twelve times a GET, so trading writes for reads is a win even at equal
    // counts — and here the counts are not equal.
    assert!(
        store.stats().puts <= 3,
        "the engine commits with {} writes; the legacy path uses 6",
        store.stats().puts
    );
}

/// The raw-bytes collection API rewrites the whole collection on every write.
///
/// `storage_read::read` returns one blob and `storage_write::write` replaces
/// it, so a caller that wants to append has to read everything, append in
/// memory, and write everything back. The streaming, OLTP and key-value lenses
/// are all built on exactly that loop.
///
/// This test measures the consequence: bytes written per append grow with the
/// size of the collection, so appending N rows costs O(N²) bytes. It is a
/// finding recorded as a test, not a bar being enforced — the fix is to move
/// those lenses onto the engine's append path, which touches only the
/// right-most leaf and its ancestors.
#[test]
fn raw_bytes_append_rewrites_the_whole_collection() {
    let (_dir, store, kernel) = metered_kernel();

    // Simulate the lens loop: read all, append one, write all back.
    let mut rows: Vec<String> = Vec::new();
    let mut bytes_at: Vec<usize> = Vec::new();

    let mut store_bytes_at: Vec<u64> = Vec::new();

    for i in 0..40 {
        store.reset();
        let existing =
            pond_storage::read::read(&kernel, "events", "main").unwrap_or_default();
        if !existing.is_empty() {
            rows = serde_json::from_slice(&existing).unwrap_or_default();
        }
        rows.push(format!("event-{:04}", i));
        let payload = serde_json::to_vec(&rows).unwrap();
        bytes_at.push(payload.len());
        pond_storage::write::write(&kernel, "events", "main", &payload, "append").unwrap();
        store_bytes_at.push(store.stats().bytes_written);
    }

    // The payload sizes above are what the lens *intended* to write. These are
    // what the store actually received, overhead included — the number that
    // gets billed and transferred.
    let store_first = store_bytes_at.first().copied().unwrap_or(0);
    let store_last = store_bytes_at.last().copied().unwrap_or(0);
    println!(
        "raw-bytes append, bytes reaching the store: #1 {} -> #40 {} ({:.1}x)",
        store_first,
        store_last,
        store_last as f64 / store_first.max(1) as f64
    );
    // 2.9x measured, against ~10x growth in the payload itself: each commit
    // also writes a manifest, a commit object and three refs whose size does
    // not depend on the collection, and at 40 tiny rows that fixed overhead
    // still dominates. The ratio therefore understates the problem rather than
    // overstating it — which is the right direction for a tripwire.
    assert!(
        store_last > store_first * 2,
        "the growth must show up in what the store receives, not only in the \
         payload the lens builds: {} -> {} bytes",
        store_first,
        store_last
    );

    let first = bytes_at.first().copied().unwrap_or(0);
    let last = bytes_at.last().copied().unwrap_or(0);
    println!(
        "raw-bytes append: write #1 sent {} bytes, write #40 sent {} bytes ({}x growth)",
        first,
        last,
        last / first.max(1)
    );

    assert!(
        last > first * 10,
        "each append should rewrite everything written so far — \
         first {} bytes, last {} bytes",
        first,
        last
    );
}
