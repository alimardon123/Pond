// engine_stream.rs — appending must not get more expensive as the stream grows.

use pond_kernel::PondKernel;
use pond_streaming_lens::engine_stream;

fn kernel(dir: &std::path::Path) -> PondKernel {
    PondKernel::new_local(dir).unwrap()
}

/// Round trip: what goes in comes out, at the right offsets.
#[test]
fn append_and_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_stream::create(&k, "log").unwrap();

    engine_stream::append(&k, "log", b"hello ", 4, 1).unwrap();
    let total = engine_stream::append(&k, "log", b"world", 4, 1).unwrap();

    assert_eq!(total, 11);
    assert_eq!(engine_stream::size(&k, "log").unwrap(), 11);
    assert_eq!(engine_stream::read(&k, "log", 0, None).unwrap(), b"hello world");
}

/// A range read must return exactly the requested window, including one that
/// starts and ends inside different segments.
#[test]
fn range_reads_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_stream::create(&k, "log").unwrap();

    let payload: Vec<u8> = (0..=255u8).collect();
    engine_stream::append(&k, "log", &payload, 16, 1).unwrap();

    assert_eq!(engine_stream::read(&k, "log", 0, Some(4)).unwrap(), payload[0..4]);
    assert_eq!(engine_stream::read(&k, "log", 20, Some(40)).unwrap(), payload[20..40]);
    // Spanning a segment boundary.
    assert_eq!(engine_stream::read(&k, "log", 14, Some(18)).unwrap(), payload[14..18]);
    // Past the end is clamped, not an error.
    assert_eq!(engine_stream::read(&k, "log", 250, Some(9999)).unwrap(), payload[250..]);
    // An empty window is empty.
    assert!(engine_stream::read(&k, "log", 10, Some(10)).unwrap().is_empty());
    assert!(engine_stream::read(&k, "log", 500, None).unwrap().is_empty());
}

/// Many appends must reconstruct exactly, and in order.
#[test]
fn many_appends_reconstruct_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_stream::create(&k, "log").unwrap();

    let mut expected: Vec<u8> = Vec::new();
    for i in 0..100u32 {
        let chunk = format!("[{:04}]", i).into_bytes();
        expected.extend_from_slice(&chunk);
        engine_stream::append(&k, "log", &chunk, 64, 1).unwrap();
    }

    assert_eq!(engine_stream::size(&k, "log").unwrap(), expected.len());
    assert_eq!(engine_stream::read(&k, "log", 0, None).unwrap(), expected);
}

/// Binary data must survive — a stream is bytes, not text.
#[test]
fn binary_data_survives() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_stream::create(&k, "log").unwrap();

    let payload: Vec<u8> = (0..1000u32).map(|i| (i % 256) as u8).collect();
    engine_stream::append(&k, "log", &payload, 128, 1).unwrap();
    assert_eq!(engine_stream::read(&k, "log", 0, None).unwrap(), payload);
}

/// An empty append changes nothing.
#[test]
fn empty_append_is_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let k = kernel(dir.path());
    engine_stream::create(&k, "log").unwrap();

    engine_stream::append(&k, "log", b"abc", 8, 1).unwrap();
    assert_eq!(engine_stream::append(&k, "log", b"", 8, 1).unwrap(), 3);
    assert_eq!(engine_stream::read(&k, "log", 0, None).unwrap(), b"abc");
}

/// The point of the whole thing: appending must not cost more as the stream
/// grows.
///
/// The original stream keeps every segment in one JSON object, so an append
/// reads all of it, adds one segment, and writes all of it back. Its cost is
/// the size of the whole stream, every time, without bound.
///
/// The engine's cost is bounded by one leaf instead. It still *grows* while a
/// leaf fills — a copy-on-write tree rewrites the leaf it touches — but it
/// stops at the leaf's capacity rather than tracking the stream, and segments
/// large enough to spill leave only a pointer in the leaf, so the leaf stays
/// small however large the stream gets.
///
/// This measures both against each other rather than asserting a bound in the
/// abstract.
#[test]
fn append_cost_is_bounded_unlike_a_whole_collection_rewrite() {
    use std::sync::Arc;


    /// Segments large enough to spill, which is what a real stream writes.
    const SEGMENT: usize = 4096;
    const APPENDS: usize = 60;

    fn measure<F>(dir: &std::path::Path, mut append: F) -> (u64, u64)
    where
        F: FnMut(&PondKernel, &[u8]),
    {
        // `Metered` from the kernel, not a counting wrapper written here. The
        // one this file used to define — like every other copy in the tree —
        // left the batch methods on their sequential defaults, so it quietly
        // de-parallelised the very work it was measuring.
        let c = Arc::new(pond_kernel::Metered::new(
            pond_kernel::LocalFSObjectStore::new(dir).unwrap(),
        ));
        let k = PondKernel::new_with_store(Box::new(Arc::clone(&c)));
        let chunk = vec![b'x'; SEGMENT];

        let mut costs = Vec::new();
        for _ in 0..APPENDS {
            c.reset();
            append(&k, &chunk);
            costs.push(c.stats().bytes_written);
        }
        let first: u64 = costs[..10].iter().sum::<u64>() / 10;
        let last: u64 = costs[APPENDS - 10..].iter().sum::<u64>() / 10;
        (first, last)
    }

    // The engine path.
    let engine_dir = tempfile::tempdir().unwrap();
    let mut created = false;
    let (engine_first, engine_last) = measure(engine_dir.path(), |k, chunk| {
        if !created {
            engine_stream::create(k, "log").unwrap();
            created = true;
        }
        engine_stream::append(k, "log", chunk, SEGMENT, 1).unwrap();
    });

    // The whole-collection rewrite it replaces: read everything, append, write
    // everything back.
    let json_dir = tempfile::tempdir().unwrap();
    let mut all: Vec<String> = Vec::new();
    let (json_first, json_last) = measure(json_dir.path(), |k, chunk| {
        let existing =
            pond_storage::read::read(k, "log", "main").unwrap_or_default();
        if !existing.is_empty() {
            all = serde_json::from_slice(&existing).unwrap_or_default();
        }
        all.push(String::from_utf8_lossy(chunk).into_owned());
        let payload = serde_json::to_vec(&all).unwrap();
        pond_storage::write::write(k, "log", "main", &payload, "append").unwrap();
    });

    println!(
        "append cost over {} appends of {} B:\n  engine        {:>9} -> {:>9} bytes ({:.1}x)\n  JSON rewrite  {:>9} -> {:>9} bytes ({:.1}x)",
        APPENDS,
        SEGMENT,
        engine_first,
        engine_last,
        engine_last as f64 / engine_first.max(1) as f64,
        json_first,
        json_last,
        json_last as f64 / json_first.max(1) as f64,
    );

    let engine_growth = engine_last as f64 / engine_first.max(1) as f64;
    let json_growth = json_last as f64 / json_first.max(1) as f64;

    // Not merely "slower than linear growth" — comfortably slower. The bare
    // `<` comparison once passed at 10.0x vs 10.0x, which is to say it caught
    // a total regression only because the two numbers rounded apart. A factor
    // of two of headroom makes a *partial* regression visible too, and stays
    // comparative so the random per-collection chunk salt cannot flake it.
    assert!(
        engine_growth * 2.0 < json_growth,
        "the engine append must grow far more slowly than a whole-collection \
         rewrite, not merely marginally: engine {:.1}x vs JSON {:.1}x",
        engine_growth,
        json_growth
    );
    assert!(
        engine_last < json_last,
        "the engine append must cost less in absolute terms by the end: \
         {} vs {} bytes",
        engine_last,
        json_last
    );
}
