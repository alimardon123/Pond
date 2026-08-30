// bulk_and_dispatch.rs — vectors must survive being inserted.
//
// Two failures meet in `VectorLens::commit`, and both lose data silently.
//
// 1. It called the legacy writer unconditionally while the search path
//    dispatches on the collection's format. On an engine-backed collection the
//    rows went into a legacy manifest and the reader looked at the engine, so
//    the commit succeeded and the vectors were not there.
//
// 2. `insert` auto-commits when its buffer reaches 10,000, and the legacy
//    writer stores a whole-collection snapshot rather than appending. So a
//    bulk load of 25,000 committed a snapshot of the first 10,000, replaced it
//    with a snapshot of the second 10,000, and replaced that with the final
//    5,000 — leaving 5,000 of 25,000, with every call returning success.
//
// The number 10,000 is the buffer limit, so nothing below it can show this:
// the failure begins exactly where the tests stopped.

use pond_kernel::PondKernel;
use pond_storage::{engine_path, UnifiedStorage};
use pond_vector_lens::VectorLens;

fn lens(dir: &std::path::Path, engine: bool) -> VectorLens {
    let kernel = PondKernel::new_local(dir).unwrap();
    if engine {
        engine_path::create(&kernel, "v").unwrap();
    }
    VectorLens::new(UnifiedStorage::new(kernel))
}

fn vector(i: usize) -> Vec<f64> {
    (0..4).map(|d| (i * 10 + d) as f64).collect()
}

/// Every vector inserted must be findable afterwards.
///
/// Asked as an exact-match query per row rather than by counting rows, because
/// a count can be right while the contents are wrong, and this is the question
/// a caller actually has: is the thing I stored still here?
fn bulk(engine: bool, rows: usize) {
    let dir = tempfile::tempdir().unwrap();
    let lens = lens(dir.path(), engine);

    for i in 0..rows {
        lens.insert("v", &format!("id-{i}"), &vector(i), None);
    }
    // Auto-commit may already have fired; this flushes whatever is left.
    let _ = lens.commit("v", "bulk");

    let mut found = 0;
    // Sample rather than query all 25,000 — one from each side of every
    // auto-commit boundary is what distinguishes "all present" from "only the
    // last batch survived".
    let probes: Vec<usize> = (0..rows).step_by((rows / 40).max(1)).collect();
    for &i in &probes {
        if let Ok(hits) = lens.search("v", &vector(i), 1, 8, 32) {
            if hits.first().map(|(d, id)| *d < 1e-9 && id == &format!("id-{i}")) == Some(true) {
                found += 1;
            }
        }
    }

    assert_eq!(
        found,
        probes.len(),
        "{} of {} sampled vectors survived a {}-row load ({} format)",
        found,
        probes.len(),
        rows,
        if engine { "engine" } else { "legacy" }
    );
}

#[test]
fn a_small_load_survives_on_a_legacy_collection() {
    bulk(false, 100);
}

#[test]
fn a_small_load_survives_on_an_engine_collection() {
    bulk(true, 100);
}

/// Past the 10,000 auto-commit boundary, where a snapshot write starts
/// discarding what came before it.
#[test]
fn a_bulk_load_past_the_auto_commit_boundary_survives_on_a_legacy_collection() {
    bulk(false, 25_000);
}

#[test]
fn a_bulk_load_past_the_auto_commit_boundary_survives_on_an_engine_collection() {
    bulk(true, 25_000);
}
