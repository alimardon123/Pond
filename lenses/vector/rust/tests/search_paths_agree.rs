// search_paths_agree.rs — three ways to answer one question, one answer.
//
// `VectorLens::search` picks a path by what happens to exist: HNSW if an HNSW
// index is present, else IVF, else a linear scan. A caller does not choose,
// and usually does not know which ran. So the three must agree — and they did
// not, in four separate ways, none of which any single-path test could see:
//
//   1. HNSW read the collection through the legacy manifest only, so building
//      an index on an engine-backed collection failed and search silently fell
//      back to scanning.
//   2. HNSW read ids from the Int64 column alone. Ids are stored as strings
//      when they do not all parse as numbers, so every result on a
//      string-keyed collection came back with an EMPTY id.
//   3. IVF *iterated* that Int64 column, so on the same collection its loop
//      body never ran and search returned NO RESULTS — indistinguishable from
//      "nothing is near your query".
//   4. HNSW returned squared Euclidean distance while the linear scan returned
//      the real one. Squaring is monotone, so the neighbours and their order
//      were identical and only the numbers differed — a caller filtering on a
//      distance threshold changed behaviour the moment somebody built an
//      index.
//
// Each of these is invisible from inside one path. Comparing the paths is the
// only thing that finds them, which is why this test exists rather than three
// separate ones.

use pond_kernel::PondKernel;
use pond_storage::{engine_path, UnifiedStorage};
use pond_vector_lens::VectorLens;

const N: usize = 300;
const DIMS: usize = 8;

/// Deterministic, well-spread vectors. String ids on purpose — that is the
/// case both index extensions mishandled.
fn vector(i: usize) -> Vec<f64> {
    (0..DIMS).map(|d| ((i * 7 + d * 13) % 977) as f64).collect()
}

fn seeded(engine: bool) -> (tempfile::TempDir, VectorLens) {
    let dir = tempfile::tempdir().unwrap();
    let kernel = PondKernel::new_local(dir.path()).unwrap();
    if engine {
        engine_path::create(&kernel, "v").unwrap();
    }
    let lens = VectorLens::new(UnifiedStorage::new(kernel));
    for i in 0..N {
        lens.insert("v", &format!("id-{i}"), &vector(i), None);
    }
    lens.commit("v", "seed").expect("commit");
    (dir, lens)
}

/// Brute force, computed here, in the units the API is supposed to report.
fn truth(query: &[f64], k: usize) -> Vec<(String, f64)> {
    let mut all: Vec<(String, f64)> = (0..N)
        .map(|i| {
            let d = vector(i)
                .iter()
                .zip(query)
                .map(|(a, b)| (a - b) * (a - b))
                .sum::<f64>()
                .sqrt();
            (format!("id-{i}"), d)
        })
        .collect();
    all.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    all.truncate(k);
    all
}

fn check(label: &str, got: &[(f64, String)], want: &[(String, f64)]) {
    assert_eq!(got.len(), want.len(), "{label}: returned {} results", got.len());

    for (i, (dist, id)) in got.iter().enumerate() {
        assert!(!id.is_empty(), "{label}: result {i} has an empty id");
        assert_eq!(id, &want[i].0, "{label}: result {i} is the wrong vector");
        assert!(
            (dist - want[i].1).abs() < 1e-6,
            "{label}: result {i} reports distance {dist} where brute force \
             gives {} — the paths disagree about units, not about ranking",
            want[i].1
        );
    }
}

/// All three paths, on both storage formats.
fn agree(engine: bool) {
    let tag = if engine { "engine" } else { "legacy" };
    let query = vector(17);
    let want = truth(&query, 3);

    // 1. No index: the linear scan, which is the reference.
    let (_d, lens) = seeded(engine);
    check(&format!("{tag}/scan"), &lens.search("v", &query, 3, 10, 50).unwrap(), &want);

    // 2. IVF.
    let (_d, lens) = seeded(engine);
    lens.build_ivf_index("v", 8, "l2")
        .unwrap_or_else(|e| panic!("{tag}: building an IVF index failed: {e}"));
    check(&format!("{tag}/ivf"), &lens.search("v", &query, 3, 10, 50).unwrap(), &want);

    // 3. HNSW.
    let (_d, lens) = seeded(engine);
    lens.build_hnsw_index("v", 16, 100, "l2")
        .unwrap_or_else(|e| panic!("{tag}: building an HNSW index failed: {e}"));
    check(&format!("{tag}/hnsw"), &lens.search("v", &query, 3, 10, 50).unwrap(), &want);
}

#[test]
fn every_search_path_agrees_on_a_legacy_collection() {
    agree(false);
}

#[test]
fn every_search_path_agrees_on_an_engine_collection() {
    agree(true);
}
