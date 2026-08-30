// dimension_order.rs — a vector must come back in the order it went in.
//
// A vector is stored one dimension per column: `dim_0`, `dim_1`, … Reading one
// back means collecting those columns and ordering them. Four places did that
// independently, and they did not agree: three sorted the column names as
// strings, and the IVF search path did not sort at all.
//
// String order and numeric order are the same up to nine dimensions and
// diverge at ten, because `"dim_10" < "dim_2"`. From there on the stored
// vectors come back permuted while the query vector — an ordinary list from
// the caller — does not, so every distance is computed between coordinates
// that do not correspond to each other.
//
// The failure is invisible from inside: search still returns k results, still
// sorted by distance, still plausible. Only comparing against the right answer
// shows it. That is why this test asks for a vector that is *in the
// collection* and checks that the collection hands back that one, at distance
// zero — a question with an unambiguous answer.
//
// Eight dimensions passes either way. Thirty-two is the test.

use pond_kernel::PondKernel;
use pond_storage::UnifiedStorage;
use pond_vector_lens::VectorLens;

fn lens(dir: &std::path::Path) -> VectorLens {
    let kernel = PondKernel::new_local(dir).unwrap();
    VectorLens::new(UnifiedStorage::new(kernel))
}

/// Distinct, deterministic vectors: row `i` is `[i, i+1, i+2, …]`.
///
/// Every dimension differs from every other, which is what makes a permutation
/// detectable — a vector of identical components would survive any reordering.
fn vector(i: usize, dims: usize) -> Vec<f64> {
    (0..dims).map(|d| (i * 100 + d) as f64).collect()
}

fn round_trip(dims: usize) {
    let dir = tempfile::tempdir().unwrap();
    let lens = lens(dir.path());
    let rows = 40;

    for i in 0..rows {
        lens.insert("v", &format!("id-{i}"), &vector(i, dims), None);
    }
    lens.commit("v", "seed").expect("commit");

    // Ask for a vector that is in the collection. The right answer is that
    // exact row, at distance zero; anything else means the stored coordinates
    // are not the ones that were written.
    let mut exact = 0;
    for i in 0..rows {
        let hits = lens.search("v", &vector(i, dims), 1, 8, 32).expect("search");
        if let Some((dist, id)) = hits.first() {
            if id == &format!("id-{i}") && *dist < 1e-9 {
                exact += 1;
            }
        }
    }

    assert_eq!(
        exact, rows,
        "at {dims} dimensions, only {exact}/{rows} exact-match queries found \
         their own vector at distance 0 — the stored dimensions are in a \
         different order from the query's"
    );
}

/// Below ten dimensions, string order and numeric order coincide. This passed
/// before the fix too, and is kept so a regression cannot be mistaken for the
/// bug returning only at scale.
#[test]
fn an_exact_match_is_found_at_eight_dimensions() {
    round_trip(8);
}

/// The exact boundary. Ten *dimensions* are `dim_0`..`dim_9`, whose string
/// order is still their numeric order — the divergence needs `dim_10` to
/// exist, which takes eleven. Pinning the first broken case rather than a
/// comfortably broken one is the difference between a test that shows where
/// the edge is and a test that shows only that an edge exists somewhere.
#[test]
fn an_exact_match_is_found_at_eleven_dimensions() {
    round_trip(11);
}

/// Well past the boundary, where the permutation touches most coordinates.
#[test]
fn an_exact_match_is_found_at_thirty_two_dimensions() {
    round_trip(32);
}

/// Nearest-neighbour order must agree with the truth, not merely be sorted.
///
/// A permuted index still returns k results in ascending distance; they are
/// just the wrong k. This compares against a brute-force answer computed here.
///
/// Note this one is the weaker of the two checks: with vectors as regular as
/// these, a permutation can leave the *ranking* intact even while every
/// distance is wrong, and it passed before the fix. The exact-match tests
/// above are what actually catch the bug. It is kept because ranking is what
/// callers consume, so a future change that breaks ranking without breaking
/// exact match should still be caught.
#[test]
fn the_nearest_neighbour_agrees_with_brute_force_at_thirty_two_dimensions() {
    let dims = 32;
    let dir = tempfile::tempdir().unwrap();
    let lens = lens(dir.path());
    let rows = 40;

    for i in 0..rows {
        lens.insert("v", &format!("id-{i}"), &vector(i, dims), None);
    }
    lens.commit("v", "seed").expect("commit");

    let query: Vec<f64> = (0..dims).map(|d| (17 * 100 + d) as f64 + 0.25).collect();

    let truth = (0..rows)
        .min_by(|&a, &b| {
            let d = |i: usize| {
                vector(i, dims)
                    .iter()
                    .zip(&query)
                    .map(|(x, q)| (x - q) * (x - q))
                    .sum::<f64>()
            };
            d(a).partial_cmp(&d(b)).unwrap()
        })
        .unwrap();

    let hits = lens.search("v", &query, 1, 8, 32).expect("search");
    assert_eq!(
        hits.first().map(|(_, id)| id.as_str()),
        Some(format!("id-{truth}").as_str()),
        "search disagreed with brute force: it returns k results in distance \
         order either way, so being sorted proves nothing about being right"
    );
}
