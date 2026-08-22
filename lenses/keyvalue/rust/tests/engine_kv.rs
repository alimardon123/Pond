// engine_kv.rs — a point lookup must cost the tree's depth, not the collection.

use pond_keyvalue_lens::engine_kv;
use pond_kernel::PondKernel;
use serde_json::json;

fn kernel(dir: &std::path::Path) -> PondKernel {
    PondKernel::new_local(dir).unwrap()
}

fn seeded(dir: &std::path::Path) -> PondKernel {
    let k = kernel(dir);
    engine_kv::create(&k, "kv").unwrap();
    engine_kv::put_many(
        &k,
        "kv",
        &[
            ("alpha".into(), json!({"n": 1})),
            ("beta".into(), json!("a string")),
            ("gamma".into(), json!(42)),
        ],
        1,
    )
    .unwrap();
    k
}

/// Values of every JSON shape must round-trip, including bare scalars — the
/// original lens wrapped those in an object and changed their shape.
#[test]
fn values_round_trip_whatever_their_shape() {
    let dir = tempfile::tempdir().unwrap();
    let k = seeded(dir.path());

    assert_eq!(engine_kv::get(&k, "kv", "alpha").unwrap(), Some(json!({"n": 1})));
    assert_eq!(engine_kv::get(&k, "kv", "beta").unwrap(), Some(json!("a string")));
    assert_eq!(engine_kv::get(&k, "kv", "gamma").unwrap(), Some(json!(42)));
    assert_eq!(engine_kv::get(&k, "kv", "absent").unwrap(), None);
}

/// Writing one key must not disturb the others.
#[test]
fn a_write_touches_only_its_key() {
    let dir = tempfile::tempdir().unwrap();
    let k = seeded(dir.path());

    engine_kv::put_many(&k, "kv", &[("beta".into(), json!("replaced"))], 1).unwrap();

    assert_eq!(engine_kv::get(&k, "kv", "beta").unwrap(), Some(json!("replaced")));
    assert_eq!(engine_kv::get(&k, "kv", "alpha").unwrap(), Some(json!({"n": 1})));
    assert_eq!(engine_kv::get(&k, "kv", "gamma").unwrap(), Some(json!(42)));
    assert_eq!(engine_kv::count(&k, "kv").unwrap(), 3);
}

/// A delete removes exactly one key and is idempotent.
#[test]
fn delete_removes_one_key_and_repeats_safely() {
    let dir = tempfile::tempdir().unwrap();
    let k = seeded(dir.path());

    engine_kv::delete_many(&k, "kv", &["beta".to_string()], 1).unwrap();
    assert_eq!(engine_kv::get(&k, "kv", "beta").unwrap(), None);
    assert!(!engine_kv::exists(&k, "kv", "beta").unwrap());
    assert_eq!(engine_kv::count(&k, "kv").unwrap(), 2);

    // Asking twice is not an error.
    engine_kv::delete_many(&k, "kv", &["beta".to_string()], 1).unwrap();
    assert_eq!(engine_kv::count(&k, "kv").unwrap(), 2);

    // The survivors are untouched.
    assert_eq!(engine_kv::get(&k, "kv", "alpha").unwrap(), Some(json!({"n": 1})));
}

/// A write after a delete brings the key back — the same convergence rule the
/// record model uses everywhere else.
#[test]
fn writing_after_a_delete_restores_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let k = seeded(dir.path());

    engine_kv::delete_many(&k, "kv", &["alpha".to_string()], 1).unwrap();
    assert_eq!(engine_kv::get(&k, "kv", "alpha").unwrap(), None);

    engine_kv::put_many(&k, "kv", &[("alpha".into(), json!("back"))], 1).unwrap();
    assert_eq!(engine_kv::get(&k, "kv", "alpha").unwrap(), Some(json!("back")));
}

#[test]
fn keys_and_get_all_agree() {
    let dir = tempfile::tempdir().unwrap();
    let k = seeded(dir.path());

    let mut names = engine_kv::keys(&k, "kv").unwrap();
    names.sort();
    assert_eq!(names, vec!["alpha", "beta", "gamma"]);

    let all = engine_kv::get_all(&k, "kv").unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(
        all.iter().find(|(k, _)| k == "gamma").map(|(_, v)| v.clone()),
        Some(json!(42))
    );
}

/// The headline: a point lookup must not read the collection.
///
/// The original lens reads every pair and filters, so `get` costs the
/// collection's size. Keying by the index makes it a descent — bounded by the
/// tree's depth however many pairs exist.
#[test]
fn a_point_lookup_does_not_scale_with_the_collection() {
    use std::sync::Arc;

    fn reads_for_one_get(pairs: usize) -> u64 {
        let dir = tempfile::tempdir().unwrap();
        // `Metered` from the kernel, not a counting wrapper written here. The
        // one this file used to define — like every other copy in the tree —
        // left the batch methods on their sequential defaults, so it quietly
        // de-parallelised the reads it was counting.
        let c = Arc::new(pond_kernel::Metered::new(
            pond_kernel::LocalFSObjectStore::new(dir.path()).unwrap(),
        ));
        let k = PondKernel::new_with_store(Box::new(Arc::clone(&c)));
        engine_kv::create(&k, "kv").unwrap();

        let batch: Vec<(String, serde_json::Value)> = (0..pairs)
            .map(|i| (format!("key-{:08}", i), json!({ "i": i })))
            .collect();
        engine_kv::put_many(&k, "kv", &batch, 1).unwrap();

        c.reset();
        let found = engine_kv::get(&k, "kv", &format!("key-{:08}", pairs / 2)).unwrap();
        assert!(found.is_some());
        c.stats().gets
    }

    let small = reads_for_one_get(100);
    let large = reads_for_one_get(5_000);
    println!(
        "point lookup: {} reads at 100 pairs, {} reads at 5000 pairs",
        small, large
    );

    assert!(
        large <= small + 2,
        "a point lookup went from {} to {} reads as the collection grew 50x — \
         it should track the tree's depth, not the collection",
        small,
        large
    );
}
