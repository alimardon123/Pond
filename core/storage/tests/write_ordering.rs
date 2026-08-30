// write_ordering.rs — a later write must not lose to an earlier one.
//
// A field's version is `(physical, logical, writer)`, compared in that order,
// and per-field last-writer-wins keeps the higher. The columnar write path used
// the row's index *within its batch* as the logical component, with a comment
// claiming this ordered a batch after the batches before it.
//
// It did not. The row index restarts at zero every batch. Update one row of an
// earlier batch inside the same millisecond and the update is row 0 of its own
// batch — `logical = 0` — against the `logical = 999` of the row it means to
// replace. The physical components tie, so the merge keeps the *older* value
// and the update is discarded: no error, no conflict, no retry, and every
// later reader sees the stale row.
//
// # Why these tests drive the encoder directly
//
// Reaching this through `write_rows` needs two batches inside one millisecond,
// which is real but not reproducible on demand — a thousand-row first batch
// takes long enough that the clock moves and the versions never tie. A test
// that depends on losing that race is a test that passes for the wrong reason
// most of the time. So the physical timestamp is supplied explicitly, which is
// the same input the clock would have produced, and the outcome becomes
// deterministic.
//
// The hazard is not hypothetical: any caller that supplies its own timestamp,
// or any pair of small batches close together, sits exactly here.

use pond_core::TypedColumn;
use pond_record::{merge_records, Value};

/// One fixed millisecond, so both batches tie on the physical component.
const SAME_MS: u64 = 1_700_000_000_000;

fn batch(rowids: &[&str], values: &[i64], physical: u64) -> Vec<(pond_index::Key, pond_record::Record)> {
    pond_storage::columnar::columns_to_records(
        &[
            ("_rowid", TypedColumn::String(rowids.iter().map(|s| s.to_string()).collect())),
            ("v", TypedColumn::Int64(values.to_vec())),
        ],
        1,
        physical,
    )
}

fn find<'a>(
    rows: &'a [(pond_index::Key, pond_record::Record)],
    rowid: &str,
) -> &'a pond_record::Record {
    rows.iter()
        .find(|(_, r)| r.get("_rowid") == Some(&Value::Str(rowid.to_string())))
        .map(|(_, r)| r)
        .expect("row is missing")
}

/// The original failure: a large batch, then a one-row update in the same
/// millisecond. The update is row 0 and used to carry the lowest version in
/// play.
#[test]
fn a_one_row_update_in_the_same_millisecond_is_not_discarded() {
    let ids: Vec<String> = (0..1000).map(|i| format!("row-{i:04}")).collect();
    let refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
    let first = batch(&refs, &(0..1000).map(|i| i * 10).collect::<Vec<_>>(), SAME_MS);

    let second = batch(&["row-0999"], &[-1], SAME_MS);

    let merged = merge_records(find(&first, "row-0999"), &second[0].1);
    assert_eq!(
        merged.get("v"),
        Some(&Value::Int(-1)),
        "the later write lost to the earlier one and was silently discarded"
    );
}

/// Every row of the earlier batch, not just its last — the failure gets worse
/// the further into the batch the target sits, so check the whole range.
#[test]
fn every_row_of_an_earlier_batch_can_be_updated_in_the_same_millisecond() {
    let ids: Vec<String> = (0..16).map(|i| format!("row-{i:04}")).collect();
    let refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
    let first = batch(&refs, &(0..16).collect::<Vec<_>>(), SAME_MS);

    for (i, id) in ids.iter().enumerate() {
        let update = batch(&[id.as_str()], &[1_000 + i as i64], SAME_MS);
        let merged = merge_records(find(&first, id), &update[0].1);
        assert_eq!(
            merged.get("v"),
            Some(&Value::Int(1_000 + i as i64)),
            "updating {id} in the same millisecond was discarded"
        );
    }
}

/// Ordering within one batch must survive the change.
///
/// This is what the row index was genuinely for: two rows carrying the same
/// id in one batch, where the later one should win. Reserving a block per
/// batch keeps that, and the assertion is here so a future simplification to
/// "one version per batch" cannot quietly take it away.
#[test]
fn the_later_of_two_updates_inside_one_batch_still_wins() {
    let rows = batch(&["row-1", "row-1"], &[20, 30], SAME_MS);
    let merged = merge_records(&rows[0].1, &rows[1].1);
    assert_eq!(
        merged.get("v"),
        Some(&Value::Int(30)),
        "the earlier row of the batch won"
    );
}

/// Versions must rise across batches even when every batch is one row and the
/// clock never moves — the case where the physical component can do no work
/// at all.
#[test]
fn versions_rise_across_single_row_batches_at_a_frozen_clock() {
    let mut previous = None;
    for step in 0..50 {
        let rows = batch(&["row-1"], &[step], SAME_MS);
        let version = rows[0].1.fields["v"].version;
        if let Some(prev) = previous {
            assert!(
                version > prev,
                "version did not advance between batches: {prev:?} then {version:?}"
            );
        }
        previous = Some(version);
    }
}

/// The end-to-end shape, through the real API rather than the encoder.
///
/// It cannot force the millisecond collision, so it is not the regression
/// guard — it is here to show the fix did not break ordinary keyed updates,
/// which is the path everything else uses.
#[test]
fn a_keyed_update_through_the_public_api_replaces_rather_than_appends() {
    use pond_kernel::PondKernel;

    let dir = tempfile::tempdir().unwrap();
    let k = PondKernel::new_local(dir.path()).unwrap();
    pond_storage::engine_path::create(&k, "t").unwrap();

    let ids: Vec<String> = (0..8).map(|i| format!("row-{i}")).collect();
    pond_storage::engine_path::write_rows(
        &k,
        "t",
        &[
            ("_rowid", TypedColumn::String(ids.clone())),
            ("v", TypedColumn::Int64((0..8).collect())),
        ],
        1,
    )
    .unwrap();

    pond_storage::engine_path::write_rows(
        &k,
        "t",
        &[
            ("_rowid", TypedColumn::String(vec!["row-7".into()])),
            ("v", TypedColumn::Int64(vec![-1])),
        ],
        1,
    )
    .unwrap();

    let cols = pond_storage::engine_path::read_rows(&k, "t").unwrap();
    let rowids = match &cols.iter().find(|(n, _)| n == "_rowid").unwrap().1 {
        TypedColumn::String(v) => v.clone(),
        other => panic!("_rowid column is {other:?}"),
    };
    let vs = match &cols.iter().find(|(n, _)| n == "v").unwrap().1 {
        TypedColumn::Int64(v) => v.clone(),
        other => panic!("v column is {other:?}"),
    };

    assert_eq!(rowids.len(), 8, "the update appended a row instead of updating one");
    let pos = rowids.iter().position(|r| r == "row-7").unwrap();
    assert_eq!(vs[pos], -1, "the update did not take effect");
}
