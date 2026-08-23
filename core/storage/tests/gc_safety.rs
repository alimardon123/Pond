// gc_safety.rs — garbage collection must never delete live data.
//
// GC is the one operation where being wrong is unrecoverable. Everything else
// can be retried, re-read, or rolled forward; a deleted blob is gone.
//
// It got this wrong. A legacy ref holds `{"hash":"..."}` and `kernel.resolve`
// reads it; an engine head holds its bytes directly, so `resolve` returned
// `None` and the engine's index nodes and spilled values never entered the
// live set. `pond gc` then deleted them. Measured before the fix: two rows
// before, zero after — silent, total data loss.
//
// These tests exist so that any future way of reaching data has to be added to
// the live-set walk before it can ship.

use pond_core::encode::TypedColumn;
use pond_kernel::PondKernel;
use pond_storage::maintenance::GarbageCollector;
use pond_storage::engine_path;

fn kernel(dir: &std::path::Path) -> PondKernel {
    PondKernel::new_local(dir).unwrap()
}

fn rows_of(k: &PondKernel, collection: &str) -> Vec<i64> {
    let columns = engine_path::read_rows(k, collection).expect("read");
    match columns.iter().find(|(n, _)| n == "id") {
        Some((_, TypedColumn::Int64(v))) => {
            let mut v = v.clone();
            v.sort();
            v
        }
        _ => Vec::new(),
    }
}

/// The headline: an engine collection survives garbage collection.
#[test]
fn gc_does_not_delete_engine_collections() {
    let dir = tempfile::tempdir().unwrap();
    let s = kernel(dir.path());

    engine_path::create(&s, "users").unwrap();
    engine_path::write_rows(
        &s,
        "users",
        &[
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            (
                "name",
                TypedColumn::String(vec!["ada".into(), "grace".into(), "alan".into()]),
            ),
        ],
        1,
    )
    .unwrap();

    assert_eq!(rows_of(&s, "users"), vec![1, 2, 3]);

    let gc = GarbageCollector::new(&s);
    gc.vacuum(None, 0, false); // VacuumResult, not a Result

    assert_eq!(
        rows_of(&s, "users"),
        vec![1, 2, 3],
        "garbage collection deleted live engine data"
    );
}

/// The subtler half: a value large enough to be spilled lives in its own blob,
/// reachable only from inside an index leaf. A walk that stopped at the leaf
/// would collect the leaf and delete the value it points at — leaving a tree
/// that decodes and a row that cannot be read.
#[test]
fn gc_does_not_delete_spilled_values() {
    let dir = tempfile::tempdir().unwrap();
    let s = kernel(dir.path());
    let big = "x".repeat(64 * 1024);

    engine_path::create(&s, "docs").unwrap();
    engine_path::write_rows(
        &s,
        "docs",
        &[
            ("id", TypedColumn::Int64(vec![1])),
            ("body", TypedColumn::String(vec![big.clone()])),
        ],
        1,
    )
    .unwrap();

    let gc = GarbageCollector::new(&s);
    gc.vacuum(None, 0, false); // VacuumResult, not a Result

    let columns = engine_path::read_rows(&s, "docs").expect("read after gc");
    match columns.iter().find(|(n, _)| n == "body") {
        Some((_, TypedColumn::String(v))) => assert_eq!(
            v,
            &vec![big],
            "the spilled value was collected as garbage"
        ),
        _ => panic!("body column is gone after gc"),
    }
}

/// Both kinds of collection in one store, and both must survive.
#[test]
fn gc_preserves_legacy_and_engine_together() {
    let dir = tempfile::tempdir().unwrap();
    let s = kernel(dir.path());

    pond_storage::write::write_rows_no_crdt(
        &s,
        "old",
        "main",
        &[("id", TypedColumn::Int64(vec![9]))],
        "seed",
    )
    .unwrap();

    engine_path::create(&s, "new").unwrap();
    engine_path::write_rows(&s, "new", &[("id", TypedColumn::Int64(vec![1]))], 1)
        .unwrap();

    let gc = GarbageCollector::new(&s);
    gc.vacuum(None, 0, false); // VacuumResult, not a Result

    assert_eq!(rows_of(&s, "new"), vec![1]);
    assert!(
        !pond_storage::read::read(&s, "old", "main")
            .unwrap()
            .is_empty(),
        "the legacy collection was collected"
    );
}

/// Running GC twice must be safe, and the second run must find nothing new to
/// delete from live data.
#[test]
fn gc_is_idempotent_over_engine_data() {
    let dir = tempfile::tempdir().unwrap();
    let s = kernel(dir.path());

    engine_path::create(&s, "users").unwrap();
    engine_path::write_rows(&s, "users", &[("id", TypedColumn::Int64(vec![1, 2]))], 1)
        .unwrap();

    let gc = GarbageCollector::new(&s);
    gc.vacuum(None, 0, false); 
    gc.vacuum(None, 0, false); 

    assert_eq!(rows_of(&s, "users"), vec![1, 2]);
}

/// Garbage collection must not destroy subject keys.
///
/// This is the same class of bug as GC deleting engine collections: a new kind
/// of persistent state arrives, and a subsystem that reasons about what is
/// live has never heard of it. The consequence here would be worse than data
/// loss — it would be *silent* data loss, since destroying a key makes every
/// subject's data unreadable with no error anywhere.
///
/// It is safe today because keys are named objects rather than blobs, and GC
/// only ever deletes blobs. That is a deliberate property of the keystore
/// design, so it is worth pinning rather than rediscovering.
#[test]
fn gc_does_not_destroy_subject_keys() {
    use pond_crypto::{KeyStore, SubjectId};

    let dir = tempfile::tempdir().unwrap();
    let s = kernel(dir.path());

    let keystore = KeyStore::new(pond_kernel::LocalFSObjectStore::new(dir.path()).unwrap());
    let subject: SubjectId = "alice".to_string();
    let key = keystore.get_or_create(&subject).unwrap();
    let sealed = pond_crypto::seal(&key, b"users/name", b"personal data");

    // Some ordinary data alongside, so GC has real work to do.
    engine_path::create(&s, "users").unwrap();
    engine_path::write_rows(&s, "users", &[("id", TypedColumn::Int64(vec![1]))], 1).unwrap();

    let gc = GarbageCollector::new(&s);
    gc.vacuum(None, 0, false);

    let after = keystore
        .get(&subject)
        .unwrap()
        .expect("the subject's key must survive garbage collection");
    assert_eq!(
        after.as_bytes(),
        key.as_bytes(),
        "the key changed, which would make every existing value unreadable"
    );
    assert_eq!(
        pond_crypto::open(&after, b"users/name", &sealed).unwrap(),
        b"personal data"
    );
}

/// A compacted pond survives garbage collection.
///
/// Compaction publishes a head under a reserved writer id holding *merged*
/// roots — index nodes that no individual writer's head names. If the live-set
/// walk enumerated writers rather than heads, or skipped the reserved id as
/// "not a real writer", those merged nodes would be unreachable and GC would
/// delete the very structure that makes reads cheap. This is the same shape as
/// the bug at the top of this file, one layer up, so it gets its own test
/// rather than an assumption that walking `heads/` covers it.
#[test]
fn gc_does_not_delete_a_compacted_head() {
    let dir = tempfile::tempdir().unwrap();
    let s = kernel(dir.path());

    engine_path::create(&s, "users").unwrap();
    for w in 1..=4u64 {
        engine_path::write_rows(
            &s,
            "users",
            &[
                ("id", TypedColumn::Int64(vec![w as i64])),
                ("name", TypedColumn::String(vec![format!("w{}", w)])),
            ],
            w,
        )
        .unwrap();
    }

    let before = rows_of(&s, "users");
    assert_eq!(before.len(), 4, "four writers, four rows");

    pond_engine::compact_heads(
        s.store_handle(),
        pond_cache::CacheConfig::default(),
        pond_storage::definition::load(&s, "users").engine_config(),
    )
    .unwrap();

    assert_eq!(
        rows_of(&s, "users"),
        before,
        "compaction alone must not change the rows"
    );

    // `collect` only reports; `vacuum` is what deletes. Asserting after
    // `collect` proves nothing, which is how this test passed for a while
    // without exercising the thing it names.
    let report = GarbageCollector::new(&s).vacuum(None, 0, false);
    println!("gc over a compacted pond: {:?}", report);

    assert_eq!(
        rows_of(&s, "users"),
        before,
        "garbage collection must not delete the merged index nodes that only \
         the compacted head names"
    );

    // And the pond is still readable through a fresh engine reader, not only
    // through the storage path.
    let mut r = pond_engine::Reader::open(s.store_handle()).unwrap();
    assert_eq!(r.scan("users").unwrap().len(), 4);
}

/// A retained history survives garbage collection.
///
/// History entries name roots the collection *used* to have. Those nodes are
/// unreachable from any live head — that is precisely what makes them look
/// like garbage — so a live-set walk that only follows heads deletes exactly
/// the trees the history exists to preserve, and time travel resolves to
/// missing blobs.
///
/// Same shape as the bug at the top of this file: a new way of reaching data
/// that the walk did not know about. It gets its own test rather than an
/// assumption that walking heads covers it.
#[test]
fn gc_does_not_delete_the_trees_a_history_points_at() {
    let dir = tempfile::tempdir().unwrap();
    let s = kernel(dir.path());

    engine_path::create(&s, "users").unwrap();
    // Several publishes, so there are superseded heads for compaction to turn
    // into history.
    for i in 1..=5i64 {
        engine_path::write_rows(
            &s,
            "users",
            &[
                ("id", TypedColumn::Int64(vec![i])),
                ("name", TypedColumn::String(vec![format!("n{}", i)])),
            ],
            1,
        )
        .unwrap();
    }

    pond_engine::compact_heads(
        s.store_handle(),
        pond_cache::CacheConfig::default(),
        pond_storage::definition::load(&s, "users").engine_config(),
    )
    .unwrap();

    let log = pond_engine::history::load(&*s.store_handle(), "users");
    assert!(
        log.entries.len() >= 2,
        "compaction should have recorded the superseded roots, got {:?}",
        log.entries.len()
    );
    let historic: Vec<String> = log.roots().map(|r| r.to_string()).collect();

    let report = GarbageCollector::new(&s).vacuum(None, 0, false);
    println!("gc with a retained history: {:?}", report);

    // Current data is intact.
    assert_eq!(rows_of(&s, "users"), vec![1, 2, 3, 4, 5]);

    // And every root the history names is still readable, which is the whole
    // point of retaining it.
    let store = s.store_handle();
    for root in &historic {
        assert!(
            store.get_blob(root).is_ok(),
            "gc deleted a node the history points at: {}",
            root
        );
    }
}

/// A field stored in its own object is live, and garbage collection has to
/// know that.
///
/// Spilling used to be per record: a leaf value was either the record or a
/// pointer to it, and following the pointer was enough. Per-field spilling
/// adds a second level — a record that sits *inline* in the leaf can still
/// name payloads stored elsewhere, and a record behind a pointer can too.
///
/// A live-set walk that stops at the record level marks those payloads dead
/// and deletes them, and the row then reads back with its largest field
/// missing. Third instance of the same shape in this file, so it gets its own
/// test rather than an assumption.
#[test]
fn gc_does_not_delete_a_fields_spilled_payload() {
    let dir = tempfile::tempdir().unwrap();
    let s = kernel(dir.path());

    engine_path::create(&s, "docs").unwrap();
    // Small columns beside one large one: the record encodes small once the
    // large field becomes a pointer, so it stays inline in the leaf and the
    // payload is reachable only through the field.
    let big = "z".repeat(64 * 1024);
    engine_path::write_rows(
        &s,
        "docs",
        &[
            ("id", TypedColumn::Int64(vec![1, 2])),
            ("tag", TypedColumn::String(vec!["a".into(), "b".into()])),
            (
                "attachment",
                TypedColumn::String(vec![big.clone(), big.clone()]),
            ),
        ],
        1,
    )
    .unwrap();

    let read_back = |what: &str| -> Vec<String> {
        match engine_path::read_rows(&s, "docs")
            .unwrap()
            .into_iter()
            .find(|(n, _)| n == what)
        {
            Some((_, TypedColumn::String(v))) => v,
            _ => Vec::new(),
        }
    };
    assert_eq!(read_back("attachment").len(), 2, "both attachments present");
    assert_eq!(read_back("attachment")[0].len(), big.len());

    let report = GarbageCollector::new(&s).vacuum(None, 0, false);
    println!("gc over field-spilled rows: {:?}", report);

    assert_eq!(rows_of(&s, "docs"), vec![1, 2], "the rows must survive");
    let after = read_back("attachment");
    assert_eq!(
        after.len(),
        2,
        "both attachments must still resolve after gc"
    );
    assert_eq!(
        after[0].len(),
        big.len(),
        "the payload must come back whole, not truncated or empty"
    );
}

/// Bytes the walk cannot interpret stop the sweep, rather than being read as
/// "nothing reachable here".
///
/// This is the failure that turns a decoder regression into permanent data
/// loss, and it is not hypothetical: an intermediate build during the record
/// format change could not decode v1 records holding a spilled field, so the
/// walk found no field pointers, and `pond gc` deleted payloads that were very
/// much alive. The read afterwards failed with "Blob ... not found".
///
/// A record that will not decode is not evidence that it names no payloads. It
/// is evidence that this build cannot tell. The two are indistinguishable to a
/// walk that treats a decode failure as an empty answer, so the walk now
/// reports itself incomplete and nothing is deleted.
#[test]
fn gc_refuses_to_sweep_when_it_cannot_read_a_record() {
    let dir = tempfile::tempdir().unwrap();
    let s = kernel(dir.path());

    engine_path::create(&s, "docs").unwrap();
    engine_path::write_rows(
        &s,
        "docs",
        &[
            ("id", TypedColumn::Int64(vec![1, 2])),
            ("tag", TypedColumn::String(vec!["a".into(), "b".into()])),
        ],
        1,
    )
    .unwrap();

    // A healthy pond sweeps.
    let clean = GarbageCollector::new(&s).vacuum(None, 0, true);
    assert!(!clean.incomplete, "a readable pond must not report incomplete");

    // Now plant a leaf value that is not a decodable record. Content
    // addressing means we cannot corrupt an existing node in place, so this
    // writes a new leaf and points a head at it — the same shape as a node
    // written by a version this build does not understand.
    let store = s.store_handle();
    let garbage = store.put_blob(b"PREC\xff not a record this build knows").unwrap();
    // A valid key, so the failure under test is the *value* not decoding
    // rather than the key. And a collection of its own, so `docs` stays
    // readable and the assertions below are about the sweep, not about a
    // broken read.
    let key = pond_index::Key::new(vec![pond_index::int(1)]).encode();
    let leaf = pond_index::Node::Leaf {
        entries: vec![(key, format!("PSPL{}", garbage).into_bytes())],
    };
    let leaf_hash = store.put_blob(&leaf.encode()).unwrap();

    let mut head = pond_record::Head::new(0xDEAD_BEEF);
    head.set_root("from_a_future_version", &leaf_hash);
    let bytes = pond_record::encode_head(&head);
    store
        .put_object(
            &pond_engine::head_key(0xDEAD_BEEF, 1, &pond_kernel::hash_bytes(&bytes)),
            &bytes,
        )
        .unwrap();

    let report = GarbageCollector::new(&s).vacuum(None, 0, false);
    assert!(
        report.incomplete,
        "a walk that met unreadable bytes must say so: {:?}",
        report
    );
    assert_eq!(
        report.deleted, 0,
        "and it must delete nothing, because it has proved nothing dead"
    );

    // The readable data is untouched.
    assert_eq!(rows_of(&s, "docs"), vec![1, 2]);
}
