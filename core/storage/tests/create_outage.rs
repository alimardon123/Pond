// create_outage.rs — a read outage must not make an existing collection look new.
//
// `create` is a no-op on a collection that already exists, and refuses one that
// holds legacy data. Both guards ask the store a question, and both took
// silence for an answer:
//
//   definition::load  ->  read_named -> get_object -> Option, and a failed
//                         read is `None`, which `unwrap_or_else(legacy)` turns
//                         into "this collection has no definition".
//   has_legacy_data   ->  list_names_prefix -> list_paths().unwrap_or_default(),
//                         so a failed LIST is an empty listing, which reads as
//                         "there is nothing here".
//
// Both fail *open*. During a transient outage — a 500, an expired credential,
// a DNS blip — an established Engine collection full of rows presents as a
// blank slate, and `create` writes a fresh definition over it: a new random
// `chunk_salt`, which decides where chunk boundaries fall, and an empty column
// list. Nothing errors. The next write chunks differently from every chunk
// already stored, so structural sharing and deterministic rebuild — the two
// properties the whole index rests on — are gone, silently.
//
// The collection is not supposed to be reformattable in place at all; the code
// says so in a comment and then does it anyway when the store stops answering.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use pond_core::TypedColumn;
use pond_kernel::{LocalFSObjectStore, ObjectStore, PondKernel};
use pond_storage::{definition, engine_path};

/// A store that can be told to start failing every read.
///
/// Writes keep working, because that is the situation being modelled: the data
/// is intact on the far side and only our ability to look at it has gone.
struct Blind {
    inner: LocalFSObjectStore,
    blind: AtomicBool,
}

impl Blind {
    fn new(inner: LocalFSObjectStore) -> Self {
        Self { inner, blind: AtomicBool::new(false) }
    }
    fn go_blind(&self) {
        self.blind.store(true, Ordering::SeqCst);
    }
    fn is_blind(&self) -> bool {
        self.blind.load(Ordering::SeqCst)
    }
}

impl ObjectStore for Blind {
    fn get_object(&self, path: &str) -> Option<Vec<u8>> {
        if self.is_blind() {
            return None; // what a failed GET looks like through this signature
        }
        self.inner.get_object(path)
    }
    fn get_path(&self, path: &str) -> Option<String> {
        if self.is_blind() {
            return None;
        }
        self.inner.get_path(path)
    }
    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        if self.is_blind() {
            return Err(io::Error::other("simulated listing failure"));
        }
        self.inner.list_paths(prefix)
    }
    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        if self.is_blind() {
            return Err(io::Error::other("simulated read failure"));
        }
        self.inner.get_blob(hash)
    }
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        self.inner.put_blob(data)
    }
    fn put_path(&self, path: &str, hash: &str) -> io::Result<()> {
        self.inner.put_path(path, hash)
    }
    fn put_object(&self, path: &str, bytes: &[u8]) -> io::Result<()> {
        self.inner.put_object(path, bytes)
    }
    fn delete_path(&self, path: &str) -> io::Result<bool> {
        self.inner.delete_path(path)
    }
    fn blob_exists(&self, hash: &str) -> bool {
        self.inner.blob_exists(hash)
    }
    fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        self.inner.delete_blob(hash)
    }
}

fn populated() -> (tempfile::TempDir, Arc<Blind>, PondKernel) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Blind::new(LocalFSObjectStore::new(dir.path()).unwrap()));
    let kernel = PondKernel::new_with_store(Box::new(Arc::clone(&store)));

    engine_path::create(&kernel, "t").unwrap();
    engine_path::write_rows(
        &kernel,
        "t",
        &[
            ("id", TypedColumn::Int64((0..1000).collect())),
            ("v", TypedColumn::Int64((0..1000).map(|i| i * 7).collect())),
        ],
        1,
    )
    .unwrap();

    (dir, store, kernel)
}

/// The collection's identity must survive a read outage.
///
/// `chunk_salt` is the load-bearing field: it decides where content-defined
/// chunk boundaries fall, so replacing it re-chunks every subsequent write
/// against boundaries nothing already stored shares.
#[test]
fn create_during_a_read_outage_does_not_replace_the_definition() {
    let (_dir, store, kernel) = populated();

    let before = definition::load(&kernel, "t");
    assert_eq!(before.format, definition::Format::Engine, "setup");

    store.go_blind();
    let result = engine_path::create(&kernel, "t");
    store.blind.store(false, Ordering::SeqCst);

    let after = definition::load(&kernel, "t");
    assert_eq!(
        after.chunk_salt, before.chunk_salt,
        "create overwrote a live collection's chunk salt while it could not \
         read: every subsequent write would chunk against boundaries nothing \
         already stored shares (create returned {:?})",
        result.as_ref().map(|_| "Ok")
    );
    assert_eq!(
        after.columns, before.columns,
        "create replaced the recorded schema with an empty one"
    );
    assert!(
        result.is_err(),
        "create reported success while unable to read the collection it was \
         asked about"
    );
}

/// The rows must still be there and still readable afterwards.
#[test]
fn the_collection_still_reads_after_a_create_during_an_outage() {
    let (_dir, store, kernel) = populated();

    store.go_blind();
    let _ = engine_path::create(&kernel, "t");
    store.blind.store(false, Ordering::SeqCst);

    let cols = engine_path::read_rows(&kernel, "t").unwrap();
    let ids = cols.iter().find(|(n, _)| n == "id").expect("id column survived");
    assert_eq!(ids.1.len(), 1000, "rows went missing");
}

/// Creating a genuinely new collection must still work — the guard must not
/// refuse everything.
#[test]
fn creating_a_new_collection_still_works() {
    let (_dir, _store, kernel) = populated();
    engine_path::create(&kernel, "fresh").unwrap();
    assert_eq!(
        definition::load(&kernel, "fresh").format,
        definition::Format::Engine
    );
}

/// And calling create twice on an existing collection is still a no-op.
#[test]
fn create_is_still_idempotent_when_reads_work() {
    let (_dir, _store, kernel) = populated();
    let before = definition::load(&kernel, "t");
    engine_path::create(&kernel, "t").unwrap();
    let after = definition::load(&kernel, "t");
    assert_eq!(before.chunk_salt, after.chunk_salt, "a second create changed the salt");
}
