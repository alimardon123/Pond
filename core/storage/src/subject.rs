// subject.rs — sealing and opening a row's fields under its subject's key.
//
// This is where `core/crypto` meets the data path. The mechanism is described
// in `docs/ERASURE.md`; what lives here is the policy that decides *what* gets
// sealed, and the behaviour when a key is gone.
//
// # Default-deny
//
// When a collection names a subject column, **every other column is sealed**.
// The alternative — listing which columns hold personal data — has a failure
// mode this does not: a field added later, by a lens that never heard of the
// policy, would silently be stored in the clear. For a protection mechanism
// the safe default is to protect everything and let the exceptions be
// explicit, not the other way round.
//
// The subject column itself stays in the clear. It is an identifier rather
// than personal detail, and sealing it would leave nothing able to tell which
// rows belong to whom — including the erasure.
//
// # When the key is gone
//
// An erased subject's fields read as **absent**, not as an error. One erased
// subject must not make a collection unreadable: a scan over a million rows
// that fails because one of them was erased is a denial of service handed to
// anyone who exercises their right to deletion.
//
// Absent is also the honest answer — the value no longer exists. What it
// cannot distinguish is "erased" from "never set", and the record of which is
// which is the keystore: a subject with no key is an erased subject.

use pond_crypto::{KeyStore, SubjectKey};
use pond_kernel::PondKernel;
use pond_record::{Record, Value};

use crate::definition::Definition;

/// Columns that are storage bookkeeping and never hold subject data.
const INTERNAL: [&str; 3] = ["_rowid", "_version", "_deleted"];

/// Should this column be sealed, given the subject column?
fn is_sealed_column(name: &str, subject_column: &str) -> bool {
    name != subject_column && !INTERNAL.contains(&name)
}

/// What a field is bound to, so a ciphertext cannot be moved elsewhere and
/// still open.
fn context(collection: &str, field: &str) -> Vec<u8> {
    format!("{}\u{1f}{}", collection, field).into_bytes()
}

/// Subject keys held for the duration of one operation.
///
/// Rows in a batch overwhelmingly share a subject — that is what a batch *is*
/// for a per-subject collection — so fetching a key per row turns one round
/// trip into thousands against an object store, which would defeat the
/// batching the whole design rests on. Measured before this existed: 201
/// keystore reads to seal 200 rows.
///
/// Scoped to one operation rather than cached globally, deliberately. A key
/// held across operations is a key that outlives an erasure performed by
/// somebody else, and the window in which an erased subject still decrypts
/// should be as short as the code can make it.
struct Keys<'a> {
    store: KeyStore<std::sync::Arc<dyn pond_kernel::ObjectStore>>,
    cache: std::collections::HashMap<String, Option<SubjectKey>>,
    _kernel: std::marker::PhantomData<&'a ()>,
}

impl Keys<'_> {
    fn new(kernel: &PondKernel) -> Self {
        Self {
            store: KeyStore::new(kernel.store_handle()),
            cache: std::collections::HashMap::new(),
            _kernel: std::marker::PhantomData,
        }
    }

    /// The subject's key, creating one if this subject is new.
    fn get_or_create(&mut self, subject: &str) -> Result<SubjectKey, String> {
        if let Some(Some(key)) = self.cache.get(subject) {
            return Ok(key.clone());
        }
        let key = self
            .store
            .get_or_create(&subject.to_string())
            .map_err(|e| format!("failed to get subject key: {}", e))?;
        self.cache.insert(subject.to_string(), Some(key.clone()));
        Ok(key)
    }

    /// The subject's key, or `None` if they have been erased.
    fn get(&mut self, subject: &str) -> Option<SubjectKey> {
        if let Some(cached) = self.cache.get(subject) {
            return cached.clone();
        }
        let key = self.store.get(&subject.to_string()).ok().flatten();
        self.cache.insert(subject.to_string(), key.clone());
        key
    }
}

/// Seal every record's fields under its subject's key.
///
/// A row whose subject column is missing or not a string is reported rather
/// than sealed under some fallback subject. Guessing would mean the row could
/// never be erased with the subject it actually belongs to — a silent,
/// permanent failure of the thing this exists for.
pub fn seal_records(
    kernel: &PondKernel,
    def: &Definition,
    collection: &str,
    records: Vec<Record>,
) -> Result<Vec<Record>, String> {
    let Some(subject_column) = def.subject_column.as_deref() else {
        return Ok(records);
    };
    let mut keys = Keys::new(kernel);
    let mut out = Vec::with_capacity(records.len());

    for record in records {
        let Some(Value::Str(subject)) = record.get(subject_column).cloned() else {
            return Err(format!(
                "collection '{}' seals rows by column '{}', but this row has no \
                 string value there — refusing to store it unsealed",
                collection, subject_column
            ));
        };
        let key = keys.get_or_create(&subject)?;
        out.push(map_fields(record, subject_column, |field, value| {
            let plaintext = pond_record::encode_value(&value);
            Value::Bytes(pond_crypto::seal(
                &key,
                &context(collection, field),
                &plaintext,
            ))
        }));
    }
    Ok(out)
}

/// Open every record's sealed fields, fetching each subject's key once.
pub fn open_records(
    kernel: &PondKernel,
    def: &Definition,
    collection: &str,
    records: Vec<Record>,
) -> Vec<Record> {
    let Some(subject_column) = def.subject_column.as_deref() else {
        return records;
    };
    let mut keys = Keys::new(kernel);
    records
        .into_iter()
        .map(|record| open_one(&mut keys, subject_column, collection, record))
        .collect()
}

/// Open a record's sealed fields.
///
/// Fields whose key is gone are dropped, which is what makes an erased subject
/// read as absent rather than as an error.
fn open_one(
    keys: &mut Keys<'_>,
    subject_column: &str,
    collection: &str,
    record: Record,
) -> Record {
    let Some(Value::Str(subject)) = record.get(subject_column).cloned() else {
        return record;
    };

    // No key means the subject was erased — or never existed. Either way there
    // is nothing to open, and every sealed field drops out.
    let key: Option<SubjectKey> = keys.get(&subject);

    let mut out = Record::new();
    if let Some(tomb) = record.deleted {
        out.delete(tomb);
    }
    for (name, field) in record.fields {
        if !is_sealed_column(&name, subject_column) {
            out.fields.insert(name, field);
            continue;
        }
        let Value::Bytes(sealed) = &field.value else {
            // Written before the policy existed, or by a path that did not
            // seal. Pass it through rather than dropping data.
            out.fields.insert(name, field);
            continue;
        };
        let opened = key.as_ref().and_then(|k| {
            pond_crypto::open(k, &context(collection, &name), sealed)
                .ok()
                .and_then(|bytes| pond_record::decode_value(&bytes))
        });
        if let Some(value) = opened {
            out.fields.insert(
                name,
                pond_record::Field {
                    value,
                    version: field.version,
                },
            );
        }
        // Otherwise the field is absent, which is the erased case.
    }
    out
}

/// Erase a subject: destroy their key.
///
/// Returns whether there was a key to destroy. Erasing twice is not an error —
/// a deletion request that arrives again must not fail.
pub fn erase_subject(kernel: &PondKernel, subject: &str) -> Result<bool, String> {
    KeyStore::new(kernel.store_handle())
        .erase(&subject.to_string())
        .map_err(|e| format!("failed to erase subject: {}", e))
}

/// Every subject with a key. What is erasable, for audit.
pub fn subjects(kernel: &PondKernel) -> Result<Vec<String>, String> {
    KeyStore::new(kernel.store_handle())
        .subjects()
        .map_err(|e| format!("failed to list subjects: {}", e))
}

/// Rebuild a record, transforming the fields that should be sealed.
fn map_fields<F>(record: Record, subject_column: &str, mut f: F) -> Record
where
    F: FnMut(&str, Value) -> Value,
{
    let mut out = Record::new();
    if let Some(tomb) = record.deleted {
        out.delete(tomb);
    }
    for (name, field) in record.fields {
        let value = if is_sealed_column(&name, subject_column) {
            f(&name, field.value)
        } else {
            field.value
        };
        out.fields.insert(
            name,
            pond_record::Field {
                value,
                version: field.version,
            },
        );
    }
    out
}
