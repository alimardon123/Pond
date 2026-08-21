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

/// Seal a record's fields under its subject's key.
///
/// A row whose subject column is missing or not a string is left in the clear
/// and reported, rather than sealed under some fallback subject. Guessing here
/// would mean the row could never be erased with the subject it actually
/// belongs to — a silent, permanent failure of the thing this exists for.
pub fn seal_record(
    kernel: &PondKernel,
    def: &Definition,
    collection: &str,
    record: Record,
) -> Result<Record, String> {
    let Some(subject_column) = def.subject_column.as_deref() else {
        return Ok(record);
    };
    let Some(Value::Str(subject)) = record.get(subject_column).cloned() else {
        return Err(format!(
            "collection '{}' seals rows by column '{}', but this row has no \
             string value there — refusing to store it unsealed",
            collection, subject_column
        ));
    };

    let store = KeyStore::new(kernel.store_handle());
    let key = store
        .get_or_create(&subject)
        .map_err(|e| format!("failed to get subject key: {}", e))?;

    Ok(map_fields(record, subject_column, |field, value| {
        let plaintext = pond_record::encode_value(&value);
        Value::Bytes(pond_crypto::seal(
            &key,
            &context(collection, field),
            &plaintext,
        ))
    }))
}

/// Open a record's sealed fields.
///
/// Fields whose key is gone are dropped, which is what makes an erased subject
/// read as absent rather than as an error.
pub fn open_record(
    kernel: &PondKernel,
    def: &Definition,
    collection: &str,
    record: Record,
) -> Record {
    let Some(subject_column) = def.subject_column.as_deref() else {
        return record;
    };
    let Some(Value::Str(subject)) = record.get(subject_column).cloned() else {
        return record;
    };

    let store = KeyStore::new(kernel.store_handle());
    // No key means the subject was erased — or never existed. Either way there
    // is nothing to open, and every sealed field drops out.
    let key: Option<SubjectKey> = store.get(&subject).ok().flatten();

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
