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
    /// Every key that can open a subject's data — current first, then the one
    /// a rotation displaced. Ordered so the common case costs one decryption
    /// attempt and only a row that raced a rotation pays for two.
    cache: std::collections::HashMap<String, Vec<SubjectKey>>,
    _kernel: std::marker::PhantomData<&'a ()>,
}

impl Keys<'_> {
    fn new(kernel: &PondKernel) -> Self {
        Self {
            store: KeyStore::new(kernel.keystore_handle()),
            cache: std::collections::HashMap::new(),
            _kernel: std::marker::PhantomData,
        }
    }

    /// The key to seal under, creating one if this subject is new.
    ///
    /// Always the current key. A rotation's displaced key can still *open*
    /// data, but nothing new is ever sealed under it.
    fn get_or_create(&mut self, subject: &str) -> Result<SubjectKey, String> {
        if let Some(keys) = self.cache.get(subject) {
            if let Some(current) = keys.first() {
                return Ok(current.clone());
            }
        }
        let key = self
            .store
            .get_or_create(&subject.to_string())
            .map_err(|e| format!("failed to get subject key: {}", e))?;
        self.cache
            .insert(subject.to_string(), vec![key.clone()]);
        Ok(key)
    }

    /// Every key that can open this subject's data. Empty means erased.
    fn opening_keys(&mut self, subject: &str) -> Vec<SubjectKey> {
        if let Some(cached) = self.cache.get(subject) {
            return cached.clone();
        }
        let keys = self
            .store
            .get_all(&subject.to_string())
            .unwrap_or_default();
        self.cache.insert(subject.to_string(), keys.clone());
        keys
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
    //
    // More than one key means a rotation is in flight: the current key opens
    // rows the rotation re-sealed, and the displaced key opens any write that
    // raced it. Trying both is what stops such a write being destroyed by an
    // operation meant to preserve it.
    let opening = keys.opening_keys(&subject);

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
        let opened = opening.iter().find_map(|k| {
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
    erase_subject_for(kernel, subject, "unspecified")
}

/// Erase a subject, recording who asked.
///
/// The audit entry is written **after** the key is destroyed, deliberately. If
/// the order were reversed and the destruction then failed, the log would
/// claim an erasure that did not happen — and a log that overstates is worse
/// than one that lags, because the second is discoverable and the first is
/// believed.
///
/// A failure to write the entry is reported but does not undo the erasure:
/// the data is already unreadable, and there is no putting it back.
pub fn erase_subject_for(
    kernel: &PondKernel,
    subject: &str,
    requested_by: &str,
) -> Result<bool, String> {
    let destroyed = KeyStore::new(kernel.keystore_handle())
        .erase(&subject.to_string())
        .map_err(|e| format!("failed to erase subject: {}", e))?;

    pond_crypto::AuditLog::new(kernel.keystore_handle())
        .record(subject, requested_by, destroyed)
        .map_err(|e| {
            format!(
                "subject '{}' was erased, but recording it failed: {}",
                subject, e
            )
        })?;
    Ok(destroyed)
}

/// Was this subject erased? For a caller who already holds the id.
pub fn was_erased(kernel: &PondKernel, subject: &str) -> Result<bool, String> {
    pond_crypto::AuditLog::new(kernel.keystore_handle())
        .was_erased(subject)
        .map_err(|e| format!("failed to read the erasure log: {}", e))
}

/// The erasure log, oldest first.
pub fn erasure_log(kernel: &PondKernel) -> Result<Vec<pond_crypto::ErasureRecord>, String> {
    pond_crypto::AuditLog::new(kernel.keystore_handle())
        .entries()
        .map_err(|e| format!("failed to read the erasure log: {}", e))
}

/// Re-seal one subject's rows under `fresh`, leaving every other row alone.
///
/// Returns the rewritten records and whether the subject had a key at all. A
/// subject with no key was erased, or never seen; minting one for them would
/// quietly restore the ability to store data for somebody who asked to be
/// forgotten, so the caller is told rather than surprised.
///
/// This does not touch the keystore. Installing the new key is a separate step
/// so the caller controls the order — the re-sealed rows must be durable
/// before the key that opens them replaces the one that opens the old rows.
#[allow(clippy::too_many_arguments)]
pub fn reseal_for_rotation(
    kernel: &PondKernel,
    def: &Definition,
    collection: &str,
    subject: &str,
    subject_column: &str,
    records: Vec<Record>,
    fresh: &SubjectKey,
    rotation_writer: u64,
) -> Result<(Vec<Record>, bool), String> {
    if def.subject_column.as_deref() != Some(subject_column) {
        return Err(format!(
            "collection '{}' does not seal rows by '{}'",
            collection, subject_column
        ));
    }

    let store = KeyStore::new(kernel.keystore_handle());
    let Some(old) = store
        .get(&subject.to_string())
        .map_err(|e| format!("failed to read the subject key: {}", e))?
    else {
        return Ok((records, false));
    };

    let mut keys = Keys::new(kernel);
    keys.cache.insert(subject.to_string(), vec![old]);

    let belongs = |r: &Record| matches!(r.get(subject_column), Some(Value::Str(s)) if s == subject);

    // The re-sealed fields need versions newer than the ones they replace.
    //
    // A write goes through the engine's per-field merge, and a field whose
    // version is unchanged does not win it — so the old ciphertext would
    // survive, sealed under a key that no longer exists, and the value would
    // be silently unreadable. That is what happened before this: rotation
    // returned success and the data came back empty.
    let now = pond_kernel::crdt::current_time_ms();
    let out = records
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            if !belongs(&r) {
                return r;
            }
            let opened = open_one(&mut keys, subject_column, collection, r);
            let version = pond_record::Version::new(now, i as u64, rotation_writer);
            map_versioned_fields(opened, subject_column, version, |field, value| {
                let plaintext = pond_record::encode_value(&value);
                Value::Bytes(pond_crypto::seal(
                    fresh,
                    &context(collection, field),
                    &plaintext,
                ))
            })
        })
        .collect();
    Ok((out, true))
}

/// Install a rotated key, after its rows have been re-sealed and published.
///
/// Separate from [`rotate_subject_key`] so the caller controls the order: the
/// new rows must be durable before the key that opens them replaces the one
/// that opens the old rows.
pub fn install_rotated_key(
    kernel: &PondKernel,
    subject: &str,
    key: &SubjectKey,
) -> Result<(), String> {
    KeyStore::new(kernel.keystore_handle())
        .replace(&subject.to_string(), key)
        .map_err(|e| format!("failed to install the rotated key: {}", e))
}

/// Retire the key a rotation displaced.
///
/// Until this is called the old key still opens data — which is the point
/// during the window when a write might have raced the rotation, and a
/// liability afterwards, since rotation exists to stop an old key working.
///
/// Call it once no write that started before the rotation can still be in
/// flight. There is no way for storage to know when that is, so it is the
/// caller's judgement rather than a timer.
pub fn finish_rotation(kernel: &PondKernel, subject: &str) -> Result<bool, String> {
    KeyStore::new(kernel.keystore_handle())
        .finish_rotation(&subject.to_string())
        .map_err(|e| format!("failed to retire the previous key: {}", e))
}

/// Every subject with a key. What is erasable, for audit.
pub fn subjects(kernel: &PondKernel) -> Result<Vec<String>, String> {
    KeyStore::new(kernel.keystore_handle())
        .subjects()
        .map_err(|e| format!("failed to list subjects: {}", e))
}

/// As [`map_fields`], but stamping the transformed fields with a new version.
///
/// Rotation needs this: an unchanged version does not win the per-field merge,
/// so the old ciphertext would survive under a key that no longer exists.
fn map_versioned_fields<F>(
    record: Record,
    subject_column: &str,
    version: pond_record::Version,
    mut f: F,
) -> Record
where
    F: FnMut(&str, Value) -> Value,
{
    let mut out = Record::new();
    if let Some(tomb) = record.deleted {
        out.delete(tomb);
    }
    for (name, field) in record.fields {
        let sealed = is_sealed_column(&name, subject_column);
        let (value, version) = if sealed {
            (f(&name, field.value), version)
        } else {
            (field.value, field.version)
        };
        out.fields
            .insert(name, pond_record::Field { value, version });
    }
    out
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
