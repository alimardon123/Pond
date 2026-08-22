// keystore.rs — the one mutable thing, and the only thing erasure touches.
//
// Everything else in this system is immutable and content-addressed, which is
// what makes it converge and cache without invalidation. The keystore is
// deliberately the opposite: small, mutable, and genuinely deletable, because
// something has to be, and it is far easier to guarantee real deletion for a
// few kilobytes of keys than for a petabyte of history.
//
// Keys are stored as named objects rather than blobs. A blob is addressed by
// its content, so writing a key as a blob would name it after itself — and a
// content-addressed object cannot be deleted in the sense that matters here,
// because anything that has seen the hash can ask for it again.

use pond_kernel::ObjectStore;
use std::io;

use crate::SubjectKey;

/// Who the data belongs to — a user, a tenant, a customer.
///
/// The unit of erasure. A subject is whatever the deployment must be able to
/// erase independently, which is a policy question rather than a storage one.
pub type SubjectId = String;

/// Where a subject's key lives.
///
/// Under a prefix of its own so the keystore can be given different
/// durability, backup, and retention treatment from the data — which it must
/// be, since a backup of the keystore that outlives an erasure undoes it.
fn key_path(subject: &SubjectId) -> String {
    format!("keys/subject-{}", hex::encode(subject.as_bytes()))
}

/// Where the key a rotation just replaced lives, until the rotation is
/// finished.
///
/// Rotation is not atomic against concurrent writes: a write landing between
/// the rotation's scan and its key install seals under the *old* key, and
/// would then be unreadable — lost, not merely stale. Keeping the old key in a
/// second slot lets a reader try both, so such a write survives instead of
/// being destroyed by an operation that was supposed to preserve it.
///
/// It is retired by [`KeyStore::finish_rotation`], and destroyed by
/// [`KeyStore::erase`] like any other copy of a subject's key — an erasure
/// that left this behind would not be an erasure.
fn previous_key_path(subject: &SubjectId) -> String {
    format!("keys/previous-{}", hex::encode(subject.as_bytes()))
}

/// Subject keys, held over any object store.
pub struct KeyStore<S: ObjectStore> {
    store: S,
}

impl<S: ObjectStore> KeyStore<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Fetch a subject's key, creating one if this is the first time.
    ///
    /// Creation is idempotent under a race in the way that matters: two
    /// callers creating the same subject concurrently can each write a key,
    /// and the loser's data would then be unreadable. So the write is followed
    /// by a read-back, and whichever key is durably present is the one used.
    /// That costs one extra round trip on first use only.
    pub fn get_or_create(&self, subject: &SubjectId) -> io::Result<SubjectKey> {
        if let Some(existing) = self.get(subject)? {
            return Ok(existing);
        }
        let fresh = SubjectKey::generate();
        self.store
            .put_object(&key_path(subject), fresh.as_bytes())?;
        // Read back rather than trusting the write, so a concurrent creator's
        // key wins consistently for both of them instead of each using its
        // own.
        Ok(self.get(subject)?.unwrap_or(fresh))
    }

    /// Fetch a subject's key. `None` means never created, or erased.
    ///
    /// The two are deliberately indistinguishable here: a caller that could
    /// tell them apart would learn whether a subject had ever existed, which
    /// is itself information about that subject.
    pub fn get(&self, subject: &SubjectId) -> io::Result<Option<SubjectKey>> {
        let Some(bytes) = self.store.get_object(&key_path(subject)) else {
            return Ok(None);
        };
        if bytes.len() != 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("key for subject is {} bytes, expected 32", bytes.len()),
            ));
        }
        let mut buf = [0u8; 32];
        buf.copy_from_slice(&bytes);
        Ok(Some(SubjectKey::from_bytes(buf)))
    }

    /// Replace a subject's key, keeping the old one readable.
    ///
    /// Used by rotation, after the subject's rows have been re-sealed under
    /// the new key and made durable. Doing it earlier would leave rows that
    /// nothing can open — an erasure nobody asked for.
    ///
    /// The displaced key is kept in the previous slot rather than discarded,
    /// so a write that raced the rotation and sealed under it is still
    /// readable. Call [`finish_rotation`](Self::finish_rotation) once those
    /// writes cannot be outstanding any more.
    pub fn replace(&self, subject: &SubjectId, key: &SubjectKey) -> io::Result<()> {
        if let Some(old) = self.get(subject)? {
            self.store
                .put_object(&previous_key_path(subject), old.as_bytes())?;
        }
        self.store.put_object(&key_path(subject), key.as_bytes())
    }

    /// Every key that can currently open this subject's data: the current one
    /// first, then the one a rotation displaced, if any.
    ///
    /// Ordered so the common case costs one decryption attempt and only a row
    /// that raced a rotation pays for two.
    pub fn get_all(&self, subject: &SubjectId) -> io::Result<Vec<SubjectKey>> {
        let mut keys = Vec::with_capacity(2);
        if let Some(current) = self.get(subject)? {
            keys.push(current);
        }
        if let Some(bytes) = self.store.get_object(&previous_key_path(subject)) {
            if bytes.len() == 32 {
                let mut buf = [0u8; 32];
                buf.copy_from_slice(&bytes);
                keys.push(SubjectKey::from_bytes(buf));
            }
        }
        Ok(keys)
    }

    /// Retire the key a rotation displaced.
    ///
    /// Until this is called, the old key still opens data — which is the point
    /// during the window when a racing write might have used it, and a
    /// liability afterwards, since rotation exists to stop an old key from
    /// working. Call it once no write that started before the rotation can
    /// still be in flight.
    pub fn finish_rotation(&self, subject: &SubjectId) -> io::Result<bool> {
        self.store.delete_path(&previous_key_path(subject))
    }

    /// Erase a subject: destroy their key.
    ///
    /// Returns whether a key was there to destroy. Erasing an already-erased
    /// subject is not an error — a deletion request that arrives twice must
    /// not fail the second time.
    ///
    /// **This is the entire erasure operation, and its limits are worth
    /// stating.** It makes the subject's ciphertext unreadable everywhere at
    /// once: in every branch, every historical root, and every replica that
    /// already copied it, without finding or rewriting any of them. What it
    /// cannot do is reach a copy of the *key* that escaped — a backup, a
    /// snapshot, a replica of the keystore. Erasure is exactly as complete as
    /// the destruction of the last copy of this key.
    pub fn erase(&self, subject: &SubjectId) -> io::Result<bool> {
        // Both slots. An erasure that left a rotation's displaced key behind
        // would leave the subject's older rows readable — which is not an
        // erasure, however it is reported.
        let previous = self.store.delete_path(&previous_key_path(subject))?;
        let current = self.store.delete_path(&key_path(subject))?;
        Ok(current || previous)
    }

    /// Every subject with a key, for auditing what is erasable.
    pub fn subjects(&self) -> io::Result<Vec<SubjectId>> {
        Ok(self
            .store
            .list_paths("keys/")?
            .into_iter()
            .filter_map(|p| {
                // Only current keys. A previous slot is the same subject, and
                // listing it twice would overstate what is erasable.
                let encoded = p.strip_prefix("keys/subject-")?;
                let bytes = hex::decode(encoded).ok()?;
                String::from_utf8(bytes).ok()
            })
            .collect())
    }

    pub fn store(&self) -> &S {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pond_kernel::LocalFSObjectStore;

    fn store() -> (tempfile::TempDir, KeyStore<LocalFSObjectStore>) {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalFSObjectStore::new(dir.path()).unwrap();
        (dir, KeyStore::new(s))
    }

    #[test]
    fn a_key_is_created_once_and_then_returned() {
        let (_d, ks) = store();
        let subject = "alice".to_string();

        let first = ks.get_or_create(&subject).unwrap();
        let second = ks.get_or_create(&subject).unwrap();
        assert_eq!(
            first.as_bytes(),
            second.as_bytes(),
            "the same subject must keep the same key, or their older data \\
             becomes unreadable"
        );
    }

    #[test]
    fn subjects_get_different_keys() {
        let (_d, ks) = store();
        let a = ks.get_or_create(&"alice".to_string()).unwrap();
        let b = ks.get_or_create(&"bob".to_string()).unwrap();
        assert_ne!(a.as_bytes(), b.as_bytes());
    }

    /// The erasure contract, end to end.
    #[test]
    fn erasing_a_subject_makes_their_data_unreadable() {
        let (_d, ks) = store();
        let subject = "alice".to_string();
        let key = ks.get_or_create(&subject).unwrap();
        let sealed = crate::seal(&key, b"users/name", b"personal data");

        // Readable while the key exists.
        assert_eq!(
            crate::open(&key, b"users/name", &sealed).unwrap(),
            b"personal data"
        );

        assert!(ks.erase(&subject).unwrap());
        assert!(ks.get(&subject).unwrap().is_none());

        // The ciphertext is still there — it must be, since it cannot be found
        // and rewritten — and it is now noise. A fresh key for the same
        // subject does not open it either.
        let reborn = ks.get_or_create(&subject).unwrap();
        assert_ne!(reborn.as_bytes(), key.as_bytes());
        assert!(
            crate::open(&reborn, b"users/name", &sealed).is_err(),
            "data from before an erasure must not be readable after it"
        );
    }

    /// A deletion request that arrives twice must not fail.
    #[test]
    fn erasing_twice_is_not_an_error() {
        let (_d, ks) = store();
        let subject = "alice".to_string();
        ks.get_or_create(&subject).unwrap();
        assert!(ks.erase(&subject).unwrap());
        assert!(!ks.erase(&subject).unwrap(), "second erase reports nothing to do");
        assert!(!ks.erase(&"never-existed".to_string()).unwrap());
    }

    #[test]
    fn subjects_can_be_listed_for_audit() {
        let (_d, ks) = store();
        for s in ["alice", "bob", "carol with spaces"] {
            ks.get_or_create(&s.to_string()).unwrap();
        }
        let mut found = ks.subjects().unwrap();
        found.sort();
        assert_eq!(found, vec!["alice", "bob", "carol with spaces"]);

        ks.erase(&"bob".to_string()).unwrap();
        let found = ks.subjects().unwrap();
        assert!(!found.contains(&"bob".to_string()));
    }

    /// Subject ids that would break a path must not.
    #[test]
    fn awkward_subject_ids_are_handled() {
        let (_d, ks) = store();
        for weird in ["../escape", "with/slash", "", "unicode-café"] {
            let subject = weird.to_string();
            let key = ks.get_or_create(&subject).unwrap();
            let fetched = ks.get(&subject).unwrap().expect("must round-trip");
            assert_eq!(key.as_bytes(), fetched.as_bytes(), "failed for {:?}", weird);
        }
    }

    /// A truncated key file is an error, not a silently wrong key.
    #[test]
    fn a_corrupt_key_is_reported() {
        let (_d, ks) = store();
        let subject = "alice".to_string();
        ks.get_or_create(&subject).unwrap();
        ks.store()
            .put_object(&super::key_path(&subject), b"too short")
            .unwrap();
        assert!(ks.get(&subject).is_err());
    }
}
