// audit.rs — the record that an erasure happened.
//
// Erasure usually has to be provable. A regulator, or the subject themselves,
// can ask whether a request was carried out, and "the data is gone" is not by
// itself an answer — absence looks the same as never having existed.
//
// # The tension, and how it is resolved
//
// A log that names erased subjects is a directory of erased people, retained
// after their data was destroyed. That is the opposite of what the erasure was
// for, and for a subject id that is itself personal data — an email address, a
// customer number — the log would retain exactly what was supposed to go.
//
// So entries record a **salted hash of the subject id**, not the id. That is
// enough to answer the question actually asked — *"was this subject erased?"*,
// where the asker already holds the id — and not enough to enumerate who was.
//
// The salt is per-store and lives with the log. Without it, a hash of a
// low-entropy id like an email is trivially reversed by guessing, so an
// unsalted log would be a directory of erased people wearing a thin disguise.
//
// # Append-only
//
// Entries are separate objects named by time and hash, never rewritten. An
// audit log that can be edited is not evidence, and one written as a single
// growing object would be rewritten on every entry — which is both a rewrite
// and a chance to lose earlier ones.

use pond_kernel::ObjectStore;
use sha2::{Digest, Sha256};
use std::io;

/// Where the log lives. A prefix of its own, so it can be given retention and
/// access rules separate from both the data and the keys.
const AUDIT_PREFIX: &str = "audit/erasures/";
const SALT_PATH: &str = "audit/salt";

/// One erasure, as recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErasureRecord {
    /// Salted hash of the subject id. Not the id.
    pub subject_digest: String,
    /// Milliseconds since the Unix epoch.
    pub at_ms: u64,
    /// Who asked, free-form — an operator, a ticket, a request id.
    pub requested_by: String,
    /// Whether a key was actually destroyed, as opposed to none being there.
    pub key_existed: bool,
}

/// The append-only erasure log.
pub struct AuditLog<S: ObjectStore> {
    store: S,
}

impl<S: ObjectStore> AuditLog<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Record an erasure.
    pub fn record(
        &self,
        subject: &str,
        requested_by: &str,
        key_existed: bool,
    ) -> io::Result<ErasureRecord> {
        let at_ms = pond_kernel::crdt::current_time_ms();
        let digest = self.digest(subject)?;
        let entry = ErasureRecord {
            subject_digest: digest,
            at_ms,
            requested_by: requested_by.to_string(),
            key_existed,
        };
        // Named by time then digest, so the listing is chronological and two
        // erasures of the same subject do not overwrite each other — a second
        // request is itself a fact worth keeping.
        let path = format!(
            "{}{:016x}-{}",
            AUDIT_PREFIX, entry.at_ms, entry.subject_digest
        );
        self.store.put_object(&path, &encode(&entry))?;
        Ok(entry)
    }

    /// Was this subject erased? Answers for a caller who already holds the id.
    pub fn was_erased(&self, subject: &str) -> io::Result<bool> {
        let digest = self.digest(subject)?;
        Ok(self
            .store
            .list_paths(AUDIT_PREFIX)?
            .iter()
            .any(|p| p.ends_with(&digest)))
    }

    /// Every entry, oldest first.
    ///
    /// Entries whose bytes will not decode are skipped rather than failing the
    /// listing: one corrupt entry must not make the rest of the log
    /// unreadable, since the rest is still evidence.
    pub fn entries(&self) -> io::Result<Vec<ErasureRecord>> {
        let mut paths = self.store.list_paths(AUDIT_PREFIX)?;
        paths.sort();
        Ok(paths
            .iter()
            .filter_map(|p| self.store.get_object(p))
            .filter_map(|b| decode(&b))
            .collect())
    }

    /// The per-store salt, created on first use.
    ///
    /// Without it a hash of a low-entropy id — an email, a customer number —
    /// is reversed by guessing, and the log becomes the directory of erased
    /// people it exists to avoid being.
    fn salt(&self) -> io::Result<[u8; 32]> {
        if let Some(bytes) = self.store.get_object(SALT_PATH) {
            if bytes.len() == 32 {
                let mut salt = [0u8; 32];
                salt.copy_from_slice(&bytes);
                return Ok(salt);
            }
        }
        let mut salt = [0u8; 32];
        getrandom::fill(&mut salt)
            .map_err(|_| io::Error::other("OS CSPRNG unavailable — cannot salt the audit log"))?;
        self.store.put_object(SALT_PATH, &salt)?;
        // Read back, so two concurrent creators agree on one salt rather than
        // each hashing under its own and producing digests that never match.
        match self.store.get_object(SALT_PATH) {
            Some(bytes) if bytes.len() == 32 => {
                let mut winner = [0u8; 32];
                winner.copy_from_slice(&bytes);
                Ok(winner)
            }
            _ => Ok(salt),
        }
    }

    fn digest(&self, subject: &str) -> io::Result<String> {
        let salt = self.salt()?;
        let mut h = Sha256::new();
        h.update(b"pond-erasure-audit-v1");
        h.update(salt);
        h.update(subject.as_bytes());
        Ok(hex::encode(&h.finalize()[..16]))
    }
}

fn encode(e: &ErasureRecord) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"PAUD");
    out.push(1);
    out.extend_from_slice(&e.at_ms.to_le_bytes());
    out.push(e.key_existed as u8);
    put_str(&mut out, &e.subject_digest);
    put_str(&mut out, &e.requested_by);
    out
}

fn decode(bytes: &[u8]) -> Option<ErasureRecord> {
    if bytes.len() < 14 || &bytes[..4] != b"PAUD" || bytes[4] != 1 {
        return None;
    }
    let at_ms = u64::from_le_bytes(bytes[5..13].try_into().ok()?);
    let key_existed = bytes[13] != 0;
    let mut pos = 14;
    let subject_digest = take_str(bytes, &mut pos)?;
    let requested_by = take_str(bytes, &mut pos)?;
    Some(ErasureRecord {
        subject_digest,
        at_ms,
        requested_by,
        key_existed,
    })
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn take_str(bytes: &[u8], pos: &mut usize) -> Option<String> {
    if *pos + 4 > bytes.len() {
        return None;
    }
    let len = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().ok()?) as usize;
    *pos += 4;
    if *pos + len > bytes.len() {
        return None;
    }
    let s = String::from_utf8(bytes[*pos..*pos + len].to_vec()).ok()?;
    *pos += len;
    Some(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pond_kernel::LocalFSObjectStore;

    fn log() -> (tempfile::TempDir, AuditLog<LocalFSObjectStore>) {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalFSObjectStore::new(dir.path()).unwrap();
        (dir, AuditLog::new(s))
    }

    #[test]
    fn an_erasure_is_recorded_and_can_be_confirmed() {
        let (_d, log) = log();
        log.record("alice@example.com", "ticket-42", true).unwrap();

        assert!(log.was_erased("alice@example.com").unwrap());
        assert!(!log.was_erased("bob@example.com").unwrap());

        let entries = log.entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].requested_by, "ticket-42");
        assert!(entries[0].key_existed);
    }

    /// The log must not be a directory of erased people.
    ///
    /// It has to answer "was this subject erased?" for someone who already
    /// holds the id, without disclosing the ids to someone who does not.
    #[test]
    fn the_log_does_not_contain_the_subject_id() {
        let (dir, log) = log();
        log.record("alice@example.com", "ticket-42", true).unwrap();

        let mut found = false;
        let mut stack = vec![dir.path().to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                    continue;
                }
                if p.to_string_lossy().contains("alice") {
                    found = true;
                }
                if let Ok(b) = std::fs::read(&p) {
                    if b.windows(b"alice@example.com".len())
                        .any(|w| w == b"alice@example.com")
                    {
                        found = true;
                    }
                }
            }
        }
        assert!(
            !found,
            "the subject id appears in the audit log — which retains, after \\
             erasure, exactly what the erasure was for"
        );
    }

    /// Two stores must not produce the same digest for the same id, or a log
    /// from one deployment would identify subjects in another.
    #[test]
    fn digests_are_salted_per_store() {
        let (_d1, a) = log();
        let (_d2, b) = log();
        a.record("alice", "x", true).unwrap();
        b.record("alice", "x", true).unwrap();
        assert_ne!(
            a.entries().unwrap()[0].subject_digest,
            b.entries().unwrap()[0].subject_digest
        );
    }

    /// A repeated request is itself a fact, so it must not overwrite the
    /// first.
    #[test]
    fn repeated_erasures_are_all_recorded() {
        let (_d, log) = log();
        log.record("alice", "first", true).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        log.record("alice", "second", false).unwrap();

        let entries = log.entries().unwrap();
        assert_eq!(entries.len(), 2, "both requests must be kept");
        assert!(entries[0].at_ms <= entries[1].at_ms, "oldest first");
        assert_eq!(entries[0].requested_by, "first");
        assert!(!entries[1].key_existed, "the second found nothing to destroy");
    }

    #[test]
    fn entries_round_trip_exactly() {
        let e = ErasureRecord {
            subject_digest: "abc123".into(),
            at_ms: 1_700_000_000_000,
            requested_by: "operator with spaces & symbols".into(),
            key_existed: false,
        };
        assert_eq!(decode(&encode(&e)), Some(e));
    }

    /// One corrupt entry must not hide the rest — the rest is still evidence.
    #[test]
    fn a_corrupt_entry_does_not_hide_the_others() {
        let (_d, log) = log();
        log.record("alice", "x", true).unwrap();
        log.record("bob", "y", true).unwrap();
        log.store
            .put_object(&format!("{}0000000000000000-bad", AUDIT_PREFIX), b"garbage")
            .unwrap();

        assert_eq!(log.entries().unwrap().len(), 2);
        assert!(decode(b"").is_none());
        assert!(decode(b"PAUD").is_none());
    }
}
