// pond_crypto — erasure for storage that cannot delete.
//
// # The problem this exists for
//
// Everything below this layer is immutable and content-addressed, which is
// what buys convergence, structural sharing, and a cache that cannot go stale.
// It also means there is no mechanism for "delete this person's data":
//
//   * a blob's name is its content, so it is referenced by every tree that
//     ever contained it, including branches and history;
//   * spilled values deduplicate, so one blob may be referenced by rows
//     belonging to different subjects — deleting a row does not free it;
//   * a reader that has an old root still resolves every node under it.
//
// Every system in this lineage discovers this late and pays for it. Datomic's
// excision is irrevocable, asynchronous, capped at a few thousand datoms, and
// was absent from Datomic Cloud entirely. lakeFS's own documentation says it
// "appears to be impossible to really delete data". Nessie ships garbage
// collection as a separate product with its own database. The retrofit is
// always expensive, which is the argument for designing it in.
//
// # The mechanism
//
// Crypto-shredding: encrypt each subject's data under a key that belongs to
// that subject alone, and keep the keys somewhere mutable. Erasing a subject
// destroys one key. The ciphertext stays — it must, since it cannot be found
// and rewritten — but it is noise, and it is noise everywhere at once: in
// every branch, every historical root, and every replica that already copied
// it.
//
// Structural garbage collection handles *cost*, by reclaiming blobs nothing
// references any more. It does not handle *law*, because it cannot reach a
// blob that history still references. These are different problems and only
// one of them has a deadline attached.
//
// # Why the encryption is deterministic
//
// Encrypting normally would break the layer underneath. A random nonce means
// the same plaintext produces different ciphertext each time, so:
//
//   * two writers with identical data compute different hashes and stop
//     converging;
//   * a rewrite of an unchanged value produces a new blob, so structural
//     sharing collapses and every version stores everything again.
//
// So the nonce is derived from the key and the plaintext — a synthetic IV.
// Identical input under one subject's key always yields identical ciphertext,
// which keeps hashes stable and dedup working, and the nonce repeats only when
// the message repeats, which is the one case where reuse is harmless because
// the ciphertext is the same message.
//
// # What this costs, stated plainly
//
//   * **No cross-subject dedup.** Two subjects storing identical bytes get
//     different ciphertext, by design — sharing a blob between subjects would
//     mean one subject's erasure could not destroy it.
//   * **Deterministic encryption confirms guesses.** Someone who can probe the
//     store and already knows both the subject key and a candidate plaintext
//     can confirm the value exists. This is the same existence oracle content
//     addressing already has, narrowed to holders of the key.
//   * **Erasure is only as good as key destruction.** If the keystore is
//     backed up, snapshotted, or replicated somewhere that outlives the
//     delete, the data is not erased. The keystore is small precisely so that
//     it can be held somewhere with real deletion.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use sha2::{Digest, Sha256};

pub mod keystore;
pub use keystore::{KeyStore, SubjectId};

/// A subject's data-encryption key.
///
/// Destroying every copy of this is what erasure means.
#[derive(Clone, PartialEq, Eq)]
pub struct SubjectKey([u8; 32]);

impl SubjectKey {
    /// Draw a fresh key from the OS CSPRNG.
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).expect("OS CSPRNG unavailable — cannot create a subject key");
        Self(bytes)
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Deliberately opaque: a key that prints itself ends up in a log, and a key
/// in a log is a subject who cannot be erased.
impl std::fmt::Debug for SubjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SubjectKey(<redacted>)")
    }
}

/// Errors from sealing or opening.
#[derive(Debug, PartialEq, Eq)]
pub enum CryptoError {
    /// The ciphertext did not authenticate. Either it was tampered with, or
    /// the key is wrong — including the case that matters: the subject was
    /// erased and this is somebody else's key.
    NotAuthentic,
    /// Too short to contain a nonce and a tag.
    Malformed,
}

impl std::fmt::Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CryptoError::NotAuthentic => f.write_str(
                "ciphertext failed authentication — wrong key, erased subject, or tampering",
            ),
            CryptoError::Malformed => f.write_str("ciphertext is too short to be valid"),
        }
    }
}

impl std::error::Error for CryptoError {}

/// Magic prefix, so sealed bytes are recognisable and cannot be mistaken for
/// plaintext by a reader that does not know about encryption.
const MAGIC: &[u8; 4] = b"PSEA";
const VERSION: u8 = 1;
const NONCE_LEN: usize = 12;
const HEADER: usize = 4 + 1 + NONCE_LEN;

/// Encrypt `plaintext` under `key`, deterministically.
///
/// The same key and plaintext always produce the same bytes, which is what
/// lets the result be content-addressed like anything else. `context` is
/// authenticated but not encrypted — bind the collection or field name here so
/// a ciphertext cannot be moved from one place to another and still open.
pub fn seal(key: &SubjectKey, context: &[u8], plaintext: &[u8]) -> Vec<u8> {
    let nonce_bytes = derive_nonce(key, context, plaintext);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key.0));

    // The header is authenticated along with the caller's context, so a
    // version or nonce swapped in transit fails to open rather than decrypting
    // to something else.
    let mut aad = Vec::with_capacity(HEADER + context.len());
    aad.extend_from_slice(MAGIC);
    aad.push(VERSION);
    aad.extend_from_slice(&nonce_bytes);
    aad.extend_from_slice(context);

    let ciphertext = cipher
        .encrypt(nonce, Payload { msg: plaintext, aad: &aad })
        .expect("ChaCha20-Poly1305 encryption cannot fail for a valid key and nonce");

    let mut out = Vec::with_capacity(HEADER + ciphertext.len());
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    out
}

/// Decrypt bytes produced by [`seal`].
///
/// Fails rather than returning garbage when the key is wrong — which is the
/// behaviour erasure depends on. After a subject's key is destroyed, their
/// data does not decode to plausible nonsense; it does not decode.
pub fn open(key: &SubjectKey, context: &[u8], sealed: &[u8]) -> Result<Vec<u8>, CryptoError> {
    if sealed.len() < HEADER || &sealed[..4] != MAGIC || sealed[4] != VERSION {
        return Err(CryptoError::Malformed);
    }
    let nonce_bytes = &sealed[5..5 + NONCE_LEN];
    let nonce = Nonce::from_slice(nonce_bytes);
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key.0));

    let mut aad = Vec::with_capacity(HEADER + context.len());
    aad.extend_from_slice(&sealed[..HEADER]);
    aad.extend_from_slice(context);

    cipher
        .decrypt(
            nonce,
            Payload {
                msg: &sealed[HEADER..],
                aad: &aad,
            },
        )
        .map_err(|_| CryptoError::NotAuthentic)
}

/// Are these bytes sealed?
pub fn is_sealed(bytes: &[u8]) -> bool {
    bytes.len() >= HEADER && &bytes[..4] == MAGIC && bytes[4] == VERSION
}

/// A nonce that depends only on the key, the context and the message.
///
/// This is the synthetic-IV construction. Using a random nonce would make
/// encryption non-deterministic, and non-deterministic bytes cannot be
/// content-addressed: two writers with the same data would compute different
/// hashes and stop converging, and rewriting an unchanged value would produce
/// a new blob instead of sharing the old one.
///
/// Deriving it from the key as well as the message means two subjects storing
/// identical bytes get different nonces and different ciphertext, so no blob
/// is ever shared across subjects — which is what makes one subject's erasure
/// complete.
fn derive_nonce(key: &SubjectKey, context: &[u8], plaintext: &[u8]) -> [u8; NONCE_LEN] {
    let mut h = Sha256::new();
    h.update(b"pond-siv-v1");
    h.update(key.as_bytes());
    h.update((context.len() as u64).to_le_bytes());
    h.update(context);
    h.update(plaintext);
    let digest = h.finalize();
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&digest[..NONCE_LEN]);
    nonce
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SubjectKey {
        SubjectKey::from_bytes([7u8; 32])
    }

    #[test]
    fn round_trip() {
        let k = key();
        let sealed = seal(&k, b"users/name", b"ada lovelace");
        assert!(is_sealed(&sealed));
        assert_eq!(open(&k, b"users/name", &sealed).unwrap(), b"ada lovelace");
    }

    /// The property the storage layer depends on: identical input yields
    /// identical bytes, so the result can be content-addressed and shared.
    #[test]
    fn sealing_is_deterministic() {
        let k = key();
        let a = seal(&k, b"ctx", b"the same message");
        let b = seal(&k, b"ctx", b"the same message");
        assert_eq!(a, b, "non-deterministic output would break content addressing");
    }

    /// Two subjects storing identical bytes must not share a blob, or one
    /// subject's erasure would leave the other's copy readable — and the
    /// erased subject's data recoverable through it.
    #[test]
    fn different_subjects_produce_different_ciphertext() {
        let a = SubjectKey::from_bytes([1u8; 32]);
        let b = SubjectKey::from_bytes([2u8; 32]);
        let msg = b"identical plaintext";
        assert_ne!(seal(&a, b"ctx", msg), seal(&b, b"ctx", msg));
    }

    /// Destroying the key destroys the data. This is the whole mechanism.
    #[test]
    fn the_wrong_key_does_not_open_it() {
        let sealed = seal(&key(), b"ctx", b"personal data");
        let other = SubjectKey::from_bytes([9u8; 32]);
        assert_eq!(
            open(&other, b"ctx", &sealed),
            Err(CryptoError::NotAuthentic),
            "an erased subject's data must not decode at all"
        );
    }

    /// A ciphertext must not be movable from one place to another.
    #[test]
    fn context_is_bound_to_the_ciphertext() {
        let k = key();
        let sealed = seal(&k, b"users/salary", b"100000");
        assert_eq!(
            open(&k, b"users/nickname", &sealed),
            Err(CryptoError::NotAuthentic),
            "moving a value to another field must not decrypt"
        );
    }

    /// Tampering is detected rather than decrypted.
    #[test]
    fn tampering_is_detected() {
        let k = key();
        let mut sealed = seal(&k, b"ctx", b"important");

        // Flip a bit in the body.
        let last = sealed.len() - 1;
        sealed[last] ^= 1;
        assert_eq!(open(&k, b"ctx", &sealed), Err(CryptoError::NotAuthentic));

        // And in the nonce, which is authenticated as associated data.
        let mut sealed = seal(&k, b"ctx", b"important");
        sealed[6] ^= 1;
        assert_eq!(open(&k, b"ctx", &sealed), Err(CryptoError::NotAuthentic));
    }

    #[test]
    fn malformed_input_is_rejected_without_panicking() {
        let k = key();
        assert_eq!(open(&k, b"ctx", b""), Err(CryptoError::Malformed));
        assert_eq!(open(&k, b"ctx", b"PSEA"), Err(CryptoError::Malformed));
        assert_eq!(open(&k, b"ctx", b"not sealed at all"), Err(CryptoError::Malformed));
        assert!(!is_sealed(b""));
        assert!(!is_sealed(b"PREC\x01"));
    }

    #[test]
    fn empty_and_large_payloads_round_trip() {
        let k = key();
        let empty = seal(&k, b"ctx", b"");
        assert_eq!(open(&k, b"ctx", &empty).unwrap(), b"");

        let big = vec![0xABu8; 1 << 20];
        let sealed = seal(&k, b"ctx", &big);
        assert_eq!(open(&k, b"ctx", &sealed).unwrap(), big);
    }

    /// A key must never reach a log.
    #[test]
    fn keys_do_not_print_themselves() {
        let rendered = format!("{:?}", key());
        assert!(!rendered.contains("7"), "a key must not appear in its own Debug: {}", rendered);
        assert!(rendered.contains("redacted"));
    }

    /// Generated keys must differ.
    #[test]
    fn generated_keys_are_distinct() {
        let keys: std::collections::HashSet<[u8; 32]> =
            (0..32).map(|_| *SubjectKey::generate().as_bytes()).collect();
        assert_eq!(keys.len(), 32);
    }
}
