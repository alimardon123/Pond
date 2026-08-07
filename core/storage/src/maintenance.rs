// Maintenance module — tombstone operations (RFC-0008: Deletion as Data)
//
// FAITHFUL PORT of Python pond-sdk/maintenance.py.
//
// Tombstones are a Layer 1 convention — the kernel doesn't know they're
// special. A tombstone is a name rebound to TOMBSTONE_HASH (SHA-256 of
// a constant marker blob). This signals "this name is logically deleted."
//
// Operations:
//   - drop_name: rebind a name to TOMBSTONE_HASH (logical delete, idempotent)
//   - is_dropped: check if a name is tombstoned
//   - resolve_active: resolve a name, returning None for unbound OR tombstoned
//   - compact_tombstones: physically remove tombstoned name rows (VACUUM)
//
// The kernel stays at 3 primitives (Write, Read, Reference). Tombstones
// are data, not a kernel feature.

use pond_kernel::PondKernel;
use sha2::{Digest, Sha256};

/// The marker blob — a constant whose SHA-256 IS the tombstone hash.
const TOMBSTONE_MARKER: &[u8] = b"__pond_tombstone__";

/// The globally-known hash that signals "this name is logically deleted."
/// It is the SHA-256 of TOMBSTONE_MARKER.
pub fn tombstone_hash() -> String {
    let mut hasher = Sha256::new();
    hasher.update(TOMBSTONE_MARKER);
    let result = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in result.iter() {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}

/// Ensure the tombstone marker blob exists in the kernel's object store.
/// Idempotent — content addressing means re-writing is a no-op.
fn ensure_tombstone_blob(kernel: &PondKernel) {
    let _ = kernel.write(TOMBSTONE_MARKER);
}

/// Logically delete a name by rebinding it to TOMBSTONE_HASH.
///
/// Idempotent: calling drop_name on an already-tombstoned name is a no-op.
///
/// After drop_name:
///   - kernel.resolve(name) returns TOMBSTONE_HASH
///   - is_dropped(kernel, name) returns true
///   - resolve_active(kernel, name) returns None
pub fn drop_name(kernel: &PondKernel, name: &str) {
    ensure_tombstone_blob(kernel);
    let _ = kernel.reference(name, &tombstone_hash());
}

/// True iff name is bound to TOMBSTONE_HASH.
///
/// Returns false for names bound to a non-tombstone hash or unbound names.
pub fn is_dropped(kernel: &PondKernel, name: &str) -> bool {
    kernel.resolve(name).as_deref() == Some(&tombstone_hash())
}

/// Resolve a name to its hash, returning None for unbound OR tombstoned names.
///
/// This is what Lens code should call when it wants "active names only."
pub fn resolve_active(kernel: &PondKernel, name: &str) -> Option<String> {
    let h = kernel.resolve(name)?;
    if h == tombstone_hash() {
        return None;
    }
    Some(h)
}

/// Remove tombstoned name rows from the kernel's namespace.
///
/// This is the Layer 0.5 maintenance operation, analogous to VACUUM in
/// PostgreSQL or `git gc` in Git. It is:
///   - Idempotent: running twice has the same effect as once.
///   - Safe: only removes names already marked deleted.
///   - Optional: the system is correct without it.
///
/// Returns the number of names compacted.
pub fn compact_tombstones(kernel: &PondKernel) -> usize {
    let ts_hash = tombstone_hash();
    let names = kernel.list_names();
    let mut compacted = 0;
    for name in &names {
        if kernel.resolve(name).as_deref() == Some(&ts_hash) {
            let _ = kernel.delete_ref(name);
            compacted += 1;
        }
    }
    compacted
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_tombstone_hash_is_stable() {
        let h1 = tombstone_hash();
        let h2 = tombstone_hash();
        assert_eq!(h1, h2, "tombstone hash must be deterministic");
        assert_eq!(h1.len(), 64, "must be 64 hex chars");
    }

    #[test]
    fn test_drop_and_is_dropped() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();

        // Write a blob and reference it
        let h = kernel.write(b"data").unwrap();
        kernel.reference("my_coll", &h).unwrap();

        // Not dropped initially
        assert!(!is_dropped(&kernel, "my_coll"));
        assert_eq!(resolve_active(&kernel, "my_coll"), Some(h.clone()));

        // Drop it
        drop_name(&kernel, "my_coll");
        assert!(is_dropped(&kernel, "my_coll"));
        assert_eq!(resolve_active(&kernel, "my_coll"), None);
        // kernel.resolve still returns the tombstone hash
        assert_eq!(kernel.resolve("my_coll"), Some(tombstone_hash()));
    }

    #[test]
    fn test_drop_is_idempotent() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("coll", &h).unwrap();

        drop_name(&kernel, "coll");
        drop_name(&kernel, "coll"); // second time is a no-op
        assert!(is_dropped(&kernel, "coll"));
    }

    #[test]
    fn test_resolve_active_for_unbound() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        assert_eq!(resolve_active(&kernel, "nonexistent"), None);
        assert!(!is_dropped(&kernel, "nonexistent"));
    }

    #[test]
    fn test_compact_tombstones() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();

        // Create 3 names, drop 2
        let h = kernel.write(b"data").unwrap();
        kernel.reference("keep", &h).unwrap();
        kernel.reference("drop1", &h).unwrap();
        kernel.reference("drop2", &h).unwrap();
        drop_name(&kernel, "drop1");
        drop_name(&kernel, "drop2");

        // Compact
        let compacted = compact_tombstones(&kernel);
        assert_eq!(compacted, 2);

        // Dropped names are now unbound (not just tombstoned)
        assert!(!is_dropped(&kernel, "drop1")); // unbound, not tombstoned
        assert_eq!(kernel.resolve("drop1"), None);
        assert!(!is_dropped(&kernel, "drop2"));
        assert_eq!(kernel.resolve("drop2"), None);

        // Active name is untouched
        assert!(!is_dropped(&kernel, "keep"));
        assert_eq!(resolve_active(&kernel, "keep"), Some(h));
    }

    #[test]
    fn test_compact_is_idempotent() {
        let dir = tempdir().unwrap();
        let kernel = PondKernel::new_local(dir.path()).unwrap();
        let h = kernel.write(b"data").unwrap();
        kernel.reference("drop", &h).unwrap();
        drop_name(&kernel, "drop");

        let c1 = compact_tombstones(&kernel);
        let c2 = compact_tombstones(&kernel);
        assert_eq!(c1, 1);
        assert_eq!(c2, 0); // already compacted
    }
}
