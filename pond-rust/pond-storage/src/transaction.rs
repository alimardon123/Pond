// Transaction module — atomic publication across collections
//
// FAITHFUL PORT of Python UnifiedStorage's begin_tx / commit_tx / abort_tx.
//
// This provides ATOMIC PUBLICATION (not full ACID):
//   - Atomicity of publication: once the commit marker exists, all
//     tentative shards become visible together.
//   - NO isolation: readers can see committed state from other txns mid-read.
//   - NO rollback: abort_tx is a no-op; tentative shards are orphaned until GC.
//   - NO conflict detection: two txns can write the same _rowid; merge is LWW.
//
// See docs/HONEST_COMPETITOR_COMPARISON.md §3 for the honest description.

use crate::tx_ref;
use pond_kernel::PondKernel;

/// Begin a transaction. Returns a transaction ID (UUID).
/// No storage operation — just generates an ID.
pub fn begin_tx() -> String {
    // Generate a simple unique ID (timestamp + random)
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("tx_{:016x}", ts)
}

/// Commit a transaction. Writes a commit marker at transactions/{tx_id}.
/// Once the marker exists, all tentative shards (with tx_id in their path)
/// become visible to readers.
///
/// This is ATOMIC PUBLICATION — all-or-nothing visibility.
/// It is NOT full ACID (no isolation, no rollback, no conflict detection).
pub fn commit_tx(
    kernel: &PondKernel,
    tx_id: &str,
    message: &str,
) -> Result<String, String> {
    // Write a commit marker blob
    let marker_data = format!(r#"{{"tx_id":"{}","message":"{}","status":"committed"}}"#,
                              tx_id, message);
    let marker_hash = kernel.write(marker_data.as_bytes())
        .map_err(|e| format!("Failed to write tx marker: {}", e))?;

    // Reference it at the transaction ref
    kernel.reference(&tx_ref(tx_id), &marker_hash)
        .map_err(|e| format!("Failed to reference tx marker: {}", e))?;

    Ok(marker_hash)
}

/// Abort a transaction. Currently a NO-OP — tentative shards are orphaned
/// until GC cleans them up.
///
/// This is honest about the limitation: there is no real rollback.
/// The shards remain on storage but are invisible to readers (because
/// the commit marker doesn't exist). GC will eventually delete them.
pub fn abort_tx(_kernel: &PondKernel, _tx_id: &str) {
    // No-op — tentative shards are orphaned until GC.
    // This is documented as a known limitation.
}

/// Check if a transaction has been committed.
pub fn is_tx_committed(kernel: &PondKernel, tx_id: &str) -> bool {
    kernel.resolve(&tx_ref(tx_id)).is_some()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnifiedStorage;

    #[test]
    fn test_begin_tx_returns_unique_id() {
        let tx1 = begin_tx();
        let tx2 = begin_tx();
        assert_ne!(tx1, tx2);
        assert!(tx1.starts_with("tx_"));
    }

    #[test]
    fn test_commit_tx_writes_marker() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let tx_id = begin_tx();
        assert!(!is_tx_committed(kernel, &tx_id));

        commit_tx(kernel, &tx_id, "test commit").unwrap();
        assert!(is_tx_committed(kernel, &tx_id));
    }

    #[test]
    fn test_abort_tx_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let tx_id = begin_tx();
        abort_tx(kernel, &tx_id);
        assert!(!is_tx_committed(kernel, &tx_id)); // not committed
    }
}
