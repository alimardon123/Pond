// Pond UnifiedStorage — Rust port of the Python unified_storage.py
//
// BROKEN INTO MODULES (following the user's request for smaller files):
//   lib.rs          — UnifiedStorage struct + public API + ref namespace helpers
//   manifest.rs     — CollectionManifest (RowGroupEntry, ColumnStats, encode/decode)
//   commit.rs       — Commit struct + write/read commit blobs + history walking
//   branch.rs       — Branch management (branch, checkout, merge)
//   shard.rs        — CRDT shard management (append, list, clear)
//   read.rs         — Read path (read, read_with_shards, read_at_snapshot)
//   write.rs        — Write path (write, append)
//   transaction.rs  — Atomic publication (begin_tx, commit_tx, abort_tx)
//
// This is a FAITHFUL PORT of the Python implementation — same commit format,
// same ref conventions, same merge logic. The Python code is the reference;
// this Rust code is the production implementation.
//
// DESIGN PRINCIPLES:
//   - Simple: each module has one responsibility
//   - Powerful: composes the kernel's 3 primitives into a full storage layer
//   - Performant: Rust native speed, no Python GIL, no dict intermediate
//   - Scalable: O(conflicting) merge, content-addressed dedup, parallel I/O
//   - Beautiful: clear module boundaries, downward dependencies only

pub mod manifest;
pub mod commit;
pub mod branch;
pub mod shard;
pub mod read;
pub mod write;
pub mod transaction;

use pond_kernel::PondKernel;
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Ref namespace helpers — match Python UnifiedStorage conventions exactly
// ---------------------------------------------------------------------------

/// Branch commit ref: collections/{name}/_branches/{branch}/commit
pub fn branch_ref(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/commit", collection, branch)
}

/// Manifest ref: collections/{name}/_branches/{branch}/manifest
pub fn manifest_ref(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/manifest", collection, branch)
}

/// Shard prefix: collections/{name}/_branches/{branch}/shards/
pub fn shards_prefix(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/shards/", collection, branch)
}

/// Transaction ref: transactions/{tx_id}
pub fn tx_ref(tx_id: &str) -> String {
    format!("transactions/{}", tx_id)
}

/// Collection definition ref: collections/{name}/definition
pub fn definition_ref(collection: &str) -> String {
    format!("collections/{}/definition", collection)
}

// ---------------------------------------------------------------------------
// UnifiedStorage — the main struct
// ---------------------------------------------------------------------------

/// The unified storage layer. Owns a PondKernel and provides:
///   - Collection management (create, read, list)
///   - Commit history (write commits, walk parent chain, undo, revert)
///   - Branching (branch, checkout, merge)
///   - CRDT shards (append_shard, read_with_shards, compact_shards)
///   - Atomic publication (begin_tx, commit_tx, abort_tx)
///
/// This is the Rust equivalent of Python's UnifiedStorage class.
/// It composes the kernel's 3 primitives (Write, Read, Ref) into a
/// full versioned storage layer with git-like branching.
pub struct UnifiedStorage {
    kernel: PondKernel,
    /// Active branch per collection (in-memory, like Python's _active_branches)
    active_branches: Mutex<std::collections::HashMap<String, String>>,
}

impl UnifiedStorage {
    /// Create a new UnifiedStorage with a local FS kernel.
    pub fn new_local(base_dir: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(Self {
            kernel: PondKernel::new_local(base_dir)?,
            active_branches: Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Create a UnifiedStorage wrapping an existing kernel.
    pub fn new(kernel: PondKernel) -> Self {
        Self {
            kernel,
            active_branches: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Get a reference to the kernel.
    pub fn kernel(&self) -> &PondKernel {
        &self.kernel
    }

    /// Get the active branch for a collection (default: "main").
    /// Matches Python's _get_active_branch.
    pub fn get_active_branch(&self, collection: &str) -> String {
        self.active_branches.lock().unwrap()
            .get(collection)
            .cloned()
            .unwrap_or_else(|| "main".to_string())
    }

    /// Set the active branch for a collection (in-memory only, like Python).
    pub fn set_active_branch(&self, collection: &str, branch: &str) {
        self.active_branches.lock().unwrap()
            .insert(collection.to_string(), branch.to_string());
    }

    /// Get the active commit ref for a collection.
    pub fn active_commit_ref(&self, collection: &str) -> String {
        let branch = self.get_active_branch(collection);
        branch_ref(collection, &branch)
    }

    /// Get the active manifest ref for a collection.
    pub fn active_manifest_ref(&self, collection: &str) -> String {
        let branch = self.get_active_branch(collection);
        manifest_ref(collection, &branch)
    }

    // Delegate to submodules
    // The actual implementations are in the module files and take
    // &UnifiedStorage (or &PondKernel) as the first argument.
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ref_namespace() {
        assert_eq!(
            branch_ref("users", "main"),
            "collections/users/_branches/main/commit"
        );
        assert_eq!(
            manifest_ref("users", "main"),
            "collections/users/_branches/main/manifest"
        );
        assert_eq!(
            shards_prefix("users", "main"),
            "collections/users/_branches/main/shards/"
        );
        assert_eq!(tx_ref("abc123"), "transactions/abc123");
        assert_eq!(definition_ref("users"), "collections/users/definition");
    }

    #[test]
    fn test_active_branch_default() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        assert_eq!(storage.get_active_branch("users"), "main");
    }

    #[test]
    fn test_set_active_branch() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        storage.set_active_branch("users", "experiment");
        assert_eq!(storage.get_active_branch("users"), "experiment");
        assert_eq!(storage.active_commit_ref("users"),
                   "collections/users/_branches/experiment/commit");
    }
}
