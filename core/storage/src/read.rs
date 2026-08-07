// Read module — read data from collections
//
// FAITHFUL PORT of Python UnifiedStorage's read / read_at_snapshot methods.

use crate::branch_ref;
use crate::commit;
use crate::manifest::CollectionManifest;
use crate::shard;
use pond_kernel::PondKernel;

/// Read the current data for a collection (from the active branch's HEAD).
///
/// Returns the raw data blob for the HEAD commit's manifest.
/// For a full row-level read (with shard merging), use read_with_shards.
pub fn read(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Result<Vec<u8>, String> {
    // Resolve HEAD commit
    let head = kernel.resolve(&branch_ref(collection, branch))
        .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;

    // Read the commit to get the manifest hash
    let commit = commit::read_commit(kernel, &head)
        .ok_or_else(|| "Failed to read HEAD commit".to_string())?;

    if commit.manifest.is_empty() {
        return Err("HEAD commit has no manifest".to_string());
    }

    // Load the manifest
    let manifest_bytes = kernel.read_blob(&commit.manifest)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;

    // Read the first row group's blob (simplified — for v0.1, one row group)
    if let Some(rg) = manifest.row_groups.first() {
        return kernel.read_blob(&rg.blob_hash)
            .map_err(|e| format!("Failed to read data blob: {}", e));
    }

    Err("Manifest has no row groups".to_string())
}

/// Read data at a specific commit — SNAPSHOT ISOLATION.
///
/// Reads ONLY the manifest at the given commit, ignoring any shards
/// written after that commit. Provides a consistent snapshot for
/// long-running analytical queries.
pub fn read_at_snapshot(
    kernel: &PondKernel,
    commit_hash: &str,
) -> Result<Vec<u8>, String> {
    let commit = commit::read_commit(kernel, commit_hash)
        .ok_or_else(|| "Commit not found".to_string())?;

    if commit.manifest.is_empty() {
        return Err("Commit has no manifest".to_string());
    }

    let manifest_bytes = kernel.read_blob(&commit.manifest)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;
    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;

    if let Some(rg) = manifest.row_groups.first() {
        return kernel.read_blob(&rg.blob_hash)
            .map_err(|e| format!("Failed to read data blob: {}", e));
    }

    Err("Manifest has no row groups".to_string())
}

/// Read the full collection data including shards (CRDT read path).
///
/// Returns the HEAD data plus all unmerged shard data.
/// For snapshot isolation, use read_at_snapshot instead.
pub fn read_full(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Vec<Vec<u8>> {
    let mut results = Vec::new();

    // Read HEAD data
    if let Ok(data) = read(kernel, collection, branch) {
        results.push(data);
    }

    // Read all shard data
    let (_, shards) = shard::read_with_shards(kernel, collection, branch);
    for (_, shard_hash) in shards {
        if let Ok(data) = kernel.read_blob(&shard_hash) {
            results.push(data);
        }
    }

    results
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnifiedStorage;
    use crate::write;

    #[test]
    fn test_read_returns_head_data() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        write::write(kernel, "users", "main", b"hello world", "initial").unwrap();
        let data = read(kernel, "users", "main").unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn test_read_at_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let c1 = write::write(kernel, "users", "main", b"v1", "first").unwrap();
        write::write(kernel, "users", "main", b"v2", "second").unwrap();

        // Read at c1 (should return v1, not v2)
        let data = read_at_snapshot(kernel, &c1).unwrap();
        assert_eq!(data, b"v1");
    }

    #[test]
    fn test_read_full_includes_shards() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        write::write(kernel, "events", "main", b"head data", "init").unwrap();
        crate::shard::append_shard(kernel, "events", "main", "s1", b"shard1").unwrap();

        let data = read_full(kernel, "events", "main");
        assert_eq!(data.len(), 2); // HEAD + 1 shard
        assert!(data.iter().any(|d| d == b"head data"));
        assert!(data.iter().any(|d| d == b"shard1"));
    }
}
