// Shard module — CRDT shard management
//
// FAITHFUL PORT of Python UnifiedStorage's append_shard / read_with_shards.
//
// CRDT shards allow concurrent multi-writer without coordination:
//   - Each writer writes its own shard to a unique path
//   - Readers union HEAD + all live shards
//   - No CAS, no coordination — works on any object store

use crate::{branch_ref, manifest_ref, shards_prefix};
use pond_kernel::PondKernel;

/// Append a CRDT shard to a branch.
///
/// The shard is written to a unique path under the branch's shards/ directory.
/// Readers will discover and merge it via read_with_shards.
///
/// Matches Python UnifiedStorage.append_shard():
///   1. Write the shard data as a blob
///   2. Reference it at a unique shard path
pub fn append_shard(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    shard_name: &str,
    data: &[u8],
) -> Result<String, String> {
    // Write the shard blob
    let shard_hash = kernel.write(data)
        .map_err(|e| format!("Failed to write shard: {}", e))?;

    // Reference it at a unique path
    let shard_ref = format!("{}{}", shards_prefix(collection, branch), shard_name);
    kernel.reference(&shard_ref, &shard_hash)
        .map_err(|e| format!("Failed to reference shard: {}", e))?;

    Ok(shard_hash)
}

/// List all shard hashes for a branch.
///
/// Scans the branch's shards/ directory and resolves each ref.
pub fn list_shards(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Vec<(String, String)> {
    let prefix = shards_prefix(collection, branch);
    let refs = kernel.list_names_prefix(&prefix);
    let mut shards = Vec::new();
    for ref_path in refs {
        if let Some(hash) = kernel.resolve(&ref_path) {
            let name = ref_path.strip_prefix(&prefix).unwrap_or(&ref_path).to_string();
            shards.push((name, hash));
        }
    }
    shards
}

/// Read the collection's HEAD manifest + all live shards.
///
/// This is the CRDT read path: union HEAD + all unmerged shards.
/// Returns the HEAD manifest hash and the list of shard hashes.
pub fn read_with_shards(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> (Option<String>, Vec<(String, String)>) {
    // Get HEAD commit
    let head_commit = kernel.resolve(&branch_ref(collection, branch));

    // Get HEAD manifest
    let head_manifest = head_commit.as_ref()
        .and_then(|h| crate::commit::read_commit(kernel, h))
        .and_then(|c| {
            if c.manifest.is_empty() { None } else { Some(c.manifest.clone()) }
        });

    // List all shards
    let shards = list_shards(kernel, collection, branch);

    (head_manifest, shards)
}

/// Clear all shards for a branch (used after merge/compact).
pub fn clear_shards(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Result<usize, String> {
    let shards = list_shards(kernel, collection, branch);
    let mut count = 0;
    for (name, _) in &shards {
        let shard_ref = format!("{}{}", shards_prefix(collection, branch), name);
        if kernel.delete_ref(&shard_ref).unwrap_or(false) {
            count += 1;
        }
    }
    Ok(count)
}

/// Count the number of live shards for a branch.
pub fn shard_count(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> usize {
    list_shards(kernel, collection, branch).len()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnifiedStorage;

    #[test]
    fn test_append_and_list_shards() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Append two shards
        let h1 = append_shard(kernel, "events", "main", "shardA", b"shard A data").unwrap();
        let h2 = append_shard(kernel, "events", "main", "shardB", b"shard B data").unwrap();

        // List shards
        let shards = list_shards(kernel, "events", "main");
        assert_eq!(shards.len(), 2);

        // Verify shard data
        let data_a = kernel.read_blob(&h1).unwrap();
        assert_eq!(data_a, b"shard A data");
    }

    #[test]
    fn test_clear_shards() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        append_shard(kernel, "events", "main", "s1", b"data1").unwrap();
        append_shard(kernel, "events", "main", "s2", b"data2").unwrap();
        assert_eq!(shard_count(kernel, "events", "main"), 2);

        let cleared = clear_shards(kernel, "events", "main").unwrap();
        assert_eq!(cleared, 2);
        assert_eq!(shard_count(kernel, "events", "main"), 0);
    }

    #[test]
    fn test_read_with_shards() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // No HEAD, no shards
        let (head, shards) = read_with_shards(kernel, "events", "main");
        assert!(head.is_none());
        assert!(shards.is_empty());

        // Add shards (no HEAD)
        append_shard(kernel, "events", "main", "s1", b"data1").unwrap();
        let (head, shards) = read_with_shards(kernel, "events", "main");
        assert!(head.is_none()); // no HEAD commit
        assert_eq!(shards.len(), 1);
    }
}
