// Write module — write data to collections
//
// Two write paths:
//   1. write() — raw bytes (JSON or any format). Simple, used by CLI.
//   2. write_rows() — structured rows encoded as PND2. Production path
//      with column stats, auto-encoding (RLE/DICT/BITPACK/RAW), and
//      proper manifest entries for pruning/projection.
//
// Both paths create a commit and update branch refs identically.

use crate::commit;
use crate::manifest::{CollectionManifest, ColumnStatsEntry, RowGroupEntry};
use crate::{branch_ref, manifest_ref};
use pond_core::{pnd2_encode_multi, pnd2_encode_i64_auto, EncodeMultiColumn, VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY};
use pond_kernel::PondKernel;

/// Write raw bytes to a collection. Creates a new commit on the active branch.
///
/// This is the simplest write path — it REPLACES the collection's data
/// (not an append). For append semantics, use shard::append_shard.
///
/// The data is stored as-is (no PND2 encoding). Use write_rows() for
/// structured data that benefits from columnar encoding + pruning.
pub fn write(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    data: &[u8],
    message: &str,
) -> Result<String, String> {
    // Write the data blob
    let data_hash = kernel.write(data)
        .map_err(|e| format!("Failed to write data: {}", e))?;

    // Get parent commit (current HEAD of active branch)
    let parent = kernel.resolve(&branch_ref(collection, active_branch));
    let parent_index = parent.as_ref()
        .and_then(|p| commit::read_commit(kernel, p))
        .map(|c| c.index + 1)
        .unwrap_or(0);

    // Build a simple manifest with one row group pointing at the data blob
    let mut manifest = CollectionManifest::new(vec![], String::new());
    manifest.add_row_group(RowGroupEntry {
        key: "rg_0000000000".to_string(),
        blob_hash: data_hash.clone(),
        n_rows: 1, // raw bytes — row count unknown
        columns: vec![],
    });
    let manifest_bytes = manifest.encode();
    let manifest_hash = kernel.write(&manifest_bytes)
        .map_err(|e| format!("Failed to write manifest: {}", e))?;

    // Write the commit
    let commit_hash = commit::write_commit(
        kernel, collection, &manifest_hash, parent.as_deref(), None,
        if message.is_empty() { "write" } else { message }, parent_index,
    ).map_err(|e| format!("Failed to write commit: {}", e))?;

    // Update branch refs
    kernel.reference(&branch_ref(collection, active_branch), &commit_hash)
        .map_err(|e| format!("Failed to update branch ref: {}", e))?;
    kernel.reference(&manifest_ref(collection, active_branch), &manifest_hash)
        .map_err(|e| format!("Failed to update manifest ref: {}", e))?;
    let _ = kernel.reference(collection, &commit_hash);

    Ok(commit_hash)
}

/// Write structured rows as a PND2 blob with proper column stats.
///
/// This is the PRODUCTION write path — it encodes rows as a PND2 blob
/// with automatic encoding selection (RLE/DICT/BITPACK/RAW per column),
/// builds a manifest with per-column stats (min/max/null_count), and
/// enables predicate pruning + projection pushdown on reads.
///
/// Args:
///   - kernel: The PondKernel handle
///   - collection: Collection name
///   - active_branch: Branch to write to
///   - columns: Column specs (name, i64 values)
///   - message: Commit message
///
/// Returns: commit hash
pub fn write_rows_i64(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    columns: &[(&str, &[i64])],
    message: &str,
) -> Result<String, String> {
    let n_rows = columns.first().map(|(_, v)| v.len()).unwrap_or(0);

    // Encode as PND2 with auto-encoding per column
    let blob = pnd2_encode_i64_auto(columns);
    let data_hash = kernel.write(&blob)
        .map_err(|e| format!("Failed to write PND2 blob: {}", e))?;

    // Get parent commit
    let parent = kernel.resolve(&branch_ref(collection, active_branch));
    let parent_index = parent.as_ref()
        .and_then(|p| commit::read_commit(kernel, p))
        .map(|c| c.index + 1)
        .unwrap_or(0);

    // Build manifest with schema + column stats
    let schema: Vec<(String, u8)> = columns.iter()
        .map(|(name, _)| (name.to_string(), VT_INT64))
        .collect();
    let key_col = columns.first().map(|(name, _)| name.to_string()).unwrap_or_default();
    let mut manifest = CollectionManifest::new(schema, key_col);

    // Build column stats entries
    let mut col_stats: Vec<ColumnStatsEntry> = Vec::new();
    for (name, values) in columns {
        if values.is_empty() {
            col_stats.push(ColumnStatsEntry {
                name: name.to_string(),
                value_type: VT_INT64,
                min: None,
                max: None,
                null_count: 0,
            });
        } else {
            let min = *values.iter().min().unwrap();
            let max = *values.iter().max().unwrap();
            col_stats.push(ColumnStatsEntry {
                name: name.to_string(),
                value_type: VT_INT64,
                min: Some(min.to_le_bytes().to_vec()),
                max: Some(max.to_le_bytes().to_vec()),
                null_count: 0,
            });
        }
    }

    manifest.add_row_group(RowGroupEntry {
        key: "rg_0000000000".to_string(),
        blob_hash: data_hash.clone(),
        n_rows: n_rows as u32,
        columns: col_stats,
    });

    let manifest_bytes = manifest.encode();
    let manifest_hash = kernel.write(&manifest_bytes)
        .map_err(|e| format!("Failed to write manifest: {}", e))?;

    // Write the commit
    let commit_hash = commit::write_commit(
        kernel, collection, &manifest_hash, parent.as_deref(), None,
        if message.is_empty() { "write_rows" } else { message }, parent_index,
    ).map_err(|e| format!("Failed to write commit: {}", e))?;

    // Update branch refs
    kernel.reference(&branch_ref(collection, active_branch), &commit_hash)
        .map_err(|e| format!("Failed to update branch ref: {}", e))?;
    kernel.reference(&manifest_ref(collection, active_branch), &manifest_hash)
        .map_err(|e| format!("Failed to update manifest ref: {}", e))?;
    let _ = kernel.reference(collection, &commit_hash);

    Ok(commit_hash)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnifiedStorage;
    use crate::commit;

    #[test]
    fn test_write_creates_commit() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let hash = write(kernel, "users", "main", b"hello world", "initial").unwrap();

        // Verify the commit exists and has the right structure
        let commit = commit::read_commit(kernel, &hash).unwrap();
        assert_eq!(commit.message, "initial");
        assert!(commit.parent.is_none()); // first commit
        assert_eq!(commit.index, 0);

        // Verify the branch ref points at the commit
        assert_eq!(
            kernel.resolve(&branch_ref("users", "main")),
            Some(hash.clone())
        );
    }

    #[test]
    fn test_write_chains_commits() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let c1 = write(kernel, "users", "main", b"v1", "first").unwrap();
        let c2 = write(kernel, "users", "main", b"v2", "second").unwrap();

        // c2's parent should be c1
        let commit2 = commit::read_commit(kernel, &c2).unwrap();
        assert_eq!(commit2.parent, Some(c1));
        assert_eq!(commit2.index, 1);
    }

    #[test]
    fn test_write_rows_i64_creates_pnd2_blob() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let ids = vec![1i64, 2, 3, 4, 5];
        let ages = vec![30i64, 25, 35, 40, 28];

        let hash = write_rows_i64(
            kernel, "users", "main",
            &[("id", &ids), ("age", &ages)],
            "insert 5 users",
        ).unwrap();

        // Verify commit exists
        let commit = commit::read_commit(kernel, &hash).unwrap();
        assert_eq!(commit.message, "insert 5 users");
        assert_eq!(commit.index, 0);

        // Verify the PND2 blob can be decoded
        let manifest_hash = &commit.manifest;
        let manifest_data = kernel.read_blob(manifest_hash).unwrap();
        let manifest = CollectionManifest::decode(&manifest_data).expect("manifest should decode");
        assert_eq!(manifest.row_groups.len(), 1);
        assert_eq!(manifest.row_groups[0].n_rows, 5);
        assert_eq!(manifest.row_groups[0].columns.len(), 2); // id + age

        // Verify column stats
        let id_stats = &manifest.row_groups[0].columns[0];
        assert_eq!(id_stats.name, "id");
        assert_eq!(id_stats.value_type, VT_INT64);
        let id_min = i64::from_le_bytes(id_stats.min.as_ref().unwrap()[..8].try_into().unwrap());
        let id_max = i64::from_le_bytes(id_stats.max.as_ref().unwrap()[..8].try_into().unwrap());
        assert_eq!(id_min, 1);
        assert_eq!(id_max, 5);

        // Verify the PND2 blob is decodable
        let blob_hash = &manifest.row_groups[0].blob_hash;
        let blob_data = kernel.read_blob(blob_hash).unwrap();
        let cols = pond_core::pnd2_decode(&blob_data).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].i64_data, ids);
        assert_eq!(cols[1].i64_data, ages);
    }
}
