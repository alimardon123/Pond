# Pond Lens Ecosystem — Design for Elegance, Performance, and Minimal Round Trips

## Problem Statement

The current Views work but have these shortcomings:
1. **Too many S3 round trips per operation** (3-5 GETs for a simple read)
2. **Full tree serialization per commit** (O(N) metadata per commit)
3. **No partial reads** (must read the entire tree to find one entry)
4. **History walk is O(N)** (linear parent chain walk)
5. **No index support** (every lookup is a scan)

## Design Goals

- **Minimize S3 round trips**: 1 GET for point lookup, 1 GET + 1 PUT for commit
- **O(1) commits**: commit cost independent of table size
- **O(log N) history**: skip pointers built into the commit structure
- **O(1) point lookups**: index-like access without scanning the tree
- **Branching is free**: O(1) (just a Reference)
- **Elegant for developers**: ViewBase handles all complexity

## Key Innovation: Delta Commits with Embedded Skip Pointers

Instead of full-snapshot trees (current approach) or sharded trees (view_helpers.py),
we use **delta commits**: each commit stores ONLY the changed entries, not the full tree.

### Structure

```
Commit blob:
{
  "type": "commit",
  "parent": "<parent_hash>",
  "skip": "<skip_pointer_hash>",        // back-pointer every 64 commits
  "delta": {
    "+": {"path/key": "blob_hash"},     // added/modified entries
    "-": ["path/key1", "path/key2"]     // deleted entries
  },
  "timestamp": 1234567890,
  "message": "commit message",
  "index": 42                            // commit depth (for skip pointer math)
}
```

### Why delta commits?

| Approach | Commit cost | Read cost | History cost | S3 reads per read |
|---|---|---|---|---|
| Full-snapshot tree (current) | O(N) (rewrite entire tree) | O(1) (read one tree) | O(N) | 2 (commit + tree) |
| Sharded tree (view_helpers) | O(1) (write one shard) | O(1) (read one shard) | O(N) | 2-3 (commit + shard) |
| **Delta commits (new)** | **O(1)** (write only delta) | **O(N_commits)** worst case, **O(1)** with compaction | **O(log N)** (skip pointers) | **1** (just the commit, delta is embedded) |

Delta commits trade read performance for commit performance. To keep reads fast,
we periodically **compact** (merge all deltas into a full-snapshot tree every K commits).

### Compaction

Every 64 commits (same interval as skip pointers), we compact:
1. Walk from the last compaction point
2. Apply all deltas
3. Write a full-snapshot tree
4. The compaction commit's delta includes a "snapshot" field pointing to this tree

```
Compaction commit (every 64th):
{
  "type": "commit",
  "parent": "<parent>",
  "skip": "<64-back>",
  "snapshot": "<full_tree_hash>",    // full state at this commit
  "delta": {"+": {}, "-": []},       // empty delta (it's a snapshot)
  "index": 64,
  ...
}
```

### Read path (with compaction)

To read key K at the current commit:
1. Read the commit (1 S3 GET)
2. If the commit has a "snapshot" field, read the snapshot tree (1 S3 GET), look up K
3. If not, walk backwards applying deltas until K is found or we hit a snapshot
   - With compaction every 64 commits, worst case is 64 delta applications
   - With skip pointers, worst case for history is O(log N)

**Total S3 reads: 1-2 for point lookup (commit + snapshot), 2-66 for full read.**

### Commit path

1. Write the new data blob(s) (1+ S3 PUT)
2. Write the commit blob with delta = {changed entries} (1 S3 PUT)
3. Reference the table name to the new commit (1 S3 PUT for root namespace)
4. Every 64th commit: also write a snapshot tree (1 extra S3 PUT)

**Total S3 writes: 2-3 per commit (blob + commit + reference), 3-4 on compaction.**

### History path (with skip pointers)

1. Read the HEAD commit (1 S3 GET)
2. Follow skip pointers: O(N/64) reads
3. Follow parent chain: O(64) reads max

**Total S3 reads: O(N/64 + 64) = O(N/64) for full history, O(1) for recent.**

### Branch path

1. Reference(branch_name, current_commit_hash) — O(1), 1 S3 PUT

**Total S3 writes: 1 (just the root namespace update).**

## Implementation

This design is implemented in `libraries/delta_view.py` as `DeltaViewBase`,
which replaces the earlier `ViewBase` from `view_helpers.py`. All three
applications (Notebook, Git, SQL) will use `DeltaViewBase`.

## Round Trip Summary

| Operation | S3 GETs | S3 PUTs | Total |
|---|---|---|---|
| Point lookup (key read) | 1-2 | 0 | 1-2 |
| Full scan (all entries) | 2-66 | 0 | 2-66 |
| Commit (1 key changed) | 0 | 2-3 | 2-3 |
| Commit + compaction | 1 | 3-4 | 4-5 |
| Branch | 0 | 1 | 1 |
| History (last 20) | 1-20 | 0 | 1-20 |
| History (depth D) | O(D/64+64) | 0 | O(log N) |
| Undo (1 step) | 0 | 1 | 1 |
| Merge | 2 | 3 | 5 |

Compare to current approach:
| Operation | Old S3 calls | New S3 calls | Improvement |
|---|---|---|---|
| Point lookup | 3 (commit + tree + blob) | 1-2 (commit + optional snapshot) | 33-50% fewer |
| Commit | 3-5 (blob + tree + commit + ref) | 2-3 (blob + commit + ref) | 25-40% fewer |
| History (depth 1000) | 1000 (linear walk) | ~20 (skip pointers) | 98% fewer |
