# Row vs Columnar vs Hybrid Format — Research for Pond

## The question

Can we have a single storage format that supports both row-based
(OLTP) and columnar (OLAP) access patterns with ONE copy of data?

## Current state

Pond stores raw bytes. Views choose format:
- SQLView: JSON (row-oriented, human-readable, verbose)
- StreamingView: length-prefixed records (row-oriented)
- GitView: raw file bytes (blob-oriented)

None of these are columnar. For OLAP workloads (scan many rows, few
columns), columnar is dramatically faster. But for OLTP (point lookups,
single-row updates), row-oriented is better.

## Options

### Option A: View chooses format (current)
- SQLView uses JSON (row), a future ColumnView uses Parquet (columnar)
- Same data can be stored in both formats (two copies) — VIOLATES one-copy principle
- OR: only one format, accepting it's suboptimal for some workloads

### Option B: Arrow IPC as the universal format
- Apache Arrow is columnar in memory but can be serialized as row-oriented IPC
- Arrow Flight/Arrow IPC supports both row and columnar access
- But: Arrow is heavy (large dependency, complex memory model)
- And: Arrow is still fundamentally columnar — row access requires reading entire columns

### Option C: PAX (Partition Attributes Across) format
- PAX stores data in pages. Each page has a header (row metadata) +
  columnar mini-pages within it.
- Row access: read the page header + one mini-page
- Columnar access: scan all mini-pages for a column across pages
- Single copy, both access patterns supported
- Used by: Parasol, Peloton (HTAP databases)
- Tradeoff: neither pure-row nor pure-columnar; both are slightly slower

### Option D: Row-group format (Parquet-like, but with row access)
- Parquet already has row groups (batches of rows stored columnar)
- Parquet has page indexes for row-level filtering
- But: Parquet doesn't support O(1) random row access (must scan pages)
- Could we add a row index to Parquet? Yes — but that's a second copy (the index)

### Option E: The "Fractal" format (novel — let me think)
- What if the format is a tree where each level alternates between
  row-oriented and column-oriented?
  - Leaf level: row-oriented (fast for single-row reads)
  - Internal level: columnar (fast for column scans)
- This is essentially what PAX does, but generalized
- The "fractal" name is made up; the concept is "multi-resolution storage"

### Option F: Just use Arrow + row index (pragmatic)
- Store data as Arrow IPC (columnar)
- Build a row index (Prolly tree mapping row_id → Arrow row group + offset)
- Row access: look up in index → read one row group → extract one row
- Columnar access: scan Arrow column chunks directly
- Single copy of data (Arrow) + one index (Prolly tree = metadata)
- The index is derived (rebuildable) → doesn't violate one-copy principle

## Recommendation: Option F (Arrow + row index)

**Why:**
1. Arrow is the industry standard for in-memory columnar (interoperable)
2. The Prolly tree index gives O(log N) row access (acceptable for OLTP)
3. Columnar scans are native Arrow (optimal for OLAP)
4. Single copy of data (Arrow) + derived index (rebuildable)
5. The index is View-level, not kernel-level (no format in the kernel)
6. Works with the existing ProllyViewBase (index is just another Prolly tree)

**How it would work:**
```
Data: Arrow IPC file (columnar, stored as one blob via kernel.Write)
Index: Prolly tree (row_id → {arrow_file_hash, row_group_idx, row_offset})
       stored as a separate Prolly tree, referenced by name

OLTP point lookup:
  1. Look up row_id in the Prolly tree index: O(log N)
  2. Read the Arrow row group from the data blob: O(1)
  3. Extract the row: O(1)

OLAP columnar scan:
  1. Read the Arrow data blob directly
  2. Scan the requested column(s): O(N) but vectorized

Single copy of data. Index is derived (rebuildable from data).
```

**What this means for Pond:**
- The kernel stays at 3 primitives (Write/Read/Reference)
- Arrow is a View-level format choice (not kernel)
- The Prolly tree index is a View-level structure (not kernel)
- Both OLTP and OLAP are served from the same data copy
- Streaming is also served (records are Arrow rows)

## What about Lance?

Lance format is essentially Option F done right:
- Columnar storage (like Parquet but with random access)
- Versioned (fragments + manifest)
- Supports both columnar scans AND random row access
- Content-addressed fragments

We could:
1. Use Lance format as a View-level library (like we use ProllyViewBase)
2. Store Lance files as Pond blobs (kernel doesn't know about Lance)
3. Build Prolly tree indexes on top of Lance for row access

This would give us: Lance's columnar performance + Pond's versioning +
Prolly tree's O(log N) point lookups + cross-View sharing.

## Decision

**Don't bake any format into the kernel.** Keep the kernel at 3 primitives.

At the View level:
- Default: JSON (simple, human-readable, works for everything)
- Optional: ArrowView (columnar for OLAP-heavy workloads)
- Optional: LanceView (columnar + random access for ML/vector workloads)
- All use the same ProllyViewBase for versioning, branching, history

The format choice is a View concern. The kernel stores bytes. Views
interpret bytes. This is already the architecture — no changes needed.

The "single format that supports both row and columnar" question is
answered by: **Arrow + Prolly tree row index** (Option F). This is a
View-level library, not a kernel feature. It can be built without
changing the kernel.
