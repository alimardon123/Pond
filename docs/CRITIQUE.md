# Open findings

Defects and gaps that are known, reproduced, and not yet fixed. Each one says
how it was verified, so nobody re-derives it, and so nothing here rests on an
assertion somebody made once.

Findings are removed from this file only when a test covers them.

## Verified, unfixed

### 1. A point read costs two round trips per writer, and writes during a read

Measured with `Metered` over the engine at 1, 16, 64 and 256 writers, each
having published one row:

| writers | open | first point read | modelled |
| ---: | ---: | ---: | ---: |
| 1 | 2 waits | 1 wait | 30 ms |
| 16 | 2 waits | 31 waits, 15 PUTs | 930 ms |
| 64 | 2 waits | 127 waits, 63 PUTs | 3.8 s |
| 256 | 2 waits | 511 waits, 255 PUTs | 15.3 s |

Opening is flat — 2 round trips at every writer count, which is what the
content-addressed head layout was built for and it holds. The *first read* is
not: it merges the per-writer trees pairwise, and every merge is a dependent
wait, so the cost is `2W - 1`. It also issues `W - 1` PUTs, so a read mutates
the store.

For a system whose stated goal is writers "from any lens, any user worldwide",
a 256-writer point read taking fifteen seconds is the gap that matters most.
The merge is already balanced, so the depth is not the problem — the problem is
that each level's merges run one after another when they are independent of
each other, and that the merged nodes are persisted eagerly rather than kept in
memory until something asks for them.

Compaction hides this when it runs. It should not be load-bearing for
correctness of the latency claim.

### 2. Nothing in the read path issues a parallel request

Every operation in `docs/ROUND_TRIP_AUDIT.md` reports a batch width of exactly
1.0. A cold 100k-row scan is 51 sequential waits, though the leaf hashes are
all known from the internal nodes before the first leaf is fetched. The
`ObjectStore` batch methods exist and the cache forwards them correctly; the
read path never calls them.

### 3. A projection reads every byte it discards

`projected scan 2/8` moves 4111.9 KiB — byte for byte what the full scan moves.
Projection only skips *spilled* fields, and small columns live inline in the
leaf. Against a columnar engine fetching two column chunks this is the gap, and
it is a layout problem rather than a code one.

### 4. Write amplification on a small append

Appending one row to a 100k-row collection writes 108.5 KiB: the leaf and the
path above it are rewritten whole. A one-row write to a fresh collection is 2
PUTs and 247 bytes; at batch 1000 it is 0.004 PUTs and 46.9 bytes per row. The
per-row cost is therefore entirely a function of batch size, and a workload of
single-row writes pays about 2300× the bytes per row that a batched one does.

### 5. A streaming tail read fetches the whole stream

`engine_stream::read` of the last 160 KB of a 4 MiB stream: 9 waits, 71 GETs,
4163 KiB read. It reads everything and returns the tail. `size()` alone costs 4
waits.

## Verified, and deliberate — but reachable, so stated

These follow from per-field last-writer-wins and atomic-publish-not-ACID. They
are not bugs against the current design; they are what a user expecting a
transaction will actually hit, and they belong in the documentation of what
Pond guarantees.

### 6. A delete followed by a concurrent partial update resurrects stale fields

Writer 1 deletes a row at t=200. Writer 2, not having seen the delete, updates
one column at t=250. After merge the row is visible again — correct, since the
update is newer than the tombstone — but the columns the update did not touch
come back with their *pre-delete* values, at their pre-delete versions. The
alternative is to have a tombstone shadow every field older than itself, which
is a semantic change to every existing read, so it is recorded here rather than
made silently.

### 7. Write skew is not detected

Two writers each read `{alice_on_call, bob_on_call}` and each removes
themselves. Both writes touch different fields, both succeed, neither is
rejected or retried, and nobody is on call. No read-write conflict detection
exists, by design — this is the concrete cost of "atomic publish, not ACID".

### 8. A writer with a lagging clock loses

Versions order on physical time first. A writer 60 s behind that writes *later*
in real time still loses to the earlier write, silently. Bounded by however far
apart the clocks in a deployment can drift.

## Open, reproduced here

- **A publish moves bytes proportional to every collection the writer owns.**
  Measured by `cargo run --release -p pond_bench --bin headscale`: a single-row
  write into one collection, at increasing collection counts.

  | collections | bytes per publish (v1) | after v2 head | modelled ms |
  | ---: | ---: | ---: | ---: |
  | 1 | 183 | 150 | 60.0 |
  | 10 | 813 | 483 | 60.0 |
  | 100 | 7,203 | 3,903 | 60.1 |
  | 1,000 | 72,003 | 39,003 | 60.7 |
  | 10,000 | 729,003 | 399,003 | 67.6 |

  A reader pays the same on open. **Round trips stay flat at 2 throughout**,
  which is why a round-trip count alone never showed this — it needed its own
  measurement.

  The v2 head encoding stores roots as their 32 raw bytes rather than as
  64 characters of hex, taking this from ~73 to ~40 bytes per collection. That
  is a constant factor and it is **not** the fix: the cost is still linear, so
  10^6 collections still means ~40 MB moved to write one row. It buys roughly
  a factor of two of headroom, nothing more.

  The cause is deliberate: a head is one writer's whole view of the pond in a
  single object, which is what makes atomic multi-collection publish free on a
  store that guarantees single-object write atomicity. `core/record/src/head.rs`
  argues it well and the argument is sound. The cost is that the object is
  rewritten whole when any one collection changes.

  The fix that keeps the property: put the collection map in a prolly tree and
  let the head carry its root. Publishing then rewrites O(log C) tree nodes and
  one small head, so atomicity is untouched — the head is still one object —
  while the bytes stop scaling with C. Resolving a collection becomes a
  descent, which `get_from_roots` already does across writers. Keeping the map
  inline below a threshold preserves today's flat 2-round-trip open for small
  ponds. Not done.

- **The two formats disagree about row order.** A legacy collection returns
  rows in insertion order; an engine collection orders by key, and a row
  written without an explicit `_rowid` gets a generated one, so `[1, 2, 3]`
  comes back as `[2, 3, 1]`. Every row and value is intact — only the order
  differs. It is a real gap in "any lens can read any collection": a caller
  that moves a table from one format to the other gets the same rows in a
  different sequence, with nothing to warn it. Recorded rather than fixed,
  because the fix is a design decision — either the engine preserves an
  insertion sequence, or the lens documents that order is not guaranteed and
  callers who need it sort.

## Reported by review, not yet verified here

Recorded so they are not lost, and marked so nobody treats them as established.
The last review asserted a lost-update bug that turned out to be a
misreading; nothing goes above this line until it has been reproduced.

- **HNSW reads every vector in the collection on every query.** Reproduced and
  measured: bytes read are exactly linear in N — ~256 bytes per vector at 500,
  1000, 2000 and 4000 vectors, so a 10-nearest-neighbour query over 4000
  vectors reads the whole 999 KiB collection. The graph walk itself is
  constant (17 round trips at every size); it is the vector fetch that is
  linear, at `extensions/indexes/hnsw/rust/src/lib.rs`, where the search loads
  all vectors "for distance computation". Sublinear search is the entire
  reason an HNSW index exists, so this cancels its benefit in bytes while
  keeping its cost in build time and storage. The fix is for the index to own
  its vectors — as IVF now does for its cluster blobs — so a search reads only
  what it visits. Not done.
- The canonical URI is not percent-encoded, so a collection named `a?b` signs
  and writes to a different key than intended.
- `engine_kv`, `engine_oltp` and `engine_stream` have no callers outside their
  own tests, and `KeyValueLens::get` is still a full scan.

## Fixed, kept here for the record

- **Merging was not commutative when versions tied exactly.** Two processes
  sharing a writer id — `stable_writer_id` derives one per host and user, not
  per process — could write different values at the same physical time and
  logical counter, and the merge kept whichever argument came first. Fixed by
  breaking the tie on the canonical encoding of the value; the laws are now
  tested as laws in `core/record/src/lib.rs`.

- **A point read cost a round trip and a PUT per writer.** Every writer
  publishes its own tree and a reader's view is their merge — and merging is a
  *write*, so a point read on a fresh reader materialised W-1 merged trees and
  stored their nodes. Measured: 1 wait and 0 PUTs at one writer; 141 waits, 100
  PUTs and 4.3 s modelled at 64. Linear in writers, on the path a KV or OLTP
  workload takes for every operation, against a design whose headline property
  is that any number of writers converge without coordination.

  A point read does not need the merge — it needs the values under one key, at
  most one per tree. `pond_index::get_from_roots` descends every root in
  lockstep, one batch per level, and the caller folds the results with the same
  `resolve_records` the merge would have applied. **141 waits to 1, 100 PUTs to
  0**, flat in writer count. The fold is only safe because the record merge was
  made unconditionally commutative and associative earlier — with the old
  tie-break, fold order would have changed the answer.
  `core/engine/tests/writer_scaling.rs` pins both the cost and the agreement
  with the merged scan.

  Scans took the same route and are fixed the same way:
  `scan_from_roots` / `scan_range_from_roots` walk every root together and
  return values grouped by key for the caller to fold. Measured at 64 writers
  with 200 rows each: **1 round trip, 0 PUTs, all 12,800 rows**, against a
  merge-first scan that paid per writer.

  Both walks also deduplicate each level. Writers branch from common history
  and nodes are content-addressed, so a shared subtree has the same hash in
  every tree holding it, and reading it once per tree fetches identical bytes
  repeatedly. Sixteen writers publishing byte-identical content now cost fewer
  than 16 requests rather than exactly 16. Collapsing duplicates is safe
  because merging a value with itself is that value — idempotence, one of the
  three laws the record merge is tested against.

- **Four ways the three vector search paths disagreed.** `VectorLens::search`
  picks HNSW if an index exists, else IVF, else a linear scan — the caller does
  not choose and usually cannot tell which ran, so they must agree. They did
  not, and each fault was invisible from inside a single path:

  1. Both index extensions read the collection through the legacy manifest
     only, so building either on an engine-backed collection failed with "no
     commits" and search silently fell back to scanning.
  2. HNSW read ids from the `Int64` column alone. Ids are stored as strings
     when they do not all parse as numbers, so every result on a string-keyed
     collection came back with an **empty id** — correct distances, unusable
     answers.
  3. IVF *iterated* that same column, so on the same collection its loop body
     never ran and search returned **no results at all**, indistinguishable
     from "nothing is near your query".
  4. HNSW returned **squared** Euclidean distance while the linear scan
     returned the real one. Squaring is monotone, so neighbours and their order
     were identical and no recall test could see it; only a caller filtering on
     a distance threshold would notice, and only once someone built an index.
     Its own unit test asserted 25 for the 3-4-5 triangle, pinning the bug.

  Also: the two builders accepted different spellings of the same metric —
  HNSW `"l2"`, IVF `"euclidean"` — each rejecting the other's, through an API
  that presents them as interchangeable.

  Fixed with shared helpers rather than parallel edits, since divergence was
  the fault: `pond_core::id_strings` reads an id column of either type,
  `pond_storage::read_all_pnd2` dispatches on format and handles all three
  legacy head shapes, and IVF now writes one blob per cluster that it owns —
  which also prunes better, because a collection row group holds vectors from
  many clusters and probing one used to fetch every group containing a member.
  `lenses/vector/rust/tests/search_paths_agree.rs` compares all three paths
  against brute force on both formats.

- **A reader could write.** Two ways, both closed. Merging is a write and a
  reader's view is the merge of every writer's tree, so point reads and scans
  stored merged nodes — fixed by walking the roots together. And `Tree::build`
  with no entries writes a five-byte empty leaf, so `root_of` on a collection
  that does not exist performed a PUT: a question that created state, and one
  a read-only credential would have rejected outright. `Tree::empty` now
  returns a tree with no stored root and the walks recognise it, so nothing has
  to exist for an empty tree to behave like one. On object storage this also
  paid the worst rate available — a PUT is roughly 12.5× a GET — for work
  nobody asked for. `core/engine/tests/reader_is_read_only.rs` checks every
  read entry point against both an existing and a missing collection; the
  missing case is where it hid.

- **Scans waited once per node instead of once per level.** A cold full scan of
  100,000 rows was 51 sequential round trips at batch width 1.0 — nothing in
  the read path issued a parallel request — although every leaf hash is known
  from the internal nodes before the first leaf is fetched. `collect` and
  `collect_range` now walk a level at a time through a new
  `NodeStore::get_batch`, which `EngineStore` routes to the backend's parallel
  batch read. Measured: **51 waits to 4**, width 1.0 to 12.8, 1610 ms to 200 ms
  modelled, on an unchanged 51 requests. Range pruning happens before the batch
  is issued, so a narrow range still reads almost nothing.
  `core/engine/tests/fan_out.rs` pins it — this is a property with no
  correctness signal, so without a direct assertion it regresses silently.

- **The vector lens lost vectors three different ways.** All three verified in
  one function, `VectorLens::commit`. (a) It called the legacy writer while
  `search` and `get_all` dispatch on format, so on an engine-backed collection
  0 of 50 sampled vectors were retrievable after a successful 100-row commit.
  (b) `insert` auto-commits every 10,000 rows and the legacy writer stores a
  whole-collection snapshot, so a 25,000-row load committed the first 10,000,
  replaced them with the second 10,000, then replaced those with the final
  5,000 — 8 of 40 sampled vectors survived, matching the reported 5,000 of
  25,000. The legacy path now writes the union of what is stored and what is
  staged; the engine path appends natively and needs no read. (c) Every
  dimension name was `Box::leak`ed on every commit — a comment called it "a
  known pattern for dynamic column names", but the leak is per commit, so a
  128-dimension collection committed a thousand times leaks 128,000
  unreachable strings. A local that outlives the borrow does the same job.
  `lenses/vector/rust/tests/bulk_and_dispatch.rs` covers both formats either
  side of the 10,000 boundary, which is where the existing tests stopped.

- **The lakehouse lens wrote rows that could not be read back.** Verified:
  against an engine-backed collection, `create_table` returned `Ok` with a
  commit hash and the following `read_table` returned `Ok([])`. The read path
  dispatched on the collection's format and the write path did not — it called
  the legacy writer unconditionally, so the rows went into a legacy manifest
  while the reader looked at the engine. Both halves looked correct in
  isolation, and every test that used one format alone passed. `create_table`
  and `insert` now dispatch as the read side does. `insert` also stops doing a
  read-modify-write on the engine path: the engine appends natively, so
  rewriting the merged set added a second copy of every existing row —
  inserting [3, 4] into [1, 2] gave [1, 1, 2, 2, 3, 4] — and skipping it also
  removes an O(table) read from every insert.

- **Vector search returned the wrong answers above ten dimensions.** Verified:
  with the previous ordering, 0 of 40 exact-match queries at 11 and at 32
  dimensions found their own vector at distance 0. A vector is stored one
  dimension per column, and four places reassembled it independently — three
  sorted the column names as strings, and the IVF search path did not sort at
  all, so it and the IVF build path used different coordinate frames. String
  order matches numeric order up to `dim_9` and diverges once `dim_10` exists,
  which is why the small tests passed. One shared
  `pond_core::dim_columns_in_order` now orders numerically at all four sites.
  `lenses/vector/rust/tests/dimension_order.rs` pins the exact boundary:
  eight dimensions passes either way, eleven is the first broken case.

- **`create` overwrote a live collection during a read outage.** Verified: with
  reads failing, `engine_path::create` on a populated Engine collection
  returned `Ok` and replaced its definition — a new random `chunk_salt` and an
  empty column list. The salt decides where content-defined chunk boundaries
  fall, so every later write would chunk against boundaries nothing already
  stored shares, destroying structural sharing and deterministic rebuild
  silently. Both of its guards failed *open*: `definition::load` turns an
  unreadable definition into `legacy()`, and `has_legacy_data` turned a failed
  listing into an empty one, so an unreachable store looked like a blank slate.
  `create` now establishes emptiness instead of assuming it, via a listing that
  reports failure (`PondKernel::try_list_names_prefix`), and refuses when the
  store lists a definition object that cannot be read — the one place "absent"
  and "unreadable" can be told apart. `core/storage/tests/create_outage.rs`;
  the reproduction fails without the guard, and three sibling tests show
  creation, idempotence and legacy refusal are unchanged.

- **A failed node read became a short answer.** `EngineStore::get` mapped every
  backend error to `None`, which a traversal reads as "empty subtree", so one
  failed GET on a 20,000-row collection returned `Ok` with **0 rows**. The
  write path already refused this — `put` panics rather than let a failed write
  become a hash referencing nothing — and the read path did the opposite two
  lines below. Failures are now counted, and every read path refuses a result
  a failure passed through. `core/engine/tests/read_errors.rs`; four of its
  five tests fail without the fix, and the fifth exists to show that an
  honestly empty result is still `Ok`.

- **Nothing on the shipped read path cached anything.** `pond_cache_config()`
  returned `CacheConfig::default()`, which has no disk tier, so four identical
  scans each paid full price and the warm column of the round-trip audit was
  unreachable through the public API. Now on by default under the platform
  cache directory, with `POND_CACHE_DIR=off` to disable: measured 22 round
  trips and 2447 KiB down to 3 and 0.1 KiB, per scan, through `read_rows`.

- **A later write could be discarded inside one millisecond.** The columnar
  path used the row index within the batch as the logical clock, which
  restarts at zero, so a one-row update carried `logical = 0` against the
  `logical = 999` of the row it replaced. Fixed with a monotonic counter that
  reserves a block per batch; see `core/storage/tests/write_ordering.rs`.

## Claimed but false — do not re-investigate

- **"A write without `_rowid` silently overwrites/loses rows."** It does not.
  A row with no supplied `_rowid` is a *new* row, by design, and the write
  appends. A probe that looked up a row by scanning for the first matching
  `id` found the older duplicate and reported a lost update; the row count
  showed the append plainly. Verified by row count before and after.
