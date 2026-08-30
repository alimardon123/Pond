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

## Reported by review, not yet verified here

Recorded so they are not lost, and marked so nobody treats them as established.
The last review asserted a lost-update bug that turned out to be a
misreading; nothing goes above this line until it has been reproduced.

- Lakehouse and Vector lenses write legacy manifests into engine collections,
  return `Ok`, and lose the data — `create_table` succeeds, `read_table`
  returns `[]`.
- HNSW reportedly reads the whole collection per query — bytes measured linear
  in N. The dimension-ordering half of this finding was verified and is fixed
  below; the per-query cost is not yet reproduced here.
- The canonical URI is not percent-encoded, so a collection named `a?b` signs
  and writes to a different key than intended.
- A head is ~85 bytes per collection and is rewritten in full on every publish,
  so a single-row write at 10^6 collections would move ~85 MB.
- `engine_kv`, `engine_oltp` and `engine_stream` have no callers outside their
  own tests, and `KeyValueLens::get` is still a full scan.

## Fixed, kept here for the record

- **Merging was not commutative when versions tied exactly.** Two processes
  sharing a writer id — `stable_writer_id` derives one per host and user, not
  per process — could write different values at the same physical time and
  logical counter, and the merge kept whichever argument came first. Fixed by
  breaking the tie on the canonical encoding of the value; the laws are now
  tested as laws in `core/record/src/lib.rs`.

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
