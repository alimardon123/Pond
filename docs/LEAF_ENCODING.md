# The remaining 12×, and why the obvious fix breaks convergence

## The measurement

A projected scan reads 26.5 KiB to deliver 2.1 KiB of small columns
(`cargo run --release -p pond_bench --bin fieldspill`). Per row that is ~135
bytes to return ~11. For a million-row scan of two small columns it is ~135 MB
where Parquet would move ~12 MB.

The gap is not "we fetch columns nobody wanted" — per-field spilling and
projection already fixed that. It is that a leaf stores whole records, and a
record repeats, once per row:

| part | bytes | what it is |
|---|---|---|
| field names | 24 | `id`, `status`, `attachment`, spelled out again |
| version stamps | 30 | a table, but per record rather than per leaf |
| record header | 12 | magic, format, counts |
| type tags and lengths | 9 | framing |
| the spilled field's hash | 32 | fetched even when the field is not |
| **actual payload** | **12** | what the caller asked for |

Two thousand rows in one leaf repeat all of that two thousand times.

## How much is actually there

Dumping a real leaf — 2000 rows, three small columns, 362,399 bytes — and
compressing it:

| | bytes | ratio |
|---|---|---|
| raw | 362,399 | |
| gzip -6 | 51,492 | **7.0x** |
| xz -6 | 34,928 | **10.4x** |

So the redundancy is real and large, and a general-purpose compressor finds
essentially all of it. That is not a coincidence: the redundancy a column store
exploits — the same field name in every row, versions that differ by a
millisecond, values from a small domain — is exactly what an LZ window finds.

**This is the useful result.** It says the remaining 12x is available without
laying values out by column at all.

## Why compressing the node bytes is wrong

The obvious implementation is to compress a node in `Node::encode` and
decompress in `Node::decode`. It is wrong, and the reason is not performance.

Every node is content-addressed, and the design rests on this:

> two writers holding the same data produce byte-identical nodes, therefore
> the same hash, therefore structural sharing, dedup, and convergent merge.

Compressed output is a property of *the compressor*, not of the data. Two
writers running different builds — a patch release of the compression library,
a different feature flag, a different backend for the same crate — can produce
different bytes for identical input. They would then write different hashes for
the same node. Nothing would be corrupt, and nothing would fail loudly: the
trees would simply stop sharing structure, dedup would quietly stop working,
and a merge that should have been O(diff) would become O(n).

Pinning a version in `Cargo.lock` does not fix it, because the writers are
different processes on different machines, possibly built months apart. This is
the same class of constraint as the chunk target and the chunk salt — which is
why those are *pinned per collection in the definition* rather than taken from
whatever the current build defaults to. A compressor's exact output cannot be
pinned that way, because it is not a parameter, it is an implementation.

## Three ways forward

**1. Hash the uncompressed bytes, store the compressed ones.**
Convergence is safe: the hash is a function of the content, so two writers agree
whatever compressor they use, and a key holds bytes that decompress to what its
hash names. The cost is that an object is no longer literally the bytes its hash
names — verifying one means decompressing it first, and an external tool cannot
check the store with `sha256sum`. It also needs a way to store under a
precomputed hash, which today's `ObjectStore` deliberately does not offer: the
trait's guarantee is that a key *is* the hash of its content, and an API that
lets a caller supply both invites storing content under the wrong name.

**2. A per-leaf dictionary, specified by us.**
Hoist field names and version stamps to the top of the leaf and have records
reference them by index. Deterministic by construction, no dependency, and it
targets exactly the redundancy measured above. The cost is that a record is no
longer self-contained: `Tree::get` returns one value's bytes today, and a value
that references its leaf's dictionary cannot be read without it. That is a
change to the `NodeStore`/`Tree` contract, in the crate everything else is built
on.

**3. Leave it, and spend the complexity elsewhere.**
135 bytes per row is not good, and it is not catastrophic either. It matters for
a full scan of a large table and not at all for the point lookups and small
range scans that most workloads are.

## What was done, and why it was none of the three

The three options above all took "use a compressor" to mean "depend on one".
Writing the codec here removes the objection entirely: its output is a function
of its input and of code that ships with the reader, so two writers cannot
disagree. Changing it later is then a deliberate format change, to be handled
the way a change to the chunk target would be — not an accident of which build
someone happens to be running.

`pond_index::pack` is a greedy LZ77 with a fixed hash, a fixed window and a
fixed token encoding. Greedy rather than optimal on purpose: the search has to
be *specified*, not merely good, and "take the longest match at the most recent
candidate position" is a rule that fits in a sentence.

`Node::encode` packs when packing is smaller and records the choice in a tag, so
a node that does not compress costs one byte rather than growing, and nodes
written before packing existed decode untouched — which a content-addressed
store requires, since it cannot rewrite what it already holds.

Measured:

| | before | after |
|---|---|---|
| a leaf of 2000 rows | 152,894 B | 24,313 B (**6.3x**) |
| projected scan of 200 rows | 26.5 KiB | **2.8 KiB** |
| updating one small field | 40.7 KiB | **2.8 KiB** |

Against a floor of 2.1 KiB — what the requested data actually weighs. The
remaining 33% is the index structure itself.

## The recommendation this replaced

Option 2, and not soon. It is the only one that is deterministic by
construction, dependency-free, and preserves "the object is the bytes its hash
names" — three properties this design has refused to trade away elsewhere and
should not trade away here. But it changes the index's core contract, and the
index is the piece every workload and every backend goes through. It deserves
its own increment with the acceptance tests written first: the existing
`incremental_insert_matches_bulk_build` and
`history_independence_across_batch_splits` are the oracle, and both must still
produce byte-identical roots.

Option 1 is tempting because it is a few lines. It should be refused for the
reason above: an object that is not the bytes its hash names is a small lie that
every future tool has to be told about.

## Also already done

Two of the four items in that table have already been paid down without
touching how values are laid out:

- version stamps went from one 24-byte stamp per *field* to a table per record
  (record format v2), and
- a spilled field's hash went from 64 characters of hex to the 32 bytes it is.

That took a record from 192 bytes to 119, and the scan floor from 40.7 KiB to
26.5 KiB. The measurement above is what is left after that.
