# Reading only the columns you asked for

Every scaling dimension in this system is now flat except one. Rows, writers
and collections all cost the same however many there are — measured, in
`docs/ROUND_TRIP_AUDIT.md` and the benchmarks beside it. Columns are not:
**selecting two columns out of fifty reads all fifty.**

This document is the design for fixing that, the arithmetic showing why the
obvious cheaper fix does not work, and an honest account of what it costs. It
is written before the code because the change is to the leaf format, and a
format change that turns out to be wrong is expensive in a store that cannot
rewrite what it already holds.

## The measurement

`cargo run --release -p pond_bench --bin projection`, 20,000 rows:

| columns | query | KiB read | modelled ms | ideal KiB | ideal ms |
| ---: | --- | ---: | ---: | ---: | ---: |
| 8 | full | 777.4 | 75.2 | – | – |
| 8 | 2 of 8 | 777.4 | 75.2 | 194.4 | 63.8 |
| 50 | full | 6085.7 | 178.9 | – | – |
| 50 | 2 of 50 | **6085.7** | **178.9** | 243.4 | 64.8 |

Not approximately the same — the same number of bytes, to the byte. A record is
stored whole as its leaf entry's value, so reaching any field means reading the
leaf, and projection drops the unwanted fields *after* they have crossed the
network. At fifty columns that is **25× more bytes than the query needs**.

The "ideal" column is a lower bound: the selected columns' share of the bytes.
A real implementation also pays for keys and for per-chunk framing, so the
achievable figure is somewhat above it. The gap is large enough that the
distinction does not change the decision.

This is the shape the comparison against a columnar engine on object storage is
about. A system that reads 6 MB to answer a query needing 250 KB is not
competitive on scan-heavy work, whatever its round-trip count.

## Why the existing spill mechanism cannot do this

`core/engine/src/spill.rs` already moves values out of leaves: a field above
`SPILL_THRESHOLD` is written as its own object and the record keeps a pointer.
`resolve_fields` fetches only the pointers a projection asks for — so
**projection already works, for spilled fields.** The obvious cheap fix is
therefore to spill more aggressively, and it does not work. The arithmetic:

A spilled field costs, inside the record, a type tag plus a 32-byte hash — call
it 33 bytes. At fifty columns of roughly ten bytes each:

```
inline           50 × 10   =   500 bytes per row, all of it in the leaf
all spilled      50 × 33   = 1,650 bytes per row, all of it in the leaf
```

A two-column projected scan must read the leaf either way. Spilling everything
would make it read **1,650 bytes per row instead of 500** — three times worse
than doing nothing — and then fetch two column objects on top. Per-record
spilling trades bytes-in-leaf for bytes-out-of-leaf at a fixed 33-byte cost per
field per row, and that trade only pays when a field is much larger than 33
bytes. It cannot pay for narrow columns, which is exactly the case that is
broken.

The pointer has to be **per leaf**, not per record. That is the whole design.

## The design

A leaf becomes a row group in the PAX sense: rows are grouped together, and
within the group the values are stored by column.

```
leaf entry     key -> row index within the group        (small, always read)
column chunk   one object per (leaf, column)            (read only if selected)
leaf footer    column name -> chunk hash, row count     (small, always read)
```

Reconstructing row *i* means taking element *i* from each selected column
chunk. Reading two columns of fifty fetches two chunks, not fifty.

Three properties that make this fit rather than fight the rest of the system:

- **Chunks are content-addressed like everything else.** Two leaves with an
  identical column share the object, and rewriting one column of a leaf leaves
  the other forty-nine chunks untouched — the same structural sharing that
  makes the data trees cheap to update, applied one level down.
- **The row group boundary already exists.** Content-defined chunking decides
  where leaves end, so the row groups come for free and are stable across
  writers — two writers with the same rows produce the same groups and
  therefore the same chunks.
- **Column pruning composes with key pruning.** The descent already skips
  subtrees outside a range using each child's key range; column selection skips
  chunks within the leaves that survive. The two are independent and multiply.

## What it costs

Stated plainly, because these are the reasons not to do it:

**A point read gets more expensive.** Today a record is one leaf entry: read
the leaf, decode, done. Under this design a full record needs every column
chunk of its group — one batch, so one extra round trip rather than one per
column, but a round trip that a row-major layout does not pay. For a key-value
or OLTP workload, which reads whole rows by key, this is a real regression and
it is the central trade. Mitigation is to keep narrow collections row-major:
the format already carries a version, and a collection records its own layout
in its definition, so both can exist and each collection can take the one that
suits it. A collection that is read by key and a collection that is scanned by
column want different layouts, and pretending otherwise is how systems end up
mediocre at both.

**Write amplification changes shape.** Updating one field of one row rewrites
that column's chunk for the whole group, rather than one record. For
append-heavy and column-update workloads this is better; for scattered
single-field updates it is worse.

**Garbage collection has more to walk.** `core/storage/src/maintenance.rs`
already walks records to find spilled payloads; it would need to walk footers
to find chunks. The `incomplete` fail-safe that stops a vacuum deleting on an
unreadable record must extend to unreadable footers — the same rule, one more
place to apply it.

**Old data stays readable forever.** A content-addressed store cannot rewrite
what it holds, so the row-major leaf encoding does not go away. Both decode,
chosen by the tag, as with record v1/v2 and head v1/v2/v3.

## What to do first

Not the format. First a benchmark that fixes the *target*: extend
`pond_bench --bin projection` to report the ideal alongside the actual at
several widths and selectivities, so the change has a number to be judged
against rather than a direction to move in. That exists now, and produced the
table above.

Then, in order:

1. Column chunk encode/decode with frozen golden bytes, in isolation — the
   codec precedent from `docs/LEAF_ENCODING.md` applies: the bytes are the
   identity, so they get pinned before anything depends on them.
2. The leaf footer, behind a tag, with the row-major path untouched.
3. A collection-level layout choice in the definition, defaulting to
   row-major, so nothing changes until a collection asks for it.
4. The projected read path, measured against the table above.
5. Only then, a default — and only for collections whose access pattern the
   measurements say benefit.

The last step is the one to resist rushing. Every measurement in this document
is a scan; none of them is the point read that this design makes slower, and
choosing a default without that number would be choosing on half the evidence.
