# Postmortem: Prolly Tree Internal-Node Encoding Bug

> **Date:** Phase G, Task 29
> **Severity:** Critical (data loss at scale)
> **Root Cause:** Single incorrect function call in `ProllyTree.build()`
> **Status:** Fixed. Architecture Laws guard against regression.

---

## Summary

A single bug in `ProllyTree.build()` caused data loss at scale (100K
records showed as 4,080) and index rebuild failures (UnicodeDecodeError).
The root cause: `encode_leaf` was used for ALL tree levels, including
internal nodes above the first level. The fix: a single boolean flag
(`is_leaf_level`).

---

## The Bug

`ProllyTree.build()` constructs a multi-level tree when the number of
entries exceeds `TARGET_CHUNK_ENTRIES` (64). The construction is
bottom-up:

1. Split entries into leaf chunks of 64.
2. Each leaf chunk is encoded with `encode_leaf` → leaf blob.
3. The (max_key, leaf_hash) pairs form the next level.
4. The next level is encoded → internal node blob.
5. Repeat until one root remains.

**The bug:** Step 3 used `encode_leaf` instead of `encode_internal`
for levels above the first. This meant internal nodes — which contain
`(max_key, child_hash)` pairs — were encoded as if they were leaf
nodes containing `(key, data_hash)` pairs.

---

## Why This Caused Data Loss

When `read_all()` traversed the tree, it decoded every node as a leaf
(because they were all encoded with the leaf type byte). For internal
nodes, the `(max_key, child_hash)` pairs were misinterpreted as
`(key, data_hash)` pairs — meaning:

- The `max_key` of each chunk appeared as a "key" in the state.
- The `child_hash` (pointing to the child node) appeared as the "data hash."
- All other keys in the chunk were invisible.

With 10,000 entries and 64-entry chunks, there are 157 chunks. Only
the 157 `max_key` values were visible — exactly the count we observed.

---

## Why This Caused Index Rebuild Failures

The index rebuild (`_rebuild_index`) calls `read_all()` to get all
key→hash mappings, then tries to `decode()` each blob. For the 157
visible entries, the "hash" was actually a child node hash — pointing
to a binary Prolly tree node, not JSON data. `json.loads()` on binary
tree-node bytes → `UnicodeDecodeError`.

---

## Why the Bug Propagated Consistently

The user's review noted: "one low-level bug propagated consistently
instead of creating random corruption. That's a good property."

This is because Pond's layers are clean. The Prolly tree is the ONLY
tree structure. Every higher-level operation (`count`, `lookup`,
`read_all`, `index rebuild`) depends on the tree being correctly
encoded. A single encoding bug propagated to ALL consumers — which
made it easy to diagnose (all symptoms pointed to the same root cause)
and easy to fix (one change in `build()` fixed all symptoms).

---

## The Fix

```python
# Before (buggy): all levels use encode_leaf
for chunk in level:
    data = BinaryProllyTree.encode_leaf(chunk)  # WRONG for levels > 1
    ...

# After (fixed): first level is leaf, subsequent levels are internal
is_leaf_level = True
while len(level) > 1:
    for chunk in level:
        if is_leaf_level:
            data = BinaryProllyTree.encode_leaf(chunk)
        else:
            data = BinaryProllyTree.encode_internal(chunk)
        ...
    is_leaf_level = False
```

Plus: removed a safety valve in `lookup()` that stopped the commit
DAG walk before reaching the snapshot commit. The valve
(`if steps > COMPACTION_THRESHOLD + 1: break`) was a premature
optimization that broke correctness for keys in older snapshots.

---

## Lessons

1. **Encoding type matters.** `encode_leaf` and `encode_internal`
   produce different binary formats. Using the wrong one doesn't
   crash — it silently produces wrong data. This is the most
   dangerous kind of bug.

2. **Scale tests are essential.** The bug only manifested with >64
   entries. All existing tests used <64 entries, so the multi-level
   tree path was never exercised. The Architecture Laws (Law 9: Scale)
   now guard against this.

3. **Consistent propagation is a good sign.** The bug propagated
   consistently (all symptoms pointed to one root cause) rather than
   randomly (different symptoms in different places). This is evidence
   that the layer boundaries are clean — a low-level bug propagates
   upward predictably.

4. **Safety valves can be dangerous.** The `steps > threshold: break`
   valve in `lookup()` was meant to prevent infinite loops, but it
   silently returned `None` for valid keys. Safety valves should
   log warnings, not silently fail.

---

## Regression Protection

- **Law 9 (Scale):** 10K records, count must equal 10K.
- **Law 10 (Index):** 10K records, index lookup must succeed.
- Both are in `architecture_laws.py` and run on every CI commit.
