# GraphView External Validation Report

> Task 12. Built from `SDK_SPEC.md` + `pond-core/pond_minimal.py` only.
> Implementation: `validation/graph_view_external.py` (≈600 LOC).
> Test suite: `validation/graph_view_external_test.py` (64 assertions,
> all passing).

---

## 1. Was the SDK sufficient? Could you implement without asking questions?

**Partial — yes for the 10 settled ambiguities (A–J), no for several
new ones.**

I was able to build a working `GraphView` with all required operations
(add_node, add_edge, get_node, get_neighbors, find_nodes_by_type,
find_edges_by_type, delete_node with cascade, delete_edge, count_nodes,
count_edges, commit, branch, checkout, merge, history) plus diff and
drop_index — entirely from `SDK_SPEC.md` and the 140-LOC kernel, with
64/64 tests passing on the first complete run (after fixing two bugs,
both of which were spec gaps rather than my mistakes — see §3).

The 10 settled ambiguities (A–J) are **genuinely settled**. I never
had to guess on:
- A (kernel construction — `PondMinimal(base_dir)` worked as documented),
- B (extractor takes decoded data — clear, though I built my indexes
  manually rather than via the extractor API),
- C (`get()` complexity — the spec describes the DAG-walk clearly
  enough that I could implement it),
- D (merge = union + merged-branch-wins — unambiguous; my test
  confirms the merged branch's value silently wins),
- E (index naming — `f"{view_name}__index__{index_name}"` is exact),
- G (diff takes hash prefixes — explicit),
- H (history's 5-key dict — explicit and verified by test),
- I (put_raw stages existing hash without encoding — explicit and
  verified by test),
- J (commit binary format — documented, though §7 itself says view
  authors don't need to know it; I used JSON instead, which the spec
  permits).

But I hit ~6 new ambiguities (see §3) that required invention, and
**one** (the tombstone-blob gap, §3 item 1) that crashed my code on
first run and forced me to read the kernel source to understand why.
Without that workaround, the spec's own example code from §4.5 / §8
/ RFC-0008 §6 would crash on a fresh kernel.

So: the spec is sufficient to **build** a GraphView, but not sufficient
to build one **without surprises**. The surprises are now in different
places than A–J — the A–J fixes worked.

---

## 2. Where was it ambiguous?

I'll cite the spec section I tried to follow for each.

**2a.** `SDK_SPEC.md` §4.4 — *"Indexes are stored as Prolly trees in
the kernel's object store."* The spec never defines what a Prolly tree
IS or how to serialize one. §7 mentions
`BinaryProllyTree.encode_commit` but says "Views do NOT need to know
this format" — and that's about commit objects, not index trees. For
an external developer who cannot read `pond-sdk/prolly_view.py`, the
Prolly tree is a black box. I substituted a JSON-encoded dict as my
"index tree" (a single kernel blob, root pointer is a Reference, per
§4.4's other requirements). This satisfies the spec's *described
behavior* but not its *literal* "Prolly tree" requirement.

**2b.** `SDK_SPEC.md` §4.4 — *"`_index/{index_name}/{index_key}` →
the data blob's hash"*. For a multi-valued index (e.g., `by_node_type`
where many nodes share type `"user"`), the index_key maps to *multiple*
data blob hashes. The spec doesn't say how. A list at the leaf?
Multiple tree entries with a common prefix? A serialized set? This
matters enormously for `find_nodes_by_type` (which returns a list). I
invented: store the index as `{index_key: [primary_keys...]}` in a
single JSON blob. The spec would let me do almost anything here.

**2c.** `SDK_SPEC.md` §3.2, §4.1 — *"`find_by()`"*. The spec mentions
`find_by(index_name, key)` repeatedly but never specifies its return
shape. Single value? List? `None` when not found? `[]` when not
found? RFC-0008 §6's example shows `"""Look up by index. Returns []
if index is dropped."""` — implying a list — but `SDK_SPEC.md` itself
doesn't restate this. (For my own `find_nodes_by_type` I avoided the
issue by inventing my own method, but a view author trying to use the
SDK's `find_by` directly would not know what to expect.)

**2d.** `SDK_SPEC.md` §5.2 — *"Switches the View's HEAD to the named
branch."* Where is "the current branch" tracked? In the kernel
namespace? In a View attribute? The spec is silent. I used an
in-memory `self._current_branch` attribute — but this means current-
branch state is lost on process restart, which seems to contradict
§5.1's claim that branches are persistent kernel References. The spec
needs to say either (a) "current branch is in-memory, lost on restart"
or (b) "current branch is stored under name
`f'{view_name}__current_branch'` and survives restart".

**2e.** `SDK_SPEC.md` §6.1 — *"Build a new Prolly tree from `merged`,
write a new snapshot commit, advance HEAD."* Does the merge commit
have 1 parent (current HEAD only) or 2 parents (current HEAD + merged
branch HEAD, like git)? The spec doesn't say. With 1 parent, my
`history()` walks only the current branch's chain — the merged
branch's commits don't appear in `history()`. With 2 parents,
`history()` would need to walk both, and the linear-index semantics
from §6.2 break down. I implemented 1-parent merge (the simpler
choice) but the spec doesn't authorize either.

**2f.** `SDK_SPEC.md` §6.2 — *"the commit's position in the DAG
(0 = first commit)"*. For a linear chain this is unambiguous. For a
branched DAG with merges (which §6.1 explicitly supports), "position
in the DAG" is undefined — global topological order? Order along the
current branch? My implementation uses `parent.index + 1`, which
gives a per-branch linear count but produces collisions/inconsistencies
across branches. The spec doesn't address this.

**2g.** `SDK_SPEC.md` §8 — *"`from maintenance import (TOMBSTONE_HASH,
drop_name, ...)`"*. The import path is unspecified. Is `pond-sdk/`
on the path? Is the module `maintenance`, `pond_sdk.maintenance`, or
`pond.maintenance`? The task rules forbade me from reading pond-sdk,
so I couldn't check; I re-defined the helpers from the RFC-0008 §2
formula. A real external developer (without my task constraints)
would also be guessing here.

**2h.** `SDK_SPEC.md` §11 (compliance checklist) — *"The View extends
`View` or `IndexedView`."* The spec assumes I'm extending an existing
base class. But §7 explicitly contemplates "developers building
alternative View implementations that need to interoperate with
`ProllyViewBase`'s commit format" — implying alternative
implementations are allowed. The spec doesn't tell me how to build a
View **without** extending `View`/`IndexedView`. Since I couldn't
read pond-sdk, I built `GraphView` directly on the kernel — which is
per §7's spirit but not per §11's letter. The spec needs to either
( a ) provide enough info to re-implement `View`/`IndexedView` from
scratch, or ( b ) explicitly authorize kernel-direct Views and
relax §11.

---

## 3. What did you have to invent?

**3.1. The tombstone-blob pre-write (forced invention — code crashed
without it).**
SDK_SPEC.md §4.5 and RFC-0008 §6 both show `drop_name(kernel, name) →
kernel.reference(name, TOMBSTONE_HASH)`. But the kernel's `reference()`
(pond_minimal.py:155-156) validates that the target hash exists on disk
and raises `ValueError("Hash ... does not refer to an existing blob")`
otherwise. On a fresh kernel, `TOMBSTONE_HASH`'s blob doesn't exist,
so the spec's example code crashes. I invented
`_ensure_tombstone_blob(kernel)` which lazily writes
`b"__pond_tombstone__"` (whose SHA-256 IS `TOMBSTONE_HASH` per RFC-0008
§2's definition) before the rebinding. **The spec author should have
caught this by running their own example.** This is the most painful
finding in this report.

**3.2. The View constructor signature.** Spec §1.1 shows
`PondMinimal(base_dir)` for the kernel but no section shows how to
construct a View. I invented `GraphView(kernel, name)` because §4.4
and §5.1 imply a `view_name` attribute is needed (it appears in
`f"{view_name}__index__{index_name}"` and `f"{view_name}__branch__{name}"`).

**3.3. Current-branch tracking.** I invented an in-memory
`self._current_branch = "main"` attribute, with `_head_ref() →
f"{self.name}__branch__{self._current_branch}"`. See §2d above.

**3.4. Key naming conventions.** Spec §2.1 says `put(key, data)` takes
a string key but gives no naming guidance. I invented `node:{node_id}`
and `edge:{from_id}:{to_id}:{edge_type}` prefixes (no leading `_` so
they survive `get_all()`'s exclusion rule from §3.3). The spec should
either mandate a convention or explicitly say "View authors choose
their own key names."

**3.5. Index storage format.** Per §2a/§2b above, I invented: each
index is a single JSON-encoded `{index_key: [primary_keys...]}` dict,
written as one kernel blob, with the root pointer as a Reference. This
satisfies §4.4's described behavior (kernel blob + Reference naming)
but is not a "Prolly tree" in the literal sense.

**3.6. Index update mode.** §4.3 says modes (eager/lazy/background)
are `IndexedView`-only. Since I built on the kernel directly, I had
to pick one. I chose **eager** (rebuild both indexes on every commit)
because it's the simplest correct choice and matches §4.3's "always
fresh" guarantee. The spec doesn't tell me what an external View
author should pick.

**3.7. Merge-commit parent count.** Per §2e above: I invented 1-parent
merge (current HEAD only). The merged branch's commits are reachable
via `diff()` against the merge commit, but not via `history()`.

**3.8. First-commit-is-snapshot rule.** Spec §7 says COMPACTION_THRESHOLD
= 4 controls snapshot vs delta, but doesn't explicitly say the FIRST
commit (no parent) must be a snapshot. A delta commit with no parent
is nonsensical (nothing to delta against). I invented `is_snapshot =
(parent is None) OR (deltas_since_last_snapshot >= 4)`. Test #4
verifies this gives the expected snapshot/delta/delta/delta/delta/snapshot
pattern.

**3.9. `find_nodes_by_type` / `find_edges_by_type` fallback when index
is absent.** Per §4.5, after `drop_index` the index Reference is
tombstoned, so `_read_index` returns None. I invented a linear-scan
fallback so the operation still returns correct results (just slower).
The spec doesn't say what should happen — should the operation raise?
Return []? Fall back? I chose fallback because the operation's
*semantics* (find all nodes of type X) is still well-defined even
without an index; the index is just an optimization.

---

## 4. What was impossible or required guessing?

**Impossible (could not determine from spec):**

- **The Prolly tree format.** §4.4 mandates "Prolly trees in the
  kernel object store" but the spec never defines the format. Without
  reading `pond-sdk/prolly_view.py` (forbidden by task rules, and
  forbidden in spirit for any external developer who isn't a Pond
  insider), I cannot build a spec-literal Prolly tree. I substituted
  JSON. This is a **spec compliance deviation** that the spec author
  should either accept (by amending §4.4 to allow any kernel-blob
  format) or fix (by defining the Prolly tree format in an appendix).

- **The exact behavior of `find_by()` for multi-valued indexes.** The
  spec mentions `find_by` but never defines its return shape, and §4.4's
  index-tree-as-map description doesn't cover multi-valued keys. I
  worked around this by inventing my own `find_nodes_by_type` /
  `find_edges_by_type` methods (which the task spec required anyway).
  But for an external developer trying to use the SDK's `find_by`
  directly, this is genuinely impossible to determine.

**Required guessing (could make a reasonable choice but spec didn't
authorize it):**

- View constructor signature (§3.2 above).
- Current-branch tracking location (§3.3 above).
- Merge-commit parent count (§3.7 above).
- First-commit-is-snapshot rule (§3.8 above).
- Index storage format (§3.5 above).
- Key naming convention (§3.4 above).
- Index update mode for kernel-direct Views (§3.6 above).
- Import path for `pond-sdk/maintenance.py` (§2g above).

---

## 5. Rate the developer experience (1-10) and explain.

**Score: 7/10.**

The original vector validation scored 5/10 with 10 ambiguities (A–J).
The current spec settles all 10 of those (verified: I never had to
guess on A–J). That's a real, measurable improvement: **+2 points**
for closing the identified gaps cleanly and concretely. The history
shape (§6.2), the merge semantics (§6.1), the diff parameters (§6.3),
and the put_raw semantics (§2.3) are now exactly as documented — my
tests confirm each one literally.

**What's good (the points I got):**
- The 10 ambiguities are settled with concrete, checkable contracts.
- §6.2's history-shape table is exemplary — exact keys, exact types.
- §6.1's merge algorithm is a 4-step recipe I could implement
  verbatim.
- §2.3's `put_raw` semantics are crisp; my zero-copy test (test #9)
  confirms the blob hash is shared, not re-encoded.
- §4.5's tombstone pattern for `drop_index` is well-explained —
  *once you work around the marker-blob gap*.
- The cross-reference table in §0 is excellent: I always knew which
  section settled which original ambiguity.
- §10 ("What is deliberately NOT in the SDK") saves enormous time —
  I never wasted effort looking for transactions, query planning,
  schema, or compression.

**What's keeping it from 9 or 10:**
- The **tombstone-blob crash** (§3.1) is the most damaging finding.
  The spec's own example code doesn't run on a fresh kernel. This is
  the kind of thing a single end-to-end test of the spec's example
  would have caught. -1 point.
- The **Prolly tree format is referenced but never defined** (§3.5,
  §4 impossibility #1). This makes "spec-literal" index implementation
  impossible for an external developer. -1 point.
- The **multi-valued index storage** gap (§2b) and the undefined
  `find_by()` return shape (§2c) mean the spec doesn't actually tell
  you how to build a useful index — only how to name its Reference.
- The **View constructor signature** and **current-branch tracking
  location** (§3.2, §3.3) are surprisingly unspecified given how
  fundamental they are.
- The **import path for `pond-sdk/maintenance.py`** (§2g) is a small
  thing but it's the first thing a developer types and the spec
  doesn't cover it.

**What would move it to 9/10:**
1. Add a one-line `kernel.write(b"__pond_tombstone__")` step (or a
   note) before the `drop_name` example in §4.5/§8/RFC-0008 §6. **One
   line of spec text fixes the worst finding.**
2. Either define the Prolly tree format in an appendix, or amend §4.4
   to say "Indexes are stored as kernel blobs in any format the View
   chooses; the root pointer is a Reference named
   `f'{view_name}__index__{index_name}'`. The literal Prolly tree
   format is internal to `ProllyViewBase`."
3. Specify `find_by()` return shape in §3.2 or §4.1: "Returns a list
   of decoded values (possibly empty); returns `[]` if the index is
   tombstoned."
4. Specify multi-valued index storage in §4.4: "For multi-valued
   indexes, the value at `_index/{index_name}/{index_key}` is a
   serialized list of primary keys."
5. Specify the View constructor signature in §1 or §2: `View(kernel,
   name)` or similar.
6. Specify current-branch tracking: either "in-memory, lost on
   restart" or "stored under `f'{view_name}__current_branch'`".
7. Specify the import path for `pond-sdk/maintenance.py`: e.g.,
   "Add `pond-sdk/` to your `PYTHONPATH` and `from maintenance
   import ...`" or restructure to `from pond_sdk.maintenance
   import ...`.

**What would move it to 10/10:** All of the above, plus a single
end-to-end worked example in the spec — e.g., "build a tiny KeyView
from scratch in 30 lines, using only the kernel and this spec" —
that the spec author has actually run. The tombstone-blob gap would
not survive such an exercise.

**Bottom line:** The spec is now genuinely good. The A–J settlements
are real and verified. The remaining gaps are mostly "the spec
describes behavior but not implementation" issues that a careful
developer can work around — except for the tombstone-blob crash,
which is a true bug-in-the-spec-example that should be fixed
immediately.
