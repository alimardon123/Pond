# Stage 6: Human Destruction Test

> Give the kernel to someone who knows nothing about Pond. Ask them to
> implement Git, Iceberg, OCI, Feature Store, LakeFS without talking to
> you. If they can't, the kernel isn't actually simple.

This is the most important test, and the one I can't run automatically.
What I CAN do is:

1. Write the spec a stranger would receive (just the 3 primitives, no more)
2. Honestly assess whether that spec is sufficient to implement each workload
3. Identify where the spec is ambiguous or insufficient

If the spec is insufficient, that's a real finding — the kernel isn't
actually as simple as the architecture claims.

---

## The spec a stranger receives

```
Pond Kernel — 3 primitives

1. Write(bytes) -> hash
   - Takes raw bytes
   - Returns a 64-character hex string (SHA-256 of the bytes)
   - Same bytes always produce the same hash (dedup for free)
   - The bytes are now stored permanently; you can read them back

2. Read(hash_or_name) -> bytes
   - If given a 64-char hex hash, returns those bytes
   - If given a name (any other string), resolves the name to a hash
     via the root namespace, then returns those bytes

3. Reference(name, hash)
   - Sets a mutable mapping: name -> hash
   - This is the ONLY mutation in the system
   - The hash must already exist (Write was called)

That's the entire kernel. There is no Tree type, no Commit type, no
Branch, no Tag, no lifecycle. If you want those concepts, build them
from these 3 primitives by storing serialized structures as blobs.

Example: a "tree" is just a blob containing JSON like
  {"entries": {"file1.txt": "abc123...", "file2.txt": "def456..."}}
You Write that JSON as bytes, get a hash back, and Reference it by name.
```

---

## Honest assessment: can a stranger implement each workload?

### 1. Git (version control)

**What Git needs:** blobs (file contents), trees (directory snapshots),
commits (tree + parent + message), branches (named pointers to commits),
tags (named pointers to commits), history walk, diff.

**Can a stranger build this from the spec?**
- Blobs: `Write(file_contents) -> hash`. ✓ Trivial.
- Trees: `Write(json({"entries": {...}})) -> hash`. ✓ The spec even gives this example.
- Commits: `Write(json({"tree": tree_hash, "parent": parent_hash, "message": "..."})) -> hash`. ✓
- Branches/tags: `Reference("main", commit_hash)`. ✓
- History walk: read commit, follow `parent` field, repeat. ✓
- Diff: read two trees, compare entries, read differing blobs. ✓

**Verdict: YES.** The spec is sufficient. A stranger could implement Git.

**Potential confusion:** the spec doesn't say how to serialize a tree.
A stranger might choose JSON, MessagePack, protobuf, or a custom binary
format. That's fine — it's a View choice, not a kernel concern. But the
spec should explicitly say "the kernel doesn't prescribe serialization;
Views choose their own."

### 2. Iceberg (table format)

**What Iceberg needs:** data files (Parquet), manifest files (metadata
about data files), manifest lists (snapshots), snapshots (point-in-time
table state), schema evolution, partition specs.

**Can a stranger build this from the spec?**
- Data files: `Write(parquet_bytes) -> hash`. ✓
- Manifest: `Write(json({"files": [...]})) -> hash`. ✓
- Manifest list: `Write(json({"manifests": [...]})) -> hash`. ✓
- Snapshot: `Write(json({"manifest_list": ml_hash, "parent": prev_snapshot}))`. ✓
- Current snapshot: `Reference("my_table", snapshot_hash)`. ✓
- Schema evolution: store schema in the snapshot metadata. ✓
- Partition specs: store in manifest metadata. ✓

**Verdict: YES.** The spec is sufficient. A stranger could implement
Iceberg's data model (not the full Iceberg spec, but the core concepts).

**Potential confusion:** Iceberg has a specific manifest file format
(Avro). A stranger might not know to use Avro. But that's a format
choice — the kernel doesn't care. They could use JSON, Parquet, or
anything else.

### 3. OCI (container registry)

**What OCI needs:** blobs (layers, configs), manifests (references to
config + layers), tags (named pointers to manifests), content digests
(SHA-256).

**Can a stranger build this from the spec?**
- Blobs: `Write(layer_bytes) -> hash`. ✓ (Note: OCI digest = "sha256:" + hash)
- Config: `Write(config_json) -> hash`. ✓
- Manifest: `Write(json({"config": {"digest": "sha256:"+config_hash}, "layers": [...]}))`. ✓
- Tag: `Reference("myapp:v1", manifest_hash)`. ✓
- Pull: Read by tag, parse manifest, read config + layers by hash. ✓

**Verdict: YES.** The spec is sufficient. OCI maps almost perfectly to
the kernel's primitives (content-addressing IS OCI's digest model).

### 4. Feature Store (ML)

**What a feature store needs:** feature definitions, feature values
(time-series), entity definitions, training set generation, point-in-time
correctness.

**Can a stranger build this from the spec?**
- Feature definitions: `Write(json({"features": [...]})) -> hash`. ✓
- Feature values: `Write(parquet_bytes_with_timestamps) -> hash`. ✓
- Entity definitions: `Write(json({"entities": [...]}))`. ✓
- Training set: walk commits to assemble features as of a timestamp. ✓
- Point-in-time: time travel via commit parent chain. ✓ (with Finding 5a caveat)

**Verdict: YES**, with the caveat that time travel is O(N) without skip
pointers (Finding 5a). A stranger would need to know to add skip pointers
at the View level for production scale.

### 5. LakeFS (Git-like versioning for object storage)

**What LakeFS needs:** branches, commits, merges, hooks, all over S3 objects.

**Can a stranger build this from the spec?**
- Branches: `Reference("branch_name", commit_hash)`. ✓
- Commits: same as Git. ✓
- Merges: create a commit with TWO parents. The kernel allows this —
  `parent` is just a field in the commit blob; nothing prevents
  `parents: [h1, h2]`. ✓
- Hooks: View-level concern (run code before/after Reference). ✓

**Verdict: YES.** The spec is sufficient. Multi-parent commits work
because the kernel doesn't enforce single-parent.

---

## Where the spec is insufficient (honest findings)

### Finding A: The spec doesn't mention serialization

The spec says "Write(bytes)" but doesn't say how to structure those bytes.
A stranger might:
- Use JSON (verbose, but readable)
- Use protobuf (compact, but requires schema)
- Use a custom binary format (optimal, but opaque)

**This is actually fine** — the kernel is intentionally serialization-
agnostic. But the spec should say so explicitly: "The kernel does not
prescribe serialization. Views choose their own format."

### Finding B: The spec doesn't mention Tree/Commit patterns

The spec gives ONE example (a tree as JSON), but doesn't explain that
Tree and Commit are reusable patterns. A stranger might reinvent them
per-View, missing the opportunity for cross-View consistency.

**Fix:** the spec should include a "View patterns" section showing how
Tree, Commit, and Reference compose. This is documentation, not a kernel
change — the patterns work as-is, they're just not obvious from the
3-primitive spec alone.

### Finding C: The spec doesn't address GC

A stranger would quickly accumulate orphaned blobs (Write without
Reference, or Reference overwritten). The spec doesn't mention that
orphaned blobs need cleanup, or that GC is a View-level concern.

**Fix:** the spec should mention: "Blobs are immutable and never
overwritten. A blob with no References pointing to it (directly or
transitively) is orphaned. Views are responsible for GC: walk
reachability from all References, sweep unreferenced blobs."

### Finding D: The spec doesn't address time travel performance

A stranger implementing time travel would walk the parent chain — O(N).
The spec doesn't warn them that this is slow at scale, or that skip
pointers are the standard fix.

**Fix:** the spec should mention: "Time travel via parent chain is O(N).
For production scale, Views should implement skip pointers (every Kth
commit stores a back-pointer to the commit K steps back)."

### Finding E: The spec doesn't address concurrency

A stranger would assume single-writer (which is correct for the current
kernel). But the spec doesn't say how multiple Views coordinate on the
same root namespace.

**Fix:** the spec should mention: "The root namespace is single-writer
in v0. Multi-writer coordination (MVCC, OCC) is a View-level concern,
not a kernel guarantee. Views that need multi-writer should implement
their own coordination (e.g., via a Raft layer on the root namespace)."

---

## VERDICT

**SUPPORTED, with documentation gaps.**

A stranger could implement Git, Iceberg, OCI, Feature Store, and LakeFS
from the 3-primitive spec. The kernel is genuinely simple enough to be
understood without context.

BUT the spec has 5 documentation gaps (Findings A-E) that would cause
confusion or suboptimal implementations. These are documentation issues,
not kernel issues — the kernel itself doesn't need to change. The
documentation should be improved to make the patterns and pitfalls
explicit.

**This is a real finding:** the kernel is simple, but the spec is not
yet sufficient for a stranger to implement production-quality Views
without guidance. The next deliverable should be a "View Author's Guide"
that addresses Findings A-E.

---

## The test I can't run

The real Stage 6 test is: hand the spec to an actual stranger (another
developer who hasn't seen this project), give them a weekend, and see
what they build. The analysis above is a substitute — I'm assessing the
spec against my own understanding of the workloads, which is biased.

If you (the user) have a colleague who'd be willing to try this, the
spec above is what they'd receive. The findings (A-E) are the gaps
they'd likely hit.
