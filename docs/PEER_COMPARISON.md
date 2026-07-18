# Pond vs. Peer Systems — Honest Comparison

Pond is NOT competing with Iceberg, Delta, or table formats. The real
peer set is systems with similar ambitions: a minimal substrate for
immutable state with mutable references. This document compares Pond
honestly against each peer — what Pond shares, what Pond differs on,
and what Pond is rediscovering (vs. genuinely novel).

---

## Git

**What it is:** Version control system. Content-addressed object store
(blobs, trees, commits, tags) + mutable refs (branches, HEAD).

**What Pond shares:**
- Content-addressed immutable objects (Law 1, 2)
- Mutable names pointing to objects (Law 3) — Git's refs are Pond's Reference
- Tree/Commit as patterns over immutable blobs (Pond learned this from Git)
- Snapshot-based versioning (not delta-based)

**What Pond differs on:**
- Git is workload-specific (source code, files, directories). Pond is
  workload-agnostic (any bytes).
- Git has a specific object model (blob/tree/commit/tag). Pond's kernel
  has no object model — Tree/Commit are View patterns.
- Git is single-repo. Pond is a substrate for many simultaneous
  "repos" (Views) sharing one object store.
- Git's namespace is hierarchical (refs/heads/main). Pond's namespace
  is flat (name -> hash). Views can build hierarchy on top.

**What Pond is rediscovering:** The content-addressed object + mutable
ref model. This is not novel — Git had it in 2005. Pond's contribution
(if any) is applying it to a wider workload space (databases, not just
source code).

**Where Pond could learn from Git:**
- Pack files for compaction (Git's `git gc` packs loose objects into
  compressed archives). Pond's Views could use a similar pattern.
- Reflog for recovery (Git tracks ref changes for undo). Pond's
  namespace could benefit from a reflog.
- Shallow clones for partial replication. Pond Views could support
  fetching only the blobs reachable from a specific commit.

---

## Irmin

**What it is:** A content-addressable store with mutable references,
designed as a data structure library (OCaml). Built by MirageOS.
Based on Git's model but programmable.

**What Pond shares:**
- Content-addressed blobs
- Mutable references (Irmin's `head` is Pond's `Reference`)
- Multiple "stores" sharing one backend
- Programmable Views (Irmin calls them "backends")

**What Pond differs on:**
- Irmin is OCaml-native. Pond is language-agnostic (specified as laws).
- Irmin has a specific merge model (3-way merge with user-provided
  conflict resolvers). Pond has no merge model — Views choose.
- Irmin is designed for MirageOS unikernels. Pond is designed for
  general-purpose data infrastructure.
- Irmin exposes a tree abstraction (Irmin tree = Pond's Tree pattern).
  Pond's kernel has no tree abstraction; it's View-level.

**What Pond is rediscovering:** The "content-addressable store as
universal data structure" idea. Irmin had it. Pond's contribution
(if any) is separating the kernel (laws) from the View (patterns)
more strictly than Irmin does.

**Where Pond could learn from Irmin:**
- Merge semantics. Irmin has a well-defined 3-way merge. Pond has
  none. Views that need merge (Git-like, LakeFS-like) would benefit
  from a View-level merge library.
- Atomic snapshots across multiple keys. Irmin supports this. Pond's
  Reference is single-key; multi-key atomicity is a View concern.

---

## IPFS / IPNS

**What it is:** IPFS is a content-addressed distributed file system.
IPNS is its mutable naming layer (one mutable pointer per node key).

**What Pond shares:**
- Content-addressed immutable objects (IPFS CIDs = Pond hashes)
- Mutable pointers (IPNS = Pond's Reference, but IPNS is one-per-node)
- Backend independence (IPFS can use file, network, bitswap; Pond can
  use FS, S3, Redis, FDB)

**What Pond differs on:**
- IPFS is P2P (bitswap, DHT). Pond is client-server (one kernel
  instance, optionally replicated).
- IPNS is one mutable pointer per node. Pond's Reference is arbitrary
  names. (Identity Destruction II, Exp 1 found that Pond COULD reduce
  to SetRoot, the IPNS model.)
- IPFS is designed for "the permanent web." Pond is designed for
  data infrastructure (databases, ML, streaming).
- IPFS has no View concept. Objects are just files. Pond's Views
  interpret bytes (SQL, vectors, streaming, Git, etc.).

**What Pond is rediscovering:** Content-addressing + mutable naming.
IPFS had it. Pond's contribution (if any) is the View layer — IPFS
objects are opaque files; Pond objects are interpreted by Views that
give them structure (tables, vectors, graphs, etc.).

**Where Pond could learn from IPFS:**
- CIDs (Content Identifiers) — IPFS's multihash format supports
  multiple hash algorithms. Pond hardcodes SHA-256. CID's flexibility
  is worth considering.
- DAG-PB and DAG-JSON — IPFS has standardized graph formats. Pond's
  Tree/Commit patterns are ad-hoc JSON. A standardized graph format
  would help View interop.
- Pinning — IPFS lets you "pin" objects to prevent GC. Pond's Views
  need a similar mechanism for "keep this object reachable."

---

## LakeFS

**What it is:** Git-like versioning for object storage. Branches,
commits, merges over S3 objects.

**What Pond shares:**
- Content-addressed immutable objects
- Mutable branches/refs
- Object storage native (LakeFS is S3-native; Pond is backend-agnostic
  but works on S3)
- Snapshot-based versioning

**What Pond differs on:**
- LakeFS is workload-specific (object storage versioning). Pond is
  workload-agnostic.
- LakeFS has commits, branches, merges as first-class concepts.
  Pond's kernel has no commits/branches — they're View patterns.
- LakeFS is a server. Pond is a library (with optional server).
- LakeFS's namespace is hierarchical (repo/branch/path). Pond's
  namespace is flat. Views can build hierarchy.

**What Pond is rediscovering:** "Git for object storage." LakeFS had
it. Pond's contribution (if any) is that LakeFS's Git-like model is
ONE View on Pond, not the architecture. Pond can also host SQL,
vectors, streaming, OCI — none of which LakeFS supports natively.

**Where Pond could learn from LakeFS:**
- Branch-as-first-class. LakeFS's branch model (copy-on-write,
  branch-specific commits) is more developed than Pond's
  "Reference is a branch" convention.
- Hook system. LakeFS has pre/post hooks on commits. Pond Views
  could benefit from a similar hook mechanism.
- Garbage collection policies. LakeFS has well-defined GC rules
  (keep reachable from any branch). Pond's GC is undefined.

---

## FoundationDB

**What it is:** A distributed transactional key-value store. Layered
architecture: minimal core (ordered KV) + layers (SQL, document, graph).

**What Pond shares:**
- Minimal substrate philosophy (FDB's core is tiny; Pond's kernel is tiny)
- Layered architecture (FDB layers = Pond Views)
- Backend independence (FDB runs on shared-nothing clusters; Pond runs
  on FS/S3/Redis/FDB)
- "The kernel coordinates; layers interpret" separation

**What Pond differs on:**
- FDB is KV (ordered key-value). Pond is content-addressed (hash-keyed).
- FDB has ACID transactions in the core. Pond has no transactions
  (Reference is single-key, last-writer-wins).
- FDB's layers are stateless (they store state in FDB itself). Pond's
  Views can have their own state (View-local caches, namespace stores).
- FDB is write-optimized for OLTP. Pond is read-optimized for analytics
  (content-addressed blobs, immutable).

**What Pond is rediscovering:** The "minimal substrate + layers"
architecture. FDB had it. Pond's contribution (if any) is that the
substrate is content-addressed (not ordered KV), which gives dedup,
integrity, and immutability for free — properties FDB doesn't have.

**Where Pond could learn from FDB:**
- Transactional layer. FDB's layers (Record Layer, Document Layer)
  build transactions on the KV substrate. Pond Views that need
  transactions could use a similar approach.
- Deterministic simulation testing. FDB's `simulation.fdb` runs
  thousands of randomized tests. Pond needs a similar testing
  discipline for distributed scenarios.
- Layer composition. FDB layers compose (Document Layer on Record
  Layer on KV). Pond Views don't yet compose (each View is standalone).

---

## Dolt

**What it is:** Git-for-data. A SQL database with version control
(branches, commits, merges, diffs) built on Prolly trees.

**What Pond shares:**
- Content-addressed immutable storage
- Versioning (branches, commits)
- "Database with Git semantics" ambition

**What Pond differs on:**
- Dolt is SQL-specific. Pond is workload-agnostic.
- Dolt uses Prolly trees (content-addressed B-trees) as the storage
  primitive. Pond uses flat content-addressed blobs; Views build
  their own tree structures.
- Dolt has merge semantics for SQL tables. Pond has no merge —
  Views choose.
- Dolt is a single database. Pond is a substrate for many
  simultaneous workloads.

**What Pond is rediscovering:** "Version control for structured data."
Dolt had it. Pond's contribution (if any) is that Dolt's model is ONE
View (SQLView with versioning), not the architecture. Pond can also
host non-SQL versioned workloads (Git, OCI, ML).

**Where Pond could learn from Dolt:**
- Prolly trees. Dolt's content-addressed B-trees are more efficient
  than Pond's flat blobs + View-level Trees. A Pond View could use
  Prolly trees for structured data.
- Diff algorithm. Dolt can diff two database versions efficiently.
  Pond Views that need diff would benefit from a similar algorithm.
- Merge semantics for structured data. Dolt's 3-way merge for SQL
  tables is well-developed. Pond Views could reuse it.

---

## Honest summary: what is Pond's actual contribution?

After comparing to 6 peers, Pond's potential contributions are:

1. **Stricter kernel/View separation than any peer.** Git, Irmin,
   LakeFS, Dolt all have some workload assumptions in the core.
   Pond's kernel has none. This is genuine but must be validated
   by workloads that break peer systems (Identity Destruction II).

2. **Content-addressing as the primary addressing model.** FDB uses
   ordered KV; Pond uses content-addressing. This gives dedup,
   integrity, immutability for free. IPFS has this too, but IPFS
   is P2P and file-oriented; Pond is client-server and
   workload-agnostic.

3. **Laws over APIs.** Most peers specify their architecture as APIs.
   Pond (after Experiment 3) specifies as laws, with APIs as
   realizations. This is a documentation/specification difference,
   but it matters for longevity.

4. **Multiple workload types on one substrate.** Git does source code.
   LakeFS does object storage. Dolt does SQL. FDB does KV. Pond does
   all of these (via Views) on one substrate. Whether this is a
   feature or a "jack of all trades, master of none" remains to be seen.

**What Pond is NOT contributing:**
- Content-addressing (Git, IPFS had it)
- Minimal substrate (FDB had it)
- Versioning (Git, LakeFS, Dolt had it)
- Layered architecture (FDB had it)
- Views/layers (FDB, Irmin had it)

Pond's contribution is the *combination*: minimal + content-addressed +
workload-agnostic + laws-specified. Whether this combination is valuable
depends on whether workloads actually benefit from sharing a substrate,
or whether they're better served by purpose-built systems (Git for
source code, LakeFS for object storage, Dolt for SQL, FDB for KV).

That question is not yet answered. The destruction phase and Identity
Destruction II are testing it.
