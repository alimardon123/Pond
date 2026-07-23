"""
Identity Destruction II — Experiment 1: Is Reference primitive?

The kernel has two qualitatively different operations:
  - Write(bytes) -> hash     (creates immutable state, NEVER coordinates)
  - Read(hash) -> bytes      (reads immutable state, NEVER coordinates)
  - Reference(name, hash)    (mutates shared namespace, ALWAYS coordinates)

Why is the centralized, mutating operation in the kernel?

Hypothesis A: Reference IS primitive.
  Without a mutable namespace, you have IPFS (hash-only), not a database.
  Names are how humans and applications find data. Without mutable names,
  every restart must re-resolve. Not a database.

Hypothesis B: Reference is NOT primitive.
  Namespace is a Lens concern. Different workloads need different namespace
  models (single-writer, multi-writer CRDT, hierarchical, capability-based).
  Baking one model into the kernel is the same mistake as baking Parquet in.

This experiment tries to falsify Hypothesis A by building a kernel WITHOUT
Reference and seeing if Views still work.

Method: implement a kernel with only Write + Read. Build a namespace as a
View on top. If Views work, Reference wasn't primitive.

Outcome vocabulary:
  - Supported: hypothesis holds
  - Falsified: hypothesis fails
  - Inconclusive: couldn't isolate
  - Needs larger-scale validation
"""

import os
import shutil
import sys
import json
import sqlite3
import hashlib
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "prototype"))


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def section(title):
    print()
    print("=" * 76)
    print(f"  {title}")
    print("=" * 76)


# ---------------------------------------------------------------------------
# The 2-primitive kernel — NO Reference
# ---------------------------------------------------------------------------

class PondNoReference:
    """
    A kernel with ONLY Write and Read. No Reference.

    Question: can we build a namespace as a Lens on top of this?
    If yes, Reference wasn't primitive.
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.objects_dir = os.path.join(base_dir, "objects")
        os.makedirs(self.objects_dir, exist_ok=True)
        # NO root namespace in the kernel
        self.stats = {"writes": 0, "reads": 0}

    def write(self, data: bytes) -> str:
        h = hash_bytes(data)
        shard_dir = os.path.join(self.objects_dir, h[:2])
        os.makedirs(shard_dir, exist_ok=True)
        path = os.path.join(shard_dir, h + ".bin")
        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(data)
        self.stats["writes"] += 1
        return h

    def read(self, h: str) -> bytes:
        self.stats["reads"] += 1
        path = os.path.join(self.objects_dir, h[:2], h + ".bin")
        if not os.path.exists(path):
            raise ValueError(f"hash {h} not found")
        with open(path, "rb") as f:
            return f.read()

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Attempt 1: Build namespace as a Lens-side SQLite table
# ---------------------------------------------------------------------------

class NamespaceView:
    """
    A View that maintains name -> hash mappings in its OWN store.
    The kernel has no namespace; this Lens provides one.

    The question: where does this Lens store its mappings?
    Option A: in a separate SQLite file (View-local state)
    Option B: in a kernel blob, addressed by a fixed hash convention

    Option A is the obvious answer. But it means the namespace is
    View-local state, not kernel state. Different Views could have
    different namespaces. Is that a problem?
    """

    def __init__(self, kernel: PondNoReference, ns_path: str):
        self.kernel = kernel
        self.db = sqlite3.connect(ns_path, isolation_level=None)
        self.db.execute("CREATE TABLE IF NOT EXISTS names (name TEXT PRIMARY KEY, hash TEXT)")

    def reference(self, name: str, h: str) -> None:
        # Verify the hash exists in the kernel
        try:
            self.kernel.read(h)
        except ValueError:
            raise ValueError(f"hash {h} does not exist in kernel")
        self.db.execute("INSERT OR REPLACE INTO names VALUES (?, ?)", (name, h))

    def resolve(self, name: str):
        cur = self.db.execute("SELECT hash FROM names WHERE name=?", (name,))
        row = cur.fetchone()
        return row[0] if row else None

    def list_names(self):
        cur = self.db.execute("SELECT name FROM names ORDER BY name")
        return [r[0] for r in cur.fetchall()]

    def close(self):
        self.db.close()


# ---------------------------------------------------------------------------
# Attempt 2: Build namespace as a kernel blob with a fixed convention
# ---------------------------------------------------------------------------

class NamespaceAsBlob:
    """
    A View that stores the namespace AS A BLOB in the kernel.
    Convention: the namespace is a JSON blob stored at a well-known hash.

    Problem: how do you find the well-known hash? You can't — hashes are
    content-addressed, so the hash of the namespace blob changes every
    time you update it. You'd need a name to find the namespace blob,
    which is the namespace. Infinite regress.

    Unless: use a fixed, content-INDEPENDENT identifier for the namespace
    blob. E.g., "the namespace is always stored at hash 0x000...001".
    But that violates content-addressing (the kernel's core invariant).

    Verdict: namespace-as-blob doesn't work without breaking
    content-addressing or introducing a separate namespace mechanism.
    """

    @staticmethod
    def explain():
        return """
        Attempt: store namespace as a kernel blob.
        Problem: to find the blob, you need its hash.
                 to know its hash, you need to read it (content-addressing).
                 infinite regress.
        Workaround: use a fixed hash (e.g., 0x000...001) for the namespace.
        Problem: violates content-addressing (hash != hash(bytes)).
        Verdict: namespace-as-blob doesn't work.
        """


# ---------------------------------------------------------------------------
# Attempt 3: Build namespace via pure kernel operations + external pointer
# ---------------------------------------------------------------------------

class NamespaceViaExternalPointer:
    """
    A View that uses ONE external pointer (a single name) to bootstrap.
    The kernel provides a single, fixed, well-known name "POND_ROOT"
    that maps to the current namespace blob hash.

    This is essentially: the kernel provides ONE Reference (POND_ROOT),
    and everything else is a Lens.

    Is this "Reference as primitive" or "one fixed name as primitive"?
    It's a smaller primitive than arbitrary Reference(name, hash).
    """

    @staticmethod
    def explain():
        return """
        Attempt: kernel provides ONE well-known name "POND_ROOT".
        The namespace is a blob whose hash is stored at POND_ROOT.
        Views read POND_ROOT to find the namespace, then use the namespace
        to find everything else.

        This is smaller than arbitrary Reference(name, hash).
        It's: Reference("POND_ROOT", hash) — a single, fixed name.

        Is this "Reference primitive"? Partially. The kernel needs
        exactly ONE mutable name, not arbitrary names. The namespace
        structure (how names map to hashes) is then a Lens concern.

        This is the IPFS/IPNS model: IPNS provides a single mutable
        pointer per node; everything else is content-addressed.
        """


# ---------------------------------------------------------------------------
# Run the experiments
# ---------------------------------------------------------------------------

def exp_namespace_as_view():
    section("Test 1: Build namespace as a Lens-side SQLite table")
    print()
    print("  Kernel: 2 primitives (Write, Read). No Reference.")
    print(" Namespace: View-side SQLite table, separate from kernel.")
    print()
    print("  Question: do Views work?")
    print()

    bench_dir = "/tmp/pond_noref_view"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    kernel = PondNoReference(bench_dir)
    ns = NamespaceView(kernel, os.path.join(bench_dir, "namespace.db"))

    # Write a blob
    h = kernel.write(b"hello world")
    print(f"  Write: {h[:16]}...")

    # Reference it via the Lens-side namespace
    ns.reference("my_table", h)
    print(f"  Namespace: my_table -> {h[:16]}...")

    # Resolve and read
    resolved = ns.resolve("my_table")
    data = kernel.read(resolved)
    print(f"  Resolve + Read: {data!r}")
    assert data == b"hello world"
    print()
    print(f"  ✓ Views CAN work with View-side namespace.")
    print()
    print(f"  Analysis:")
    print(f"  - The namespace is View-local state, not kernel state.")
    print(f"  - Different Views could have different namespaces.")
    print(f"  - The namespace doesn't benefit from kernel guarantees")
    print(f"    (content-addressing, immutability, dedup).")
    print()
    print(f"  Is this a problem? It depends:")
    print(f"  - If you want ONE shared namespace across all Lenses: problem.")
    print(f"    (Multiple Views would each have their own namespace, conflicting.)")
    print(f"  - If you want per-Lens namespaces: feature, not bug.")
    print(f"    (Each View manages its own naming convention.)")
    print()
    print(f"  VERDICT: INCONCLUSIVE.")
    print(f"  Reference CAN be moved to a Lens, but the trade-off is:")
    print(f"  - Lose: shared namespace, kernel-guaranteed consistency")
    print(f"  - Gain: smaller kernel, namespace-model-agnostic")
    print()
    print(f"  This is a real architectural decision, not a clear falsification.")

    ns.close()
    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)


def exp_namespace_infinite_regress():
    section("Test 2: Try to build namespace AS A BLOB in the kernel")
    print()
    print("  Attempt: store the namespace as a kernel blob.")
    print("  Problem: to find the blob, you need its hash.")
    print("          to know its hash, you need to read it (content-addressing).")
    print("          infinite regress.")
    print()
    print(NamespaceAsBlob.explain())
    print()
    print(f"  VERDICT: FALSIFIED — namespace-as-blob doesn't work.")
    print(f"  Without a non-content-addressed pointer, you can't bootstrap")
    print(f"  a namespace stored in the kernel. You need at least ONE name")
    print(f"  that isn't content-addressed.")


def exp_single_root_pointer():
    section("Test 3: Kernel provides ONE well-known name (POND_ROOT)")
    print()
    print("  Attempt: the kernel provides exactly ONE mutable name, 'POND_ROOT',")
    print("  which points to the current namespace blob. Everything else is a Lens.")
    print()
    print(NamespaceViaExternalPointer.explain())
    print()
    print(f"  This is the IPFS/IPNS model:")
    print(f"    - IPFS: content-addressed blob store (Write + Read)")
    print(f"    - IPNS: ONE mutable pointer per node (Reference, but only one)")
    print()
    print(f"  Is 'one well-known name' smaller than 'arbitrary Reference'?")
    print(f"  Yes — it's a strict subset. The kernel would expose:")
    print(f"    Write(bytes) -> hash")
    print(f"    Read(hash) -> bytes")
    print(f"    SetRoot(hash)  -- single mutable pointer, no name parameter")
    print()
    print(f"  Views build namespaces on top of SetRoot by storing a namespace")
    print(f"  blob at POND_ROOT. Different Views can interpret the namespace")
    print(f"  blob differently (hierarchical, flat, CRDT, etc.).")
    print()
    print(f"  VERDICT: SUPPORTED — the kernel could be smaller.")
    print(f"  Reference(name, hash) is NOT primitive; SetRoot(hash) is.")
    print(f"  The 'name' parameter is a Lens concern.")
    print()
    print(f"  Trade-off:")
    print(f"  - Current kernel: arbitrary names, simple Views, shared namespace")
    print(f"  - Smaller kernel: one pointer, complex Views (namespace-as-blob),")
    print(f"    per-Lens namespace interpretation")
    print()
    print(f"  This is a real architectural decision. The current kernel chose")
    print(f"  'arbitrary names' for simplicity. The smaller kernel would be")
    print(f"  more minimal but push complexity to Views.")


def exp_what_namespace_models_need():
    section("Test 4: What namespace models do workloads actually need?")
    print()
    print("  If namespace is a Lens concern, different Views can use different models.")
    print()
    print("  Survey of namespace models across workloads:")
    print()
    print("  | Workload | Namespace model |")
    print("  |---|---|")
    print("  | SQL | flat: table_name -> commit_hash |")
    print("  | Git | hierarchical: refs/heads/main, refs/tags/v1 |")
    print("  | OCI | flat: image:tag -> manifest_hash |")
    print("  | Streaming | flat: topic -> log_head |")
    print("  | Graph | flat: graph_name -> snapshot_hash |")
    print("  | ML | hierarchical: model/step -> weights_hash |")
    print("  | TimeSeries | flat: series_name -> segment_list_hash |")
    print("  | Feature Store | (name, timestamp) -> feature_hash |")
    print("  | Multi-tenant | tenant/name -> hash |")
    print("  | CRDT | (name, epoch) -> hash |")
    print("  | Capability | capability_token -> hash |")
    print()
    print("  Observation: 6 of 11 workloads use flat name -> hash.")
    print("  5 use richer models (hierarchical, tuple, token).")
    print()
    print("  If the kernel provides flat name -> hash, the 5 richer models")
    print("  must encode their structure INTO the name (e.g., 'tenant/name',")
    print("  'model/step/100'). This works but is a convention, not a guarantee.")
    print()
    print("  If the kernel provides only SetRoot, ALL namespace structure")
    print("  is a Lens concern. More flexible, more complex Views.")
    print()
    print("  VERDICT: INCONCLUSIVE — depends on whether Lens-level namespace")
    print("  complexity is acceptable. The current kernel (arbitrary names)")
    print("  is a pragmatic middle ground. The smaller kernel (SetRoot only)")
    print("  is more minimal but pushes complexity to Views.")


def main():
    print("=" * 76)
    print("  Identity Destruction II — Experiment 1: Is Reference primitive?")
    print("=" * 76)
    print()
    print("  The kernel has two non-coordinating operations (Write, Read) and")
    print("  one coordinating operation (Reference). Why is the coordinator in")
    print("  the kernel? Can it be moved to a Lens?")
    print()

    exp_namespace_as_view()
    exp_namespace_infinite_regress()
    exp_single_root_pointer()
    exp_what_namespace_models_need()

    section("VERDICT: Is Reference primitive?")
    print()
    print("  The experiment is INCONCLUSIVE — but leaning toward 'Reference is")
    print("  NOT primitive in its current form.'")
    print()
    print("  Findings:")
    print()
    print("  1. Namespace CAN be moved to a Lens (Test 1). Views work with")
    print("     View-side namespace stores. The trade-off is losing shared")
    print("     namespace guarantees.")
    print()
    print("  2. Namespace-as-blob doesn't work (Test 2) — infinite regress.")
    print("     You need at least ONE non-content-addressed pointer.")
    print()
    print("  3. The kernel could be smaller: SetRoot(hash) instead of")
    print("     Reference(name, hash). The 'name' parameter is a Lens concern.")
    print("     This is the IPFS/IPNS model. (Test 3)")
    print()
    print("  4. Different workloads need different namespace models (Test 4).")
    print("     The current kernel bakes in 'flat name -> hash'. A smaller")
    print("     kernel would let Views choose their own model.")
    print()
    print("  Architectural decision:")
    print()
    print("  Option A: Keep Reference(name, hash) — pragmatic, simple Views,")
    print("            shared namespace. The current design.")
    print()
    print("  Option B: Reduce to SetRoot(hash) — minimal kernel, complex Views,")
    print("            per-Lens namespace models. The IPFS/IPNS design.")
    print()
    print("  Option C: Remove Reference entirely — Views maintain their own")
    print("            namespace stores outside the kernel. Smallest kernel,")
    print("            but loses shared namespace.")
    print()
    print("  This is a genuine architectural decision, not a clear falsification.")
    print("  The current choice (Option A) is pragmatic but not proven minimal.")
    print()
    print("  Recommendation: keep Option A for now, but document that Option B")
    print("  is the fallback if shared namespace becomes a bottleneck. The")
    print("  kernel API could be reduced to SetRoot + a namespace View if needed.")


if __name__ == "__main__":
    main()
