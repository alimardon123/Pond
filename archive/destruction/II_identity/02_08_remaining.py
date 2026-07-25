"""
Identity Destruction II — Experiments 2, 4, 5, 6, 7, 8

Continuing the attack on foundational assumptions. Each experiment
tries to falsify a different assumption. Each ends in:
  Supported / Falsified / Inconclusive / Needs larger-scale validation

Experiments:
  2. Namespace model: is name->hash the right model?
  4. Can names disappear? (deletion, reachability, GC)
  5. Can references be CRDTs? (multi-writer/multi-region)
  6. Can two namespaces overlap? Compose? Conflict?
  7. Is hash primitive? (alternatives: location, capability, content-query)
  8. Is immutability binary? (tiered immutability)
"""

import os
import shutil
import sys
import json
import sqlite3
import hashlib
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "prototype"))
from kernel import PondMinimal, hash_bytes
from views_minimal import write_tree, read_tree, write_commit, read_commit


def section(title):
    print()
    print("=" * 76)
    print(f"  {title}")
    print("=" * 76)


# ---------------------------------------------------------------------------
# Experiment 2: Namespace model — is name->hash the right model?
# ---------------------------------------------------------------------------

def exp2_namespace_model():
    section("Experiment 2: Is name->hash the right namespace model?")
    print()
    print("  Current model: Reference(name, hash) — flat string -> hash mapping.")
    print("  Question: is this the right model, or just one option?")
    print()

    # Survey alternative namespace models
    models = [
        ("Flat name -> hash",
         "Reference('orders', h)",
         "Current. Simple. Views can encode structure in the name string."),

        ("(name, epoch) -> hash",
         "Reference('orders', epoch=5, h)",
         "Versioned namespace. Each name has multiple epochs. Useful for"
         " time-travel without walking commit chains. But: who advances epoch?"
         " Concurrent writers race on epoch increment."),

        ("Hierarchical paths",
         "Reference('refs/heads/main', h)",
         "Git's model. Names are paths. Allows namespace operations like"
         " 'list all refs/heads/*'. But: the kernel doesn't natively support"
         " path operations; Views must implement them."),

        ("Tenant + name",
         "Reference('tenant_42/orders', h)",
         "Multi-tenant. Encodes tenant in the name. Works with flat model"
         " but loses tenant isolation guarantees."),

        ("Capability token",
         "Reference(cap_token, h) where cap_token grants specific access",
         "Capability-based security. Names are unforgeable tokens. But:"
         " requires the kernel to verify capabilities, which is a security"
         " concern, not a storage concern."),

        ("Graph edge",
         "Reference(src, edge_label, dst_hash)",
         "Graph-structured namespace. Names are (src, label) pairs. Useful"
         " for graph databases. But: this is a Lens concern (GraphView"
         " already builds this on top of flat namespace)."),

        ("Content query",
         "Lookup(predicate) -> [hashes]",
         "No names; find objects by predicate. Pure content-addressing."
         " But: O(N) scan per lookup. Unusable at scale without indexes"
         " (which are mutable state — back to needing a namespace)."),
    ]

    print("  Model                          | Example                           | Analysis")
    print("  -------------------------------|-----------------------------------|----------")
    for name, example, analysis in models:
        print(f"  {name:<32}| {example:<35}| {analysis[:40]}")

    print()
    print("  Analysis:")
    print()
    print("  1. Flat name->hash is the SIMPLEST model. All others can be")
    print("     ENCODED into it (hierarchical paths = 'refs/heads/main' as a string;")
    print("     tenant+name = 'tenant_42/orders' as a string).")
    print()
    print("  2. The richer models (epoch, capability, graph edge, content query)")
    print("     offer SEMANTIC guarantees that flat names don't:")
    print("     - epoch: atomic version advancement")
    print("     - capability: unforgeable access tokens")
    print("     - graph edge: typed edges between objects")
    print("     - content query: no names at all")
    print()
    print("  3. These richer guarantees CANNOT be provided by Views on top of")
    print("     flat names — they require kernel-level enforcement. For example:")
    print("     - A View can't enforce epoch atomicity (concurrent writers race).")
    print("     - A View can't enforce capability unforgeability (strings are forgeable).")
    print()
    print("  4. BUT: the current kernel doesn't enforce these either. They're")
    print("     explicitly out of scope (Formal Spec, 'What the laws do NOT guarantee').")
    print()
    print("  VERDICT: SUPPORTED — flat name->hash is the right KERNEL model.")
    print("  Richer namespace models are View/infrastructure concerns, not kernel")
    print("  primitives. The kernel provides the minimal mutable surface; Views")
    print("  and infrastructure layers (Raft, capability systems, graph engines)")
    print("  build richer semantics on top.")
    print()
    print("  This is consistent with the Admission Rule: richer namespace models")
    print("  fail the 'Universal' criterion (only some workloads need epochs,")
    print("  capabilities, or graph edges) and the 'Impossible outside kernel'")
    print("  criterion (Views CAN encode them, just without kernel enforcement).")


# ---------------------------------------------------------------------------
# Experiment 4: Can names disappear? (deletion, reachability, GC)
# ---------------------------------------------------------------------------

def exp4_name_deletion():
    section("Experiment 4: Can names disappear?")
    print()
    print("  Current API: Reference(name, hash) — creates/updates a name.")
    print("  No Delete operation. Names can be overwritten but not removed.")
    print()
    print("  Question: is name deletion needed? What breaks without it?")
    print()

    bench_dir = "/tmp/pond_name_deletion"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    # Write a blob and reference it
    h = kernel.write(b"some data")
    kernel.reference("temp_table", h)
    print(f"  Created name 'temp_table' -> {h[:16]}...")

    # Can we delete it? Current API has no Delete.
    # Workaround 1: overwrite with a sentinel
    sentinel = kernel.write(b"")  # empty blob as tombstone
    kernel.reference("temp_table", sentinel)
    print(f"  Workaround 1: overwrote with empty blob (tombstone)")
    print(f"    Now 'temp_table' -> {sentinel[:16]}... (empty bytes)")

    # Workaround 2: just leave it (namespace pollution)
    print(f"  Workaround 2: leave it (namespace pollution)")
    print(f"    'temp_table' still exists, just points to tombstone")

    # Workaround 3: Lens-level namespace store with delete
    # (Views that need deletion maintain their own namespace)
    print(f"  Workaround 3: Lens-level namespace with delete")
    print(f"    Views that need deletion use their own namespace store,")
    print(f"    not the kernel's root namespace.")

    print()
    print("  Analysis:")
    print()
    print("  1. Name deletion is NOT a kernel primitive. The kernel's root")
    print("     namespace is append/update-only (Reference creates or overwrites).")
    print()
    print("  2. This is consistent with Law 3 (names are mutable) — mutability")
    print("     includes overwrite, but the laws don't require deletion.")
    print()
    print("  3. Deletion has semantic implications:")
    print("     - If a name is deleted, the blob it pointed to might become orphaned.")
    print("     - GC (Finding 6) needs to know which names exist to determine reachability.")
    print("     - If names can disappear, GC must handle 'name was deleted, blob orphaned.'")
    print()
    print("  4. Views that need deletion can:")
    print("     - Use a tombstone convention (point to empty blob)")
    print("     - Maintain their own namespace store (with delete) outside the kernel")
    print("     - Use a 'deleted_names' set (a blob listing deleted names)")
    print()
    print("  5. Adding Delete to the kernel would:")
    print("     - Add a fourth primitive (violates minimality)")
    print("     - Require GC to handle deletion (more complex)")
    print("     - Not be universally needed (OCI, ML, TimeSeries don't delete)")
    print()
    print("  VERDICT: SUPPORTED — name deletion is NOT a kernel primitive.")
    print("  The kernel's namespace is update-only. Views that need deletion")
    print("  use tombstones or maintain their own namespace stores. This is")
    print("  consistent with the Admission Rule (deletion fails 'Universal').")
    print()
    print("  CAVEAT: GC (Finding 6) needs to handle the case where a name was")
    print("  overwritten (old blob orphaned) — but NOT where a name was deleted")
    print("  (since deletion isn't a kernel operation). This simplifies GC.")

    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Experiment 5: Can references be CRDTs? (multi-writer/multi-region)
# ---------------------------------------------------------------------------

def exp5_crdt_references():
    section("Experiment 5: Can references be CRDTs?")
    print()
    print("  Scenario: multiple writers in different regions update the same")
    print("  name concurrently. Current kernel: last-writer-wins (single root namespace).")
    print("  Question: could references be CRDTs for multi-writer/multi-region?")
    print()
    print("  CRDT (Conflict-free Replicated Data Type) options for references:")
    print()

    crdts = [
        ("Last-Writer-Wins (LWW)",
         "Current. Each Reference has a timestamp; highest timestamp wins.",
         "Requires synchronized clocks. Current kernel does this implicitly"
         " (last write to SQLite wins)."),

        ("Multi-Value (MV-Register)",
         "Concurrent writes produce multiple values; View resolves.",
         "Like DynamoDB. The namespace returns a set of hashes; the Lens"
         " picks one (or merges). Requires vector clocks."),

        ("Grow-Only Set (G-Set)",
         "Names map to a SET of hashes, never removed.",
         "Append-only. Useful for event logs, not for 'current value' semantics."),

        ("Add-Wins Set (OR-Set)",
         "Names map to a set; adds win over removes.",
         "Useful for tag sets, not for 'current version' semantics."),

        ("Versioned (LWW-Register with version)",
         "Reference(name, hash, version); highest version wins.",
         "Like LWW but with explicit version instead of timestamp."
         " Requires version monotonicity."),
    ]

    for name, desc, analysis in crdts:
        print(f"  {name}:")
        print(f"    {desc}")
        print(f"    {analysis}")
        print()

    print("  Analysis:")
    print()
    print("  1. The current kernel's Reference is LWW (last-writer-wins).")
    print("     This is a CRDT — the simplest one.")
    print()
    print("  2. Richer CRDTs (MV-Register, OR-Set) could be implemented as")
    print("     Lens-level namespace stores. The kernel's Reference stays LWW;")
    print("     Views that need richer CRDT semantics build them on top.")
    print()
    print("  3. CRDTs require metadata (vector clocks, version vectors) that")
    print("     the kernel doesn't provide. A Lens-level CRDT namespace would")
    print("     store this metadata in blobs, interpreted by the Lens.")
    print()
    print("  4. The kernel's LWW Reference is sufficient for single-writer")
    print("     scenarios (the current scope). Multi-writer requires either:")
    print("     - A coordination layer (Raft) on the root namespace, OR")
    print("     - A CRDT namespace implemented as a Lens")
    print()
    print("  5. Neither requires kernel changes. The kernel's LWW Reference is")
    print("     the minimal mutable surface; richer coordination is layered on top.")
    print()
    print("  VERDICT: SUPPORTED — references are already LWW CRDTs.")
    print("  Richer CRDT semantics (MV-Register, OR-Set) are View/infrastructure")
    print("  concerns, not kernel primitives. The kernel provides the minimal")
    print("  mutable surface; Views and coordination layers build richer semantics.")
    print()
    print("  This is consistent with the destruction phase's finding that")
    print("  content-addressing handles idempotent writes but NOT concurrent")
    print("  reference races. CRDTs are the fix — and they're a Lens concern.")


# ---------------------------------------------------------------------------
# Experiment 6: Can two namespaces overlap? Compose? Conflict?
# ---------------------------------------------------------------------------

def exp6_namespace_composition():
    section("Experiment 6: Can namespaces overlap? Compose? Conflict?")
    print()
    print("  Current: one root namespace per kernel instance. All names are global.")
    print("  Question: can namespaces be composed? Can they overlap? Conflict?")
    print()

    bench_dir = "/tmp/pond_ns_composition"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    # Test 1: two names pointing to the same hash (overlap)
    h = kernel.write(b"shared data")
    kernel.reference("view_a_table", h)
    kernel.reference("view_b_table", h)
    print(f"  Test 1: Two names, same hash")
    print(f"    'view_a_table' -> {h[:16]}...")
    print(f"    'view_b_table' -> {h[:16]}...")
    print(f"    Both resolve to same bytes: {kernel.read('view_a_table') == kernel.read('view_b_table')}")
    print(f"    ✓ Namespaces CAN overlap (multiple names, same object). No conflict.")
    print()

    # Test 2: two Views with different naming conventions
    h1 = kernel.write(b"sql data")
    h2 = kernel.write(b"git data")
    kernel.reference("sql:users", h1)   # SQL View's convention
    kernel.reference("git:refs/heads/main", h2)  # Git View's convention
    print(f"  Test 2: Two Views with different naming conventions")
    print(f"    'sql:users' -> {h1[:16]}... (SQL View)")
    print(f"    'git:refs/heads/main' -> {h2[:16]}... (Git View)")
    print(f"    ✓ Views CAN coexist with prefix conventions. No kernel enforcement needed.")
    print()

    # Test 3: name collision (conflict)
    h3 = kernel.write(b"version 1")
    h4 = kernel.write(b"version 2")
    kernel.reference("shared_name", h3)
    kernel.reference("shared_name", h4)  # overwrites!
    print(f"  Test 3: Name collision (two Views write same name)")
    print(f"    View A: 'shared_name' -> {h3[:16]}...")
    print(f"    View B: 'shared_name' -> {h4[:16]}... (overwrites)")
    print(f"    Final: 'shared_name' -> {kernel.resolve('shared_name')[:16]}...")
    print(f"    ✗ Name collision: last-writer-wins. View A's data is orphaned.")
    print()

    print("  Analysis:")
    print()
    print("  1. Namespaces CAN overlap (multiple names, same object). This is")
    print("     a feature: Views can share data without copying.")
    print()
    print("  2. Views CAN coexist with naming conventions (prefix, path). The")
    print("     kernel doesn't enforce conventions; Views agree on them.")
    print()
    print("  3. Name collisions are resolved last-writer-wins. This is a KNOWN")
    print("     limitation for multi-View scenarios. Mitigations:")
    print("     - Naming conventions (prefix per View: 'sql:*', 'git:*')")
    print("     - Lens-level namespace stores (each View has its own namespace)")
    print("     - Coordination layer (Raft) for multi-View consistency")
    print()
    print("  4. Namespace COMPOSITION (merging two namespaces) is not a kernel")
    print("     operation. Views that need composition (e.g., merge two Git repos)")
    print("     implement it at the Lens level (read both namespaces, write a merged one).")
    print()
    print("  VERDICT: SUPPORTED — namespaces can overlap and coexist.")
    print("  Name collisions are resolved LWW (known limitation, mitigations exist).")
    print("  Namespace composition is a Lens concern, not a kernel primitive.")
    print()
    print("  This is consistent with the Admission Rule: namespace composition")
    print("  fails 'Universal' (not all Lenses need it) and 'Impossible outside")
    print("  kernel' (Views CAN implement it by reading and writing names).")

    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Experiment 7: Is hash primitive? (alternatives)
# ---------------------------------------------------------------------------

def exp7_hash_alternatives():
    section("Experiment 7: Is hash primitive? (alternatives)")
    print()
    print("  Current: Write(bytes) returns SHA-256(bytes). Hash is the object identifier.")
    print("  Question: is content-addressing (hash = H(bytes)) the right model?")
    print("  Alternatives: location addressing, capability tokens, content queries.")
    print()
    print("  (Note: Experiment 1 of Identity Destruction I already tested removing")
    print("   content-addressing. Verdict: DERIVED — kernel works without it but")
    print("   loses dedup, integrity, immutability. This experiment goes deeper.)")
    print()

    alternatives = [
        ("Content-addressing (current)",
         "hash = SHA-256(bytes)",
         "Dedup, integrity, immutability. Universal. Survives 2045 (swap hash algo)."),

        ("Location addressing",
         "id = sequential integer or path",
         "No dedup. No integrity. No immutability enforcement. Simpler but weaker."),

        ("Capability token",
         "id = unforgeable random token",
         "Security property (only token holders can access). But: token must be"
         " stored somewhere (back to needing a namespace). Doesn't give dedup."),

        ("Content query",
         "no ids; find by predicate",
         "O(N) scan per lookup. Unusable at scale without indexes (mutable state)."
         " Essentially: no addressing at all, just search."),

        ("Hybrid (content + location)",
         "id = (hash, location_hint)",
         "Content-addressing for identity + location hint for performance."
         " This is what IPFS CIDs + provider records do. Pond could adopt it"
         " as a Lens-level optimization without changing the kernel."),

        ("Multi-hash (IPFS CID)",
         "id = (hash_algo, hash)",
         "Supports multiple hash algorithms. Future-proofs against SHA-256 break."
         " Pond hardcodes SHA-256; CID is more flexible. Could be a kernel change"
         " OR a Lens-level convention (prefix hash with algo identifier)."),
    ]

    for name, model, analysis in alternatives:
        print(f"  {name}:")
        print(f"    Model: {model}")
        print(f"    Analysis: {analysis}")
        print()

    print("  Analysis:")
    print()
    print("  1. Content-addressing is the ONLY model that gives dedup + integrity")
    print("     + immutability simultaneously. Other models sacrifice one or more.")
    print()
    print("  2. The kernel's choice of SHA-256 is NOT primitive — it's an")
    print("     implementation choice. The PRIMITIVE is 'hash = H(bytes) for some")
    print("     cryptographic hash function H.' SHA-256 could be swapped for")
    print("     BLAKE3, SHA-3, or a post-quantum hash without changing the laws.")
    print()
    print("  3. Multi-hash (IPFS CID style) is a candidate for kernel admission:")
    print("     - Universal? Yes — all Lenses benefit from hash agility.")
    print("     - Impossible outside kernel? Yes — Views can't change the hash")
    print("       function the kernel uses.")
    print("     - Immutable? Yes.")
    print("     - Storage-independent? Yes.")
    print("     - Decades-stable? Yes — multi-hash future-proofs against breaks.")
    print()
    print("     Multi-hash PASSES the Admission Rule. It's a candidate for v0.8.")
    print()
    print("  4. Capability tokens and content queries are NOT kernel primitives.")
    print("     They require infrastructure (token issuance, indexes) beyond the kernel.")
    print()
    print("  VERDICT: SUPPORTED — content-addressing (hash = H(bytes)) is primitive.")
    print("  The specific hash function (SHA-256) is NOT primitive — it's swappable.")
    print("  Multi-hash (IPFS CID style) is a candidate for kernel admission in v0.8.")
    print()
    print("  FINDING: the kernel should adopt multi-hash to future-proof against")
    print("  SHA-256 breaks (quantum computing, cryptanalytic advances). This is")
    print("  a real architectural improvement, not just an optimization.")


# ---------------------------------------------------------------------------
# Experiment 8: Is immutability binary? (tiered immutability)
# ---------------------------------------------------------------------------

def exp8_immutability_spectrum():
    section("Experiment 8: Is immutability binary?")
    print()
    print("  Current: objects are EITHER mutable (OPEN, Lens-level buffer) OR")
    print("  immutable (Written to kernel). Binary. Once Written, never changes.")
    print()
    print("  Question: could there be tiered immutability?")
    print("  E.g., 'mutable for 1 hour, then immutable' or 'mutable until sealed'.")
    print()

    tiers = [
        ("Binary (current)",
         "OPEN (mutable, Lens-level) -> Written (immutable, kernel-level)",
         "Simple. Clear invariant. Matches Git's model."),

        ("Time-bounded mutability",
         "Written objects are mutable for T seconds, then become immutable.",
         "Useful for: streaming (buffer recent events, then freeze)."
         " But: requires the kernel to track time and enforce transitions."
         " Violates Law 1 (immutability) for T seconds. Complex."),

        ("Explicit seal",
         "Objects are mutable until explicitly sealed (Seal operation).",
         "This is what v0.1 had (OPEN/SEALED lifecycle). Removed in v0.4"
         " because Views can buffer in memory and Write when ready."
         " Reintroducing it would re-add a kernel concept."),

        ("Tiered storage",
         "Objects immutable but move between storage tiers (hot -> warm -> cold).",
         "This is placement, not mutability. The object doesn't change;"
         " it just lives on different backends. Already a Lens concern"
         " (placement capability in RFC 4)."),

        ("Copy-on-write mutability",
         "Objects are mutable; writes create new versions (CoW).",
         "This is what mutable databases (Postgres) do. Pond's model is"
         " already CoW at the Lens level (new commit = new tree + new blobs)."
         " The kernel doesn't need to know about CoW."),
    ]

    for name, desc, analysis in tiers:
        print(f"  {name}:")
        print(f"    {desc}")
        print(f"    Analysis: {analysis}")
        print()

    print("  Analysis:")
    print()
    print("  1. Binary immutability is the SIMPLEST model that satisfies Law 1.")
    print("     Any tiered model adds complexity to the kernel.")
    print()
    print("  2. Time-bounded mutability violates Law 1. If objects can change")
    print("     for T seconds, reads during that window might see different bytes.")
    print("     This breaks snapshot consistency (Invariant 4). REJECTED.")
    print()
    print("  3. Explicit seal (OPEN/SEALED) was rejected in v0.4 (Rejected Designs #3).")
    print("     Views can buffer in memory; the kernel doesn't need to track OPEN vs SEALED.")
    print()
    print("  4. Tiered storage (hot/warm/cold) is placement, not mutability.")
    print("     Already a Lens concern. No kernel change needed.")
    print()
    print("  5. Copy-on-write is already how Views work. No kernel change needed.")
    print()
    print("  VERDICT: SUPPORTED — immutability is binary.")
    print("  Tiered immutability either violates Law 1 (time-bounded) or is already")
    print("  handled at the Lens level (CoW, placement). The kernel's binary")
    print("  immutability is the minimal model that satisfies the laws.")
    print()
    print("  No kernel change needed. Views that need 'mutable-then-immutable'")
    print("  semantics (e.g., streaming buffers) implement them at the Lens level")
    print("  using in-memory state + Write when ready.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 76)
    print("  Identity Destruction II — Experiments 2, 4, 5, 6, 7, 8")
    print("  Continuing the attack on foundational assumptions.")
    print("=" * 76)

    exp2_namespace_model()
    exp4_name_deletion()
    exp5_crdt_references()
    exp6_namespace_composition()
    exp7_hash_alternatives()
    exp8_immutability_spectrum()

    section("IDENTITY DESTRUCTION II — SUMMARY")
    print()
    print("  Experiment                                    | Verdict")
    print("  ----------------------------------------------|------------------------------------------")
    print("  1. Is Reference primitive?                    | INCONCLUSIVE (leaning: not in current form)")
    print("  2. Is name->hash the right namespace model?   | SUPPORTED (flat is minimal; richer = View)")
    print("  3. Is the kernel an API or laws?              | SUPPORTED (laws endure, APIs evolve)")
    print("  4. Can names disappear?                       | SUPPORTED (deletion is View concern)")
    print("  5. Can references be CRDTs?                   | SUPPORTED (LWW is CRDT; richer = View)")
    print("  6. Can namespaces overlap/compose/conflict?   | SUPPORTED (overlap ok; collision LWW)")
    print("  7. Is hash primitive?                         | SUPPORTED (CA is primitive; SHA-256 isn't)")
    print("  8. Is immutability binary?                    | SUPPORTED (tiered violates Law 1 or = View)")
    print()
    print("  Findings:")
    print()
    print("  - 6 of 7 experiments SUPPORTED the current architecture.")
    print("  - 1 INCONCLUSIVE (Reference might not be primitive in current form).")
    print("  - 1 NEW FINDING: multi-hash (IPFS CID style) should be admitted to the")
    print("    kernel. It passes the Admission Rule and future-proofs against hash breaks.")
    print()
    print("  Real architectural decisions surfaced:")
    print("  - Reference(name, hash) vs SetRoot(hash) — pragmatic vs minimal. Documented.")
    print("  - Multi-hash admission — candidate for v0.8. Passes Admission Rule.")
    print("  - Namespace model — flat is minimal; richer models are View concerns.")
    print("  - Immutability is binary — tiered models violate laws or are View concerns.")
    print()
    print("  The 3-primitive kernel (Write/Read/Reference) survived Identity Destruction II")
    print("  with one open question (Reference's primitiveness) and one candidate")
    print("  improvement (multi-hash). No falsifications. No new primitives needed.")
    print()
    print("  HONEST CAVEAT: these experiments are still mostly analytical. The real test")
    print("  is implementing a workload that breaks the kernel — e.g., a CRDT View that")
    print("  needs multi-writer namespace semantics the kernel can't provide. That's the")
    print("  next phase: adversarial workload implementation.")


if __name__ == "__main__":
    main()
