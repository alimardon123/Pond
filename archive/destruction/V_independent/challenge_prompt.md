You are a systems engineer who has never seen the Pond project before.
You are given a formal specification for a storage kernel and asked to
implement a Git-like version control View on top of it.

You have NO access to any existing Pond code. You must implement using
ONLY the specification below.

After implementing, you must honestly report:
1. Was the spec sufficient? Could you implement without asking questions?
2. Where was the spec ambiguous?
3. What did you have to invent (patterns, conventions, structures)?
4. What was impossible or required guessing?

--- THE SPECIFICATION (everything you receive) ---

Pond Kernel — Formal Specification

THE LAWS (architectural invariants):

Law 1: Objects are immutable.
  Once written, the bytes at a hash never change. The hash is a pure
  function of the bytes (content-addressing): h = H(b) where H is a
  cryptographic hash function (SHA-256).

Law 2: Objects are addressable.
  Every object has a stable identifier (its hash). The identifier is
  derivable (anyone with the bytes can compute the hash) and verifiable
  (anyone with the bytes can check the hash matches).

Law 3: Names are mutable.
  Names (strings) can be mapped to object hashes. The mapping is mutable:
  names can be created and updated. The namespace is the only mutable
  state in the system.

Law 4: References never mutate objects.
  Updating a name to point to a different hash does not modify any
  object. Objects are only created and read, never modified.

Law 5: Objects are backend-independent.
  The laws make no assumption about where objects are stored. Any
  backend that can "store bytes by key" and "fetch bytes by key"
  satisfies the laws.

THE API (one realization of the laws):

  Write(data: bytes) -> hash: string
    - Takes raw bytes
    - Returns a 64-character hex string (SHA-256 of the bytes)
    - Same bytes always produce the same hash (dedup for free)
    - The bytes are now stored permanently; you can read them back
    - The bytes at this hash never change (Law 1)

  Read(hash_or_name: str) -> bytes
    - If given a 64-char hex hash, returns those bytes
    - If given a name (any other string), resolves the name to a hash
      via the root namespace, then returns those bytes
    - Error: NOT_FOUND if hash doesn't exist or name isn't bound

  Reference(name: str, hash: str) -> ()
    - Sets a mutable mapping: name -> hash
    - This is the ONLY mutation in the system
    - The hash must already exist (Write was called)
    - If name was previously bound, the old binding is replaced
    - Error: HASH_NOT_FOUND if hash doesn't exist

COMPOSITION LAWS:

  - Reference chains: if name N resolves to hash H1, and H1's bytes
    mention hash H2, the kernel does NOT auto-resolve H2. Deeper
    indirection is a View concern.

  - Reference moves: overwriting a name orphans the old hash. The
    kernel does not track reference history.

  - GC: not provided. Orphaned objects accumulate. Views define
    reachability and implement GC.

  - Backend substitution: same operation sequence on different backends
    produces the same state.

  - Snapshot: reading at a hash is a consistent snapshot. Reading at
    a name resolves to the current hash, then reads that snapshot.

  - Branching: a branch is a name pointing to a commit hash. Creating
    a branch is just Reference(branch_name, commit_hash). O(1).

  - Cross-View isolation: the kernel has no isolation. Views must use
    naming conventions or separate kernel instances.

WHAT THE LAWS DO NOT GUARANTEE:
  - Multi-writer coordination (no CAS, no transactions)
  - Causal consistency
  - Transactional visibility
  - Cross-region linearizability
  - Garbage collection
  - Time travel performance (walking history is O(N))

--- END OF SPECIFICATION ---

YOUR TASK:

Implement a Git-like version control View. Your View must support:
1. add(path, content) — stage a file for commit
2. commit(message) — create a commit with staged files
3. read_file(path) — read a file from the current commit
4. log() — show commit history
5. branch(name) — create a branch
6. checkout(name) — switch to a branch or commit

Your implementation must use ONLY the three API operations
(Write, Read, Reference) plus whatever internal structures you
choose to build on top.

Use Python. Assume the kernel is a class with methods:
  kernel.write(data: bytes) -> str  (returns hash)
  kernel.read(hash_or_name: str) -> bytes
  kernel.reference(name: str, hash: str)
  kernel.resolve(name: str) -> str | None  (resolve name to hash)

Implement your View as a class. Then test it with a small scenario:
  - init repo
  - add file1.txt with "hello"
  - add file2.txt with "world"
  - commit "initial"
  - add file1.txt with "hello world" (modify)
  - commit "update file1"
  - create branch "feature"
  - checkout "feature"
  - add file3.txt
  - commit "add file3 on feature"
  - checkout "main" (back to main)
  - log() on main
  - read_file("file1.txt") on main
  - read_file("file2.txt") on main
  - file3.txt should NOT exist on main

After implementing, report honestly:
1. Was the spec sufficient?
2. Where was it ambiguous?
3. What did you invent?
4. What was impossible or required guessing?

Write the code and the report. Save the code as independent_git_view.py
and the report as independent_report.md.
