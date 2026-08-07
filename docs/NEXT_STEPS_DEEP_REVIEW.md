# Pond — Deep Review and Next Steps

> **Status:** Post-Round-52 review. This document captures the deep
> architectural review after completing the Go SDK + multi-column C ABI
> encoder + decode-path benchmark. It proposes the next steps aligned
> with the long-term vision: build the whole project in Rust with
> first-class Python SDK support, distributed as a small minimal
> lightweight binary (DuckDB philosophy), with a generic cross-language
> SDK solution that doesn't block performance or functionality.
>
> **Audience:** Pond contributors (human or AI agents). Read this before
> proposing the next round of work.

---

## 1. Where we are now

### 1.1 What's built

| Component | Status | Location |
|---|---|---|
| Storage kernel (Python) | ✅ Frozen | `pond-core/kernel.py` (~199 LOC) |
| Python SDK (lenses, prolly tree, extensions) | ✅ Production | `pond-sdk/` (~7300 LOC) |
| Production lenses (KeyValue, Lakehouse, Vector, Streaming) | ✅ Production | `lenses/` |
| Rust PND2 codec + C ABI | ✅ Full decoder parity, 3 encoders | `pond-rust/pond-core/` |
| Rust PyO3 wrapper | ✅ Thin glue (delegates to pond-core) | `pond-rust/pond-python/` |
| Go SDK (PND2 codec only) | ✅ Bindings + tests + benchmarks | `sdk-go/` |
| Benchmark (Rust vs Python vs C ABI) | ✅ Run, results captured | `scripts/benchmark_decode_paths.py` |

### 1.2 Benchmark findings (Round 52)

The decode-path benchmark revealed three important things:

1. **Rust decoder is 3x faster than pure-Python** (PyO3 path) — confirms
   Design Goal 3.3 (optimizations live above the core, but the core
   should not be pathologically slow).

2. **C ABI batch is 5-11x faster than PyO3 for numeric data** (169M
   rows/s vs 14M rows/s for 100K-row numeric blobs). This is the key
   result for cross-language SDKs: non-Python consumers get the full
   Rust speed without Python object conversion overhead.

3. **Per-row FFI calls are 2-3x slower than batch for string columns.**
   This motivated the new `pond_result_column_str_array` batch accessor,
   which brings string-heavy C ABI throughput to parity with PyO3.

### 1.3 What's NOT built yet

The Go SDK only exposes the PND2 **codec** (encode + decode). It does
NOT expose:
- Storage kernel operations (Write, Read, Ref)
- Collection management (ProllyTree, commits, branches, history)
- Lenses (KeyValue, Lakehouse, etc.)
- Extensions (indexing, pruning, semantic)

This is intentional and honest — the storage kernel is currently Python-
only. A future Rust implementation of the storage kernel would enable
full Go/Java/Node storage support.

---

## 2. The long-term vision

Quoting the user (Round 53):

> **We will build the whole project in Rust later with first-class
> Python SDK support.** And I would like you to prepare the generic
> solutions for other language SDKs so adding new features in Rust
> shouldn't require too much effort at all for other languages, or even
> adding whole other language support should also be simple, easy, no
> effort generic solution that doesn't block performance or full
> functionality of project at all and follows our design principles.
>
> **Final product will be a small minimal lightweight binary package
> that can be downloaded and executed anywhere and any language** just
> like following DuckDB philosophies.
>
> **I am thinking of creating another project that is a Spark/OLTP/
> Streaming/Flink alternative — small, lightweight but powerful
> (distributable if necessary) execution engine that can work with any
> source but especially pairs well with our Pond storage** to complete
> the full architecture of multi-workload platform that includes LTAP
> too. But that is out of scope for now.

### 2.1 What this means concretely

Three long-term goals shape every near-term decision:

1. **Rust-first storage stack.** The storage kernel (currently Python)
   will eventually be rewritten in Rust. The Python SDK will become a
   thin PyO3 wrapper over the Rust kernel — same pattern we already
   established for the PND2 codec.

2. **Generic cross-language SDK.** Adding a new language SDK (Java,
   Node, Ruby, etc.) should be a few days of work, not weeks. Adding a
   new Rust feature should automatically benefit all language SDKs
   without per-language updates.

3. **Single-binary distribution.** The final product is one binary
   (plus a Python wheel) that any language can use. Like DuckDB: one
   `duckdb` binary, callable from Python/Go/Java/Node/R/C++/Rust/CLI.

### 2.2 The DuckDB model (reference)

DuckDB's architecture is instructive:

```
duckdb binary (C++)
   ↓
┌────────────────┬───────────────┬────────────────┐
│ Python wheel   │ Go binding    │ Node binding   │  ...
│ (PyO3-style)   │ (cgo)         │ (N-API)        │
└────────────────┴───────────────┴────────────────┘
```

- One C++ core. One header (`duckdb.h`).
- Each language SDK is a thin wrapper over the C ABI.
- The same binary runs embedded (in-process) or as a server.
- Distribution: pip / npm / go get / apt / brew / direct download.

Pond's target architecture should mirror this:

```
pond binary (Rust)
   ↓
┌────────────────┬───────────────┬────────────────┐
│ Python wheel   │ Go binding    │ Node binding   │  ...
│ (pond-python)  │ (sdk-go)      │ (sdk-node)     │
└────────────────┴───────────────┴────────────────┘
```

---

## 3. The generic cross-language SDK problem

### 3.1 What "generic" means

Today, adding a new Rust feature requires:

1. Implement the feature in `pond-rust/pond-core/src/lib.rs`.
2. Add C ABI wrappers (`extern "C"` functions).
3. Add declarations to `pond_core.h`.
4. Add bindings in `sdk-go/internal/cabi/cabi.go`.
5. Add high-level wrappers in `sdk-go/pond/pond.go`.
6. (If Python-facing) Add PyO3 wrappers in `pond-python/src/lib.rs`.
7. Update tests in 3 places (Rust, C, Go).
8. Update `pond_core.h` documentation.

That's 8 touch points per feature. For a Java or Node SDK, add 3 more
touch points each. This does NOT scale.

**The goal:** adding a Rust feature should require 1-2 touch points
(the Rust implementation + a single IDL annotation). All language
bindings should be auto-generated.

### 3.2 Three approaches, ranked by complexity

#### Option A: Hand-written bindings (current state)

- ✅ Simple to start with
- ✅ Full control over idiomatic API per language
- ❌ Doesn't scale — N features × M languages = N×M touch points
- ❌ Easy to forget a language when adding a feature

#### Option B: C header → language bindings codegen

Write a code generator that reads `pond_core.h` and emits:
- Go bindings (cgo wrappers + high-level types)
- Java bindings (JNI + JNA-style)
- Node bindings (N-API)
- Python ctypes bindings (fallback when PyO3 isn't available)

- ✅ One source of truth (the C header)
- ✅ Adding a feature = update C header, re-run codegen
- ❌ C headers are hard to parse (preprocessor, macros)
- ❌ Generated code is less idiomatic than hand-written
- ❌ Doesn't help with PyO3 (which needs Rust types, not C)

#### Option C: Rust macro → C header + language bindings

Define a Rust macro (`#[pond_ffi]`) that:
1. Generates the `extern "C"` wrapper automatically
2. Emits the C header declaration
3. Emits language-specific binding files (Go, Java, Node, Python)

- ✅ One source of truth (the Rust function definition)
- ✅ Adding a feature = annotate the Rust function, done
- ✅ Generated bindings can be language-idiomatic (with templates)
- ❌ More upfront work to build the macro + codegen
- ❌ Rust macros are complex; debugging is harder

### 3.3 Recommendation: Option C (incrementally)

Start with a simpler version: a `build.rs` script that scans the Rust
source for `#[no_mangle] pub extern "C"` functions and generates:
1. The C header (`pond_core.h`) — always in sync with the source
2. A JSON manifest of all C ABI functions (name, args, return type)
3. Language-specific binding files from templates

The JSON manifest is the key — it's the single source of truth that
language-specific codegens consume. Each language SDK has a small
template that maps JSON manifest entries to idiomatic code.

```
pond-rust/pond-core/src/lib.rs
    ↓ (build.rs scans for #[no_mangle] extern "C")
pond-rust/pond-core/abi_manifest.json
    ↓
┌────────────────┬───────────────┬────────────────┐
│ C header gen   │ Go binding gen│ Node binding   │ ...
│ (pond_core.h)  │ (cabi.go)     │ (cabi.node.cc) │
└────────────────┴───────────────┴────────────────┘
```

This is **not needed today** (we only have 2 language SDKs), but it's
the right design to prepare for. The first step is to extract the ABI
manifest — that's a low-risk, high-value refactor.

### 3.4 What NOT to do

- ❌ Don't adopt a heavy IDL like Protocol Buffers or Cap'n Proto — they
  solve a different problem (wire format) and add runtime deps.
- ❌ Don't use SWIG — it generates ugly, non-idiomatic code.
- ❌ Don't build a custom IDL language — use Rust attributes + build.rs.
- ❌ Don't generate PyO3 bindings from the C ABI — PyO3 needs Rust
  types, so the Python wrapper must stay hand-written (it's only ~400
  lines and changes rarely).

---

## 4. The binary distribution problem

### 4.1 What "small minimal lightweight binary" means

The final product should be:
- **One binary** (or one shared library + one Python wheel)
- **No runtime dependencies** (statically linked where possible)
- **Cross-platform** (Linux, macOS, Windows; x86_64 + arm64)
- **Small** (target: < 10 MB; DuckDB is ~30 MB)
- **Embeddable** (in-process via FFI) AND **server-mode** (CLI binary)

### 4.2 What this requires

1. **The Rust core must be a single cdylib** — `libpond_core.so` /
   `libpond_core.dylib` / `pond_core.dll`. This is already the case.

2. **The Python wheel must bundle the cdylib** — `pip install pond`
   should work without requiring a separate Rust toolchain. Use
   `maturin` (the standard PyO3 packaging tool) to build wheels.

3. **Each language SDK is a thin wrapper** — Go's cgo, Java's JNI,
   Node's N-API. Each compiles to a small package that links the
   cdylib.

4. **A CLI binary** for server-mode / interactive use — a small Rust
   binary that links `pond_core` and exposes a REPL or HTTP API.

### 4.3 Distribution channels (target)

| Channel | Package | Size (target) |
|---|---|---|
| Python | `pip install pond` | ~5 MB wheel |
| Go | `go get github.com/pond/pond-go` | ~2 MB (links cdylib at build time) |
| Node | `npm install @pond/pond-node` | ~5 MB (bundles cdylib) |
| Java | Maven `org.pond:pond-java` | ~5 MB (bundles cdylib as JNI) |
| CLI | Direct download | ~5 MB static binary |
| Homebrew | `brew install pond` | ~5 MB |
| apt/yum | `apt install pond` | ~5 MB |

### 4.4 What's missing for binary distribution

Today, `libpond_core.so` is ~650 KB (very small!). The path to binary
distribution is:

1. ✅ Already done: single cdylib, no external deps
2. ⏳ TODO: `maturin` build for Python wheels
3. ⏳ TODO: GitHub Releases with pre-built binaries for Linux/macOS/Windows
4. ⏳ TODO: CLI binary (`pond` command)
5. ⏳ TODO: Cross-compilation setup (arm64, musl-linux for static builds)

These are packaging tasks, not architectural changes. They can be done
incrementally without touching the core design.

---

## 5. Proposed next steps (prioritized)

### Tier 1 — High value, low risk (do next)

#### 5.1 Extract ABI manifest from Rust source

Write a `build.rs` in `pond-rust/pond-core/` that scans `src/lib.rs`
for `#[no_mangle] pub extern "C"` functions and emits
`abi_manifest.json` with their signatures. This is the foundation for
the generic codegen approach (Option C above).

**Why:** This is the single highest-value next step. It creates the
source of truth that all future language SDKs consume. It's low risk
because it's additive (doesn't change any existing code).

**Size:** ~1 day. ~200 lines of build.rs + JSON schema.

#### 5.2 Auto-generate `pond_core.h` from the manifest

Use the manifest to generate the C header. Compare against the hand-
written header to verify correctness. Once verified, replace the hand-
written header with the generated one.

**Why:** Eliminates the "forgot to update the header" bug class.
Demonstrates the codegen approach works end-to-end.

**Size:** ~1 day. ~150 lines of codegen + verification script.

#### 5.3 Add the Rust storage kernel skeleton

Create `pond-rust/pond-kernel/` (a new workspace member) with the
minimal storage kernel API: `Write`, `Read`, `Ref`, `Resolve`. Initially
backed by an in-memory `BTreeMap` (no persistence) — just to prove the
API shape.

This is the **first step toward the Rust-first storage stack**. It
doesn't replace the Python kernel — it runs alongside it, exposing the
same API through the C ABI so Go/Java/Node can use it.

**Why:** Unblocks full cross-language storage support. Currently the Go
SDK can only encode/decode PND2 blobs — it can't store them. With a
Rust storage kernel, the Go SDK could call `pond_write(blob)` /
`pond_read(hash)` / `pond_ref(name, hash)` directly.

**Size:** ~3-5 days. ~500 LOC for the in-memory kernel + C ABI + tests.

The ProllyTree-based persistent kernel is a MUCH larger project (the
Python implementation is ~1500 LOC) — that's Tier 3.

### Tier 2 — Medium value, medium risk (do after Tier 1)

#### 5.4 Auto-generate Go bindings from the manifest

Replace `sdk-go/internal/cabi/cabi.go` (currently hand-written) with a
generated version. The generator reads `abi_manifest.json` and emits
cgo wrappers + high-level Go types.

**Why:** Validates the codegen approach for a real language SDK. Once
this works for Go, adding Java/Node is just writing a new template.

**Size:** ~2 days. ~300 lines of Go codegen + template.

#### 5.5 Add `maturin`-based Python wheel build

Replace the current `build.sh` hardlink hack with proper `maturin`
packaging. Publish to TestPyPI as a proof of concept.

**Why:** Enables `pip install pond` — the standard Python distribution
channel. Required for the "small binary package" vision.

**Size:** ~1 day. maturin config + CI workflow + TestPyPI publish.

#### 5.6 Add a `pond` CLI binary

Create `pond-rust/pond-cli/` (new workspace member) with a minimal CLI:
- `pond decode <file>` — decode a PND2 blob, print as JSON
- `pond encode <file>` — encode JSON to a PND2 blob
- `pond version` — print version

**Why:** First step toward the "executable anywhere" DuckDB-philosophy
binary. Demonstrates the cdylib can be linked into a standalone binary.

**Size:** ~1 day. ~200 LOC using `clap` for arg parsing.

### Tier 3 — Large scope (do when ready)

#### 5.7 Implement the ProllyTree in Rust

Port `pond-sdk/prolly_tree.py` (~764 LOC) to Rust. This is the
persistent storage backend. Once done, the Rust storage kernel can
persist to disk / S3 / any object store.

**Why:** Replaces the Python kernel entirely. The Python SDK becomes a
thin PyO3 wrapper (like pond-python already is for the PND2 codec).

**Size:** ~2-3 weeks. The Python implementation is well-tested and
serves as the reference. This is a port, not a redesign.

#### 5.8 Port the production lenses to Rust

KeyValueLens, LakehouseLens, VectorLens, StreamingLens — each is
currently Python. Porting them to Rust would make them available to all
language SDKs.

**Why:** Completes the Rust-first vision. But this is a LOT of work and
the lenses are still evolving — porting too early means re-porting
when the design changes.

**Size:** ~4-6 weeks total (1-1.5 weeks per lens).

**Recommendation:** Defer until the storage kernel is stable in Rust.
Until then, lenses stay in Python.

#### 5.9 Cross-compilation + release pipeline

Set up GitHub Actions to build `libpond_core` + `pond` CLI for:
- Linux x86_64 (glibc + musl)
- Linux arm64
- macOS x86_64 + arm64 (universal binary)
- Windows x86_64

Publish to GitHub Releases + Homebrew + (eventually) apt/yum.

**Why:** Required for the "downloadable and executable anywhere" vision.

**Size:** ~1 week of CI/CD work (cross-compilation is fiddly).

---

## 6. What NOT to do next (anti-recommendations)

### 6.1 Don't port the lenses to Rust yet

The lenses are still evolving (the user just refactored KeyValueLens to
be collection-agnostic in Task 59). Porting them now means re-porting
later. Wait until the design is stable.

### 6.2 Don't build the execution engine yet

The user mentioned a future "Spark/OLTP/Streaming/Flink alternative"
project that pairs with Pond. That's explicitly **out of scope** for
now. The storage layer must be proven first (per DESIGN_GOALS.md §1:
"the storage model must be proven first").

### 6.3 Don't add more language SDKs yet

The user said "no need more language SDK for now." The Go SDK is
sufficient to validate the cross-language architecture. Adding Java/
Node now would multiply maintenance burden without validating the
codegen approach. Wait until the ABI manifest + codegen (5.1, 5.2, 5.4)
is in place.

### 6.4 Don't prematurely optimize the C ABI

The benchmark showed the C ABI is already fast (169M rows/s for
numeric data). The string column overhead is fixed with the batch
accessor. Further optimization (e.g., zero-copy slice sharing, SIMD)
can wait until there's a real workload that needs it.

---

## 7. Design principle checklist for the next round

Before starting any of the Tier 1 items, verify the proposal against
all 8 design principles:

| Principle | Question to ask |
|---|---|
| Simple (3.1) | Does this make the kernel bigger? (Must be no.) |
| Powerful (3.2) | Is this a composition of existing primitives, or a new primitive? (Prefer composition.) |
| Performant (3.3) | Does the optimization live above the core? (Must be yes.) |
| Scalable (3.4) | Can I delete this without breaking lower layers? (Must be yes.) |
| Efficient (3.5) | Is this rebuildable from a snapshot? (Must be yes for derived structures.) |
| Beautiful (3.6) | Do dependencies flow downward only? (Must be yes.) |
| Functional (3.7) | Is this a missing Lens/Structure, or a kernel feature? (Prefer Lens/Structure.) |
| Storage-Independent (3.8) | Does this work with any execution engine? (Must be yes.) |

For the proposed Tier 1 items:
- **5.1 ABI manifest:** Additive, no kernel change, enables codegen. ✅ all 8.
- **5.2 Generate C header:** Replaces hand-written header, no behavior change. ✅ all 8.
- **5.3 Rust storage kernel skeleton:** New workspace member, doesn't touch Python kernel. ✅ all 8 (downward dependency only).

---

## 8. Summary

The architecture is in good shape. The Round 50-52 work established:
- A clean Rust workspace (`pond-core` + `pond-python`)
- A working cross-language C ABI (131 checks passing)
- A Go SDK proving the cross-language approach works
- A benchmark validating the performance is real (169M rows/s for
  numeric decode via C ABI)

The next 3 steps (Tier 1) move toward the long-term vision without
rushing:
1. **ABI manifest** — foundation for generic codegen
2. **Generate C header** — proves codegen works end-to-end
3. **Rust storage kernel skeleton** — first step toward Rust-first stack

These are all low-risk, additive changes that don't touch the existing
Python stack. They prepare the ground for the eventual Rust port
without committing to it prematurely.

The user's vision — small binary, DuckDB philosophy, generic cross-
language SDK, future execution engine — is achievable from the current
architecture. The path is clear; the work is incremental.
