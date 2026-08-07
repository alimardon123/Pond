# Migration Strategy — Python to Rust

> **Date:** 2026-08-07
> **Purpose:** Answer "should we keep changing Python or migrate to Rust now?"
> **Answer:** Stop adding new Python features. Fix critical correctness bugs
> in Python (like the CRDT merge). Invest all new development in Rust.
> Python becomes the reference implementation that guides the Rust port.

---

## The transition plan

### Phase 0 — Current state (now)
- Python: kernel.py, unified_storage.py (5,540 LOC), pond_storage.py, all lenses, all extensions
- Rust: pond-kernel (3 primitives + ObjectStore trait), bindings/python/core (PND2 codec), pond-python (PyO3 wrapper), pond-cli
- Go: sdk-go (PND2 codec bindings via cgo)
- Python is the primary implementation. Rust has the kernel + codec + CLI.

### Phase 1 — Port UnifiedStorage to Rust (next 4-6 weeks)
- Port `unified_storage.py` (5,540 LOC) to Rust as `core/storage/`
- This is the big one: manifest management, CRDT shards, commits, branch/merge, read/write paths
- Python `UnifiedStorage` becomes a PyO3 wrapper (like pond-python already is for the codec)
- All lenses continue to use Python; they call into Rust UnifiedStorage via PyO3

### Phase 2 — Port lenses to Rust (ongoing, one at a time)
- Port KeyValueLens → Rust. Python KeyValueLens delegates to Rust via PyO3.
- Port LakehouseLens → Rust (needs Arrow path — Tier 1.1.1).
- Port StreamingLens → Rust.
- Each lens is ported independently. Python lens stays as fallback until Rust version is tested.

### Phase 3 — Final state (6-12 months)
- Rust is the core (kernel, storage, codec, CLI)
- Python is a thin PyO3 wrapper (like pond-python for the codec)
- Go SDK links against Rust via C ABI
- Other language SDKs (Java, Node) link against the same C ABI
- No duplicate logic — Python delegates to Rust for everything

---

## Why not migrate all at once?

1. **The Python code is the reference.** It's tested, it works (mostly), and it documents the design. Porting without a reference would be error-prone.

2. **Lenses depend on Python libraries.** LakehouseLens uses PyArrow and DuckDB. These don't have mature Rust equivalents yet. Porting the lens means either:
   - Calling Python from Rust (defeats the purpose)
   - Using Rust Arrow crates (arrow-rs) + DuckDB Rust bindings (immature)
   - Implementing a native Rust SQL engine (huge effort)

3. **5,540 LOC is a lot.** Porting unified_storage.py all at once would take weeks of uninterrupted work with no test coverage during the transition. Porting incrementally (kernel → storage → lenses) allows testing at each step.

4. **The Rust kernel + CLI already work.** Users can use `pond init/write/read/branch/merge` today. The Python UnifiedStorage handles the complex parts (manifests, shards, commits) that haven't been ported yet.

---

## What changes in Python vs Rust going forward

| Work type | Python | Rust |
|---|---|---|
| Critical correctness bugs (CRDT merge, etc.) | ✅ Fix immediately | ✅ Port the fix |
| New features (partitioning, Z-Order, Arrow path) | ❌ Don't add | ✅ Build in Rust |
| Performance optimizations | ❌ Don't optimize | ✅ Build in Rust |
| New lenses | ❌ Don't add | ✅ Build in Rust |
| Test coverage | ✅ Keep tests passing | ✅ Add Rust tests |

---

## Repository organization after full migration

Current structure (transitional):
```
pond_repo/
├── bindings/python/core/          # Python kernel (will become thin wrapper)
├── bindings/python/sdk/           # Python SDK (will become thin wrapper)
├──           # Rust workspace (will become the core)
│   ├── bindings/python/core/      # PND2 codec
│   ├── pond-kernel/    # Storage kernel (3 primitives)
│   ├── pond-python/    # PyO3 wrapper
│   └── pond-cli/       # CLI binary
├── bindings/go/             # Go SDK
├── pond/               # Pip shim
├── lenses/             # Python lenses (will port to Rust one at a time)
├── ...
```

Future structure (after Phase 3):
```
pond_repo/
├── core/               # Rust workspace (renamed from )
│   ├── kernel/         # 3 primitives + ObjectStore trait
│   ├── codec/          # PND2 encode/decode (renamed from bindings/python/core/)
│   ├── storage/        # UnifiedStorage (manifest, shards, commits)
│   ├── cli/            # pond binary
│   └── python/         # PyO3 wrapper (thin)
├── sdk-python/         # Python SDK (lenses, extensions) — calls into core
├── bindings/go/             # Go SDK — calls into core via C ABI
├── lenses/             # Lens implementations (Rust + Python wrappers)
├── services/           # Cross-cutting services
├── labs/               # Experiments
├── tests/
├── docs/
└── ...
```

The rename ( → core/) happens when Rust is the primary implementation.
Until then, the current names are fine — just document which `bindings/python/core` is which.

---

## Duplicate code during transition

Yes, there will be duplicate code during the transition:
- `bindings/python/core/kernel.py` (Python) and `core/kernel/src/lib.rs` (Rust)
- `bindings/python/sdk/extensions/physical_structures/unified_storage.py` (Python) and future `core/storage/` (Rust)

This is intentional and acceptable:
- The Python code is the **reference implementation** — it documents the design
- The Rust code is the **production implementation** — it's what users actually run
- When a Rust port reaches feature parity + test parity, the Python version becomes a thin wrapper
- The Python version is never deleted (it's the reference), but it stops being the primary path

This is the same model as PyTorch (C++ core, Python frontend) and DuckDB (C++ core, Python/R/Go/Java frontends).
