# Cross-Language SDK Architecture — Design Document

> **Date:** 2026-08-07
> **Purpose:** Define the generic, simple, performant, fully-functional
> cross-language SDK architecture. Answer: how do Python, Go, Java, Node,
> and future languages get full Pond access with zero effort?
>
> **Key decisions:**
> 1. One C ABI header (`pond.h`) — auto-generated from Rust source
> 2. Each language SDK is a thin FFI wrapper (no logic, no duplication)
> 3. Python uses PyO3 (first-class), Go uses cgo, others use FFI/JNI/N-API
> 4. The Rust SDK IS the core — no separate "Rust SDK"
> 5. Cross-language extensions/lenses via C ABI plugin protocol

---

## 1. The problem

Currently:
- `core/codec/` has a C ABI (`pond_core.h`) for PND2 codec only
- `core/kernel/` has a C ABI for the 3 primitives (Write, Read, Ref)
- `core/storage/` has NO C ABI — it's Rust-only
- `bindings/go/` links against `libpond_core.a` (codec only, no storage)
- Python uses PyO3 for codec, but UnifiedStorage is still Python

**Gap:** a Go/Java/Node program can encode/decode PND2 blobs, but can't
write, branch, merge, or read collections. The storage layer has no C ABI.

**Goal:** any language should get FULL Pond access (kernel + storage + codec)
through one C ABI, with zero per-language logic.

---

## 2. The solution: unified C ABI

### 2.1 One header, one library

```

├── pond-kernel/     → C ABI for Write/Read/Ref (already exists)
├── pond-storage/    → C ABI for write/read/branch/merge/shard (NEW — needed)
├── bindings/python/core/       → C ABI for PND2 encode/decode (already exists)
├── pond-arrow/      → C ABI for PND2→Arrow (NEW — optional, for Arrow users)
└── pond-cli/        → the `pond` binary (links all crates)
```

All C ABI functions go into ONE header: `pond.h`. One library: `libpond.a`
(or `libpond.so`). Any language links against this ONE library and gets
full access.

### 2.2 The C API surface (unified)

```c
// === Kernel (3 primitives) ===
PondKernel* pond_kernel_new(const char* base_dir);
void        pond_kernel_free(PondKernel* k);
const char* pond_kernel_write(PondKernel* k, const uint8_t* data, size_t len);
int         pond_kernel_read(PondKernel* k, const char* hash_or_name,
                              const uint8_t** out, size_t* out_len);
int         pond_kernel_reference(PondKernel* k, const char* name, const char* hash);
const char* pond_kernel_resolve(PondKernel* k, const char* name);

// === Storage (unified) ===
PondStorage* pond_storage_new(const char* base_dir);
void         pond_storage_free(PondStorage* s);
const char*  pond_storage_write(PondStorage* s, const char* collection,
                                 const uint8_t* data, size_t len, const char* message);
int          pond_storage_read(PondStorage* s, const char* collection,
                                const uint8_t** out, size_t* out_len);
const char*  pond_storage_branch(PondStorage* s, const char* collection, const char* branch);
int          pond_storage_checkout(PondStorage* s, const char* collection, const char* branch);
const char*  pond_storage_merge(PondStorage* s, const char* collection,
                                 const char* source, const char* target, const char* message);
int          pond_storage_history(PondStorage* s, const char* collection,
                                   PondHistoryEntry** out, size_t* out_len);
const char*  pond_storage_undo(PondStorage* s, const char* collection, int steps);
const char*  pond_storage_revert(PondStorage* s, const char* collection, const char* commit);
// ... append_shard, read_with_shards, begin_tx, commit_tx, etc.

// === Codec (PND2) ===
PondResult* pond_pnd2_decode(const uint8_t* blob, size_t len);
// ... (already exists in pond_core.h)

// === Memory management ===
void pond_string_free(char* s);
void pond_data_free(uint8_t* data, size_t len);
void pond_history_free(PondHistoryEntry* entries, size_t len);
```

### 2.3 How each language uses it

| Language | Mechanism | Effort to port | What the SDK looks like |
|---|---|---|---|
| **Python** | PyO3 (first-class) | ~400 LOC wrapper | `import pond` — calls Rust via PyO3 |
| **Go** | cgo | ~200 LOC wrapper | `import "pond"` — calls C ABI via cgo |
| **Java** | JNI | ~300 LOC wrapper | `import org.pond.Pond` — calls C ABI via JNI |
| **Node** | N-API | ~200 LOC wrapper | `const pond = require('pond')` — calls C ABI via N-API |
| **C/C++** | direct | `#include "pond.h"` | link against `libpond.a` |
| **Rust** | direct | `use pond_storage::UnifiedStorage` | it IS the core — no wrapper needed |
| **WASM** | wasm-bindgen | ~100 LOC wrapper | runs in browser |

**Key insight:** the C ABI is the SINGLE source of truth. Adding a new
language = writing a thin FFI wrapper (~200 LOC). No logic, no duplication.

---

## 3. Python: PyO3 (first-class) vs C ABI

Python gets PyO3 because:
1. **Performance:** PyO3 builds Python objects natively (no ctypes overhead)
2. **Ergonomics:** `import pond` feels native (no ctypes struct definitions)
3. **GIL safety:** PyO3 handles the GIL correctly

Python's PyO3 wrapper calls into `pond-storage`, `bindings/python/core`, and `pond-kernel`
directly (Rust-to-Rust, no C ABI overhead). This is the fastest path.

Other languages use the C ABI (Rust → extern "C" → FFI). This adds ~1µs per
call but avoids the GIL/JVM/GC overhead.

**So Python is special** — it gets PyO3 (first-class). Everyone else gets the
C ABI. Both paths use the same Rust logic.

---

## 4. The Rust SDK IS the core

There is no separate "Rust SDK." The Rust crates ARE the SDK:

```

├── pond-kernel/    → PondKernel (Write, Read, Ref)
├── pond-storage/   → UnifiedStorage (write, read, branch, merge, shard, transaction)
├── bindings/python/core/      → PND2 codec (encode, decode, all encodings/vtypes)
├── pond-arrow/     → Arrow bridge (PND2 → RecordBatch)
├── pond-cli/       → CLI binary
└── pond-python/    → PyO3 wrapper (for Python)
```

A Rust program uses these crates directly:
```rust
use pond_storage::UnifiedStorage;
let storage = UnifiedStorage::new_local(".pond")?;
storage.write("users", data, "initial")?;
```

No wrapper needed. The Rust API IS the SDK. The C ABI is for OTHER languages.

---

## 5. Cross-language extensions/lenses (the DuckDB model)

DuckDB allows extensions in C++, Python, and R. Pond can do the same.

### 5.1 The plugin protocol

A Pond extension/lens is a shared library (`.so`/`.dylib`/`.dll`) that
implements a C ABI plugin protocol:

```c
// pond_plugin.h
typedef struct {
    const char* name;         // "KeyValue", "Lakehouse", "Vector", etc.
    const char* version;
    // Called when the plugin is loaded
    int (*init)(PondStorage* storage);
    // Called to write data (lens-specific encoding)
    int (*write)(PondStorage* storage, const char* collection,
                 const uint8_t* data, size_t len, const char* message);
    // Called to read data (lens-specific decoding)
    int (*read)(PondStorage* storage, const char* collection,
                const uint8_t** out, size_t* out_len);
} PondPlugin;

// Entry point — the plugin exports this function
PondPlugin* pond_plugin_create(void);
```

### 5.2 How it works

1. A lens (e.g., KeyValueLens) is compiled as a shared library
2. Pond loads it via `dlopen` / `LoadLibrary`
3. The lens calls back into Pond's C ABI to read/write/branch
4. The lens owns its data format; Pond owns the storage

### 5.3 Language support for plugins

| Language | How to write a Pond plugin |
|---|---|
| **Rust** | Implement `PondPlugin` trait, compile to cdylib |
| **C/C++** | Implement the C struct, compile to .so/.dylib/.dll |
| **Python** | Use PyO3 to expose a Python lens as a C plugin (future) |
| **Go** | Use cgo to export Go functions as C callbacks (future) |

This matches DuckDB's model: extensions can be written in any language
that can produce a shared library with a C ABI entry point.

### 5.4 When to implement this

NOT NOW. The plugin protocol is a Phase 3 feature. The priority is:
1. Get the C ABI for pond-storage done (so all languages get full access)
2. Get the PyO3 wrapper done (so Python gets first-class access)
3. Port the existing lenses to Rust (so they're available to all languages)
4. THEN add the plugin protocol (so users can write lenses in any language)

---

## 6. Repository organization (clean and organized)

### Current structure (transitional — Python + Rust coexist):

```
pond_repo/
├── bindings/python/core/          # Python kernel (reference implementation)
├── bindings/python/sdk/           # Python SDK (reference implementation)
├──           # Rust workspace (production implementation)
│   ├── bindings/python/core/      # PND2 codec + C ABI
│   ├── pond-kernel/    # Storage kernel + C ABI
│   ├── pond-storage/   # UnifiedStorage (no C ABI yet)
│   ├── pond-arrow/     # PND2 → Arrow bridge
│   ├── pond-python/    # PyO3 wrapper
│   └── pond-cli/       # CLI binary
├── bindings/go/             # Go SDK (codec only — needs updating)
├── lenses/             # Python lenses (to be ported to Rust)
├── services/           # Python services
├── pond-labs/          # Experiments
├── tests/
├── docs/
└── scripts/
```

### Target structure (after full migration):

```
pond/
├── core/               # Rust workspace (renamed from )
│   ├── kernel/         # 3 primitives + ObjectStore trait + C ABI
│   ├── storage/        # UnifiedStorage + C ABI
│   ├── codec/          # PND2 encode/decode + C ABI (renamed from bindings/python/core/)
│   ├── arrow/          # PND2 → Arrow bridge
│   ├── cli/            # CLI binary
│   └── python/         # PyO3 wrapper
├── sdk-python/         # Python SDK (thin wrapper — calls core via PyO3)
├── bindings/go/             # Go SDK (thin wrapper — calls core via C ABI)
├── sdk-java/           # Java SDK (thin wrapper — calls core via JNI)
├── sdk-node/           # Node SDK (thin wrapper — calls core via N-API)
├── lenses/             # Lens implementations (Rust + language bindings)
├── services/           # Cross-cutting services
├── labs/               # Experiments
├── tests/
├── docs/
└── scripts/
```

The rename happens when Rust is the primary implementation. Until then,
the current names are fine.

### Key organization principles:
1. **One responsibility per directory** — `kernel/` does storage primitives,
   `storage/` does versioning, `codec/` does PND2, `cli/` does UI
2. **No circular dependencies** — kernel ← storage ← codec ← cli
3. **Each language SDK is a thin wrapper** — no logic, no duplication
4. **The C ABI lives WITH the Rust crate** that implements it — `pond.h`
   is generated from the Rust source, not maintained separately

---

## 7. Next steps (prioritized)

### Step 1: Add C ABI to pond-storage (1-2 days)
- Add `#[no_mangle] extern "C"` wrappers for write, read, branch, merge, etc.
- Generate a unified `pond.h` header (kernel + storage + codec)
- This is the foundation — without it, other languages can't access storage

### Step 2: Update Go SDK to use the full C ABI (1 day)
- Currently Go only links `libpond_core.a` (codec)
- Update to link `libpond_storage.a` + `libpond_kernel.a` (or a unified lib)
- Add Go bindings for write, read, branch, merge, history

### Step 3: PyO3 wrapper for pond-storage (1-2 days)
- Expose `UnifiedStorage` to Python via PyO3
- Python lenses call into Rust UnifiedStorage instead of Python UnifiedStorage
- This is the big step — Python starts using Rust for storage

### Step 4: Clean up repo organization (0.5 days)
- Move `` → `core/` (or keep current names, just document)
- Ensure each directory has a clear README
- Remove dead code and stale references

### Step 5: Port KeyValueLens to Rust (2-3 days)
- First lens port — proves the pattern
- Python KeyValueLens delegates to Rust via PyO3

### Step 6: Plugin protocol design (future — Phase 3)
- Define `PondPlugin` C ABI
- Allow lenses to be written in any language
- This is the DuckDB model for extensions

---

## 8. Summary

| Question | Answer |
|---|---|
| How do other languages get full Pond access? | One unified C ABI (`pond.h`). Each language SDK is a ~200 LOC FFI wrapper. |
| Does Python use the C ABI too? | No — Python gets PyO3 (first-class, faster, more ergonomic). |
| Does Go use the C ABI? | Yes — Go uses cgo to call the C ABI. Currently only codec; needs storage. |
| Can other languages create extensions/lenses? | Yes — via the plugin protocol (Phase 3, future). Same as DuckDB. |
| Is there a separate Rust SDK? | No — the Rust crates ARE the SDK. `use pond_storage::UnifiedStorage`. |
| How to keep the repo clean? | One responsibility per directory, no circular deps, thin wrappers. |
