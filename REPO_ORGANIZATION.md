# Pond Repository Organization Rules

> This document codifies the folder structure, naming conventions, and
> promotion process for the Pond repository. Every agent (human or AI)
> working on Pond MUST follow these rules.

---

## 1. Top-level folder structure

```
pond_repo/
├── pond-core/          # Layer 0: Storage Kernel (FROZEN)
├── pond-sdk/           # Layers 1+2: Lens SDK + Physical Structures + Extensions
├── lenses/             # Layer 3: Production-ready Lens implementations
├── services/           # Cross-cutting services (transport, schema, replication)
├── pond-labs/          # Development & experimental code (NOT production)
├── tests/              # All tests, organized by type/purpose
├── scripts/            # Verification scripts (property tests, differentials, hazards)
├── docs/               # Documentation (whitepaper, formal algebras, RFCs)
├── tla/                # TLA+ formal specification
├── archive/            # Historical code (preserved for reference, NOT active)
└── (top-level docs)    # README.md, DESIGN_GOALS.md, PACKAGES.md, etc.
```

---

## 2. Folder rules

### 2.1 `pond-core/` — Storage Kernel (FROZEN)

**Contains:** The 3-primitive kernel (Write, Read, Ref). ~140 LOC.
**Rule:** FROZEN. Do not add features. Only bug fixes.
**Naming:** `kernel.py` (was `pond_minimal.py`).

### 2.2 `pond-sdk/` — Lens SDK + Extensions

**Contains:**
- The shared namespace base (`base_lens.py` → `PondLens`)
- The universal storage backend (`prolly_tree.py` → `ProllyLensBase`, `ProllyTree`)
- Binary encoding (`binary_encoding.py`)
- Tombstone helpers (`maintenance.py`)
- App-facing KV lens (`keyvalue_lens.py` → `KeyValueLens`, `KeylessLens`, `CrossLens`)
- Lazy row query API (`row_query.py` → `LensQuery`)
- Collection metadata (`collection.py`)
- Extensions subdirectory (see §3 below)

**Rule:** pond-sdk depends only on pond-core. No lens-to-lens imports.

**Naming convention:**
- Infrastructure files: `{role}.py` (e.g., `kernel.py`, `maintenance.py`)
- Lens files: `{role}_lens.py` (e.g., `keyvalue_lens.py`)
- The file `row_query.py` is NOT named `query.py` to avoid confusion with
  "the query method for data in Pond" — it is a lazy row-level query BUILDER
  for iterating/filtering/joining rows from any iterable lens.

### 2.3 `lenses/` — Production-ready Lens implementations

**Contains:** Lenses that are production-quality and ready for use.
**Current:** `lenses/lakehouse/` (LakehouseLens), `lenses/vector/` (VectorLens)
**Rule:**
- Each lens in its own subdirectory: `lenses/{lens_name}/`
- Main file: `lenses/{lens_name}/{lens_name}_lens.py`
- Lenses here extend `PondLens` directly (from `pond-sdk/base_lens.py`).
- **NO lens-to-lens inheritance.** Each production lens owns its own storage
  code, even if that means duplication. This keeps lenses independent and
  removable. (See §4 below.)
- Lenses may use pond-sdk extensions (mixins, physical structures).

### 2.4 `services/` — Cross-cutting services

**Contains:** Services that sit between the kernel and lenses.
**Current:** `services/transport/`, `services/schema/`, `services/replication/`
**Rule:** Services depend only on pond-core (not on pond-sdk or lenses).

### 2.5 `pond-labs/` — Development & experimental code

**Contains:** Code that is NOT yet production-ready. Organized by purpose:
```
pond-labs/
├── lenses/         # Lens prototypes in development (e.g., feature_store_lens.py)
├── tracks/         # Lab tracks (compatibility, benchmarks, case studies)
├── demos/          # Demonstration scripts (e.g., interop_demo.py)
└── benchmarks/     # Performance benchmarks (e.g., loc_benchmark.py)
```
**Rule:**
- Code here is experimental. It may break. It may be deleted.
- **Promotion process:** When code in `pond-labs/` is approved for production,
  it is moved to the appropriate production location:
  - Lenses → `lenses/{lens_name}/`
  - Services → `services/{service_name}/`
  - SDK extensions → `pond-sdk/extensions/`
  - SDK core → `pond-sdk/`
- The promotion is documented in the worklog with a clear rationale.

### 2.6 `tests/` — All tests, organized by type/purpose

**Contains:** All test files. Organized by test objective:
```
tests/
├── test_all.py          # Single pytest entry point (runs everything)
├── architecture/        # Architecture law tests (executable specification)
│   └── architecture_laws.py
├── lens_algebra/        # RFC-0007 Lens algebra property tests
│   ├── lens_laws.py
│   └── run_lens_laws_ci.py
└── integration/         # Integration tests (multi-lens, cross-lens)
    ├── test_shared_lenses.py
    ├── test_lens_architecture.py
    └── test_lens_query.py
```
**Rule:**
- Every test file lives in `tests/` or a subdirectory.
- Test files are NOT in `pond-sdk/` or `lenses/` — those directories contain
  only production code.
- The single entry point is `tests/test_all.py` (run via `pytest tests/test_all.py`).
- Test subdirectories reflect test PURPOSE, not the code being tested.

### 2.7 `scripts/` — Verification scripts

**Contains:** Property tests, differential tests, hazard simulators, benchmarks.
**Rule:** Scripts that verify the architecture (not tests of specific lenses).

### 2.8 `docs/` — Documentation

**Contains:** Whitepaper, formal algebras, RFCs, guides.
**Rule:** Documentation only. No code.

### 2.9 `archive/` — Historical code

**Contains:** Old code preserved for reference.
**Rule:** NOT active. Do not import from archive in production code.
May have broken imports — acceptable since it's not run.

---

## 3. Extension system

Extensions expand Pond's capabilities. They are **independent and generic** —
they work with any lens/storage/abstraction that meets their interface.

### 3.1 Where extensions live

```
pond-sdk/extensions/
├── indexing/              # Auto-indexing mixins (KV-style secondary indexes)
│   ├── __init__.py        # Package marker + exports
│   └── auto_index.py      # AutoIndexMixin + IndexedLens (convenience class)
├── semantic/              # Semantic model adapters (Ossie, future Cube/dbt)
│   ├── base.py            # SemanticModelAdapter (abstract interface)
│   └── ossie.py           # SemanticMixin + SemanticLens + OssieAdapter
└── physical_structures/   # Physical Structures (BloomFilter, Statistics, ZoneMap)
    ├── base.py            # PhysicalStructure (abstract base)
    ├── bloom_filter.py
    ├── statistics.py
    └── zone_map.py
```

**Extension categories:**
- **indexing/** — Mixins that add secondary index management to KV-style lenses.
  Composable via multiple inheritance. Supported: KeyValueLens and subclasses.
- **semantic/** — Mixins and adapters for semantic model management (metrics,
  dimensions, relationships). Composable via multiple inheritance. Supported:
  KeyValueLens and subclasses.
- **physical_structures/** — Standalone derived structures (not mixins). Built
  on any collection via build/load/query class methods. Supported: any
  collection (KV or tabular) — these are storage-format-agnostic.

### 3.2 Extension principles

1. **Independent:** Extensions do NOT depend on any specific lens. They
   define a minimal interface and work with any lens that meets it.

2. **Generic:** Extensions are composable via mixins. A lens adds an
   extension's capability by mixing in the mixin:
   ```python
   class MyLens(KeyValueLens, AutoIndexMixin, SemanticMixin):
       pass
   ```

3. **Documented interface:** Each extension's docstring MUST list the
   attributes/methods the host lens must expose. Example:
   ```
   GENERIC: works with any lens that exposes:
     - self.kernel         — the PondMinimal kernel
     - self.name           — the collection name
     - self.base           — a persistent ProllyLensBase
     - self.put(key, data) — stage a key→value mapping
     ...
   ```

4. **Storage-agnostic where possible:** Extensions work through the lens's
   API, not through direct kernel access. This makes them compatible with
   any lens that uses ProllyTreeIndex (the unified storage backend).

### 3.3 Extension types

- **Mixins** (`AutoIndexMixin`, `SemanticMixin`): composable with KV-style
  lenses. Add methods via multiple inheritance.
- **Physical Structures** (`BloomFilter`, `Statistics`, `ZoneMap`): derived
  structures stored as kernel blobs. Work with any collection (KV or tabular).
- **Adapters** (`OssieAdapter`): translate between Pond's internal format
  and external standards. Plug into mixins.

### 3.4 Extension metadata

Every extension class declares metadata as class attributes so tooling
can introspect what it supports:

```python
class AutoIndexMixin:
    extension_type = "mixin"                                    # "mixin" | "physical_structure" | "adapter"
    supported_lens_types = ["KeyValueLens", "KeylessLens", "SemanticLens"]
    supported_storage = ["ProllyTreeIndex"]
    not_supported = ["LakehouseLens", "FeatureStoreLens"]       # use Physical Structures instead
```

This lets tools (and humans) answer:
- "Can I use AutoIndexMixin with LakehouseLens?" → No (it's in `not_supported`).
- "What storage does SemanticMixin require?" → ProllyTreeIndex.
- "What lens types does BloomFilter work with?" → Any (it's a Physical Structure).

---

## 4. No lens-to-lens inheritance for production lenses

**Rule:** Production lenses (in `lenses/`) MUST NOT inherit from each other.
Each lens extends `PondLens` directly and owns its own storage code.

**Rationale:**
- Lens-to-lens inheritance creates coupling. If Lens A inherits from Lens B,
  removing Lens B breaks Lens A.
- Duplication is preferred over coupling for production code. The design
  principles value independence (each lens is removable) over DRY.
- Shared infrastructure lives in `pond-sdk/` (PondLens base, ProllyTreeIndex
  storage, mixins). Lenses USE this infrastructure; they don't inherit from
  each other.

**Exception:** Lenses in `pond-labs/` (experimental) may inherit from
production lenses during prototyping. But before promotion to `lenses/`,
the inheritance must be removed and the lens must own its own code.

**Example:** `FeatureStoreLens` (in `pond-labs/lenses/`) previously
inherited from `LakehouseLens`. After applying this rule, it now extends
`PondLens` directly and has its own ProllyTreeIndex storage code
(duplicated from LakehouseLens). This is intentional.

---

## 5. Naming conventions

### 5.1 Files

- **Kernel:** `kernel.py` (not `pond_minimal.py`)
- **Base lens:** `base_lens.py` (not `pond_lens.py`)
- **Prolly tree:** `prolly_tree.py` (not `prolly_view.py`)
- **Row query:** `row_query.py` (not `query.py` — avoids confusion with
  "the query method for data in Pond")
- **Indexing extension:** `indexing.py` (not `auto_index.py`)
- **Lens files:** `{role}_lens.py` (e.g., `keyvalue_lens.py`, `lakehouse_lens.py`,
  `vector_lens.py`)
- **Test files:** `test_{purpose}.py` (e.g., `test_shared_lenses.py`)

### 5.2 Classes

- **Kernel:** `PondMinimal`
- **Base lens:** `PondLens`
- **Lenses:** `{Name}Lens` (e.g., `KeyValueLens`, `LakehouseLens`, `FeatureStoreLens`)
- **Mixins:** `{Capability}Mixin` (e.g., `AutoIndexMixin`, `SemanticMixin`)
- **Storage:** `ProllyLensBase`, `ProllyTree`
- **Query:** `LensQuery` (the row-level lazy query builder)

### 5.3 Folders

- **Production lenses:** `lenses/{lens_name}/` (e.g., `lenses/lakehouse/`)
- **Lab lenses:** `pond-labs/lenses/` (flat, not per-lens subdirectory)
- **Tests:** `tests/{type}/` (e.g., `tests/architecture/`, `tests/integration/`)
- **Extensions:** `pond-sdk/extensions/{category}/` (e.g., `pond-sdk/extensions/semantic/`)

---

## 6. Promotion process

When code in `pond-labs/` is approved for production:

1. **Document the decision** in `worklog.md` with:
   - What is being promoted
   - Why it's ready (tests pass, design review complete, etc.)
   - Where it's going

2. **Move the code** via `git mv`:
   - Lab lens → `lenses/{lens_name}/`
   - Lab service → `services/{service_name}/`
   - Lab extension → `pond-sdk/extensions/{category}/`

3. **Update imports** across the codebase (use `scripts/update_imports.py`
   as a template).

4. **Remove lens-to-lens inheritance** if any exists (see §4).

5. **Update `KNOWLEDGE_GRAPH.md`** with the new file paths.

6. **Run `tests/test_all.py`** to verify nothing broke.

7. **Update `tests/test_all.py`** if the script path changed.

---

## 7. Dependency rules

```
pond-core (FROZEN, ~140 LOC)
    │
    ├── pond-sdk (depends on pond-core only)
    │   ├── base_lens.py ← pond-core
    │   ├── keyvalue_lens.py ← base_lens, prolly_tree, binary_encoding, maintenance, row_query
    │   ├── prolly_tree.py ← binary_encoding
    │   ├── indexing.py ← prolly_tree, keyvalue_lens (lazy import)
    │   ├── row_query.py ← (no deps)
    │   ├── collection.py ← pond-core
    │   ├── maintenance.py ← pond-core
    │   └── extensions/ ← pond-sdk core
    │
    ├── lenses/ (depend on pond-core + pond-sdk, NEVER on each other)
    │   ├── lakehouse/ ← pond-core, pond-sdk, duckdb, pyarrow
    │   └── vector/ ← pond-sdk (uses mock_kernel for tests)
    │
    ├── services/ (depend on pond-core only)
    │   ├── transport/ ← pond-core
    │   ├── schema/ ← pond-core
    │   └── replication/ ← pond-core
    │
    └── pond-labs/ (depend on pond-core + pond-sdk + lenses)
        ├── lenses/ ← pond-core, pond-sdk
        ├── tracks/ ← pond-core, pond-sdk, lenses
        ├── demos/ ← pond-core, pond-sdk, lenses
        └── benchmarks/ ← pond-core, pond-sdk, lenses
```

**Rules:**
- No Lens depends on another Lens.
- All Lenses depend only on `pond-sdk` (and `pond-core`).
- `pond-sdk` depends only on `pond-core`.
- `pond-core` depends on nothing.
- Services depend only on `pond-core` (not on `pond-sdk`).
- `pond-labs` depends on everything (it's experimental).

---

## 8. The weekly question

From `DESIGN_GOALS.md` §4:

> Every week, ask: **If I deleted everything except `pond-core` and
> `pond-sdk`, would the architecture still make sense?**

If the answer is "no" — if the architecture only makes sense with a
specific lens or service present — the design has leaked.

These rules exist to keep the answer "yes."
