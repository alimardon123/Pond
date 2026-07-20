# Pond Package Structure

Per RFC-0006 (Layered Architecture), the repository is organized into
packages matching the architectural layers.

## Structure

```
pond/
├── pond-core/                  # Layer 0: Storage Calculus (FROZEN)
│   ├── pond_minimal.py         # 3 primitives: Write, Read, Reference (~140 LOC)
│   └── __init__.py
│
├── pond-sdk/                   # Layers 1+2: State + Access Calculus
│   ├── prolly_view.py          # Layer 1: ProllyViewBase (delta commits, trees, branching)
│   ├── binary_encoding.py      # Binary Prolly tree encoding (metadata optimization)
│   ├── auto_index.py           # Layer 2: IndexedView (auto-indexing, incremental)
│   ├── view_sdk.py             # View base class + CrossView + SemanticView + adapters
│   └── __init__.py
│
├── pond-sql/                   # Layer 3: Domain — SQL database
│   ├── sql_view.py             # SQL View (CREATE, INSERT, SELECT, UPDATE, DELETE, ALTER)
│   └── __init__.py
│
├── pond-streaming/             # Layer 3: Domain — Streaming
│   ├── streaming_view.py       # Kafka-like topics, consumer groups, retention
│   └── __init__.py
│
├── pond-git/                   # Layer 3: Domain — Version control
│   ├── pond_git.py             # Git-like VCS (init, add, commit, branch, merge)
│   └── __init__.py
│
├── pond-notebook/              # Layer 3: Domain — Knowledge base
│   ├── notebook.py             # Notebook (pages, search, attachments, history)
│   └── __init__.py
│
├── pond-feature-store/         # Layer 3: Domain — ML Feature Store
│   ├── feature_store.py        # Feature definitions, online/offline serving, lineage
│   └── __init__.py
│
├── pond-semantic/              # Layer 3: Domain — Semantic models
│   ├── ossie_adapter.py        # Apache Ossie adapter (one of many possible)
│   └── __init__.py
│
├── rfcs/                       # Architecture specifications
│   ├── RFC-0001-what-is-a-view.md
│   ├── RFC-0002-elegance-metrics.md
│   ├── RFC-0003-kernel-specification.md    # ACCEPTED (frozen)
│   ├── RFC-0004-view-composition.md
│   ├── RFC-0005-derived-structures.md
│   └── RFC-0006-layered-architecture.md
│
├── docs/                       # Reference documents
│   ├── FORMAL_SPEC.md
│   ├── FORMAL_ALGEBRA.md
│   ├── VIEW_AUTHORS_GUIDE.md
│   ├── VIEW_INTEROP_SPEC.md
│   ├── REJECTED_DESIGNS.md
│   ├── NON_GOALS.md
│   ├── PEER_COMPARISON.md
│   └── PROBLEM_TAXONOMY.md
│
├── destruction/                # Destruction-phase experiments (historical)
│   ├── 01_mathematical.py
│   ├── 02_economic.py
│   ├── 03_distributed.py
│   ├── 04_storage.py
│   ├── 05_scale.py
│   ├── 06_human.md
│   ├── II_identity/
│   ├── III_adversarial/
│   ├── IV_namespace_attack/
│   └── V_independent/
│
├── engineering/                # Engineering milestones
│   ├── 01_concurrency.py
│   ├── 02_gc.py
│   └── 03_s3_backend.py
│
└── README.md
```

## Package Dependencies

```
pond-core        → (nothing — standalone, 3 primitives)
pond-sdk         → pond-core
pond-sql         → pond-sdk
pond-streaming   → pond-sdk
pond-git         → pond-sdk
pond-notebook    → pond-sdk
pond-feature-store → pond-sdk + pond-semantic (optional)
pond-semantic    → pond-sdk
```

No domain package depends on another domain package.
All domain packages depend only on pond-sdk.
pond-sdk depends only on pond-core.
pond-core depends on nothing.

This is a strict dependency DAG with no cycles.
