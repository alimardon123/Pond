# lenses/

Workload-specific lenses over Pond's unified storage.

## Structure

Each lens lives in its own subdirectory with `python/` and `rust/`
subdirectories for multi-language support:

```
lenses/
├── base/                  # Cross-language lens protocol (C ABI header)
│   ├── pond_lens.h        # PLACEHOLDER — no lens C ABI yet
│   └── README.md
├── keyvalue/
│   ├── python/            # KeyValueLens (production)
│   │   ├── keyvalue_lens.py
│   │   └── __init__.py
│   ├── rust/              # Future Rust port (placeholder)
│   │   └── README.md
│   └── README.md
├── lakehouse/
│   ├── python/            # LakehouseLens (production)
│   ├── rust/              # Future Rust port (placeholder)
│   └── README.md
├── oltp/
│   ├── python/            # OLTPLens (production)
│   └── rust/              # Future Rust port (placeholder)
├── streaming/
│   ├── python/            # StreamingLens (production)
│   ├── rust/              # Future Rust port (placeholder)
│   └── README.md
├── vector/
│   ├── python/            # VectorLens (production)
│   ├── rust/              # Future Rust port (placeholder)
│   └── README.md
└── README.md              # This file
```

## Migration plan

All lenses today are Python-only. When a lens is ported to Rust:
1. Implement the lens logic in Rust (in `rust/`), calling `pond_kernel`
   and `pond_storage` directly.
2. Expose the lens via `lenses/base/pond_lens.h` C ABI.
3. The Python wrapper in `python/` becomes a thin PyO3 binding to the
   Rust implementation.

The first lens to be ported will be **KeyValueLens** (simplest API surface).

## Lens list

| Lens | Purpose | Status |
|---|---|---|
| KeyValueLens | Key-value storage with point lookups | Production (Python) |
| LakehouseLens | Tabular storage with DuckDB SQL | Production (Python) |
| OLTPLens | OLTP with memtable + batch flush | Production (Python) |
| StreamingLens | Kafka-like streaming with partitions | Production (Python) |
| VectorLens | Vector storage with IVF ANN | Production (Python) |
