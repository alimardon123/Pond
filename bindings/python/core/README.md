# bindings/python/core

The Pond kernel. **FROZEN.**

## What it is

The minimal storage substrate: 3 operations on 6 substrates, ~140 LOC, stdlib only.

## Files

- `pond_minimal.py` — the kernel. Three primitives:
  - `Write(bytes) → hash` — create immutable content-addressed blob
  - `Read(hash) → bytes` — fetch blob by hash (or by name)
  - `Ref(name, hash) → ()` — mutable name→hash mapping (the only mutation)

## Design constraints

- **Never grow.** Any change requires a new RFC that disproves the
  lower-bound proof. The kernel stays ~140 LOC.
- **No dependencies** beyond Python stdlib (hashlib, sqlite3, os, json).
- **No knowledge** of format, domain, structure, schema, optimization,
  coordination, or policy. The kernel stores and retrieves bytes.

## Usage

```python
import sys; sys.path.insert(0, "bindings/python/core")
from pond_minimal import PondMinimal

kernel = PondMinimal("/tmp/my-pond")
h = kernel.write(b"hello")
assert kernel.read(h) == b"hello"
kernel.reference("greeting", h)
assert kernel.read("greeting") == b"hello"
```

## What lives above this

- `bindings/python/sdk/` — Lens SDK (ProllyViewBase, Lens base class, indexes)
- `lenses/` — Lens implementations (lakehouse, vector)
- `services/` — cross-cutting services (transport, schema, replication)
- `pond-labs/` — experiments and demos

Nothing in this directory depends on anything above it.
