# lenses/vector/rust/

Placeholder for a future Rust port of VectorLens.

The IVF index implementation lives in `extensions/indexing/rust/` (not here)
because it's an extension, not a lens. VectorLens would use the IVF index
extension, just like the Python VectorLens uses the Python IVF extension.

## Status

**Not yet ported.** The Python implementation (`../python/vector_lens.py`)
is the production reference.

The IVF index (with Bug 10 fixed) is available at:
`extensions/indexing/rust/` — `IVFIndex` with per-cluster blob references.
