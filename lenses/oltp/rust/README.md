# lenses/oltp/rust/

Placeholder for the Rust reference implementation of OltpLens.

## Status

**Not yet ported.** The current production implementation is Python-only
(see `../python/oltp_lens.py`).

## Migration plan

When this lens is ported to Rust:
1. Implement the lens logic in Rust, calling `pond_kernel` and `pond_storage`
   directly (no Python dependency).
2. Expose the lens via `lenses/base/pond_lens.h` C ABI.
3. The Python wrapper in `../python/` becomes a thin PyO3 binding to this
   Rust implementation.

The first lens to be ported will be KeyValueLens (simplest API surface).
