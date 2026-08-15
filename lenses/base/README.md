# lenses/base/

Shared cross-language files for the lens protocol.

## Files

- `pond_lens.h` — C ABI header for cross-language lenses (PLACEHOLDER)

## Status

**No lenses are exposed via C ABI yet.** All production lenses today are
Python-only (see `lenses/{name}/python/`).

When the first lens is ported to Rust (planned: KeyValueLens), this
header will define the lens protocol functions. Until then, this
directory exists to establish the convention and reserve the namespace.
