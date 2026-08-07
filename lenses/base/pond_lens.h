/*
 * Pond Lens Protocol — C ABI for cross-language lenses
 *
 * STATUS: PLACEHOLDER. No lenses are exposed via this header yet.
 * All production lenses today are Python-only (lenses/{name}/python/).
 *
 * When the first lens is ported to Rust (planned: KeyValueLens), this
 * header will define the lens protocol:
 *   - pond_lens_new(store_handle) -> LensHandle*
 *   - pond_lens_put(handle, key, value, vlen) -> int
 *   - pond_lens_get(handle, key) -> int (with out-param)
 *   - pond_lens_delete(handle, key) -> int
 *   - pond_lens_scan(handle, prefix) -> IteratorHandle*
 *   - pond_lens_free(handle)
 *
 * The protocol mirrors pond.h's storage layer — opaque handles,
 * heap-allocated strings (caller frees with pond_string_free),
 * and 0/-1 return codes for success/failure.
 */
#ifndef POND_LENS_H
#define POND_LENS_H

#include "pond.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Placeholder — no lens functions defined yet. */

#ifdef __cplusplus
}
#endif

#endif /* POND_LENS_H */
