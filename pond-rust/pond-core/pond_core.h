/*
 * Pond Core — C ABI Header
 *
 * This header defines the C ABI for Pond's pure-Rust core library
 * (`pond-core`, produces `libpond_core.a` / `libpond_core.so`).
 *
 * Other language SDKs (Go, Java, Node, C, C++, Zig) call these
 * functions via FFI/cgo/JNI/NAPI to get native PND2 encode/decode.
 * The C ABI is the universal interop layer — any language that can
 * call C functions can use Pond's Rust core.
 *
 * Usage:
 *   1. Link against libpond_core.a (static) or libpond_core.so (dynamic)
 *   2. #include "pond_core.h"
 *   3. Call pond_pnd2_decode() to decode, pond_result_* to access,
 *      pond_result_free() to cleanup
 *   4. Call pond_pnd2_encode_i64() to encode, pond_blob_free() to cleanup
 */

#ifndef POND_CORE_H
#define POND_CORE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle for decoded PND2 data */
typedef struct PondResult PondResult;

/*
 * Decode a PND2 blob into a PondResult handle.
 *
 * @param blob     Pointer to PND2 blob bytes
 * @param blob_len Length of blob in bytes
 * @return PondResult handle, or NULL on error
 *
 * The caller must free the result with pond_result_free().
 * Only uncompressed PND2 blobs are supported (caller decompresses if needed).
 */
PondResult* pond_pnd2_decode(const uint8_t* blob, size_t blob_len);

/*
 * Get the number of columns in a decoded result.
 */
size_t pond_result_num_columns(const PondResult* result);

/*
 * Get a column's name (null-terminated string).
 * Valid until the result is freed.
 */
const char* pond_result_column_name(const PondResult* result, size_t index);

/*
 * Get a column's value type.
 * Returns: 1=INT64, 2=FLOAT64, 3=STRING, 5=BINARY
 */
uint8_t pond_result_column_vtype(const PondResult* result, size_t index);

/*
 * Get the number of values in a column.
 */
size_t pond_result_column_len(const PondResult* result, size_t index);

/*
 * Get INT64 column data as a pointer to an array of int64_t.
 * The pointer is valid until the result is freed.
 * Use pond_result_column_len() to get the array length.
 */
const int64_t* pond_result_column_i64(const PondResult* result, size_t index);

/*
 * Get FLOAT64 column data as a pointer to an array of double.
 * The pointer is valid until the result is freed.
 */
const double* pond_result_column_f64(const PondResult* result, size_t index);

/*
 * Get a STRING column value at a specific row index.
 * Returns a null-terminated string, valid until the result is freed.
 */
const char* pond_result_column_str(const PondResult* result, size_t col_index, size_t row_index);

/*
 * Get a BINARY column value at a specific row index.
 *
 * @param result     PondResult handle
 * @param col_index  column index
 * @param row_index  row index
 * @param out_ptr    output: pointer to the binary value's bytes
 *                   (valid until the result is freed; NULL for null-sentinel rows)
 * @param out_len    output: length of the binary value in bytes
 * @return 0 on success, -1 on null result / out-of-bounds / non-BINARY column
 */
int32_t pond_result_column_bin(const PondResult* result, size_t col_index,
                                size_t row_index,
                                const uint8_t** out_ptr, size_t* out_len);

/*
 * Free a decoded result. Must be called exactly once.
 */
void pond_result_free(PondResult* result);

/*
 * Encode an array of int64_t values into a PND2 blob (single column, RAW encoding).
 *
 * @param values     Pointer to int64_t array
 * @param n_values   Number of values
 * @param out_blob   Output: pointer to blob bytes (caller must free with pond_blob_free)
 * @param out_blob_len Output: length of blob in bytes
 * @return 0 on success, -1 on error
 */
int32_t pond_pnd2_encode_i64(const int64_t* values, size_t n_values,
                              uint8_t** out_blob, size_t* out_blob_len);

/*
 * Encode an array of double values into a PND2 blob (single column, RAW encoding).
 *
 * @param values     Pointer to double array
 * @param n_values   Number of values
 * @param out_blob   Output: pointer to blob bytes (caller must free with pond_blob_free)
 * @param out_blob_len Output: length of blob in bytes
 * @return 0 on success, -1 on error
 */
int32_t pond_pnd2_encode_f64(const double* values, size_t n_values,
                              uint8_t** out_blob, size_t* out_blob_len);

/*
 * Encode an array of null-terminated C strings into a PND2 blob
 * (single column, RAW encoding).
 *
 * @param values     Pointer to array of `const char*` (each null-terminated)
 * @param n_values   Number of strings
 * @param out_blob   Output: pointer to blob bytes (caller must free with pond_blob_free)
 * @param out_blob_len Output: length of blob in bytes
 * @return 0 on success, -1 on error
 */
int32_t pond_pnd2_encode_str(const char** values, size_t n_values,
                              uint8_t** out_blob, size_t* out_blob_len);

/*
 * Free a blob returned by pond_pnd2_encode_i64 / _f64 / _str / encoder_build.
 */
void pond_blob_free(uint8_t* blob, size_t blob_len);

/* ============================================================= *
 * Multi-column encoder (builder pattern)
 *
 * The single-column encoders above don't compose into multi-column
 * blobs. This builder API lets you incrementally build a multi-column
 * PND2 blob:
 *
 *   PondEncoder* enc = pond_encoder_new(n_rows);
 *   pond_encoder_add_i64_column(enc, "id", id_values, n_rows);
 *   pond_encoder_add_f64_column(enc, "score", score_values, n_rows);
 *   pond_encoder_add_str_column(enc, "name", name_ptrs, n_rows);
 *   uint8_t* blob; size_t blob_len;
 *   pond_encoder_build(enc, &blob, &blob_len);
 *   pond_encoder_free(enc);
 *   // ... use blob ...
 *   pond_blob_free(blob, blob_len);
 *
 * All added columns MUST have the same n_rows value passed to
 * pond_encoder_new(). Adding a column with a different length returns -1.
 * ============================================================= */

/* Opaque handle for the multi-column encoder. */
typedef struct PondEncoder PondEncoder;

/* Create a new encoder. Caller must free with pond_encoder_free. */
PondEncoder* pond_encoder_new(size_t n_rows);

/* Add an INT64 column. Computes min/max stats for free.
 * Returns 0 on success, -1 on null pointer / wrong n_rows. */
int32_t pond_encoder_add_i64_column(PondEncoder* enc, const char* name,
                                     const int64_t* values, size_t n_values);

/* Add a FLOAT64 column. Computes min/max stats for free.
 * Returns 0 on success, -1 on null pointer / wrong n_rows. */
int32_t pond_encoder_add_f64_column(PondEncoder* enc, const char* name,
                                     const double* values, size_t n_values);

/* Add a STRING column. No stats (strings don't have meaningful min/max).
 * `values` is an array of `const char*` (each null-terminated).
 * Returns 0 on success, -1 on null pointer / wrong n_rows. */
int32_t pond_encoder_add_str_column(PondEncoder* enc, const char* name,
                                     const char** values, size_t n_values);

/* Build the PND2 blob from all added columns.
 * Returns 0 on success (writes blob + length), -1 on error.
 * Caller owns the blob and must free it with pond_blob_free. */
int32_t pond_encoder_build(PondEncoder* enc,
                            uint8_t** out_blob, size_t* out_blob_len);

/* Free a PondEncoder. Safe to call on NULL. */
void pond_encoder_free(PondEncoder* enc);

#ifdef __cplusplus
}
#endif

#endif /* POND_CORE_H */
