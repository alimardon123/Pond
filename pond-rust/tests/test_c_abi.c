/*
 * Pond Rust Core — C ABI end-to-end test
 *
 * Verifies the C ABI works correctly from a real C program:
 *   1. Encode an i64 array into a PND2 blob via pond_pnd2_encode_i64
 *   2. Decode it back via pond_pnd2_decode
 *   3. Verify column count, name, vtype, length, and value bytes
 *   4. Test error paths (null pointers, empty blob, bad magic)
 *   5. Verify memory cleanup via pond_result_free / pond_blob_free
 *
 * Build (run from pond-rust/):
 *   cc tests/test_c_abi.c -Ipond-core -Ltarget/release \
 *     -lpond_core -lpthread -ldl -lm -o target/test_c_abi && ./target/test_c_abi
 *
 * On success: prints "ALL C ABI TESTS PASSED" and exits 0.
 * On failure: prints the offending assertion and exits 1.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "pond_core.h"

static int failures = 0;
static int passes = 0;

#define CHECK(cond, msg) do {                                  \
    if (!(cond)) {                                             \
        printf("FAIL: %s (line %d)\n", (msg), __LINE__);       \
        failures++;                                            \
    } else {                                                   \
        passes++;                                              \
    }                                                          \
} while (0)

int main(void) {
    printf("=== Pond C ABI Tests ===\n");

    /* ------------------------------------------------------------- */
    /* Test 1: Encode + decode round-trip (i64 column)               */
    /* ------------------------------------------------------------- */
    printf("\n[Test 1] Encode/decode round-trip (i64)\n");
    int64_t input[] = {1, 2, 3, 100, -50, 999999, 0, -1};
    size_t n_input = sizeof(input) / sizeof(input[0]);

    uint8_t *blob = NULL;
    size_t blob_len = 0;
    int32_t rc = pond_pnd2_encode_i64(input, n_input, &blob, &blob_len);
    CHECK(rc == 0, "encode returns 0 on success");
    CHECK(blob != NULL, "encode produces non-null blob");
    CHECK(blob_len > 13, "blob length exceeds PND2 header");

    /* Verify magic + version */
    CHECK(memcmp(blob, "PND2", 4) == 0, "blob has PND2 magic");
    CHECK(blob[4] == 2, "blob is PND2 version 2");

    /* Decode it back */
    PondResult *result = pond_pnd2_decode(blob, blob_len);
    CHECK(result != NULL, "decode produces non-null result");

    /* Verify schema */
    size_t n_cols = pond_result_num_columns(result);
    CHECK(n_cols == 1, "decoded result has 1 column");

    const char *name = pond_result_column_name(result, 0);
    CHECK(name != NULL, "column name is non-null");
    CHECK(strcmp(name, "v") == 0, "column name is 'v'");

    uint8_t vtype = pond_result_column_vtype(result, 0);
    CHECK(vtype == 1, "column vtype is INT64 (1)");

    size_t n_values = pond_result_column_len(result, 0);
    CHECK(n_values == n_input, "column has correct value count");

    /* Verify data */
    const int64_t *data = pond_result_column_i64(result, 0);
    CHECK(data != NULL, "i64 data pointer is non-null");
    int all_match = 1;
    for (size_t i = 0; i < n_input; i++) {
        if (data[i] != input[i]) {
            printf("  mismatch at index %zu: got %lld, expected %lld\n",
                   i, (long long)data[i], (long long)input[i]);
            all_match = 0;
            break;
        }
    }
    CHECK(all_match, "all i64 values match after round-trip");

    /* f64 accessor should return NULL on an i64 column */
    const double *f64_data = pond_result_column_f64(result, 0);
    CHECK(f64_data == NULL, "f64 accessor returns NULL on INT64 column");

    /* str accessor should return NULL on an i64 column */
    const char *str_val = pond_result_column_str(result, 0, 0);
    CHECK(str_val == NULL, "str accessor returns NULL on INT64 column");

    pond_result_free(result);
    pond_blob_free(blob, blob_len);
    printf("  (cleanup OK)\n");

    /* ------------------------------------------------------------- */
    /* Test 2: Empty array (n_values = 0)                            */
    /* ------------------------------------------------------------- */
    printf("\n[Test 2] Encode empty array\n");
    uint8_t *empty_blob = NULL;
    size_t empty_len = 0;
    rc = pond_pnd2_encode_i64(NULL, 0, &empty_blob, &empty_len);
    CHECK(rc != 0, "encode rejects null/empty input");
    CHECK(empty_blob == NULL, "no blob allocated on rejected encode");

    /* ------------------------------------------------------------- */
    /* Test 3: NULL pointer inputs                                    */
    /* ------------------------------------------------------------- */
    printf("\n[Test 3] NULL pointer handling\n");
    CHECK(pond_pnd2_decode(NULL, 100) == NULL, "decode rejects null blob");
    CHECK(pond_pnd2_decode((const uint8_t*)"x", 0) == NULL, "decode rejects zero-length blob");

    uint8_t garbage[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13};
    PondResult *bad = pond_pnd2_decode(garbage, sizeof(garbage));
    CHECK(bad == NULL, "decode rejects blob with bad magic");

    /* Out-of-bounds accessors on a NULL result */
    CHECK(pond_result_num_columns(NULL) == 0, "num_columns(NULL) returns 0");
    CHECK(pond_result_column_name(NULL, 0) == NULL, "column_name(NULL, 0) returns NULL");
    CHECK(pond_result_column_vtype(NULL, 0) == 0, "column_vtype(NULL, 0) returns 0");
    CHECK(pond_result_column_len(NULL, 0) == 0, "column_len(NULL, 0) returns 0");
    CHECK(pond_result_column_i64(NULL, 0) == NULL, "column_i64(NULL, 0) returns NULL");
    CHECK(pond_result_column_f64(NULL, 0) == NULL, "column_f64(NULL, 0) returns NULL");
    CHECK(pond_result_column_str(NULL, 0, 0) == NULL, "column_str(NULL, 0, 0) returns NULL");

    /* Out-of-bounds column index on a valid result */
    uint8_t *b2 = NULL;
    size_t l2 = 0;
    int64_t one_val[] = {42};
    pond_pnd2_encode_i64(one_val, 1, &b2, &l2);
    PondResult *r2 = pond_pnd2_decode(b2, l2);
    CHECK(pond_result_column_name(r2, 99) == NULL, "column_name out-of-bounds returns NULL");
    CHECK(pond_result_column_i64(r2, 99) == NULL, "column_i64 out-of-bounds returns NULL");
    CHECK(pond_result_column_str(r2, 0, 99) == NULL, "column_str row out-of-bounds returns NULL");
    pond_result_free(r2);
    pond_blob_free(b2, l2);

    /* Free of NULL is safe */
    pond_result_free(NULL);
    pond_blob_free(NULL, 0);
    printf("  (all NULL paths safe)\n");

    /* ------------------------------------------------------------- */
    /* Test 4: Larger dataset (1000 values)                          */
    /* ------------------------------------------------------------- */
    printf("\n[Test 4] Larger dataset (1000 values)\n");
    size_t N = 1000;
    int64_t *big = malloc(N * sizeof(int64_t));
    for (size_t i = 0; i < N; i++) {
        big[i] = (int64_t)(i * i - 7 * i);
    }
    uint8_t *big_blob = NULL;
    size_t big_blob_len = 0;
    rc = pond_pnd2_encode_i64(big, N, &big_blob, &big_blob_len);
    CHECK(rc == 0, "encode 1000 values succeeds");

    PondResult *big_result = pond_pnd2_decode(big_blob, big_blob_len);
    CHECK(big_result != NULL, "decode 1000-value blob succeeds");
    CHECK(pond_result_column_len(big_result, 0) == N, "1000 values decoded");

    const int64_t *big_data = pond_result_column_i64(big_result, 0);
    int big_match = 1;
    for (size_t i = 0; i < N; i++) {
        if (big_data[i] != big[i]) { big_match = 0; break; }
    }
    CHECK(big_match, "all 1000 values match after round-trip");

    pond_result_free(big_result);
    pond_blob_free(big_blob, big_blob_len);
    free(big);

    /* ------------------------------------------------------------- */
    /* Summary                                                       */
    /* ------------------------------------------------------------- */
    printf("\n=== Summary ===\n");
    printf("Passed: %d\n", passes);
    printf("Failed: %d\n", failures);

    if (failures > 0) {
        printf("C ABI TESTS FAILED\n");
        return 1;
    }
    printf("ALL C ABI TESTS PASSED\n");
    return 0;
}
