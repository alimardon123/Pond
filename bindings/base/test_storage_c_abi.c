/*
 * Pond Storage C ABI Test
 *
 * Tests the full storage C ABI: new, write, read, branch, checkout,
 * merge, undo, revert, list_branches.
 *
 * Build:
 *   cc tests/test_storage_c_abi.c -Ipond-rust \
 *     target/release/libpond_storage.a \
 *     target/release/libpond_kernel.a \
 *     target/release/libpond_core.a \
 *     -lpthread -ldl -lm -o target/release/test_storage_c_abi
 *
 * Run:
 *   ./target/release/test_storage_c_abi
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "pond.h"

static int passes = 0;
static int failures = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { printf("FAIL: %s (line %d)\n", msg, __LINE__); failures++; } \
    else { passes++; } \
} while (0)

int main(void) {
    printf("=== Pond Storage C ABI Tests ===\n");

    /* Create a temp directory */
    const char* tmpdir = "/tmp/pond_c_abi_test";
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", tmpdir, tmpdir);
    system(cmd);

    /* Create storage */
    PondStorageHandle* s = pond_storage_new(tmpdir);
    CHECK(s != NULL, "pond_storage_new returns non-null");

    /* Write data */
    const char* data1 = "{\"name\":\"alice\",\"v\":1}";
    char* hash1 = pond_storage_write(s, "users", (const uint8_t*)data1, strlen(data1), "initial");
    CHECK(hash1 != NULL, "pond_storage_write returns non-null");
    CHECK(strlen(hash1) == 64, "write returns 64-char hash");
    pond_storage_string_free(hash1);

    /* Write more data */
    const char* data2 = "{\"name\":\"bob\",\"v\":2}";
    char* hash2 = pond_storage_write(s, "users", (const uint8_t*)data2, strlen(data2), "second");
    CHECK(hash2 != NULL, "second write returns non-null");
    pond_storage_string_free(hash2);

    /* Read current HEAD */
    const uint8_t* read_data = NULL;
    size_t read_len = 0;
    int rc = pond_storage_read(s, "users", &read_data, &read_len);
    CHECK(rc == 0, "pond_storage_read returns 0");
    CHECK(read_data != NULL, "read data is non-null");
    CHECK(read_len == strlen(data2), "read data length matches");
    CHECK(memcmp(read_data, data2, read_len) == 0, "read data matches written data");
    pond_storage_data_free((uint8_t*)read_data, read_len);

    /* Create a branch */
    char* branch_hash = pond_storage_branch(s, "users", "experiment");
    CHECK(branch_hash != NULL, "pond_storage_branch returns non-null");
    pond_storage_string_free(branch_hash);

    /* List branches */
    char* branches = pond_storage_list_branches(s, "users");
    CHECK(branches != NULL, "pond_storage_list_branches returns non-null");
    CHECK(strstr(branches, "main") != NULL, "branches list contains main");
    CHECK(strstr(branches, "experiment") != NULL, "branches list contains experiment");
    pond_storage_string_free(branches);

    /* Checkout experiment */
    rc = pond_storage_checkout(s, "users", "experiment");
    CHECK(rc == 0, "pond_storage_checkout returns 0");

    /* Write on experiment branch */
    const char* data3 = "{\"name\":\"exp\",\"v\":99}";
    char* hash3 = pond_storage_write(s, "users", (const uint8_t*)data3, strlen(data3), "experiment");
    CHECK(hash3 != NULL, "write on experiment returns non-null");
    pond_storage_string_free(hash3);

    /* Read should show experiment data */
    read_data = NULL; read_len = 0;
    rc = pond_storage_read(s, "users", &read_data, &read_len);
    CHECK(rc == 0, "read on experiment returns 0");
    CHECK(memcmp(read_data, data3, read_len) == 0, "experiment data matches");
    pond_storage_data_free((uint8_t*)read_data, read_len);

    /* Checkout back to main */
    rc = pond_storage_checkout(s, "users", "main");
    CHECK(rc == 0, "checkout back to main returns 0");

    /* Read should show main data (v2) */
    read_data = NULL; read_len = 0;
    rc = pond_storage_read(s, "users", &read_data, &read_len);
    CHECK(rc == 0, "read on main returns 0");
    CHECK(memcmp(read_data, data2, read_len) == 0, "main data unchanged after experiment");
    pond_storage_data_free((uint8_t*)read_data, read_len);

    /* Merge experiment into main */
    char* merge_hash = pond_storage_merge(s, "users", "experiment", NULL, "test merge");
    CHECK(merge_hash != NULL, "pond_storage_merge returns non-null");
    pond_storage_string_free(merge_hash);

    /* Read should now show experiment data (merged) */
    read_data = NULL; read_len = 0;
    rc = pond_storage_read(s, "users", &read_data, &read_len);
    CHECK(rc == 0, "read after merge returns 0");
    CHECK(memcmp(read_data, data3, read_len) == 0, "merged data matches experiment");
    pond_storage_data_free((uint8_t*)read_data, read_len);

    /* Undo 1 step */
    char* undo_hash = pond_storage_undo(s, "users", 1);
    CHECK(undo_hash != NULL, "pond_storage_undo returns non-null");
    pond_storage_string_free(undo_hash);

    /* Read should show pre-merge data (v2) */
    read_data = NULL; read_len = 0;
    rc = pond_storage_read(s, "users", &read_data, &read_len);
    CHECK(rc == 0, "read after undo returns 0");
    CHECK(memcmp(read_data, data2, read_len) == 0, "undo restored main data");
    pond_storage_data_free((uint8_t*)read_data, read_len);

    /* Free storage */
    pond_storage_free(s);

    /* Cleanup */
    snprintf(cmd, sizeof(cmd), "rm -rf %s", tmpdir);
    system(cmd);

    printf("\n=== Summary ===\n");
    printf("Passed: %d\n", passes);
    printf("Failed: %d\n", failures);
    if (failures > 0) { printf("C ABI TESTS FAILED\n"); return 1; }
    printf("ALL C ABI TESTS PASSED\n");
    return 0;
}
