// Package cabi is the thin cgo layer over Pond's unified C ABI.
//
// It links against libpond_storage.a (which pulls in libpond_kernel.a and
// libpond_storage.a automatically). This gives Go full access to all three
// layers: kernel (Write/Read/Ref), storage (write/read/branch/merge),
// and codec (PND2 encode/decode).
//
// Users should NOT import this package directly. Use github.com/pond/pond-go/pond.
package cabi

// #cgo CFLAGS: -I${SRCDIR}/../../../../bindings/base
// #cgo LDFLAGS: ${SRCDIR}/../../../../target/release/libpond_storage.a ${SRCDIR}/../../../../target/release/libpond_kernel.a -lpthread -ldl -lm
//
// #include <stdlib.h>
// #include "pond.h"
import "C"

import (
        "strings"
        "unsafe"
)

// Re-export the C constants as Go constants for type-safe usage.
const (
        VT_INT64   = 1
        VT_FLOAT64 = 2
        VT_STRING  = 3
        VT_NULL    = 4
        VT_BINARY  = 5

        ENC_RAW     = 0
        ENC_RLE     = 1
        ENC_DICT    = 2
        ENC_BITPACK = 3
)

// PondResult is the opaque C handle for a decoded PND2 blob.
type PondResult = C.PondResult

// PondEncoder is the opaque C handle for the multi-column encoder.
type PondEncoder = C.PondEncoder

// Decode wraps pond_pnd2_decode.
func Decode(blob []byte) *PondResult {
        if len(blob) == 0 {
                return nil
        }
        return C.pond_pnd2_decode((*C.uint8_t)(unsafe.Pointer(&blob[0])),
                C.size_t(len(blob)))
}

// ResultNumColumns wraps pond_result_num_columns.
func ResultNumColumns(r *PondResult) int {
        if r == nil {
                return 0
        }
        return int(C.pond_result_num_columns(r))
}

// ResultColumnName wraps pond_result_column_name. Returns the column name
// as a Go string (copied — safe to use after ResultFree).
func ResultColumnName(r *PondResult, idx int) string {
        if r == nil {
                return ""
        }
        cs := C.pond_result_column_name(r, C.size_t(idx))
        if cs == nil {
                return ""
        }
        return C.GoString(cs)
}

// ResultColumnVtype wraps pond_result_column_vtype.
func ResultColumnVtype(r *PondResult, idx int) uint8 {
        if r == nil {
                return 0
        }
        return uint8(C.pond_result_column_vtype(r, C.size_t(idx)))
}

// ResultColumnLen wraps pond_result_column_len.
func ResultColumnLen(r *PondResult, idx int) int {
        if r == nil {
                return 0
        }
        return int(C.pond_result_column_len(r, C.size_t(idx)))
}

// ResultColumnI64 wraps pond_result_column_i64.
//
// Returns a slice backed by the C array. The slice is valid ONLY while
// the PondResult is alive — callers MUST NOT use it after calling
// ResultFree. For a safe copy, use ResultColumnI64Copy.
func ResultColumnI64(r *PondResult, idx int) []int64 {
        if r == nil {
                return nil
        }
        p := C.pond_result_column_i64(r, C.size_t(idx))
        if p == nil {
                return nil
        }
        n := ResultColumnLen(r, idx)
        if n == 0 {
                return nil
        }
        return unsafe.Slice((*int64)(unsafe.Pointer(p)), n)
}

// ResultColumnI64Copy is the safe version of ResultColumnI64 — it copies
// the values into a Go-owned slice that survives ResultFree.
func ResultColumnI64Copy(r *PondResult, idx int) []int64 {
        src := ResultColumnI64(r, idx)
        if src == nil {
                return nil
        }
        out := make([]int64, len(src))
        copy(out, src)
        return out
}

// ResultColumnF64 wraps pond_result_column_f64. Same lifetime caveat as
// ResultColumnI64.
func ResultColumnF64(r *PondResult, idx int) []float64 {
        if r == nil {
                return nil
        }
        p := C.pond_result_column_f64(r, C.size_t(idx))
        if p == nil {
                return nil
        }
        n := ResultColumnLen(r, idx)
        if n == 0 {
                return nil
        }
        return unsafe.Slice((*float64)(unsafe.Pointer(p)), n)
}

// ResultColumnF64Copy is the safe version of ResultColumnF64.
func ResultColumnF64Copy(r *PondResult, idx int) []float64 {
        src := ResultColumnF64(r, idx)
        if src == nil {
                return nil
        }
        out := make([]float64, len(src))
        copy(out, src)
        return out
}

// ResultColumnStr wraps pond_result_column_str. Returns the value as a
// Go string (copied — safe to use after ResultFree).
func ResultColumnStr(r *PondResult, colIdx, rowIdx int) string {
        if r == nil {
                return ""
        }
        cs := C.pond_result_column_str(r, C.size_t(colIdx), C.size_t(rowIdx))
        if cs == nil {
                return ""
        }
        return C.GoString(cs)
}

// ResultColumnStrArray wraps pond_result_column_str_array (BATCH accessor).
//
// Returns a Go []string containing ALL string values from the column in
// one call. This is MUCH faster than calling ResultColumnStr in a loop
// for columns with many rows — the per-row variant has cgo overhead per
// call, while this variant pays cgo overhead only once per column.
//
// The strings are copied into Go-owned memory, so they remain valid after
// ResultFree.
func ResultColumnStrArray(r *PondResult, colIdx int) []string {
        if r == nil {
                return nil
        }
        n := ResultColumnLen(r, colIdx)
        if n == 0 {
                return []string{}
        }
        arr := C.pond_result_column_str_array(r, C.size_t(colIdx))
        if arr == nil {
                return nil
        }
        // Walk the C array of char* pointers and copy each into a Go string.
        out := make([]string, n)
        for i := 0; i < n; i++ {
                // arr[i] is a *C.char — dereference via indexing on the pointer array.
                // Go's cgo allows indexing on C pointer types.
                ptr := (*[1 << 30](*C.char))(unsafe.Pointer(arr))[i]
                if ptr != nil {
                        out[i] = C.GoString(ptr)
                }
        }
        return out
}

// ResultColumnBin wraps pond_result_column_bin. Returns the value as a
// Go byte slice (copied — safe to use after ResultFree).
func ResultColumnBin(r *PondResult, colIdx, rowIdx int) ([]byte, error) {
        if r == nil {
                return nil, errNullResult
        }
        var ptr *C.uint8_t
        var length C.size_t
        rc := C.pond_result_column_bin(r, C.size_t(colIdx), C.size_t(rowIdx),
                &ptr, &length)
        if rc != 0 {
                return nil, errColumnAccess
        }
        if ptr == nil || length == 0 {
                return []byte{}, nil
        }
        return C.GoBytes(unsafe.Pointer(ptr), C.int(length)), nil
}

// ResultFree wraps pond_result_free. Safe to call on nil.
func ResultFree(r *PondResult) {
        if r != nil {
                C.pond_result_free(r)
        }
}

// EncodeI64 wraps pond_pnd2_encode_i64. Returns a Go-owned blob (copied
// from the C allocation — safe to hold indefinitely).
func EncodeI64(values []int64) ([]byte, error) {
        if len(values) == 0 {
                return nil, errEmptyInput
        }
        var blobPtr *C.uint8_t
        var blobLen C.size_t
        rc := C.pond_pnd2_encode_i64((*C.int64_t)(unsafe.Pointer(&values[0])),
                C.size_t(len(values)), &blobPtr, &blobLen)
        if rc != 0 {
                return nil, errEncode
        }
        return copyBlob(blobPtr, blobLen), nil
}

// EncodeF64 wraps pond_pnd2_encode_f64.
func EncodeF64(values []float64) ([]byte, error) {
        if len(values) == 0 {
                return nil, errEmptyInput
        }
        var blobPtr *C.uint8_t
        var blobLen C.size_t
        rc := C.pond_pnd2_encode_f64((*C.double)(unsafe.Pointer(&values[0])),
                C.size_t(len(values)), &blobPtr, &blobLen)
        if rc != 0 {
                return nil, errEncode
        }
        return copyBlob(blobPtr, blobLen), nil
}

// EncodeStr wraps pond_pnd2_encode_str.
func EncodeStr(values []string) ([]byte, error) {
        if len(values) == 0 {
                return nil, errEmptyInput
        }
        cstrs := make([]*C.char, len(values))
        for i, s := range values {
                cstrs[i] = C.CString(s)
                defer C.free(unsafe.Pointer(cstrs[i]))
        }
        var blobPtr *C.uint8_t
        var blobLen C.size_t
        rc := C.pond_pnd2_encode_str(&cstrs[0], C.size_t(len(cstrs)),
                &blobPtr, &blobLen)
        if rc != 0 {
                return nil, errEncode
        }
        return copyBlob(blobPtr, blobLen), nil
}

// EncoderNew wraps pond_encoder_new.
func EncoderNew(nRows int) *PondEncoder {
        return C.pond_encoder_new(C.size_t(nRows))
}

// EncoderAddI64Column wraps pond_encoder_add_i64_column.
func EncoderAddI64Column(enc *PondEncoder, name string, values []int64) error {
        if enc == nil {
                return errNullEncoder
        }
        cname := C.CString(name)
        defer C.free(unsafe.Pointer(cname))
        if len(values) == 0 {
                return errEmptyInput
        }
        rc := C.pond_encoder_add_i64_column(enc, cname,
                (*C.int64_t)(unsafe.Pointer(&values[0])), C.size_t(len(values)))
        if rc != 0 {
                return errColumnAdd
        }
        return nil
}

// EncoderAddF64Column wraps pond_encoder_add_f64_column.
func EncoderAddF64Column(enc *PondEncoder, name string, values []float64) error {
        if enc == nil {
                return errNullEncoder
        }
        cname := C.CString(name)
        defer C.free(unsafe.Pointer(cname))
        if len(values) == 0 {
                return errEmptyInput
        }
        rc := C.pond_encoder_add_f64_column(enc, cname,
                (*C.double)(unsafe.Pointer(&values[0])), C.size_t(len(values)))
        if rc != 0 {
                return errColumnAdd
        }
        return nil
}

// EncoderAddStrColumn wraps pond_encoder_add_str_column.
func EncoderAddStrColumn(enc *PondEncoder, name string, values []string) error {
        if enc == nil {
                return errNullEncoder
        }
        cname := C.CString(name)
        defer C.free(unsafe.Pointer(cname))
        if len(values) == 0 {
                return errEmptyInput
        }
        cstrs := make([]*C.char, len(values))
        for i, s := range values {
                cstrs[i] = C.CString(s)
                defer C.free(unsafe.Pointer(cstrs[i]))
        }
        rc := C.pond_encoder_add_str_column(enc, cname, &cstrs[0],
                C.size_t(len(cstrs)))
        if rc != 0 {
                return errColumnAdd
        }
        return nil
}

// EncoderBuild wraps pond_encoder_build.
func EncoderBuild(enc *PondEncoder) ([]byte, error) {
        if enc == nil {
                return nil, errNullEncoder
        }
        var blobPtr *C.uint8_t
        var blobLen C.size_t
        rc := C.pond_encoder_build(enc, &blobPtr, &blobLen)
        if rc != 0 {
                return nil, errEncode
        }
        return copyBlob(blobPtr, blobLen), nil
}

// EncoderFree wraps pond_encoder_free. Safe to call on nil.
func EncoderFree(enc *PondEncoder) {
        if enc != nil {
                C.pond_encoder_free(enc)
        }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// copyBlob copies a C-allocated blob into a Go-owned slice and frees the C
// allocation. We copy (rather than share) because Go's GC doesn't track
// C memory — sharing would lead to use-after-free bugs if the user held
// a Go slice after pond_blob_free was called.
func copyBlob(ptr *C.uint8_t, length C.size_t) []byte {
        if ptr == nil || length == 0 {
                return nil
        }
        out := C.GoBytes(unsafe.Pointer(ptr), C.int(length))
        C.pond_blob_free(ptr, length)
        return out
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
        errNullResult   = &Error{Code: "null_result", Msg: "PondResult handle is nil"}
        errColumnAccess = &Error{Code: "column_access", Msg: "column index out of bounds or vtype mismatch"}
        errEmptyInput   = &Error{Code: "empty_input", Msg: "input slice is empty"}
        errEncode       = &Error{Code: "encode_failed", Msg: "Rust encoder returned an error"}
        errNullEncoder  = &Error{Code: "null_encoder", Msg: "PondEncoder handle is nil"}
        errColumnAdd    = &Error{Code: "column_add_failed", Msg: "could not add column (bad name/values/length)"}
)

// Error is the package's error type. Code is a stable string for
// programmatic matching; Msg is human-readable.
type Error struct {
        Code string
        Msg  string
}

func (e *Error) Error() string { return e.Code + ": " + e.Msg }

// ===========================================================================
// Storage C ABI — UnifiedStorage (write, read, branch, merge, etc.)
// ===========================================================================

// PondStorage is the opaque C handle for UnifiedStorage.
type PondStorage = C.PondStorageHandle

// StorageNew creates a new UnifiedStorage with a local FS backend.
func StorageNew(baseDir string) (*PondStorage, error) {
        cDir := C.CString(baseDir)
        defer C.free(unsafe.Pointer(cDir))
        h := C.pond_storage_new(cDir)
        if h == nil {
                return nil, &Error{Code: "storage_new_failed", Msg: "failed to create storage"}
        }
        return h, nil
}

// StorageFree frees a storage handle. Safe on nil.
func StorageFree(s *PondStorage) {
        if s != nil {
                C.pond_storage_free(s)
        }
}

// StorageWrite writes data to a collection on the active branch.
// Returns the commit hash.
func StorageWrite(s *PondStorage, collection string, data []byte, message string) (string, error) {
        cColl := C.CString(collection)
        defer C.free(unsafe.Pointer(cColl))
        cMsg := C.CString(message)
        defer C.free(unsafe.Pointer(cMsg))
        hash := C.pond_storage_write(s, cColl,
                (*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)), cMsg)
        if hash == nil {
                return "", &Error{Code: "storage_write_failed", Msg: "write failed"}
        }
        result := C.GoString(hash)
        C.pond_storage_string_free(hash)
        return result, nil
}

// StorageRead reads data from a collection's active branch.
func StorageRead(s *PondStorage, collection string) ([]byte, error) {
        cColl := C.CString(collection)
        defer C.free(unsafe.Pointer(cColl))
        var outData *C.uint8_t
        var outLen C.size_t
        rc := C.pond_storage_read(s, cColl, &outData, &outLen)
        if rc != 0 {
                return nil, &Error{Code: "storage_read_failed", Msg: "read failed"}
        }
        if outData == nil || outLen == 0 {
                return []byte{}, nil
        }
        result := C.GoBytes(unsafe.Pointer(outData), C.int(outLen))
        C.pond_storage_data_free((*C.uint8_t)(unsafe.Pointer(outData)), outLen)
        return result, nil
}

// StorageBranch creates a branch from the active branch.
func StorageBranch(s *PondStorage, collection, branchName string) (string, error) {
        cColl := C.CString(collection)
        defer C.free(unsafe.Pointer(cColl))
        cBranch := C.CString(branchName)
        defer C.free(unsafe.Pointer(cBranch))
        hash := C.pond_storage_branch(s, cColl, cBranch)
        if hash == nil {
                return "", &Error{Code: "storage_branch_failed", Msg: "branch failed"}
        }
        result := C.GoString(hash)
        C.pond_storage_string_free(hash)
        return result, nil
}

// StorageCheckout switches the active branch.
func StorageCheckout(s *PondStorage, collection, branchName string) error {
        cColl := C.CString(collection)
        defer C.free(unsafe.Pointer(cColl))
        cBranch := C.CString(branchName)
        defer C.free(unsafe.Pointer(cBranch))
        rc := C.pond_storage_checkout(s, cColl, cBranch)
        if rc != 0 {
                return &Error{Code: "storage_checkout_failed", Msg: "checkout failed"}
        }
        return nil
}

// StorageMerge merges a source branch into a target branch.
// If target is empty, uses the active branch.
func StorageMerge(s *PondStorage, collection, sourceBranch, targetBranch, message string) (string, error) {
        cColl := C.CString(collection)
        defer C.free(unsafe.Pointer(cColl))
        cSrc := C.CString(sourceBranch)
        defer C.free(unsafe.Pointer(cSrc))
        var cTgt *C.char
        if targetBranch != "" {
                cTgt = C.CString(targetBranch)
                defer C.free(unsafe.Pointer(cTgt))
        }
        cMsg := C.CString(message)
        defer C.free(unsafe.Pointer(cMsg))
        hash := C.pond_storage_merge(s, cColl, cSrc, cTgt, cMsg)
        if hash == nil {
                return "", &Error{Code: "storage_merge_failed", Msg: "merge failed"}
        }
        result := C.GoString(hash)
        C.pond_storage_string_free(hash)
        return result, nil
}

// StorageUndo undoes the last N commits.
func StorageUndo(s *PondStorage, collection string, steps int) (string, error) {
        cColl := C.CString(collection)
        defer C.free(unsafe.Pointer(cColl))
        hash := C.pond_storage_undo(s, cColl, C.size_t(steps))
        if hash == nil {
                return "", &Error{Code: "storage_undo_failed", Msg: "undo failed"}
        }
        result := C.GoString(hash)
        C.pond_storage_string_free(hash)
        return result, nil
}

// StorageRevert reverts to a specific commit.
func StorageRevert(s *PondStorage, collection, commitHash string) error {
        cColl := C.CString(collection)
        defer C.free(unsafe.Pointer(cColl))
        cHash := C.CString(commitHash)
        defer C.free(unsafe.Pointer(cHash))
        rc := C.pond_storage_revert(s, cColl, cHash)
        if rc != 0 {
                return &Error{Code: "storage_revert_failed", Msg: "revert failed"}
        }
        return nil
}

// StorageListBranches lists branches for a collection.
func StorageListBranches(s *PondStorage, collection string) ([]string, error) {
        cColl := C.CString(collection)
        defer C.free(unsafe.Pointer(cColl))
        result := C.pond_storage_list_branches(s, cColl)
        if result == nil {
                return nil, &Error{Code: "storage_list_branches_failed", Msg: "list_branches failed"}
        }
        joined := C.GoString(result)
        C.pond_storage_string_free(result)
        if joined == "" {
                return []string{}, nil
        }
        return strings.Split(joined, "\n"), nil
}
