// Package pond provides Go bindings for Pond's PND2 binary codec.
//
// PND2 is Pond's columnar binary format for storing typed data on the
// immutable storage substrate. This package lets Go programs encode and
// decode PND2 blobs with the same byte-level compatibility as the Python
// SDK — blobs produced by Go are decodable by Python and vice versa.
//
// # Architecture
//
// This package links against libpond_core.a (the Rust C ABI in
// pond-rust/pond-core/). All encode/decode logic lives in Rust; this
// package is a thin Go wrapper. See sdk-go/internal/cabi for the cgo
// layer.
//
// # Scope
//
// This package currently exposes PND2 codec operations only:
//   - Encode: build PND2 blobs from Go values (single-column + multi-column)
//   - Decode: parse PND2 blobs into Go values (all encodings, all vtypes)
//
// Storage kernel operations (Write/Read/Ref) are NOT yet available to
// Go callers — they require the Python kernel. A future Rust
// implementation of the storage kernel would enable full Go storage
// support.
//
// # Quick start
//
//      // Encode a 3-column blob
//      enc := pond.NewEncoder(3)
//      enc.AddInt64Column("id", []int64{1, 2, 3})
//      enc.AddFloat64Column("score", []float64{1.5, 2.5, 3.5})
//      enc.AddStringColumn("name", []string{"alice", "bob", "carol"})
//      blob, err := enc.Build()
//      enc.Free()
//
//      // Decode it back
//      result, err := pond.Decode(blob)
//      defer result.Free()
//      for _, col := range result.Columns {
//          fmt.Printf("%s (vtype=%d, n=%d)\n", col.Name, col.Vtype, col.Len())
//      }
package pond

import (
        "fmt"

        "github.com/pond/pond-go/internal/cabi"
)

// VType enumerates PND2 value types.
type VType uint8

const (
        VTInt64   VType = cabi.VT_INT64
        VTFloat64 VType = cabi.VT_FLOAT64
        VTString  VType = cabi.VT_STRING
        VTNull    VType = cabi.VT_NULL
        VTBinary  VType = cabi.VT_BINARY
)

// String returns a human-readable name for the value type.
func (v VType) String() string {
        switch v {
        case VTInt64:
                return "INT64"
        case VTFloat64:
                return "FLOAT64"
        case VTString:
                return "STRING"
        case VTNull:
                return "NULL"
        case VTBinary:
                return "BINARY"
        default:
                return fmt.Sprintf("UNKNOWN(%d)", uint8(v))
        }
}

// Result is a decoded PND2 blob. It owns C memory — callers MUST call
// Free when done. After Free, all derived slices and strings are invalid.
type Result struct {
        handle   *cabi.PondResult
        Columns  []Column
}

// Column is a decoded PND2 column. The Values field is one of:
//   - VTInt64:   []int64
//   - VTFloat64: []float64
//   - VTString:  []string
//   - VTBinary:  [][]byte
//   - VTNull:    nil (use Len() for the row count)
type Column struct {
        Name   string
        Vtype  VType
        Values any
}

// Len returns the number of values in the column.
func (c Column) Len() int {
        switch v := c.Values.(type) {
        case []int64:
                return len(v)
        case []float64:
                return len(v)
        case []string:
                return len(v)
        case [][]byte:
                return len(v)
        }
        return 0
}

// Int64 returns the column's values as []int64. Panics if the column is
// not VTInt64.
func (c Column) Int64() []int64 {
        v, _ := c.Values.([]int64)
        return v
}

// Float64 returns the column's values as []float64.
func (c Column) Float64() []float64 {
        v, _ := c.Values.([]float64)
        return v
}

// String returns the column's values as []string.
func (c Column) String() []string {
        v, _ := c.Values.([]string)
        return v
}

// Binary returns the column's values as [][]byte.
func (c Column) Binary() [][]byte {
        v, _ := c.Values.([][]byte)
        return v
}

// Decode parses a PND2 blob into a Result. The caller MUST call
// result.Free() when done.
//
// Returns an error if the blob is malformed or uses an unsupported
// encoding (zstd compression is not supported — callers must
// decompress first).
func Decode(blob []byte) (*Result, error) {
        if len(blob) == 0 {
                return nil, fmt.Errorf("pond: cannot decode empty blob")
        }
        h := cabi.Decode(blob)
        if h == nil {
                return nil, fmt.Errorf("pond: decode failed (bad magic, malformed header, or zstd-compressed)")
        }

        n := cabi.ResultNumColumns(h)
        cols := make([]Column, n)
        for i := 0; i < n; i++ {
                cols[i] = Column{
                        Name:  cabi.ResultColumnName(h, i),
                        Vtype: VType(cabi.ResultColumnVtype(h, i)),
                }
                // Extract values based on vtype
                switch cols[i].Vtype {
                case VTInt64:
                        // Use the copy variant so the slice survives Result.Free.
                        cols[i].Values = cabi.ResultColumnI64Copy(h, i)
                case VTFloat64:
                        cols[i].Values = cabi.ResultColumnF64Copy(h, i)
                case VTString:
                        // BATCH accessor: get all strings in one cgo call instead of
                        // N per-row calls. ~3x faster for large string columns.
                        cols[i].Values = cabi.ResultColumnStrArray(h, i)
                case VTBinary:
                        nRows := cabi.ResultColumnLen(h, i)
                        bs := make([][]byte, nRows)
                        for r := 0; r < nRows; r++ {
                                b, err := cabi.ResultColumnBin(h, i, r)
                                if err != nil {
                                        return nil, fmt.Errorf("pond: column %d row %d: %w", i, r, err)
                                }
                                bs[r] = b
                        }
                        cols[i].Values = bs
                }
        }

        return &Result{handle: h, Columns: cols}, nil
}

// Free releases the C memory backing this Result. Safe to call multiple
// times. After Free, all slices and strings derived from this Result
// are invalid.
func (r *Result) Free() {
        if r != nil && r.handle != nil {
                cabi.ResultFree(r.handle)
                r.handle = nil
        }
}

// EncodeInt64 encodes a slice of int64 values into a single-column PND2
// blob. The blob is Go-owned (no Free needed).
func EncodeInt64(values []int64) ([]byte, error) {
        return cabi.EncodeI64(values)
}

// EncodeFloat64 encodes a slice of float64 values into a single-column
// PND2 blob.
func EncodeFloat64(values []float64) ([]byte, error) {
        return cabi.EncodeF64(values)
}

// EncodeString encodes a slice of strings into a single-column PND2 blob.
func EncodeString(values []string) ([]byte, error) {
        return cabi.EncodeStr(values)
}

// Encoder builds multi-column PND2 blobs incrementally. All added
// columns must have the same number of rows (passed to NewEncoder).
//
// Usage:
//
//      enc := pond.NewEncoder(3)
//      enc.AddInt64Column("id", []int64{1, 2, 3})
//      enc.AddFloat64Column("score", []float64{1.5, 2.5, 3.5})
//      blob, err := enc.Build()
//      enc.Free()
type Encoder struct {
        handle *cabi.PondEncoder
        nRows  int
}

// NewEncoder creates a new multi-column encoder. All added columns must
// have exactly nRows values.
func NewEncoder(nRows int) *Encoder {
        return &Encoder{
                handle: cabi.EncoderNew(nRows),
                nRows:  nRows,
        }
}

// AddInt64Column adds an INT64 column with the given name. Computes
// min/max stats for free.
func (e *Encoder) AddInt64Column(name string, values []int64) error {
        return cabi.EncoderAddI64Column(e.handle, name, values)
}

// AddFloat64Column adds a FLOAT64 column with min/max stats.
func (e *Encoder) AddFloat64Column(name string, values []float64) error {
        return cabi.EncoderAddF64Column(e.handle, name, values)
}

// AddStringColumn adds a STRING column (no stats — strings don't have
// meaningful min/max in the PND2 stat layout).
func (e *Encoder) AddStringColumn(name string, values []string) error {
        return cabi.EncoderAddStrColumn(e.handle, name, values)
}

// Build finalizes the encoder and returns the PND2 blob. After Build,
// the encoder is still usable (you can add more columns and Build again),
// but the resulting blob will only contain the columns added so far.
func (e *Encoder) Build() ([]byte, error) {
        return cabi.EncoderBuild(e.handle)
}

// Free releases the encoder's C memory. Safe to call multiple times.
// Must be called once when the encoder is no longer needed.
func (e *Encoder) Free() {
        if e != nil && e.handle != nil {
                cabi.EncoderFree(e.handle)
                e.handle = nil
        }
}

// ===========================================================================
// Storage — UnifiedStorage (write, read, branch, merge, history, undo)
// ===========================================================================

// Storage provides full Pond storage access (write, read, branch, merge,
// history, undo, revert) via the unified C ABI. It wraps the Rust
// UnifiedStorage, which is the same code path used by the pond CLI.
//
// Usage:
//
//      store, _ := pond.NewStorage("/path/to/.pond")
//      defer store.Free()
//      store.Write("users", []byte(`{"name":"alice"}`), "initial")
//      data, _ := store.Read("users")
//      store.Branch("users", "experiment")
//      store.Checkout("users", "experiment")
//      store.Write("users", []byte(`{"name":"bob"}`), "experiment")
//      store.Checkout("users", "main")
//      store.Merge("users", "experiment", "", "merge experiment")
type Storage struct {
        handle *cabi.PondStorage
}

// NewStorage creates a new Storage with a local FS backend.
func NewStorage(baseDir string) (*Storage, error) {
        h, err := cabi.StorageNew(baseDir)
        if err != nil {
                return nil, err
        }
        return &Storage{handle: h}, nil
}

// Free releases the storage handle. Must be called when done.
func (s *Storage) Free() {
        if s != nil && s.handle != nil {
                cabi.StorageFree(s.handle)
                s.handle = nil
        }
}

// Write writes data to a collection on the active branch.
// Returns the commit hash.
func (s *Storage) Write(collection string, data []byte, message string) (string, error) {
        return cabi.StorageWrite(s.handle, collection, data, message)
}

// Read reads data from a collection's active branch.
func (s *Storage) Read(collection string) ([]byte, error) {
        return cabi.StorageRead(s.handle, collection)
}

// Branch creates a new branch from the active branch.
func (s *Storage) Branch(collection, branchName string) (string, error) {
        return cabi.StorageBranch(s.handle, collection, branchName)
}

// Checkout switches the active branch.
func (s *Storage) Checkout(collection, branchName string) error {
        return cabi.StorageCheckout(s.handle, collection, branchName)
}

// Merge merges a source branch into a target branch.
// If targetBranch is empty, uses the active branch.
func (s *Storage) Merge(collection, sourceBranch, targetBranch, message string) (string, error) {
        return cabi.StorageMerge(s.handle, collection, sourceBranch, targetBranch, message)
}

// Undo undoes the last N commits on the active branch.
func (s *Storage) Undo(collection string, steps int) (string, error) {
        return cabi.StorageUndo(s.handle, collection, steps)
}

// Revert reverts the active branch to a specific commit.
func (s *Storage) Revert(collection, commitHash string) error {
        return cabi.StorageRevert(s.handle, collection, commitHash)
}

// ListBranches lists all branches for a collection.
func (s *Storage) ListBranches(collection string) ([]string, error) {
        return cabi.StorageListBranches(s.handle, collection)
}
