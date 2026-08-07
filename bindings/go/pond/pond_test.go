package pond_test

import (
        "os"
        "path/filepath"
        "runtime"
        "strings"
        "testing"

        "github.com/pond/pond-go/pond"
)

// TestEncodeDecodeInt64RoundTrip verifies the single-column INT64 path.
func TestEncodeDecodeInt64RoundTrip(t *testing.T) {
        input := []int64{1, 2, 3, 100, -50, 999999, 0, -1}
        blob, err := pond.EncodeInt64(input)
        if err != nil {
                t.Fatalf("EncodeInt64 failed: %v", err)
        }
        if len(blob) < 13 {
                t.Fatalf("blob too short: %d bytes", len(blob))
        }
        if string(blob[:4]) != "PND2" {
                t.Fatalf("blob missing PND2 magic: %q", blob[:4])
        }

        r, err := pond.Decode(blob)
        if err != nil {
                t.Fatalf("Decode failed: %v", err)
        }
        defer r.Free()

        if len(r.Columns) != 1 {
                t.Fatalf("expected 1 column, got %d", len(r.Columns))
        }
        col := r.Columns[0]
        if col.Name != "v" {
                t.Errorf("column name: got %q, want %q", col.Name, "v")
        }
        if col.Vtype != pond.VTInt64 {
                t.Errorf("vtype: got %s, want INT64", col.Vtype)
        }
        if col.Len() != len(input) {
                t.Fatalf("n_values: got %d, want %d", col.Len(), len(input))
        }
        got := col.Int64()
        for i, want := range input {
                if got[i] != want {
                        t.Errorf("value[%d]: got %d, want %d", i, got[i], want)
                }
        }
}

// TestEncodeDecodeFloat64RoundTrip verifies the single-column FLOAT64 path.
func TestEncodeDecodeFloat64RoundTrip(t *testing.T) {
        input := []float64{1.5, 2.5, 3.5, -0.5, 99.99, 0.0, -1.0, 1e10}
        blob, err := pond.EncodeFloat64(input)
        if err != nil {
                t.Fatalf("EncodeFloat64 failed: %v", err)
        }

        r, err := pond.Decode(blob)
        if err != nil {
                t.Fatalf("Decode failed: %v", err)
        }
        defer r.Free()

        if len(r.Columns) != 1 {
                t.Fatalf("expected 1 column, got %d", len(r.Columns))
        }
        col := r.Columns[0]
        if col.Vtype != pond.VTFloat64 {
                t.Errorf("vtype: got %s, want FLOAT64", col.Vtype)
        }
        got := col.Float64()
        for i, want := range input {
                if got[i] != want {
                        t.Errorf("value[%d]: got %f, want %f", i, got[i], want)
                }
        }
}

// TestEncodeDecodeStringRoundTrip verifies the single-column STRING path.
func TestEncodeDecodeStringRoundTrip(t *testing.T) {
        input := []string{"alpha", "beta", "gamma", "delta", ""}
        blob, err := pond.EncodeString(input)
        if err != nil {
                t.Fatalf("EncodeString failed: %v", err)
        }

        r, err := pond.Decode(blob)
        if err != nil {
                t.Fatalf("Decode failed: %v", err)
        }
        defer r.Free()

        col := r.Columns[0]
        if col.Vtype != pond.VTString {
                t.Errorf("vtype: got %s, want STRING", col.Vtype)
        }
        got := col.String()
        if len(got) != len(input) {
                t.Fatalf("n_values: got %d, want %d", len(got), len(input))
        }
        for i, want := range input {
                if got[i] != want {
                        t.Errorf("value[%d]: got %q, want %q", i, got[i], want)
                }
        }
}

// TestMultiColumnEncoder verifies the builder-pattern multi-column encoder.
func TestMultiColumnEncoder(t *testing.T) {
        const nRows = 4
        enc := pond.NewEncoder(nRows)
        defer enc.Free()

        idVals := []int64{10, 20, 30, 40}
        if err := enc.AddInt64Column("id", idVals); err != nil {
                t.Fatalf("AddInt64Column: %v", err)
        }

        scoreVals := []float64{1.5, 2.5, 3.5, 4.5}
        if err := enc.AddFloat64Column("score", scoreVals); err != nil {
                t.Fatalf("AddFloat64Column: %v", err)
        }

        nameVals := []string{"alice", "bob", "carol", "dave"}
        if err := enc.AddStringColumn("name", nameVals); err != nil {
                t.Fatalf("AddStringColumn: %v", err)
        }

        blob, err := enc.Build()
        if err != nil {
                t.Fatalf("Build: %v", err)
        }

        r, err := pond.Decode(blob)
        if err != nil {
                t.Fatalf("Decode: %v", err)
        }
        defer r.Free()

        if len(r.Columns) != 3 {
                t.Fatalf("expected 3 columns, got %d", len(r.Columns))
        }

        // Column 0: id
        c0 := r.Columns[0]
        if c0.Name != "id" {
                t.Errorf("col 0 name: got %q, want %q", c0.Name, "id")
        }
        if c0.Vtype != pond.VTInt64 {
                t.Errorf("col 0 vtype: got %s, want INT64", c0.Vtype)
        }
        for i, want := range idVals {
                if c0.Int64()[i] != want {
                        t.Errorf("col 0[%d]: got %d, want %d", i, c0.Int64()[i], want)
                }
        }

        // Column 1: score
        c1 := r.Columns[1]
        if c1.Name != "score" {
                t.Errorf("col 1 name: got %q, want %q", c1.Name, "score")
        }
        if c1.Vtype != pond.VTFloat64 {
                t.Errorf("col 1 vtype: got %s, want FLOAT64", c1.Vtype)
        }
        for i, want := range scoreVals {
                if c1.Float64()[i] != want {
                        t.Errorf("col 1[%d]: got %f, want %f", i, c1.Float64()[i], want)
                }
        }

        // Column 2: name
        c2 := r.Columns[2]
        if c2.Name != "name" {
                t.Errorf("col 2 name: got %q, want %q", c2.Name, "name")
        }
        if c2.Vtype != pond.VTString {
                t.Errorf("col 2 vtype: got %s, want STRING", c2.Vtype)
        }
        for i, want := range nameVals {
                if c2.String()[i] != want {
                        t.Errorf("col 2[%d]: got %q, want %q", i, c2.String()[i], want)
                }
        }
}

// TestDecodePythonBlobs verifies that the Go decoder can decode blobs
// produced by the Python encoder. This proves byte-level compatibility.
//
// The test blobs are generated by pond-rust/tests/generate_test_blobs.py.
// If they don't exist, the test is skipped (run the Python script first).
func TestDecodePythonBlobs(t *testing.T) {
        blobDir := pythonBlobDir()
        if blobDir == "" {
                t.Skip("could not locate bindings/base/test_blobs/ — run generate_test_blobs.py first")
        }

        tests := []struct {
                filename    string
                wantVtype   pond.VType
                wantNValues int
        }{
                {"i64_raw.bin", pond.VTInt64, 8},
                {"f64_raw.bin", pond.VTFloat64, 8},
                {"str_raw.bin", pond.VTString, 5},
                {"i64_rle.bin", pond.VTInt64, 100},
                {"str_dict.bin", pond.VTString, 10},
                {"i64_bitpack.bin", pond.VTInt64, 200},
                {"bin_raw.bin", pond.VTBinary, 5},
        }

        for _, tc := range tests {
                t.Run(tc.filename, func(t *testing.T) {
                        path := filepath.Join(blobDir, tc.filename)
                        blob, err := os.ReadFile(path)
                        if err != nil {
                                t.Skipf("blob file missing: %s (run generate_test_blobs.py)", path)
                        }

                        r, err := pond.Decode(blob)
                        if err != nil {
                                t.Fatalf("Decode(%s): %v", tc.filename, err)
                        }
                        defer r.Free()

                        if len(r.Columns) != 1 {
                                t.Fatalf("expected 1 column, got %d", len(r.Columns))
                        }
                        col := r.Columns[0]
                        if col.Vtype != tc.wantVtype {
                                t.Errorf("vtype: got %s, want %s", col.Vtype, tc.wantVtype)
                        }
                        if col.Len() != tc.wantNValues {
                                t.Errorf("n_values: got %d, want %d", col.Len(), tc.wantNValues)
                        }
                })
        }
}

// TestDecodeEmptyBlob verifies the error path.
func TestDecodeEmptyBlob(t *testing.T) {
        _, err := pond.Decode(nil)
        if err == nil {
                t.Error("expected error decoding nil blob, got nil")
        }
        _, err = pond.Decode([]byte{})
        if err == nil {
                t.Error("expected error decoding empty blob, got nil")
        }
}

// TestDecodeGarbage verifies malformed blobs are rejected.
func TestDecodeGarbage(t *testing.T) {
        garbage := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
        _, err := pond.Decode(garbage)
        if err == nil {
                t.Error("expected error decoding garbage, got nil")
        }
}

// pythonBlobDir locates the bindings/base/test_blobs/ directory by
// walking up from the test file's location. Returns "" if not found.
func pythonBlobDir() string {
        _, thisFile, _, ok := runtime.Caller(0)
        if !ok {
                return ""
        }
        // thisFile = .../sdk-go/pond/pond_test.go
        // walk up: pond/ → sdk-go/ → pond_repo/
        dir := filepath.Dir(thisFile)
        for i := 0; i < 5; i++ {
                candidate := filepath.Join(dir, "pond-rust", "tests", "test_blobs")
                if info, err := os.Stat(candidate); err == nil && info.IsDir() {
                        return candidate
                }
                dir = filepath.Dir(dir)
        }
        return ""
}

// ===========================================================================
// Storage tests — full storage access via C ABI
// ===========================================================================

func TestStorageWriteAndRead(t *testing.T) {
        dir := t.TempDir()
        store, err := pond.NewStorage(dir)
        if err != nil {
                t.Fatalf("NewStorage: %v", err)
        }
        defer store.Free()

        data := []byte(`{"name":"alice","v":1}`)
        hash, err := store.Write("users", data, "initial")
        if err != nil {
                t.Fatalf("Write: %v", err)
        }
        if len(hash) != 64 {
                t.Errorf("hash length: got %d, want 64", len(hash))
        }

        read, err := store.Read("users")
        if err != nil {
                t.Fatalf("Read: %v", err)
        }
        if string(read) != string(data) {
                t.Errorf("read mismatch: got %s, want %s", read, data)
        }
}

func TestStorageBranchAndMerge(t *testing.T) {
        dir := t.TempDir()
        store, _ := pond.NewStorage(dir)
        defer store.Free()

        // Write initial data
        store.Write("users", []byte(`{"v":1}`), "initial")

        // Create branch
        store.Branch("users", "experiment")

        // List branches
        branches, _ := store.ListBranches("users")
        if !contains(branches, "main") || !contains(branches, "experiment") {
                t.Errorf("branches: got %v, want main+experiment", branches)
        }

        // Checkout experiment and write
        store.Checkout("users", "experiment")
        store.Write("users", []byte(`{"v":99}`), "experiment")

        // Read should show experiment data
        read, _ := store.Read("users")
        if !strings.Contains(string(read), "99") {
                t.Errorf("experiment data: got %s, want v=99", read)
        }

        // Checkout main — should still have v=1
        store.Checkout("users", "main")
        read, _ = store.Read("users")
        if !strings.Contains(string(read), `"v":1`) {
                t.Errorf("main data: got %s, want v=1", read)
        }

        // Merge experiment into main
        _, err := store.Merge("users", "experiment", "", "test merge")
        if err != nil {
                t.Fatalf("Merge: %v", err)
        }

        // Read should now show experiment data (merged)
        read, _ = store.Read("users")
        if !strings.Contains(string(read), "99") {
                t.Errorf("merged data: got %s, want v=99", read)
        }
}

func TestStorageUndo(t *testing.T) {
        dir := t.TempDir()
        store, _ := pond.NewStorage(dir)
        defer store.Free()

        store.Write("users", []byte(`{"v":1}`), "first")
        store.Write("users", []byte(`{"v":2}`), "second")
        store.Write("users", []byte(`{"v":3}`), "third")

        // Undo 1 step → should be v=2
        store.Undo("users", 1)
        read, _ := store.Read("users")
        if !strings.Contains(string(read), `"v":2`) {
                t.Errorf("after undo 1: got %s, want v=2", read)
        }
}

func contains(slice []string, s string) bool {
        for _, v := range slice {
                if v == s {
                        return true
                }
        }
        return false
}
