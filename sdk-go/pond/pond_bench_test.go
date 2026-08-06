package pond_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pond/pond-go/pond"
)

// BenchmarkDecodeNumeric measures Go decode throughput for a numeric-heavy
// blob (3 INT64/FLOAT64 columns, no strings).
//
// This is the fast path — the C ABI returns raw pointers that Go can
// access with zero copy (after the initial pointer setup).
func BenchmarkDecodeNumeric(b *testing.B) {
	blob := makeNumericBlob(b, 100_000)
	b.ResetTimer()
	b.SetBytes(int64(len(blob)))
	for i := 0; i < b.N; i++ {
		r, err := pond.Decode(blob)
		if err != nil {
			b.Fatal(err)
		}
		r.Free()
	}
}

// BenchmarkDecodeMixed measures Go decode throughput for a mixed blob
// (1 INT64 + 1 FLOAT64 + 1 STRING column — typical real-world workload).
//
// Uses the batch string accessor for the STRING column — one cgo call
// per column instead of N per-row calls.
func BenchmarkDecodeMixed(b *testing.B) {
	blob := makeMixedBlob(b, 100_000)
	b.ResetTimer()
	b.SetBytes(int64(len(blob)))
	for i := 0; i < b.N; i++ {
		r, err := pond.Decode(blob)
		if err != nil {
			b.Fatal(err)
		}
		r.Free()
	}
}

// BenchmarkDecodeStringHeavy measures Go decode throughput for a
// string-heavy blob (3 STRING columns — worst case for FFI overhead).
func BenchmarkDecodeStringHeavy(b *testing.B) {
	blob := makeStringHeavyBlob(b, 100_000)
	b.ResetTimer()
	b.SetBytes(int64(len(blob)))
	for i := 0; i < b.N; i++ {
		r, err := pond.Decode(blob)
		if err != nil {
			b.Fatal(err)
		}
		r.Free()
	}
}

// BenchmarkMultiColEncode measures multi-column encode throughput.
func BenchmarkMultiColEncode(b *testing.B) {
	nRows := 100_000
	ids := make([]int64, nRows)
	scores := make([]float64, nRows)
	names := make([]string, nRows)
	for i := 0; i < nRows; i++ {
		ids[i] = int64(i)
		scores[i] = float64(i) * 1.5
		names[i] = "user_" + itoa(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc := pond.NewEncoder(nRows)
		enc.AddInt64Column("id", ids)
		enc.AddFloat64Column("score", scores)
		enc.AddStringColumn("name", names)
		_, err := enc.Build()
		if err != nil {
			b.Fatal(err)
		}
		enc.Free()
	}
}

// ---------------------------------------------------------------------------
// Helpers — generate test blobs via the Python encoder (cross-lang compat)
// ---------------------------------------------------------------------------

func makeNumericBlob(tb testing.TB, nRows int) []byte {
	tb.Helper()
	// Build a 3-column INT64 blob using the Go encoder (no Python needed).
	ids := make([]int64, nRows)
	vals := make([]int64, nRows)
	scores := make([]float64, nRows)
	for i := 0; i < nRows; i++ {
		ids[i] = int64((i * 7) % 1000000)
		vals[i] = int64((i * 13) % 500000)
		scores[i] = float64(i) * 1.5
	}
	enc := pond.NewEncoder(nRows)
	if err := enc.AddInt64Column("id", ids); err != nil {
		tb.Fatal(err)
	}
	if err := enc.AddInt64Column("val", vals); err != nil {
		tb.Fatal(err)
	}
	if err := enc.AddFloat64Column("score", scores); err != nil {
		tb.Fatal(err)
	}
	blob, err := enc.Build()
	if err != nil {
		tb.Fatal(err)
	}
	enc.Free()
	return blob
}

func makeMixedBlob(tb testing.TB, nRows int) []byte {
	tb.Helper()
	ids := make([]int64, nRows)
	scores := make([]float64, nRows)
	names := make([]string, nRows)
	for i := 0; i < nRows; i++ {
		ids[i] = int64((i * 7) % 1000000)
		scores[i] = float64(i) * 1.5
		names[i] = "user_" + itoa(i)
	}
	enc := pond.NewEncoder(nRows)
	if err := enc.AddInt64Column("id", ids); err != nil {
		tb.Fatal(err)
	}
	if err := enc.AddFloat64Column("score", scores); err != nil {
		tb.Fatal(err)
	}
	if err := enc.AddStringColumn("name", names); err != nil {
		tb.Fatal(err)
	}
	blob, err := enc.Build()
	if err != nil {
		tb.Fatal(err)
	}
	enc.Free()
	return blob
}

func makeStringHeavyBlob(tb testing.TB, nRows int) []byte {
	tb.Helper()
	a := make([]string, nRows)
	b2 := make([]string, nRows)
	c := make([]string, nRows)
	for i := 0; i < nRows; i++ {
		a[i] = "name_" + itoa(i)
		b2[i] = "email_" + itoa(i) + "@test.com"
		c[i] = "tag_" + itoa(i%100)
	}
	enc := pond.NewEncoder(nRows)
	if err := enc.AddStringColumn("a", a); err != nil {
		tb.Fatal(err)
	}
	if err := enc.AddStringColumn("b", b2); err != nil {
		tb.Fatal(err)
	}
	if err := enc.AddStringColumn("c", c); err != nil {
		tb.Fatal(err)
	}
	blob, err := enc.Build()
	if err != nil {
		tb.Fatal(err)
	}
	enc.Free()
	return blob
}

// itoa is a minimal int → string converter (avoids strconv import).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// Suppress unused-import warnings for the helpers above (the test files
// use these via the testing package).
var _ = os.ReadFile
var _ = filepath.Join
var _ = runtime.Caller
var _ = time.Now
