// Module github.com/pond/pond-go provides Go bindings for Pond's PND2
// binary codec. It links against `libpond_core.a` (the Rust C ABI in
// pond-rust/pond-core/) via cgo.
//
// Architectural role: sdk-go is a peer to pond-sdk/ (Python SDK) — both
// bind to pond-core's storage layer. Currently the Go SDK exposes PND2
// codec operations (encode/decode) only; storage kernel operations
// (Write/Read/Ref) require the Python kernel and are not yet available
// to Go callers.
module github.com/pond/pond-go

go 1.22
