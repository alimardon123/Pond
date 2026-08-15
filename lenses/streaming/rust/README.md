# lenses/streaming/rust/

Rust implementation of StreamingLens — chunked storage for large objects.

## Status

**Implemented (core API).** The following operations are ported:

| Operation | Status | Notes |
|---|---|---|
| `write_stream(collection, data, segment_size, message)` | ✅ | Write complete stream |
| `read_stream(collection, start, end)` | ✅ | Range read (efficient) |
| `append_stream(collection, data, segment_size, message)` | ✅ | Append to existing stream |
| `stream_size(collection)` | ✅ | Total size in bytes |
| `segment_count(collection)` | ✅ | Number of segments |
| `create_topic` / `produce` / `consume` | ❌ | Not ported (Kafka-like API) |
| `commit_offset` / `replay_from` | ❌ | Not ported (consumer groups) |

The Python implementation (`../python/streaming_lens.py`) remains the
full-featured reference (589 LOC). The Rust port covers the core chunked
storage pattern (write/read/append/size/count).

## Architecture

```
StreamingLens (this crate)
  ↓ calls
pond_storage::UnifiedStorage (Rust core)
  ↓ calls
pond_kernel::PondKernel (Rust core)
  ↓ calls
ObjectStore trait (LocalFSObjectStore or S3ObjectStore)
```

## How It Works

A large object (video, music, log file) is split into fixed-size segments.
Each segment is stored as a JSON row:

```json
[
  {"offset": 0, "data": "<base64>"},
  {"offset": 1048576, "data": "<base64>"},
  {"offset": 2097152, "data": "<base64>"}
]
```

Range-read fetches only the segments that overlap `[start, end)` —
efficient for large streams where you only need a small portion.

## Usage

```rust
use pond_streaming_lens::StreamingLens;
use pond_storage::UnifiedStorage;

let storage = UnifiedStorage::new_local("/var/lib/pond").unwrap();
let lens = StreamingLens::new(storage);

// Write a 5MB video as 1MB segments
let video_data = vec![0u8; 5 * 1024 * 1024];
lens.write_stream("video1", &video_data, 1024 * 1024, "upload video").unwrap();

// Read bytes 1.5MB to 2.5MB (only fetches overlapping segments)
let chunk = lens.read_stream("video1", 1_500_000, Some(2_500_000)).unwrap();
assert_eq!(chunk.len(), 1_000_000);

// Append more data
lens.append_stream("video1", &vec![0xFF; 1024], 1024, "append").unwrap();

// Get stream metadata
println!("Size: {} bytes", lens.stream_size("video1").unwrap());
println!("Segments: {}", lens.segment_count("video1").unwrap());
```

## Tests

9 unit tests cover the core operations:
- `test_write_and_read_full_stream` — write + read full
- `test_read_range` — range read (middle of stream)
- `test_stream_size` — total size
- `test_segment_count` — segment count
- `test_append_stream` — append to existing stream
- `test_empty_stream` — nonexistent collection returns empty
- `test_single_byte_segments` — 1-byte segments
- `test_large_segment_covers_all` — segment larger than data
- `test_base64_encode_decode` — base64 round-trip

Run tests:
```bash
cargo test -p pond_streaming_lens
```
