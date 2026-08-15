# lenses/oltp/rust/

Rust implementation of OLTPLens — fast KV with in-memory memtable + batch flush.

## Status

**Implemented (core API).** The following operations are ported:

| Operation | Status | Notes |
|---|---|---|
| `put(key, value)` | ✅ | Fast write (in-memory memtable) |
| `delete(key)` | ✅ | Tombstone (in-memory) |
| `get(key)` | ✅ | Read (memtable first, then storage) |
| `exists(key)` | ✅ | Check existence |
| `keys()` | ✅ | List keys (memtable + storage) |
| `count()` | ✅ | Count entries |
| `flush()` | ✅ | Flush memtable to storage |
| `pending_count()` | ✅ | Count unflushed entries |
| Auto-flush at threshold | ✅ | Triggers when memtable reaches flush_threshold |

The Python implementation (`../python/oltp_lens.py`) is the reference (198 LOC).
The Rust port covers the full core API.

## Architecture

```
OLTPLens (this crate)
  ↓ uses
pond_storage::UnifiedStorage (Rust core)
  ↓ calls
pond_kernel::PondKernel (Rust core)
  ↓ calls
ObjectStore trait (LocalFSObjectStore or S3ObjectStore)
```

## How It Works

Each OLTPLens instance has an in-memory `memtable` (HashMap). Writes go to
the memtable (sub-µs). When the memtable is full (or `flush()` is called),
it flushes to storage as a commit — amortizing S3 latency across N writes.

Multiple processes can each have their own OLTPLens instance. They flush
independently — no coordination, no CAS, no locks. CRDT merge handles
conflicts deterministically.

This is the SAME pattern as RocksDB's LSM-tree:
- SST files → commits (concurrent-safe)
- Compaction → `compact_shards` (already in UnifiedStorage)
- Multi-process → each flushes independently (CRDT handles conflicts)

## Usage

```rust
use pond_oltp_lens::OLTPLens;
use pond_storage::UnifiedStorage;
use serde_json::json;

let storage = UnifiedStorage::new_local("/var/lib/pond").unwrap();
let oltp = OLTPLens::new(storage, "kv", 1000);

// Fast writes (in-memory, sub-µs)
oltp.put("user:1", &json!({"name": "alice", "age": 30}));
oltp.put("user:2", &json!({"name": "bob", "age": 25}));
oltp.delete("user:2");

// Reads check memtable first (0 GETs), then storage
let user = oltp.get("user:1").unwrap();
assert_eq!(user["name"], "alice");

// Flush to storage (1 PUT, amortized across all writes)
oltp.flush().unwrap();
```

## Tests

8 unit tests cover the core operations:
- `test_put_and_get` — basic put + get
- `test_get_nonexistent` — get returns None for missing keys
- `test_delete_in_memtable` — delete in memtable
- `test_flush_and_cold_read` — flush + read from new instance
- `test_keys_and_count` — list keys and count
- `test_overwrite_key` — put overwrites existing keys
- `test_pending_count` — count unflushed entries
- `test_auto_flush_at_threshold` — auto-flush triggers at threshold

Run tests:
```bash
cargo test -p pond_oltp_lens
```
