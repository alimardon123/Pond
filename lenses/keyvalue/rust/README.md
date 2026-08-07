# lenses/keyvalue/rust/

Rust implementation of KeyValueLens — the first lens ported from Python to Rust.

## Status

**Implemented (core API).** The following operations are ported:

| Operation | Status | Notes |
|---|---|---|
| `put(collection, key, value)` | ✅ | Stage a key→value mapping |
| `get(collection, key)` | ✅ | Read a single value by key |
| `delete(collection, key)` | ✅ | Stage a deletion (tombstone) |
| `commit(collection, message)` | ✅ | Flush staged changes to storage |
| `get_all(collection)` | ✅ | Read all key→value pairs |
| `keys(collection)` | ✅ | List all keys |
| `exists(collection, key)` | ✅ | Check if a key exists |
| `count(collection)` | ✅ | Count rows |
| `where(...)` / `select(...)` / `map(...)` | ❌ | Not ported (LensQuery) |
| `build_zone_maps(...)` | ❌ | Not ported |
| `attach_indexer(...)` | ❌ | Not ported |
| `read_with_pruning(...)` | ❌ | Not ported |

The Python implementation (`../python/keyvalue_lens.py`) remains the
full-featured reference. The Rust port covers the core KV operations
needed by most applications.

## Architecture

```
KeyValueLens (this crate)
  ↓ calls
pond_storage::UnifiedStorage (Rust core)
  ↓ calls
pond_kernel::PondKernel (Rust core)
  ↓ calls
ObjectStore trait (LocalFSObjectStore or S3ObjectStore)
```

The lens is a thin layer over UnifiedStorage. It adds key-value semantics
(per-row key→value storage with staged puts/deletes) on top of the
collection model.

## Storage Format

Each row is stored as a JSON object with the user-supplied key injected
as the `_key` field:

```json
[
  {"_key": "user:1", "name": "alice", "age": 30},
  {"_key": "user:2", "name": "bob", "age": 25}
]
```

The `_key` field is stripped when reading via `get()` / `get_all()`.

## Usage

```rust
use pond_keyvalue_lens::KeyValueLens;
use pond_storage::UnifiedStorage;
use serde_json::json;

let storage = UnifiedStorage::new_local("/var/lib/pond").unwrap();
let lens = KeyValueLens::new(storage);

// Stage changes
lens.put("users", "user:1", &json!({"name": "alice", "age": 30}));
lens.put("users", "user:2", &json!({"name": "bob", "age": 25}));

// Commit to storage
let commit = lens.commit("users", "insert alice and bob").unwrap();

// Read back
let alice = lens.get("users", "user:1").unwrap();
assert_eq!(alice["name"], "alice");

// Delete
lens.delete("users", "user:2");
lens.commit("users", "remove bob").unwrap();
assert!(!lens.exists("users", "user:2"));
```

## Tests

8 unit tests cover the core operations:
- `test_put_and_get` — basic put + get
- `test_get_nonexistent` — get returns None for missing keys
- `test_delete` — delete removes a key
- `test_keys_and_count` — list keys and count
- `test_get_all` — read all key→value pairs
- `test_overwrite_key` — put overwrites existing keys
- `test_commit_with_no_changes_fails` — commit with empty staging fails
- `test_multiple_collections` — multiple collections work independently

Run tests:
```bash
cargo test -p pond_keyvalue_lens
```

## Migration Plan (for remaining lenses)

This port establishes the pattern for future lens ports:

1. **Create `lenses/{name}/rust/Cargo.toml`** depending on `pond_kernel`
   and `pond_storage`.
2. **Implement the core API** in `src/lib.rs`. Use `serde_json` for
   serialization (matches the Python lens's JSON format).
3. **Add unit tests** using `tempfile` for temp directories.
4. **Add to workspace** in the root `Cargo.toml`.
5. **Update this README** with the ported operations table.
6. **Future**: Add C ABI functions to `lenses/base/pond_lens.h` for
   cross-language access.

The remaining lenses to port (in priority order):
1. ~~KeyValueLens~~ (done)
2. LakehouseLens — tabular storage with DuckDB SQL
3. StreamingLens — Kafka-like streaming with partitions
4. OLTPLens — memtable + batch flush
5. VectorLens — vector storage with IVF ANN
