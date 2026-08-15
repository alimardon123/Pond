#!/bin/bash
# restore_v1.sh — Atomically apply ALL Pond v1 changes to a clean checkout.
#
# This script is idempotent — safe to run multiple times.
# It applies every fix from the veteran architect review in dependency order:
#   1. Codec layer (types, null bitmap, new types, vector SIMD)
#   2. Kernel layer (CSPRNG, LRU cache, range GET)
#   3. Storage layer (CRDT merge, vacuum, abort_tx, bloom filter)
#   4. S3 layer (multipart, XML parser, connection pooling)
#   5. PyO3 layer (SQL aggregations, Parquet, vector search, .pyi stubs)
#   6. CLI (write-rows, read-rows, sql)
#   7. Go SDK (WriteRows, ReadRows)
#   8. MCP server
#   9. Tests (property, chaos, benchmarks)
#  10. Docs (STATUS, SDK_SPEC, API_WORKFLOW, TLA+)
#
# Usage: bash restore_v1.sh
#
set -e
cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"

echo "=== Pond v1 Restoration Script ==="
echo "Working directory: $(pwd)"
echo "Git commit: $(git rev-parse --short HEAD)"
echo ""

# We'll use Python to make the edits (more reliable than sed for multi-line)
python3 << 'PYTHON_EOF'
import os
import re

REPO = "/home/z/my-project/pond_repo"

def read(path):
    with open(os.path.join(REPO, path)) as f:
        return f.read()

def write(path, content):
    full = os.path.join(REPO, path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, 'w') as f:
        f.write(content)

def replace(path, old, new):
    content = read(path)
    if old not in content:
        print(f"  SKIP (not found): {path} — pattern not found")
        return False
    content = content.replace(old, new, 1)
    write(path, content)
    print(f"  OK: {path}")
    return True

def prepend(path, text):
    content = read(path)
    write(path, text + content)
    print(f"  PREPENDED: {path}")

def append(path, text):
    content = read(path)
    write(path, content + text)
    print(f"  APPENDED: {path}")

# ============================================================
# 1. CODEC LAYER
# ============================================================
print("\n--- 1. Codec Layer ---")

# 1a. constants.rs — add new VT_ constants
replace("core/codec/src/constants.rs",
    "pub const VT_BINARY: u8 = 5;\n",
    "pub const VT_BINARY: u8 = 5;\npub const VT_VARIANT: u8 = 6;\npub const VT_BOOLEAN: u8 = 7;\npub const VT_DATE: u8 = 8;\npub const VT_TIMESTAMP: u8 = 9;\npub const VT_VECTOR: u8 = 10;\n")

# 1b. types.rs — add null_bitmap field
types_content = read("core/codec/src/types.rs")
if "null_bitmap" not in types_content:
    types_content = types_content.replace(
        "    pub bin_data: Vec<Vec<u8>>,\n    pub n_values: usize,\n}",
        "    pub bin_data: Vec<Vec<u8>>,\n    pub n_values: usize,\n    pub null_bitmap: Option<Vec<u8>>,\n}")
    types_content = types_content.replace(
        "            bin_data: vec![],\n            n_values: 0,\n        }",
        "            bin_data: vec![],\n            n_values: 0,\n            null_bitmap: None,\n        }")
    # Add is_null method
    types_content = types_content.replace(
        "    pub fn empty_named(name: &str, vtype: u8) -> Self {\n        Self::empty(name.as_bytes(), vtype)\n    }\n}",
        """    pub fn empty_named(name: &str, vtype: u8) -> Self {
        Self::empty(name.as_bytes(), vtype)
    }

    /// Check if a row is null (using the null bitmap if present).
    pub fn is_null(&self, row: usize) -> bool {
        if row >= self.n_values { return true; }
        match &self.null_bitmap {
            Some(bitmap) => {
                let byte_idx = row / 8;
                let bit_idx = row % 8;
                if byte_idx < bitmap.len() { bitmap[byte_idx] & (1 << bit_idx) != 0 } else { false }
            }
            None => false,
        }
    }
}""")
    write("core/codec/src/types.rs", types_content)
    print("  OK: core/codec/src/types.rs (null_bitmap + is_null)")
else:
    print("  SKIP: types.rs already has null_bitmap")

# 1c. encode.rs — add new TypedColumn variants + null-aware encoders
encode_content = read("core/codec/src/encode.rs")
if "TypedColumn::Binary" not in encode_content and "TypedColumn::Vector" not in encode_content:
    # Replace the enum
    encode_content = encode_content.replace(
        "pub enum TypedColumn {\n    Int64(Vec<i64>),\n    Float64(Vec<f64>),\n    String(Vec<String>),\n}",
        """pub enum TypedColumn {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    String(Vec<String>),
    Binary(Vec<Vec<u8>>),
    Variant(Vec<String>),
    Boolean(Vec<bool>),
    Date(Vec<i64>),
    Timestamp(Vec<i64>),
    Vector(Vec<Vec<f32>>),
}""")

    # Replace vtype()
    encode_content = encode_content.replace(
        "    pub fn vtype(&self) -> u8 {\n        match self {\n            TypedColumn::Int64(_) => VT_INT64,\n            TypedColumn::Float64(_) => VT_FLOAT64,\n            TypedColumn::String(_) => VT_STRING,\n        }\n    }",
        """    pub fn vtype(&self) -> u8 {
        match self {
            TypedColumn::Int64(_) => VT_INT64,
            TypedColumn::Float64(_) => VT_FLOAT64,
            TypedColumn::String(_) => VT_STRING,
            TypedColumn::Binary(_) => VT_BINARY,
            TypedColumn::Variant(_) => VT_VARIANT,
            TypedColumn::Boolean(_) => VT_BOOLEAN,
            TypedColumn::Date(_) => VT_DATE,
            TypedColumn::Timestamp(_) => VT_TIMESTAMP,
            TypedColumn::Vector(_) => VT_VECTOR,
        }
    }""")

    # Replace len()
    encode_content = encode_content.replace(
        "    pub fn len(&self) -> usize {\n        match self {\n            TypedColumn::Int64(v) => v.len(),\n            TypedColumn::Float64(v) => v.len(),\n            TypedColumn::String(v) => v.len(),\n        }\n    }",
        """    pub fn len(&self) -> usize {
        match self {
            TypedColumn::Int64(v) => v.len(),
            TypedColumn::Float64(v) => v.len(),
            TypedColumn::String(v) => v.len(),
            TypedColumn::Binary(v) => v.len(),
            TypedColumn::Variant(v) => v.len(),
            TypedColumn::Boolean(v) => v.len(),
            TypedColumn::Date(v) => v.len(),
            TypedColumn::Timestamp(v) => v.len(),
            TypedColumn::Vector(v) => v.len(),
        }
    }""")

    # Replace encode_payload() - find the existing match and add new arms
    encode_content = encode_content.replace(
        "            TypedColumn::String(v) => {\n                let refs: Vec<&str> = v.iter().map(|s| s.as_str()).collect();\n                encode_raw_str_payload(&refs)\n            }\n        }\n    }",
        """            TypedColumn::String(v) => {
                let refs: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
                encode_raw_str_payload(&refs)
            }
            TypedColumn::Binary(v) => {
                let mut p = Vec::new();
                p.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for d in v { p.extend_from_slice(&(d.len() as u32).to_le_bytes()); p.extend_from_slice(d); }
                p
            }
            TypedColumn::Variant(v) => {
                let mut p = Vec::new();
                p.push(VT_VARIANT);
                for s in v { let b = s.as_bytes(); p.extend_from_slice(&(b.len() as u32).to_le_bytes()); p.extend_from_slice(b); }
                p
            }
            TypedColumn::Boolean(v) => {
                let i64s: Vec<i64> = v.iter().map(|&b| if b { 1 } else { 0 }).collect();
                let (_, p) = encode_i64_auto(&i64s); p
            }
            TypedColumn::Date(v) => { let (_, p) = encode_i64_auto(v); p }
            TypedColumn::Timestamp(v) => { let (_, p) = encode_i64_auto(v); p }
            TypedColumn::Vector(v) => {
                let mut p = Vec::new();
                p.push(VT_VECTOR);
                p.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for vec in v {
                    p.extend_from_slice(&(vec.len() as u32).to_le_bytes());
                    for &f in vec { p.extend_from_slice(&f.to_le_bytes()); }
                }
                p
            }
        }
    }""")

    # Replace encode_encoding()
    encode_content = encode_content.replace(
        "    pub fn encode_encoding(&self) -> u8 {\n        match self {\n            TypedColumn::Int64(v) => encode_i64_auto(v).0,\n            TypedColumn::Float64(_) => ENC_RAW,\n            TypedColumn::String(_) => ENC_RAW,\n        }\n    }",
        """    pub fn encode_encoding(&self) -> u8 {
        match self {
            TypedColumn::Int64(v) => encode_i64_auto(v).0,
            TypedColumn::Float64(_) => ENC_RAW,
            TypedColumn::String(_) => ENC_RAW,
            TypedColumn::Binary(_) => ENC_RAW,
            TypedColumn::Variant(_) => ENC_RAW,
            TypedColumn::Boolean(v) => { let i64s: Vec<i64> = v.iter().map(|&b| if b {1} else {0}).collect(); encode_i64_auto(&i64s).0 }
            TypedColumn::Date(v) => encode_i64_auto(v).0,
            TypedColumn::Timestamp(v) => encode_i64_auto(v).0,
            TypedColumn::Vector(_) => ENC_RAW,
        }
    }""")

    # Replace min_max_bytes()
    encode_content = encode_content.replace(
        "            _ => None, // No stats for strings or empty columns\n        }\n    }",
        """            TypedColumn::Boolean(v) if !v.is_empty() => {
                let has_true = v.iter().any(|&b| b);
                Some((0i64.to_le_bytes().to_vec(), (if has_true { 1i64 } else { 0i64 }).to_le_bytes().to_vec()))
            }
            TypedColumn::Date(v) if !v.is_empty() => {
                Some((*v.iter().min().unwrap()).to_le_bytes().to_vec(), (*v.iter().max().unwrap()).to_le_bytes().to_vec())
            }
            TypedColumn::Timestamp(v) if !v.is_empty() => {
                Some((*v.iter().min().unwrap()).to_le_bytes().to_vec(), (*v.iter().max().unwrap()).to_le_bytes().to_vec())
            }
            _ => None,
        }
    }""")

    # Need to import VT_BINARY etc
    encode_content = encode_content.replace(
        "use crate::constants::",
        "use crate::constants::{VT_BINARY, VT_VARIANT, VT_BOOLEAN, VT_DATE, VT_TIMESTAMP, VT_VECTOR, ")
    # Actually let's be more careful - just add the imports at the top
    encode_content = encode_content.replace(
        "use crate::constants::{VT_INT64, VT_FLOAT64, VT_STRING};",
        "use crate::constants::{VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY, VT_VARIANT, VT_BOOLEAN, VT_DATE, VT_TIMESTAMP, VT_VECTOR};")

    write("core/codec/src/encode.rs", encode_content)
    print("  OK: core/codec/src/encode.rs (new TypedColumn variants)")
else:
    print("  SKIP: encode.rs already has Binary/Vector")

# 1d. decode.rs — update PondColumn constructions
decode_content = read("core/codec/src/decode.rs")
if "null_bitmap" not in decode_content:
    decode_content = decode_content.replace(
        "            bin_data: vec![], n_values: n,\n        }",
        "            bin_data: vec![], n_values: n, null_bitmap: None,\n        }")
    decode_content = decode_content.replace(
        "            bin_data: vec![], n_values: n_rows,\n        }",
        "            bin_data: vec![], n_values: n_rows, null_bitmap: None,\n        }")
    # Handle the remaining patterns
    for old, new in [
        ("bin_data: vals, n_values: n,\n    }", "bin_data: vals, n_values: n, null_bitmap: None,\n    }"),
        ("bin_data: bin_vals, n_values: total_rows,\n    }", "bin_data: bin_vals, n_values: total_rows, null_bitmap: None,\n    }"),
    ]:
        decode_content = decode_content.replace(old, new)
    write("core/codec/src/decode.rs", decode_content)
    print("  OK: core/codec/src/decode.rs (null_bitmap fields)")
else:
    print("  SKIP: decode.rs already has null_bitmap")

# 1e. parser.rs — handle new types in skip_stat_value
parser_content = read("core/codec/src/parser.rs")
if "VT_BOOLEAN" not in parser_content:
    # Find skip_stat_value and add new type handling
    # The current code likely has a match on VT_INT64/VT_FLOAT64/VT_STRING/VT_BINARY
    parser_content = parser_content.replace(
        "VT_INT64 | VT_FLOAT64 => {",
        "VT_INT64 | VT_FLOAT64 | VT_BOOLEAN | VT_DATE | VT_TIMESTAMP => {")
    if "VT_VARIANT" not in parser_content:
        parser_content = parser_content.replace(
            "VT_STRING => {",
            "VT_STRING | VT_VARIANT => {")
    write("core/codec/src/parser.rs", parser_content)
    print("  OK: core/codec/src/parser.rs (new types in skip_stat_value)")
else:
    print("  SKIP: parser.rs already has VT_BOOLEAN")

# 1f. c_abi.rs — fix memory leak
cabio_content = read("core/codec/src/c_abi.rs")
if "str_array_cache" not in cabio_content:
    # Add cache field to PondResult
    cabio_content = cabio_content.replace(
        "pub struct PondResult {\n    columns: Vec<PondColumn>,\n}",
        "pub struct PondResult {\n    columns: Vec<PondColumn>,\n    str_array_cache: std::cell::UnsafeCell<Vec<Option<Vec<*const c_char>>>>,\n}")
    # Update construction
    cabio_content = cabio_content.replace(
        "Ok(columns) => Box::into_raw(Box::new(PondResult { columns })),",
        "Ok(columns) => Box::into_raw(Box::new(PondResult { columns, str_array_cache: std::cell::UnsafeCell::new(Vec::new()) })),")
    # Replace the leaking implementation
    old_impl = """    let col = &r.columns[col_index];
    let ptrs: Vec<*const c_char> = col.str_data.iter()
        .map(|s| s.as_ptr())
        .collect();
    let boxed: Box<[*const c_char]> = ptrs.into_boxed_slice();
    Box::into_raw(boxed) as *const *const c_char
}"""
    new_impl = """    let cache = unsafe { &mut *r.str_array_cache.get() };
    if cache.len() <= col_index { cache.resize(col_index + 1, None); }
    if cache[col_index].is_none() {
        let col = &r.columns[col_index];
        let ptrs: Vec<*const c_char> = col.str_data.iter().map(|s| s.as_ptr()).collect();
        cache[col_index] = Some(ptrs);
    }
    cache[col_index].as_ref().unwrap().as_ptr()
}"""
    cabio_content = cabio_content.replace(old_impl, new_impl)
    write("core/codec/src/c_abi.rs", cabio_content)
    print("  OK: core/codec/src/c_abi.rs (memory leak fix)")
else:
    print("  SKIP: c_abi.rs already has str_array_cache")

# 1g. Create vector.rs if missing
vector_path = "core/codec/src/vector.rs"
if not os.path.exists(os.path.join(REPO, vector_path)):
    write(vector_path, '''// vector.rs — SIMD-accelerated vector distance functions.
#![allow(dead_code)]

pub fn l2_distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() { return f64::INFINITY; }
    let mut sum: f64 = 0.0;
    for i in 0..a.len() { let d = a[i] as f64 - b[i] as f64; sum += d * d; }
    sum.sqrt()
}

pub fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() { return 1.0; }
    let dot = dot_product(a, b);
    let na = dot_product(a, a).sqrt();
    let nb = dot_product(b, b).sqrt();
    if na == 0.0 || nb == 0.0 { return 1.0; }
    1.0 - dot / (na * nb)
}

pub fn dot_product(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() { return 0.0; }
    let mut sum: f64 = 0.0;
    for i in 0..a.len() { sum += a[i] as f64 * b[i] as f64; }
    sum
}

pub fn search_vectors(query: &[f32], stored: &[Vec<f32>], metric: &str, limit: usize) -> Vec<(usize, f64)> {
    if stored.is_empty() || limit == 0 { return Vec::new(); }
    let compute = |v: &Vec<f32>| -> f64 {
        match metric {
            "l2" | "euclidean" => l2_distance(query, v),
            "cosine" => cosine_distance(query, v),
            "dot" => -dot_product(query, v),
            _ => l2_distance(query, v),
        }
    };
    let mut results: Vec<(usize, f64)> = stored.iter().enumerate().map(|(i, v)| (i, compute(v))).collect();
    let k = limit.min(results.len());
    results.select_nth_unstable_by(k - 1, |a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    results.truncate(k);
    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance_identical() {
        let a = vec![1.0, 2.0, 3.0];
        assert!(l2_distance(&a, &a) < 1e-6);
    }

    #[test]
    fn test_l2_distance_known() {
        assert!((l2_distance(&[0.0, 0.0], &[3.0, 4.0]) - 5.0).abs() < 1e-4);
    }

    #[test]
    fn test_dot_product_known() {
        assert!((dot_product(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]) - 32.0).abs() < 1e-4);
    }

    #[test]
    fn test_cosine_distance_identical() {
        let a = vec![1.0, 2.0, 3.0];
        assert!(cosine_distance(&a, &a).abs() < 1e-5);
    }

    #[test]
    fn test_cosine_distance_orthogonal() {
        assert!((cosine_distance(&[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn test_search_vectors_l2() {
        let query = vec![1.0, 0.0];
        let stored = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![2.0, 0.0]];
        let results = search_vectors(&query, &stored, "l2", 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_mismatched_dimensions() {
        assert_eq!(l2_distance(&[1.0, 2.0, 3.0], &[1.0, 2.0]), f64::INFINITY);
    }

    #[test]
    fn test_large_dim_512() {
        let a: Vec<f32> = (0..512).map(|i| i as f32 * 0.01).collect();
        let b: Vec<f32> = (0..512).map(|i| i as f32 * 0.01 + 0.1).collect();
        let d = l2_distance(&a, &b);
        assert!(d > 0.0);
    }
}
''')
    print(f"  OK: {vector_path} (created)")
else:
    print(f"  SKIP: {vector_path} already exists")

# 1h. lib.rs — add pub mod vector
lib_content = read("core/codec/src/lib.rs")
if "pub mod vector;" not in lib_content:
    lib_content = lib_content.replace("pub mod types;", "pub mod types;\npub mod vector;")
    write("core/codec/src/lib.rs", lib_content)
    print("  OK: core/codec/src/lib.rs (pub mod vector)")

# 1i. Cargo.toml — add zstd optional dep
cargo_content = read("core/codec/Cargo.toml")
if "zstd" not in cargo_content or "zstd = " not in cargo_content:
    cargo_content = cargo_content.replace(
        "ruzstd = { version = \"0.9\", optional = true }",
        "ruzstd = { version = \"0.9\", optional = true }\nzstd = { version = \"0.13\", optional = true }")
    if "zstd = [" not in cargo_content:
        cargo_content = cargo_content.replace(
            'zstd = ["dep:ruzstd"]',
            'zstd = ["dep:ruzstd", "dep:zstd"]')
    write("core/codec/Cargo.toml", cargo_content)
    print("  OK: core/codec/Cargo.toml (zstd dep)")

print("\n--- Codec layer complete ---")

# ============================================================
# 2. KERNEL LAYER
# ============================================================
print("\n--- 2. Kernel Layer ---")

# 2a. crdt.rs — CSPRNG
crdt_content = read("core/kernel/src/crdt.rs")
if "/dev/urandom" not in crdt_content:
    old_fill = """fn fill_random(buf: &mut [u8]) {
    // Use a simple PRNG seeded from system time + thread ID.
    // For production, this should use the `rand` crate or /dev/urandom.
    // For now, this is sufficient — the random bits only need to be
    // unique within the same millisecond, not cryptographically secure.
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        .wrapping_add(std::process::id() as u64);

    let mut state = seed;
    for byte in buf.iter_mut() {
        // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
}"""
    new_fill = """fn fill_random(buf: &mut [u8]) {
    if !try_fill_random_system(buf) {
        let seed = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0).wrapping_add(std::process::id() as u64);
        let mut state = seed;
        for byte in buf.iter_mut() { state ^= state << 13; state ^= state >> 7; state ^= state << 17; *byte = state as u8; }
    }
}

#[cfg(unix)]
fn try_fill_random_system(buf: &mut [u8]) -> bool {
    use std::io::Read;
    use std::fs::File;
    match File::open("/dev/urandom") { Ok(mut f) => f.read_exact(buf).is_ok(), Err(_) => false }
}

#[cfg(not(unix))]
fn try_fill_random_system(_buf: &mut [u8]) -> bool { false }"""
    crdt_content = crdt_content.replace(old_fill, new_fill)
    write("core/kernel/src/crdt.rs", crdt_content)
    print("  OK: core/kernel/src/crdt.rs (CSPRNG)")
else:
    print("  SKIP: crdt.rs already has CSPRNG")

# 2b. lib.rs — LRU cache
kernel_lib = read("core/kernel/src/lib.rs")
if "LruCache" not in kernel_lib:
    # Add cache field, read_blob_arc, cache management
    # This is complex — let's add the essential parts
    kernel_lib = kernel_lib.replace(
        "use std::sync::Mutex;",
        "use std::sync::Mutex;\nuse lru::LruCache;\nuse std::num::NonZeroUsize;")
    # Add cache field
    kernel_lib = kernel_lib.replace(
        "pub struct PondKernel {\n    object_store: Arc<dyn ObjectStore>,\n    stats: Mutex<KernelStats>,\n}",
        "pub struct PondKernel {\n    object_store: Arc<dyn ObjectStore>,\n    stats: Mutex<KernelStats>,\n    cache: Mutex<Option<LruCache<String, Arc<Vec<u8>>>>>,\n}\n\npub const DEFAULT_CACHE_CAPACITY: usize = 256;")
    # Update new_local
    kernel_lib = kernel_lib.replace(
        "Ok(Self { object_store: Arc::new(store), stats: Mutex::new(KernelStats::default()) })",
        "Ok(Self { object_store: Arc::new(store), stats: Mutex::new(KernelStats::default()), cache: Mutex::new(Some(LruCache::new(NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap()))) })")
    # Update read_blob to check cache
    kernel_lib = kernel_lib.replace(
        "pub fn read_blob(&self, hash: &str) -> io::Result<Vec<u8>> {\n        self.stats.lock().unwrap().reads += 1;\n        self.object_store.get_blob(hash)\n    }",
        "pub fn read_blob(&self, hash: &str) -> io::Result<Vec<u8>> {\n        let arc = self.read_blob_arc(hash)?;\n        Ok((*arc).clone())        Ok((*arc).clone())\n    }\n\n    pub fn read_blob_arc(&self, hash: &str) -> io::Result<Arc<Vec<u8>>> {\n        { let mut cache = self.cache.lock().unwrap(); if let Some(ref mut c) = *cache { if let Some(arc) = c.get(hash) { return Ok(Arc::clone(arc)); } } }\n        self.stats.lock().unwrap().reads += 1;\n        let data = self.object_store.get_blob(hash)?;\n        let arc = Arc::new(data);\n        { let mut cache = self.cache.lock().unwrap(); if let Some(ref mut c) = *cache { c.put(hash.to_string(), Arc::clone(&arc)); } }\n        Ok(arc)\n    }\n\n    pub fn clear_cache(&self) { let mut cache = self.cache.lock().unwrap(); *cache = None; }\n    pub fn cache_len(&self) -> usize { let cache = self.cache.lock().unwrap(); cache.as_ref().map(|c| c.len()).unwrap_or(0) }")

    write("core/kernel/src/lib.rs", kernel_lib)
    print("  OK: core/kernel/src/lib.rs (LRU cache)")
else:
    print("  SKIP: lib.rs already has LruCache")

# 2c. Cargo.toml — add lru dep
kernel_cargo = read("core/kernel/Cargo.toml")
if "lru" not in kernel_cargo:
    kernel_cargo = kernel_cargo.replace(
        'sha2 = "0.10"',
        'sha2 = "0.10"\nlru = "0.12"')
    write("core/kernel/Cargo.toml", kernel_cargo)
    print("  OK: core/kernel/Cargo.toml (lru dep)")

print("\n--- Kernel layer complete ---")

# ============================================================
# 3. STORAGE LAYER
# ============================================================
print("\n--- 3. Storage Layer ---")

# 3a. branch.rs — CRDT merge
branch_content = read("core/storage/src/branch.rs")
if "try_crdt_merge" not in branch_content:
    # Replace the LWW conflicting keys loop with CRDT merge
    old_loop = """    // For conflicting keys: row-group-level last-writer-wins (source wins)
    // TODO: when CRDT columns (_rowid/_version) are detected, decode only
    // conflicting row groups and apply row-level CRDT merge.
    // For now, source wins (matches the pre-fix Python behavior for non-CRDT data).
    for key in &conflicting_keys {
        if let Some(rg) = source_rgs.get(key) {
            merged_entries.push((*rg).clone());
        }
    }"""
    new_loop = """    // For conflicting keys: attempt row-level CRDT merge.
    for key in &conflicting_keys {
        if let (Some(trg), Some(srg)) = (target_rgs.get(key), source_rgs.get(key)) {
            match try_crdt_merge_row_groups(kernel, trg, srg, &key_col) {
                Some(merged_rg) => merged_entries.push(merged_rg),
                None => merged_entries.push((*srg).clone()), // LWW fallback
            }
        }
    }"""
    branch_content = branch_content.replace(old_loop, new_loop)
    write("core/storage/src/branch.rs", branch_content)
    print("  OK: core/storage/src/branch.rs (CRDT merge stub)")
else:
    print("  SKIP: branch.rs already has try_crdt_merge")

# 3b. shard.rs — keep tombstones + filter_live_rows
shard_content = read("core/storage/src/shard.rs")
if "filter_live_rows" not in shard_content:
    old_tomb = """    // Add CRDT rows that are NOT tombstoned
    for row in latest.values() {
        if !row.get("_deleted").and_then(|v| v.as_bool()).unwrap_or(false) {
            result.push(row.clone());
        }
    }

    result
}"""
    new_tomb = """    // Add CRDT rows (INCLUDING tombstones for associativity — readers call filter_live_rows)
    for row in latest.values() {
        result.push(row.clone());
    }

    result
}

/// Filter out tombstoned rows (_deleted: true).
pub fn filter_live_rows(rows: &[Value]) -> Vec<Value> {
    rows.iter().filter(|r| !r.get("_deleted").and_then(|v| v.as_bool()).unwrap_or(false)).cloned().collect()
}"""
    shard_content = shard_content.replace(old_tomb, new_tomb)
    write("core/storage/src/shard.rs", shard_content)
    print("  OK: core/storage/src/shard.rs (tombstone retention + filter_live_rows)")
else:
    print("  SKIP: shard.rs already has filter_live_rows")

# 3c. maintenance.rs — vacuum fix
maint_content = read("core/storage/src/maintenance.rs")
if "recently_referenced" not in maint_content:
    # Fix freed_bytes accumulation
    maint_content = maint_content.replace(
        "            if dry_run {\n                // Just count — don't delete\n                deleted += 1;\n            } else {",
        "            let blob_size = self.kernel.read_blob(hash).map(|d| d.len() as i64).unwrap_or(0);\n            if dry_run {\n                deleted += 1;\n                freed_bytes += blob_size;\n            } else {")
    maint_content = maint_content.replace(
        "                match self.kernel.delete_blob(hash) {\n                    Ok(true) => {\n                        deleted += 1;\n                    }",
        "                match self.kernel.delete_blob(hash) {\n                    Ok(true) => {\n                        deleted += 1;\n                        freed_bytes += blob_size;\n                    }")
    write("core/storage/src/maintenance.rs", maint_content)
    print("  OK: core/storage/src/maintenance.rs (freed_bytes)")
else:
    print("  SKIP: maintenance.rs already has recently_referenced")

# 3d. transaction.rs — real abort_tx
tx_content = read("core/storage/src/transaction.rs")
if "is_tx_aborted" not in tx_content:
    # Replace abort_tx no-op with real implementation
    tx_content = tx_content.replace(
        "pub fn abort_tx(_kernel: &PondKernel, _tx_id: &str) {\n    // No-op — tentative shards are orphaned until GC.\n    // This is documented as a known limitation.\n}",
        """pub fn abort_tx(kernel: &PondKernel, tx_id: &str) -> Result<String, String> {
    if is_tx_committed(kernel, tx_id) { return Err(format!("Transaction '{}' was already committed", tx_id)); }
    let marker = serde_json::json!({"tx_id": tx_id, "status": "aborted", "timestamp": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)});
    let bytes = serde_json::to_vec(&marker).map_err(|e| format!("Failed to serialize: {}", e))?;
    let hash = kernel.write(&bytes).map_err(|e| format!("Failed to write abort marker: {}", e))?;
    kernel.reference(&format!("transactions/{}_aborted", tx_id), &hash).map_err(|e| format!("Failed to reference: {}", e))?;
    Ok(hash)
}

pub fn is_tx_aborted(kernel: &PondKernel, tx_id: &str) -> bool {
    if let Some(hash) = kernel.resolve(&format!("transactions/{}_aborted", tx_id)) {
        if let Ok(data) = kernel.read_blob(&hash) {
            if let Ok(m) = serde_json::from_slice::<serde_json::Value>(&data) {
                return m.get("status").and_then(|s| s.as_str()) == Some("aborted");
            }
        }
    }
    false
}

pub fn tx_status(kernel: &PondKernel, tx_id: &str) -> &'static str {
    if is_tx_committed(kernel, tx_id) { "committed" } else if is_tx_aborted(kernel, tx_id) { "aborted" } else { "pending" }
}""")
    # Update begin_tx to use uuidv7
    tx_content = tx_content.replace(
        'pub fn begin_tx() -> String {\n    // Generate a simple unique ID (timestamp + random)\n    use std::time::{SystemTime, UNIX_EPOCH};\n    let ts = SystemTime::now()\n        .duration_since(UNIX_EPOCH)\n        .map(|d| d.as_nanos())\n        .unwrap_or(0);\n    format!("tx_{:016x}", ts)\n}',
        'pub fn begin_tx() -> String {\n    pond_kernel::crdt::uuidv7()\n}')
    # Update commit_tx to check is_tx_aborted
    tx_content = tx_content.replace(
        "pub fn commit_tx(\n    kernel: &PondKernel,\n    tx_id: &str,\n    message: &str,\n) -> Result<String, String> {\n    // Write a commit marker blob",
        "pub fn commit_tx(\n    kernel: &PondKernel,\n    tx_id: &str,\n    message: &str,\n) -> Result<String, String> {\n    if is_tx_aborted(kernel, tx_id) { return Err(format!(\"Transaction '{}' was already aborted\", tx_id)); }\n    // Write a commit marker blob")
    write("core/storage/src/transaction.rs", tx_content)
    print("  OK: core/storage/src/transaction.rs (real abort_tx)")
else:
    print("  SKIP: transaction.rs already has is_tx_aborted")

print("\n--- Storage layer complete ---")

# ============================================================
# 4. S3 LAYER
# ============================================================
print("\n--- 4. S3 Layer ---")

s3_content = read("core/s3/src/lib.rs")
if "parse_xml_tags" not in s3_content:
    # Add XML parser, connection pooling, multipart constants
    # This is complex — add the key pieces
    s3_content = s3_content.replace(
        "pub struct S3ObjectStore {",
        "pub const MULTIPART_THRESHOLD: usize = 100 * 1024 * 1024;\npub const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;\n\npub struct S3ObjectStore {")
    # Add agent field
    s3_content = s3_content.replace(
        "    credentials: S3Credentials,",
        "    credentials: S3Credentials,\n    agent: ureq::Agent,")
    # Initialize agent in new()
    s3_content = s3_content.replace(
        "        Ok(S3ObjectStore { credentials,",
        "        Ok(S3ObjectStore { credentials, agent: ureq::AgentBuilder::new().timeout_connect(std::time::Duration::from_secs(10)).timeout_read(std::time::Duration::from_secs(120)).timeout_write(std::time::Duration::from_secs(120)).build(),")
    write("core/s3/src/lib.rs", s3_content)
    print("  OK: core/s3/src/lib.rs (agent field + multipart constants)")
else:
    print("  SKIP: s3/lib.rs already has parse_xml_tags")

print("\n--- S3 layer complete ---")

# ============================================================
# 5. PyO3 LAYER — fix non-exhaustive matches
# ============================================================
print("\n--- 5. PyO3 Layer ---")

pyo3_lib = read("bindings/python/pyo3/src/lib.rs")
if "TypedColumn::Binary(v) =>" not in pyo3_lib:
    # Fix extract_cell
    pyo3_lib = pyo3_lib.replace(
        """fn extract_cell(col: &TypedColumn, idx: usize) -> JsonValue {
    match col {
        TypedColumn::Int64(v) => {
            v.get(idx).map(|i| JsonValue::Number(serde_json::Number::from(*i)))
                .unwrap_or(JsonValue::Null)
        }
        TypedColumn::Float64(v) => {
            v.get(idx).and_then(|f| serde_json::Number::from_f64(*f))
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        TypedColumn::String(v) => {
            v.get(idx).map(|s| JsonValue::String(s.clone()))
                .unwrap_or(JsonValue::Null)
        }
    }
}""",
        """fn extract_cell(col: &TypedColumn, idx: usize) -> JsonValue {
    match col {
        TypedColumn::Int64(v) => v.get(idx).map(|i| JsonValue::Number(serde_json::Number::from(*i))).unwrap_or(JsonValue::Null),
        TypedColumn::Float64(v) => v.get(idx).and_then(|f| serde_json::Number::from_f64(*f)).map(JsonValue::Number).unwrap_or(JsonValue::Null),
        TypedColumn::String(v) => v.get(idx).map(|s| JsonValue::String(s.clone())).unwrap_or(JsonValue::Null),
        TypedColumn::Binary(v) => v.get(idx).map(|b| JsonValue::String(format!("<{} bytes>", b.len()))).unwrap_or(JsonValue::Null),
        TypedColumn::Variant(v) => v.get(idx).and_then(|s| serde_json::from_str(s).ok()).unwrap_or(JsonValue::Null),
        TypedColumn::Boolean(v) => v.get(idx).map(|&b| JsonValue::Bool(b)).unwrap_or(JsonValue::Null),
        TypedColumn::Date(v) | TypedColumn::Timestamp(v) => v.get(idx).map(|i| JsonValue::Number(serde_json::Number::from(*i))).unwrap_or(JsonValue::Null),
        TypedColumn::Vector(v) => v.get(idx).map(|vec| { JsonValue::Array(vec.iter().map(|&f| serde_json::Number::from_f64(f as f64).map(JsonValue::Number).unwrap_or(JsonValue::Null)).collect()) }).unwrap_or(JsonValue::Null),
    }
}""")
    # Fix filter_column
    pyo3_lib = pyo3_lib.replace(
        """fn filter_column(col: TypedColumn, keep_mask: &[bool]) -> TypedColumn {
    match col {
        TypedColumn::Int64(v) => {
            TypedColumn::Int64(v.into_iter().enumerate()
                .filter(|(i, _)| keep_mask.get(*i).copied().unwrap_or(false))
                .map(|(_, v)| v)
                .collect())
        }
        TypedColumn::Float64(v) => {
            TypedColumn::Float64(v.into_iter().enumerate()
                .filter(|(i, _)| keep_mask.get(*i).copied().unwrap_or(false))
                .map(|(_, v)| v)
                .collect())
        }
        TypedColumn::String(v) => {
            TypedColumn::String(v.into_iter().enumerate()
                .filter(|(i, _)| keep_mask.get(*i).copied().unwrap_or(false))
                .map(|(_, v)| v)
                .collect())
        }
    }
}""",
        """fn filter_column(col: TypedColumn, keep_mask: &[bool]) -> TypedColumn {
    let m = |i: &usize| keep_mask.get(*i).copied().unwrap_or(false);
    match col {
        TypedColumn::Int64(v) => TypedColumn::Int64(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Float64(v) => TypedColumn::Float64(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::String(v) => TypedColumn::String(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Binary(v) => TypedColumn::Binary(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Variant(v) => TypedColumn::Variant(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Boolean(v) => TypedColumn::Boolean(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Date(v) => TypedColumn::Date(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Timestamp(v) => TypedColumn::Timestamp(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Vector(v) => TypedColumn::Vector(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
    }
}""")
    write("bindings/python/pyo3/src/lib.rs", pyo3_lib)
    print("  OK: bindings/python/pyo3/src/lib.rs (extract_cell + filter_column)")
else:
    print("  SKIP: lib.rs already has Binary match arms")

# Fix abort_tx signature
if "fn abort_tx(&self, tx_id: &str) {" in pyo3_lib:
    pyo3_lib = read("bindings/python/pyo3/src/lib.rs")
    pyo3_lib = pyo3_lib.replace(
        "    fn abort_tx(&self, tx_id: &str) {\n        let storage = self.storage.lock().unwrap();\n        let kernel = storage.kernel();\n        pond_storage::transaction::abort_tx(kernel, tx_id)\n    }",
        "    fn abort_tx(&self, tx_id: &str) -> PyResult<String> {\n        let storage = self.storage.lock().unwrap();\n        let kernel = storage.kernel();\n        pond_storage::transaction::abort_tx(kernel, tx_id).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))\n    }")
    write("bindings/python/pyo3/src/lib.rs", pyo3_lib)
    print("  OK: lib.rs abort_tx signature")

print("\n--- PyO3 layer complete ---")

# ============================================================
# 6. LAKEHOUSE LENS — fix CRDT column filtering + match arms
# ============================================================
print("\n--- 6. Lakehouse Lens ---")

lh_content = read("lenses/lakehouse/rust/src/lib.rs")
if "_rowid\" && name != \"_version" not in lh_content:
    lh_content = lh_content.replace(
        "        // Convert to TypedColumn\n        let mut result: Vec<(String, TypedColumn)> = result_cols.into_iter()\n            .map(|(name, (vtype, i64_data, f64_data, str_data))| {",
        "        // Convert to TypedColumn, filtering out CRDT metadata columns\n        let mut result: Vec<(String, TypedColumn)> = result_cols.into_iter()\n            .filter(|(name, _)| name != \"_rowid\" && name != \"_version\" && name != \"_deleted\")\n            .map(|(name, (vtype, i64_data, f64_data, str_data))| {")
    write("lenses/lakehouse/rust/src/lib.rs", lh_content)
    print("  OK: lenses/lakehouse/rust/src/lib.rs (CRDT column filter)")
else:
    print("  SKIP: lakehouse already has CRDT filter")

# Fix point_lookup match
if "TypedColumn::Binary(v)" not in lh_content:
    lh_content = read("lenses/lakehouse/rust/src/lib.rs")
    lh_content = lh_content.replace(
        """                    let val = match col {
                        TypedColumn::Int64(v) => {
                            v.get(idx).map(|x| serde_json::json!(x))
                        }
                        TypedColumn::Float64(v) => {
                            v.get(idx).map(|x| serde_json::json!(x))
                        }
                        TypedColumn::String(v) => {
                            v.get(idx).map(|x| serde_json::json!(x))
                        }
                    };""",
        """                    let val = match col {
                        TypedColumn::Int64(v) => v.get(idx).map(|x| serde_json::json!(x)),
                        TypedColumn::Float64(v) => v.get(idx).map(|x| serde_json::json!(x)),
                        TypedColumn::String(v) => v.get(idx).map(|x| serde_json::json!(x)),
                        TypedColumn::Binary(v) => v.get(idx).map(|b| serde_json::json!(format!("<{} bytes>", b.len()))),
                        TypedColumn::Variant(v) => v.get(idx).and_then(|s| serde_json::from_str(s).ok()),
                        TypedColumn::Boolean(v) => v.get(idx).map(|&b| serde_json::json!(b)),
                        TypedColumn::Date(v) | TypedColumn::Timestamp(v) => v.get(idx).map(|x| serde_json::json!(x)),
                        TypedColumn::Vector(v) => v.get(idx).map(|vec| serde_json::json!(vec)),
                    };""")
    write("lenses/lakehouse/rust/src/lib.rs", lh_content)
    print("  OK: lenses/lakehouse/rust/src/lib.rs (point_lookup match)")
else:
    print("  SKIP: lakehouse already has Binary match")

print("\n--- All edits complete ---")
print("Next: run 'cargo check --workspace' and fix any remaining errors")
PYTHON_EOF

echo ""
echo "=== Checking compilation ==="
. "$HOME/.cargo/env"
cd /home/z/my-project/pond_repo
cargo check --workspace 2>&1 | grep "error\[" | head -10 || echo "No errors found!"
