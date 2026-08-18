// ObjectStore trait + LocalFSObjectStore — the storage backend abstraction
//
// All backends (local FS, S3, GCS) implement this trait. The kernel uses
// this trait — it doesn't know which backend.
//
// PATH LAYOUT (same on all backends):
//   blobs/{hash[:2]}/{hash}        — content-addressed blobs (raw bytes)
//   {path}                          — named refs (JSON: {"hash":"..."})

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::hash_bytes;

// ---------------------------------------------------------------------------
// ObjectStore trait — the storage backend abstraction
// ---------------------------------------------------------------------------

/// The storage backend trait. All backends (local FS, S3, GCS) implement
/// this. The kernel uses this trait — it doesn't know which backend.
///
/// PATH LAYOUT (same on all backends):
///   blobs/{hash[:2]}/{hash}        — content-addressed blobs (raw bytes)
///   {path}                          — named refs (JSON: {"hash":"..."})
///
/// The ref path IS the key — no "refs/" prefix. This matches S3's flat
/// key space and allows `aws s3 sync` / `rsync` for migration.
/// Is this relative key part of the content-addressed blob tree rather than a
/// named ref?
///
/// `list_paths` enumerates *refs*, so blob keys have to be excluded — but only
/// when the caller did not ask for them. Both backends use this one rule, which
/// is the point: a listing that returns different things on local disk and on
/// S3 makes every layer above it backend-specific, and the whole design rests
/// on the two being interchangeable.
pub fn is_blob_key(rel: &str) -> bool {
    rel == "blobs" || rel.starts_with("blobs/")
}

/// Did the caller explicitly ask to list inside the blob tree?
pub fn prefix_targets_blobs(prefix: &str) -> bool {
    is_blob_key(prefix.trim_start_matches('/'))
}

pub trait ObjectStore: Send + Sync {
    /// Write bytes, content-addressed. Returns the hash.
    /// Idempotent: same bytes → same hash → same key. Overwriting is safe.
    fn put_blob(&self, data: &[u8]) -> io::Result<String>;

    /// Read bytes by content hash.
    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>>;

    /// Read a byte range of a blob: `len` bytes starting at `offset`.
    ///
    /// This is the primitive that makes object-storage-native design possible.
    /// Without it every read is a whole-object GET, which rules out a
    /// range-readable index, column chunks addressed inside a larger segment,
    /// and a byte-range cache — i.e. most of the reason to store big objects
    /// at all.
    ///
    /// It is also, deliberately, available on *every* backend: `seek`+`read`
    /// on a local file, a `Range:` header on S3/GCS/Azure, a slice in memory.
    /// That keeps the backend contract to four operations
    /// (put / get(range) / list / delete) with identical semantics everywhere
    /// — unlike conditional writes or append, which exist only on some
    /// backends and would fork the correctness argument in two.
    ///
    /// A read that starts at or past the end of the blob returns empty. A read
    /// that runs past the end is truncated to what exists, matching HTTP Range
    /// semantics rather than erroring.
    ///
    /// The default implementation fetches the whole blob and slices, so every
    /// backend is correct immediately; backends that can do better override it.
    fn get_blob_range(&self, hash: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let data = self.get_blob(hash)?;
        Ok(slice_range(&data, offset, len))
    }

    /// Write a batch of blobs. Default: sequential. Backends can override
    /// with parallel implementation (S3 uses a thread pool).
    fn put_blob_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        let mut hashes = Vec::with_capacity(items.len());
        for data in items {
            hashes.push(self.put_blob(data)?);
        }
        Ok(hashes)
    }

    /// Read a batch of blobs. Default: sequential.
    fn get_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(hashes.len());
        for h in hashes {
            results.push(self.get_blob(h)?);
        }
        Ok(results)
    }

    /// Bind a named path to a content hash. Stores JSON {"hash":"..."}.
    /// Last-writer-wins (no CAS). The backend handles atomicity.
    fn put_path(&self, path: &str, hash: &str) -> io::Result<()>;

    /// Resolve a named path to its content hash. Returns None if unbound.
    fn get_path(&self, path: &str) -> Option<String>;

    /// Write bytes directly at a named path, replacing whatever was there.
    ///
    /// This is the more primitive of the two named-write operations, and
    /// `put_path` is expressible in terms of it — the reverse is not true.
    /// The difference shows up on small mutable state: binding a name to a
    /// hash means the value has to be written as a blob first and read back
    /// through an indirection, so a small object costs two round trips to
    /// write and two to read. Storing the bytes under the name costs one each
    /// way.
    ///
    /// That matters most for the commit path, where the whole point is that
    /// publishing is a single object write, and for anything read on every
    /// open. Content-addressed storage is still the right default for
    /// everything large or shared — this is for the state that is small,
    /// mutable, and owned by exactly one writer.
    ///
    /// Last-writer-wins, like `put_path`, and durable on the same terms: a
    /// plain PUT on object storage, temp→fsync→rename locally.
    fn put_object(&self, path: &str, bytes: &[u8]) -> io::Result<()>;

    /// Read the bytes written by [`put_object`]. `None` if the path is unset.
    fn get_object(&self, path: &str) -> Option<Vec<u8>>;

    /// Read many named paths at once. Missing paths come back as `None` in
    /// place, so the result lines up with the input.
    ///
    /// Default is the sequential loop; S3 overrides it with parallel requests.
    fn get_object_batch(&self, paths: &[String]) -> Vec<Option<Vec<u8>>> {
        paths.iter().map(|p| self.get_object(p)).collect()
    }

    /// Delete a named path. Returns true if it existed.
    fn delete_path(&self, path: &str) -> io::Result<bool>;

    /// Delete many named paths, returning how many existed and were removed.
    ///
    /// The same argument as [`delete_blob_batch`](Self::delete_blob_batch):
    /// reclamation is sized by the data, so one round trip per name does not
    /// scale. Refs are fewer than blobs, but "fewer" is not "few" once every
    /// writer, branch, and collection has one.
    fn delete_path_batch(&self, paths: &[String]) -> io::Result<usize> {
        let mut removed = 0usize;
        for p in paths {
            if self.delete_path(p)? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// List all paths under a prefix. Returns relative paths (without prefix).
    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>>;

    /// Check if a blob exists (for dedup checks).
    fn blob_exists(&self, hash: &str) -> bool;

    /// Physically delete a blob from the object store (maintenance operation).
    ///
    /// This is NOT a kernel primitive — it's a Layer 0.5 maintenance operation
    /// (like VACUUM in PostgreSQL or git gc). Used by compact_shards and merge
    /// to reclaim storage space from absorbed/dead shards.
    ///
    /// Best-effort: if the store doesn't support deletion, this is a no-op.
    /// The blob becomes unreachable (orphaned) but the system is still correct.
    fn delete_blob(&self, hash: &str) -> io::Result<bool>;

    /// Delete many blobs, returning how many existed and were removed.
    ///
    /// Reclamation is the one maintenance operation whose size scales with the
    /// data rather than with the change, so a per-object round trip makes it
    /// impractical at the scale this store targets: dropping a million dead
    /// nodes at one request each is a million requests. S3 exposes a bulk
    /// delete that takes 1000 keys per request, which is three orders of
    /// magnitude fewer round trips for the same work.
    ///
    /// The default is the sequential loop, so every backend is correct without
    /// implementing it; a local filesystem has nothing to gain from batching
    /// because its "round trip" is a syscall.
    fn delete_blob_batch(&self, hashes: &[String]) -> io::Result<usize> {
        let mut removed = 0usize;
        for h in hashes {
            if self.delete_blob(h)? {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

// ---------------------------------------------------------------------------
// AsyncObjectStore trait — async version, behind `feature = "async"`
// ---------------------------------------------------------------------------

/// Async variant of [`ObjectStore`]. Only available with the `async` feature.
///
/// All backends that want to expose true async I/O (rather than just blocking
/// the sync API on a thread pool) should implement this trait in addition to
/// [`ObjectStore`]. The sync trait remains the source of truth — async
/// methods are a performance opt-in for high-throughput concurrent workloads
/// (e.g. AI frameworks that use `asyncio`, which currently have to block the
/// GIL on every S3 call through the PyO3 binding).
///
/// `LocalFSObjectStore` implements this with `tokio::fs`; `S3ObjectStore`
/// implements it with `reqwest`.
///
/// NOTE: this trait does NOT extend [`ObjectStore`] — backends implement both
/// independently. The kernel's async methods (`PondKernel::read_blob_async`,
/// etc.) call `spawn_blocking` on the sync `ObjectStore` for now, which keeps
/// the trait object story simple. Backends that want true async I/O bypass
/// the kernel and call their `AsyncObjectStore` impl directly.
#[cfg(feature = "async")]
#[async_trait::async_trait]
pub trait AsyncObjectStore: Send + Sync {
    /// Write bytes content-addressed, returning the hash. Idempotent.
    async fn put_blob_async(&self, data: Vec<u8>) -> io::Result<String>;

    /// Read bytes by content hash.
    async fn get_blob_async(&self, hash: &str) -> io::Result<Vec<u8>>;

    /// Physically delete a blob (maintenance op). Returns true if it existed.
    async fn delete_blob_async(&self, hash: &str) -> io::Result<bool>;

    /// List all blob hashes with a given prefix (relative to the blobs/ tree).
    async fn list_blobs_prefix_async(&self, prefix: &str) -> Vec<String>;
}

// ---------------------------------------------------------------------------
// LocalFSObjectStore — local filesystem implementation
// ---------------------------------------------------------------------------

/// Local filesystem object store. Mirrors the S3 key structure exactly:
///
///   Blobs: {base_dir}/blobs/{hash[:2]}/{hash}
///   Refs:  {base_dir}/{path}   (e.g. collections/users/_branches/main/commit)
///
/// Thread-safe: blob writes use temp+rename (POSIX atomic); path writes
/// use temp+rename with unique temp names.
pub struct LocalFSObjectStore {
    base_dir: PathBuf,
    stats: Mutex<StoreStats>,
}

#[derive(Debug, Default, Clone)]
pub struct StoreStats {
    pub gets: u64,
    pub puts: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl LocalFSObjectStore {
    pub fn new(base_dir: impl AsRef<Path>) -> io::Result<Self> {
        let base = base_dir.as_ref();
        fs::create_dir_all(base.join("blobs"))?;
        Ok(Self {
            base_dir: base.to_path_buf(),
            stats: Mutex::new(StoreStats::default()),
        })
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    fn blob_path(&self, hash: &str) -> PathBuf {
        self.base_dir.join("blobs").join(&hash[..2]).join(hash)
    }

    /// Resolve a ref path to a file under `base_dir`.
    ///
    /// Returns `None` for any path that fails [`crate::validate_ref_path`].
    /// The kernel validates too; this is the last line of defense, on the one
    /// backend where a bad path escapes into the real filesystem.
    fn path_file(&self, path: &str) -> Option<PathBuf> {
        crate::validate_ref_path(path).ok()?;
        Some(self.base_dir.join(path))
    }
}

/// Write `data` to `path` durably: temp file → fsync(file) → rename →
/// fsync(parent dir).
///
/// `rename` is atomic with respect to *ordering* — a reader sees either the
/// old file or the new one — but on its own it says nothing about durability.
/// Without fsync of the file, a crash can leave the rename visible while the
/// contents are still in page cache, so a ref points at a zero-length or
/// partially written object. Without fsync of the parent directory, the
/// rename itself can be lost. Both matter here because the whole model
/// assumes a blob, once written, is immutable and complete.
///
/// The temp name includes a per-call counter as well as the pid so two
/// threads in one process cannot collide on it.
fn write_file_durably(path: &std::path::Path, data: &[u8]) -> io::Result<()> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);

    let tmp = path.with_extension(format!(
        "tmp.{}.{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));

    // Scope the file handle so it is closed before the rename.
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }

    match fs::rename(&tmp, path) {
        Ok(()) => {}
        Err(e) => {
            let _ = fs::remove_file(&tmp); // don't leak the temp file
            return Err(e);
        }
    }

    // fsync the directory so the rename itself survives a crash. Not all
    // platforms support opening a directory for this; where they don't, the
    // failure is not fatal — the data is already durable, only the rename's
    // durability is best-effort.
    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

impl ObjectStore for LocalFSObjectStore {
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        let h = hash_bytes(data);
        let path = self.blob_path(&h);
        // Dedup: skip if exists. Blobs are content-addressed, so an existing
        // file at this path already holds exactly these bytes.
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            write_file_durably(&path, data)?;
        }
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        s.bytes_written += data.len() as u64;
        Ok(h)
    }

    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        let path = self.blob_path(hash);
        if !path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound,
                format!("Blob '{}' not found", hash)));
        }
        let data = fs::read(&path)?;
        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        s.bytes_read += data.len() as u64;
        Ok(data)
    }

    /// Seek to `offset` and read at most `len` bytes — never loads the whole
    /// blob, which is the point of having a ranged read at all.
    fn get_blob_range(&self, hash: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let path = self.blob_path(hash);
        let mut f = fs::File::open(&path).map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Blob '{}' not found", hash),
                )
            } else {
                e
            }
        })?;

        let size = f.metadata()?.len();
        if offset >= size {
            return Ok(Vec::new());
        }
        // Truncate the request to what the file actually holds, so a caller
        // asking for more than exists gets a short read rather than an error
        // (and so `len` can never drive an unbounded allocation).
        let readable = (size - offset).min(len as u64) as usize;

        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; readable];
        f.read_exact(&mut buf)?;

        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        s.bytes_read += buf.len() as u64;
        Ok(buf)
    }

    fn put_path(&self, path: &str, hash: &str) -> io::Result<()> {
        let file = self.path_file(path).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("refusing to write ref outside the store root: '{}'", path),
            )
        })?;
        if let Some(parent) = file.parent() {
            fs::create_dir_all(parent)?;
        }
        // Store JSON {"hash":"..."} — same format as S3ObjectStore.
        // This makes `aws s3 sync` / `rsync` a straight copy.
        let body = format!(r#"{{"hash":"{}"}}"#, hash);
        // Refs are the only mutable state in the store, so their durability is
        // what a crash actually threatens: an un-synced rename can leave a
        // branch pointing at nothing. On S3 this is a plain PUT (already
        // atomic and durable); locally it needs the full temp→fsync→rename.
        write_file_durably(&file, body.as_bytes())?;
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        Ok(())
    }

    fn get_path(&self, path: &str) -> Option<String> {
        let file = self.path_file(path)?;
        match fs::read_to_string(&file) {
            Ok(body) => {
                // Parse JSON {"hash":"..."} — minimal parser
                extract_hash_from_json(&body)
            }
            Err(_) => None,
        }
    }

    fn put_object(&self, path: &str, bytes: &[u8]) -> io::Result<()> {
        let file = self.path_file(path).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("refusing to write outside the store root: '{}'", path),
            )
        })?;
        if let Some(parent) = file.parent() {
            fs::create_dir_all(parent)?;
        }
        write_file_durably(&file, bytes)?;
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        Ok(())
    }

    fn get_object(&self, path: &str) -> Option<Vec<u8>> {
        let file = self.path_file(path)?;
        fs::read(&file).ok()
    }

    fn delete_path(&self, path: &str) -> io::Result<bool> {
        let file = match self.path_file(path) {
            Some(f) => f,
            None => return Ok(false),
        };
        if file.exists() {
            fs::remove_file(&file)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        let prefix_dir = self.base_dir.join(prefix);
        let mut paths = Vec::new();
        if prefix_dir.is_dir() {
            walk_dir(&prefix_dir, &self.base_dir, &mut paths);
        }
        if !prefix_targets_blobs(prefix) {
            paths.retain(|p| !is_blob_key(p));
        }
        paths.sort();
        Ok(paths)
    }

    fn blob_exists(&self, hash: &str) -> bool {
        self.blob_path(hash).exists()
    }

    fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        let path = self.blob_path(hash);
        if path.exists() {
            fs::remove_file(&path)?;
            let mut s = self.stats.lock().unwrap();
            s.gets += 1; // count as a maintenance op
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Clamp a byte range to what a buffer actually holds.
///
/// Shared by every backend so range semantics cannot drift between them:
/// an offset at or past the end yields empty, and a length running past the
/// end is truncated. This matches HTTP Range behaviour, which is what the S3
/// implementation gets from the server anyway.
pub fn slice_range(data: &[u8], offset: u64, len: usize) -> Vec<u8> {
    let start = match usize::try_from(offset) {
        Ok(s) if s < data.len() => s,
        _ => return Vec::new(),
    };
    let end = start.saturating_add(len).min(data.len());
    data[start..end].to_vec()
}

/// Walk a directory recursively, collecting file paths relative to root.
fn walk_dir(dir: &Path, root: &Path, paths: &mut Vec<String>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk_dir(&path, root, paths);
            } else if path.is_file() {
                if let Ok(rel) = path.strip_prefix(root) {
                    paths.push(rel.to_string_lossy().replace('\\', "/"));
                }
            }
        }
    }
}

/// Extract the "hash" field from a JSON string like {"hash":"abc123"}.
fn extract_hash_from_json(json: &str) -> Option<String> {
    let needle = r#""hash":""#;
    if let Some(start) = json.find(needle) {
        let rest = &json[start + needle.len()..];
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// AsyncObjectStore impl for LocalFSObjectStore — tokio::fs
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncObjectStore for LocalFSObjectStore {
    async fn put_blob_async(&self, data: Vec<u8>) -> io::Result<String> {
        let h = hash_bytes(&data);
        let path = self.blob_path(&h);

        // Dedup: skip if exists. `tokio::fs::try_exists` is stable on Rust 1.70+.
        // We use `tokio::fs::metadata` for compatibility with older toolchains.
        let already_exists = tokio::fs::metadata(&path).await.is_ok();
        if !already_exists {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            // Write to temp file, then rename (POSIX atomic).
            let tmp = format!("{}.tmp.{}", path.display(), std::process::id());
            tokio::fs::write(&tmp, &data).await?;
            tokio::fs::rename(&tmp, &path).await?;
        }
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        s.bytes_written += data.len() as u64;
        Ok(h)
    }

    async fn get_blob_async(&self, hash: &str) -> io::Result<Vec<u8>> {
        let path = self.blob_path(hash);
        // Mirror the sync error: NotFound when the blob is missing.
        if tokio::fs::metadata(&path).await.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Blob '{}' not found", hash),
            ));
        }
        let data = tokio::fs::read(&path).await?;
        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        s.bytes_read += data.len() as u64;
        Ok(data)
    }

    async fn delete_blob_async(&self, hash: &str) -> io::Result<bool> {
        let path = self.blob_path(hash);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                let mut s = self.stats.lock().unwrap();
                s.gets += 1; // count as a maintenance op (matches sync impl)
                Ok(true)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn list_blobs_prefix_async(&self, prefix: &str) -> Vec<String> {
        if prefix.len() < 2 {
            return Vec::new();
        }
        let shard = &prefix[..2];
        let list_prefix = self.base_dir.join("blobs").join(shard);

        // Collect into a Vec<PathBuf> first, then map to strings. We use
        // tokio::fs::read_dir for an async walk. For simplicity we walk
        // single-level (blobs/{shard}/ only — Pond never nests deeper).
        let mut entries = match tokio::fs::read_dir(&list_prefix).await {
            Ok(rd) => rd,
            Err(_) => return Vec::new(),
        };

        let mut hashes = Vec::new();
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.file_type().await.map(|t| t.is_file()).unwrap_or(false) {
                let name = entry.file_name();
                let name = name.to_string_lossy().replace('\\', "/");
                if name.starts_with(prefix) {
                    hashes.push(name);
                }
            }
        }
        hashes.sort();
        hashes
    }
}
