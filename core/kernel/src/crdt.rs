// UUIDv7 + HLC — distributed row identification and clock-skew-safe versioning
//
// These are the CRDT primitives:
//   - UUIDv7: time-ordered unique IDs for _rowid (distributed, no coordinator)
//   - HLC: Hybrid Logical Clock for _version (monotonic under clock skew)
//
// Used by:
//   - upsert_shard: generates _rowid (UUIDv7) + _version (HLC) per row
//   - merge: compares _version to determine which row wins (latest wins)
//   - delete_shard: writes tombstone with _deleted=True + new _version

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// UUIDv7 — time-ordered UUID for distributed row identification
// ---------------------------------------------------------------------------

/// Monotonic counter state for same-millisecond uniqueness (per-process).
struct MonotonicState {
    last_ms: u64,
    counter: u16,
}

static MONOTONIC: Mutex<MonotonicState> = Mutex::new(MonotonicState {
    last_ms: 0,
    counter: 0,
});

/// Generate a UUIDv7 string (36 chars, dashed format).
///
/// Format (128 bits):
///   - 48 bits: Unix epoch milliseconds (big-endian)
///   - 4 bits: version (7)
///   - 12 bits: random_a
///   - 2 bits: variant (10)
///   - 62 bits: random_b
///
/// Time-ordered: earlier timestamps sort before later ones lexicographically.
/// Distributed-friendly: no central ID allocator needed.
pub fn uuidv7() -> String {
    let timestamp_ms = current_time_ms();
    uuidv7_with_timestamp(timestamp_ms)
}

/// Generate a UUIDv7 with a specific timestamp (for testing).
pub fn uuidv7_with_timestamp(timestamp_ms: u64) -> String {
    let mut rand_bytes = [0u8; 10];
    fill_random(&mut rand_bytes);

    // Build 16 bytes
    let ts_bytes = timestamp_ms.to_be_bytes(); // 8 bytes, take first 6
    let mut bytes = [0u8; 16];

    // Bytes 0-5: timestamp (48 bits, big-endian)
    bytes[0] = ts_bytes[2];
    bytes[1] = ts_bytes[3];
    bytes[2] = ts_bytes[4];
    bytes[3] = ts_bytes[5];
    bytes[4] = ts_bytes[6];
    bytes[5] = ts_bytes[7];

    // Byte 6: version (4 bits = 7) + random_a high (4 bits)
    bytes[6] = 0x70 | (rand_bytes[0] & 0x0F);

    // Byte 7: random_a low (8 bits)
    bytes[7] = rand_bytes[1];

    // Byte 8: variant (2 bits = 10) + random_b high (6 bits)
    bytes[8] = 0x80 | (rand_bytes[2] & 0x3F);

    // Bytes 9-15: random_b remaining (56 bits)
    bytes[9..16].copy_from_slice(&rand_bytes[3..10]);

    format_uuid(&bytes)
}

/// Generate a UUIDv7 with guaranteed monotonic ordering within a process.
///
/// If called multiple times within the same millisecond, uses a counter
/// to ensure strict ordering. 4096 unique IDs per millisecond per process.
pub fn uuidv7_monotonic() -> String {
    let mut state = MONOTONIC.lock().unwrap();
    let now_ms = current_time_ms();

    let (ts, counter) = if now_ms <= state.last_ms {
        state.counter += 1;
        if state.counter > 0xFFF {
            // Counter overflow — advance to next millisecond
            state.last_ms += 1;
            state.counter = 0;
            (state.last_ms, 0u16)
        } else {
            (state.last_ms, state.counter)
        }
    } else {
        state.last_ms = now_ms;
        state.counter = 0;
        (now_ms, 0u16)
    };

    let mut rand_bytes = [0u8; 8];
    fill_random(&mut rand_bytes);

    let ts_bytes = ts.to_be_bytes();
    let mut bytes = [0u8; 16];

    bytes[0] = ts_bytes[2];
    bytes[1] = ts_bytes[3];
    bytes[2] = ts_bytes[4];
    bytes[3] = ts_bytes[5];
    bytes[4] = ts_bytes[6];
    bytes[5] = ts_bytes[7];

    // Embed counter in random_a (12 bits)
    bytes[6] = 0x70 | ((counter >> 8) as u8 & 0x0F);
    bytes[7] = (counter & 0xFF) as u8;

    bytes[8] = 0x80 | (rand_bytes[0] & 0x3F);
    bytes[9..16].copy_from_slice(&rand_bytes[1..8]);

    format_uuid(&bytes)
}

/// Extract the Unix millisecond timestamp from a UUIDv7 string.
pub fn uuidv7_timestamp(uuid_str: &str) -> Option<u64> {
    let hex: String = uuid_str.chars().filter(|c| *c != '-').collect();
    if hex.len() < 12 {
        return None;
    }
    let ts_high = u32::from_str_radix(&hex[0..8], 16).ok()?;
    let ts_low = u16::from_str_radix(&hex[8..12], 16).ok()?;
    Some(((ts_high as u64) << 16) | (ts_low as u64))
}

/// Format 16 bytes as a standard UUID string (36 chars with dashes).
fn format_uuid(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

/// Fill a buffer with cryptographically secure random bytes.
///
/// UUIDv7's random bits are what keep row IDs unique across machines that
/// generate them in the same millisecond, so they must not be predictable or
/// correlated. The previous implementation was an xorshift64 re-seeded on
/// every call from `now_nanos + pid` — two calls within the same clock tick
/// produced identical bytes, and the sequence was trivially predictable.
///
/// `getrandom` is the OS CSPRNG: `getrandom(2)`/`/dev/urandom` on Linux,
/// `getentropy` on macOS/BSD, `BCryptGenRandom` on Windows. It is already in
/// the dependency tree (via ring, through the S3 backend's TLS stack), so this
/// adds no new third-party surface.
fn fill_random(buf: &mut [u8]) {
    getrandom::fill(buf).expect("OS CSPRNG unavailable — cannot generate unique row IDs");
}

/// Milliseconds since the Unix epoch.
///
/// Exposed because the physical component of a version has to come from
/// somewhere, and every caller agreeing on one clock reading is what makes
/// versions comparable across writers.
pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Shard naming
// ---------------------------------------------------------------------------

/// This machine's writer identity, stable across restarts.
///
/// # The precondition, and how easy it is to break
///
/// A writer id must belong to **one live process**. That is not a style rule:
/// it is what makes a head safe to publish with a plain PUT and no
/// compare-and-swap, because nobody else writes under that writer's prefix.
///
/// This derives the id from hostname and username, which is stable — a writer
/// that restarts recovers its own head — and which two processes on one
/// machine therefore *share*. Two containers built from one image share it
/// too. That is a misconfiguration, not an exotic one.
///
/// Violating it used to lose rows silently: both processes published at the
/// same sequence, and a reader kept one head. It no longer does — readers keep
/// every head at a writer's highest sequence, and a writer reopening folds in
/// every head at its own prefix, so the collision heals on the next publish.
/// It still costs an extra read and it is still worth avoiding.
///
/// Set `POND_WRITER_ID` (or `pond --writer`) to give a process its own
/// identity. Use it whenever more than one process writes the same pond from
/// one machine, or when hostname and username are not unique across the
/// machines that do.
pub fn stable_writer_id() -> u64 {
    let host = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|s| s.trim().to_string());
    let user = std::env::var("USER").ok().or_else(|| std::env::var("USERNAME").ok());

    match (host, user) {
        (None, None) => {
            let mut bytes = [0u8; 8];
            fill_random(&mut bytes);
            u64::from_be_bytes(bytes)
        }
        (h, u) => {
            let mut hasher = Sha256::new();
            hasher.update(h.unwrap_or_default().as_bytes());
            hasher.update(b"\x00");
            hasher.update(u.unwrap_or_default().as_bytes());
            let out = hasher.finalize();
            u64::from_be_bytes(out[..8].try_into().unwrap_or([0u8; 8]))
        }
    }
}

/// The writer identity a process should use: `POND_WRITER_ID` if set,
/// otherwise [`stable_writer_id`].
///
/// The value is a *name*, not a number — it is hashed, so "laptop" and "7" are
/// both fine and neither is special. Hashing rather than parsing avoids the
/// ambiguity of a name that happens to be numeric, and makes accidental
/// collisions between hand-picked ids as unlikely as between derived ones: two
/// people who both reach for `1` do not collide, whereas with parsing they
/// would.
pub fn writer_id_from_env() -> u64 {
    if let Some(id) = EXPLICIT_WRITER_ID.get() {
        return *id;
    }
    match std::env::var("POND_WRITER_ID") {
        Ok(name) if !name.trim().is_empty() => writer_id_from_name(name.trim()),
        _ => stable_writer_id(),
    }
}

/// Set by an entry point that was given an identity on its command line.
static EXPLICIT_WRITER_ID: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

/// Name this process's writer identity, once, at startup.
///
/// Exists so a `--writer` flag and the `POND_WRITER_ID` variable resolve to
/// the same id through the same function, rather than the flag being read in
/// one place and the variable in another — which is how the two drift apart
/// and one of them silently stops working.
///
/// Returns the id in force. Later calls do not override an earlier one: an
/// identity that could change mid-process is worse than one that is wrong,
/// because the writer would publish under two prefixes and supersede neither.
pub fn set_writer_id(name: &str) -> u64 {
    let id = writer_id_from_name(name);
    let _ = EXPLICIT_WRITER_ID.set(id);
    *EXPLICIT_WRITER_ID.get().unwrap_or(&id)
}

/// Derive a writer id from a name.
pub fn writer_id_from_name(name: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"pond-writer\x00");
    hasher.update(name.as_bytes());
    let out = hasher.finalize();
    let id = u64::from_be_bytes(out[..8].try_into().unwrap_or([0u8; 8]));
    // `u64::MAX` is reserved for compacted heads, so a name must never land on
    // it. One in 2^64, and a silent collision there would make a writer's heads
    // indistinguishable from a compaction's.
    if id == u64::MAX {
        id - 1
    } else {
        id
    }
}

pub fn shard_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    static WRITER_ID: OnceLock<u64> = OnceLock::new();
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let writer = *WRITER_ID.get_or_init(|| {
        let mut b = [0u8; 8];
        fill_random(&mut b);
        u64::from_be_bytes(b)
    });
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}-{:016x}-{:08x}", ts, writer, seq)
}

// ---------------------------------------------------------------------------
// HLC — Hybrid Logical Clock for clock-skew-safe _version
// ---------------------------------------------------------------------------

/// Hybrid Logical Clock — monotonic under clock skew, totally ordered
/// across writers.
///
/// Each process has its own HLC instance. The clock advances on every tick
/// (before each write). The logical counter increments when the physical
/// clock hasn't advanced.
///
/// # Why there is a writer id
///
/// `(physical, logical)` alone does **not** give a total order across
/// machines: two nodes ticking in the same millisecond, each with logical=0,
/// emit *byte-identical* versions. Last-writer-wins then has a tie it cannot
/// break, so the winner depends on iteration order and `merge(A,B)` stops
/// agreeing with `merge(B,A)` — the convergence property the whole CRDT
/// design rests on. A per-writer id makes the order total: ties fall back to
/// comparing writer ids, which is arbitrary but *deterministic and identical
/// on every replica*.
///
/// Usage:
///   let mut clock = HLC::new();
///   let v1 = clock.tick();  // before write 1
///   let v2 = clock.tick();  // before write 2 (may have same physical, higher logical)
///   assert!(v1 < v2);  // lexicographic = chronological
pub struct HLC {
    physical: u64,
    logical: u64,
    writer: u64,
}

impl HLC {
    /// Create a clock with a random writer id drawn from the OS CSPRNG.
    ///
    /// Random (rather than, say, pid) because the id must be distinct across
    /// machines, not just across processes on one host.
    pub fn new() -> Self {
        let mut bytes = [0u8; 8];
        fill_random(&mut bytes);
        Self { physical: 0, logical: 0, writer: u64::from_be_bytes(bytes) }
    }

    /// Create a clock with an explicit writer id.
    ///
    /// Use this when a node has a stable identity that should persist across
    /// restarts, so its versions keep tie-breaking the same way.
    pub fn with_writer_id(writer: u64) -> Self {
        Self { physical: 0, logical: 0, writer }
    }

    /// This clock's writer id.
    pub fn writer_id(&self) -> u64 {
        self.writer
    }

    /// Advance the clock and return the current HLC value as a hex string.
    ///
    /// Format: 48 hex chars — 16 physical_ms + 16 logical + 16 writer id, all
    /// big-endian, so lexicographic order == (physical, logical, writer)
    /// order. Values written before writer ids existed are 32 chars; they
    /// compare correctly against 48-char values because a prefix sorts before
    /// any longer string sharing it.
    pub fn tick(&mut self) -> String {
        let now = current_time_ms();
        if now > self.physical {
            self.physical = now;
            self.logical = 0;
        } else {
            self.logical += 1;
        }
        self.encode()
    }

    /// Observe another HLC value (e.g., from a remote write).
    /// Updates the local clock to be at least as high as the observed value.
    pub fn observe(&mut self, other: &str) {
        if let Some((other_physical, other_logical)) = Self::decode(other) {
            let now = current_time_ms();
            if now > self.physical && now > other_physical {
                self.physical = now;
                self.logical = 0;
            } else if other_physical > self.physical {
                self.physical = other_physical;
                self.logical = other_logical + 1;
            } else if other_physical == self.physical {
                if other_logical > self.logical {
                    self.logical = other_logical + 1;
                } else {
                    self.logical += 1;
                }
            } else {
                self.logical += 1;
            }
        }
    }

    /// Encode the current HLC state as a 48-char hex string.
    fn encode(&self) -> String {
        format!("{:016x}{:016x}{:016x}", self.physical, self.logical, self.writer)
    }

    /// Decode an HLC string into (physical, logical).
    ///
    /// Accepts both the current 48-char form and the legacy 32-char form
    /// (physical + logical, no writer id) so collections written before
    /// writer ids existed keep resolving.
    fn decode(s: &str) -> Option<(u64, u64)> {
        if s.len() != 32 && s.len() != 48 {
            return None;
        }
        let physical = u64::from_str_radix(&s[0..16], 16).ok()?;
        let logical = u64::from_str_radix(&s[16..32], 16).ok()?;
        Some((physical, logical))
    }

    /// Check if a string is a valid HLC value.
    /// Check if a string is a valid HLC value (48-char current form, or the
    /// legacy 32-char form without a writer id).
    pub fn is_valid(s: &str) -> bool {
        (s.len() == 48 || s.len() == 32) && s.chars().all(|c| c.is_ascii_hexdigit())
    }
}

impl Default for HLC {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #[test]
    fn a_writer_name_is_hashed_not_parsed() {
        // "7" is a name like any other. Parsing it as an id would mean two
        // people who both reach for a small number collide, while hashing
        // makes hand-picked ids exactly as safe as derived ones.
        assert_ne!(writer_id_from_name("7"), 7);
        assert_eq!(writer_id_from_name("7"), writer_id_from_name("7"));
        assert_ne!(writer_id_from_name("alice"), writer_id_from_name("bob"));
    }

    #[test]
    fn a_name_never_collides_with_the_compaction_identity() {
        // u64::MAX is reserved. A name landing there would make that writer's
        // heads indistinguishable from a compaction's, which readers treat
        // very differently.
        assert_ne!(writer_id_from_name("compactor"), u64::MAX);
        assert_ne!(writer_id_from_name(""), u64::MAX);
    }

    #[test]
    fn distinct_names_give_distinct_prefixes() {
        let names = ["alice", "bob", "carol", "laptop-1", "laptop-2", "0", "1"];
        let mut ids: Vec<u64> = names.iter().map(|n| writer_id_from_name(n)).collect();
        let before = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), before, "names must not share an identity");
    }

    /// Without an explicit identity, the id is derived and therefore stable —
    /// which is what lets a writer recover its own head after a restart, and
    /// also what makes two processes on one machine share one.
    #[test]
    fn the_derived_identity_is_stable_across_calls() {
        assert_eq!(stable_writer_id(), stable_writer_id());
    }

    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_uuidv7_format() {
        let id = uuidv7();
        assert_eq!(id.len(), 36, "UUIDv7 must be 36 chars");
        assert_eq!(id.chars().nth(8), Some('-'));
        assert_eq!(id.chars().nth(13), Some('-'));
        assert_eq!(id.chars().nth(18), Some('-'));
        assert_eq!(id.chars().nth(23), Some('-'));
        // Version 7
        assert_eq!(id.chars().nth(14), Some('7'));
    }

    #[test]
    fn test_uuidv7_time_ordered() {
        let id1 = uuidv7();
        thread::sleep(Duration::from_millis(2));
        let id2 = uuidv7();
        assert!(id1 < id2, "UUIDv7 must be time-ordered: {} < {}", id1, id2);
    }

    #[test]
    fn test_uuidv7_monotonic() {
        let ids: Vec<String> = (0..100).map(|_| uuidv7_monotonic()).collect();
        for i in 0..ids.len() - 1 {
            assert!(ids[i] < ids[i + 1], "Monotonic violated at {}", i);
        }
    }

    #[test]
    fn test_uuidv7_timestamp_extraction() {
        let ts = current_time_ms();
        let id = uuidv7_with_timestamp(ts);
        let extracted = uuidv7_timestamp(&id).unwrap();
        assert_eq!(extracted, ts, "Timestamp mismatch");
    }

    #[test]
    fn test_uuidv7_uniqueness() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10000 {
            let id = uuidv7();
            assert!(seen.insert(id), "Duplicate UUID generated");
        }
        assert_eq!(seen.len(), 10000);
    }

    #[test]
    fn test_hlc_monotonic() {
        let mut clock = HLC::new();
        let v1 = clock.tick();
        let v2 = clock.tick();
        let v3 = clock.tick();
        assert!(v1 < v2, "HLC must be monotonic: {} < {}", v1, v2);
        assert!(v2 < v3, "HLC must be monotonic: {} < {}", v2, v3);
    }

    #[test]
    fn test_hlc_format() {
        let mut clock = HLC::new();
        let v = clock.tick();
        assert_eq!(v.len(), 48, "HLC is physical + logical + writer id");
        assert!(HLC::is_valid(&v), "HLC must be valid");
        // Legacy 32-char values (written before writer ids) stay valid.
        assert!(HLC::is_valid(&"0".repeat(32)));
    }

    /// The writer id must be the same on every call in one environment, and
    /// different for a different user. Instability here is not a cosmetic
    /// problem: it leaks a head object per run.
    #[test]
    fn test_stable_writer_id_is_stable() {
        let a = stable_writer_id();
        let b = stable_writer_id();
        assert_eq!(a, b, "the same environment must produce the same writer id");
        assert_ne!(a, 0, "a writer id of zero would collide with an unset one");
    }

    /// Shard names must be unique even when generated in a tight loop, where
    /// the clock does not advance between calls.
    ///
    /// Regression: the previous generator was a bare nanosecond timestamp, so
    /// two writers in the same tick produced the same shard name and one
    /// silently overwrote the other's rows.
    #[test]
    fn test_shard_id_unique_within_a_clock_tick() {
        let ids: std::collections::HashSet<String> =
            (0..10_000).map(|_| shard_id()).collect();
        assert_eq!(ids.len(), 10_000, "shard ids must not collide");
    }

    /// Shard ids from concurrent threads are distinct.
    #[test]
    fn test_shard_id_unique_across_threads() {
        let ids = std::sync::Mutex::new(std::collections::HashSet::new());
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    let batch: Vec<String> = (0..1000).map(|_| shard_id()).collect();
                    let mut guard = ids.lock().unwrap();
                    for id in batch {
                        assert!(guard.insert(id), "duplicate shard id across threads");
                    }
                });
            }
        });
        assert_eq!(ids.into_inner().unwrap().len(), 8000);
    }

    /// Two clocks on different nodes must never emit the same version, even
    /// when they tick in the same millisecond with the same logical counter.
    ///
    /// Regression: without a writer id these were byte-identical, so
    /// last-writer-wins had an unbreakable tie and merge order decided the
    /// winner — `merge(A,B) != merge(B,A)`.
    #[test]
    fn test_hlc_distinct_across_writers_in_same_millisecond() {
        let mut a = HLC::with_writer_id(1);
        let mut b = HLC::with_writer_id(2);
        let va = a.tick();
        let vb = b.tick();
        assert_ne!(va, vb, "two writers must not produce identical versions");

        // And the order is total and deterministic: whichever compares
        // greater does so consistently, on every replica.
        assert!(va < vb, "ties break by writer id, lower id sorts first");
    }

    /// A 1000-node burst inside one millisecond yields 1000 distinct versions.
    #[test]
    fn test_hlc_no_collisions_across_many_writers() {
        let mut seen = std::collections::HashSet::new();
        for id in 0..1000u64 {
            let mut c = HLC::with_writer_id(id);
            assert!(seen.insert(c.tick()), "version collision at writer {}", id);
        }
        assert_eq!(seen.len(), 1000);
    }

    /// Ordering still works between legacy 32-char and current 48-char values.
    #[test]
    fn test_hlc_legacy_values_still_order() {
        let mut c = HLC::with_writer_id(7);
        let new = c.tick();
        let legacy_same_instant = &new[..32];
        assert!(
            legacy_same_instant < new.as_str(),
            "a legacy value sorts before a writer-tagged one at the same instant"
        );
        assert!(HLC::decode(&new).is_some());
        assert!(HLC::decode(legacy_same_instant).is_some());
    }

    #[test]
    fn test_hlc_observe() {
        let mut clock1 = HLC::new();
        let mut clock2 = HLC::new();

        let _v1 = clock1.tick();
        thread::sleep(Duration::from_millis(2));
        let v2 = clock2.tick();

        // clock1 observes v2 — should advance
        clock1.observe(&v2);
        let v3 = clock1.tick();
        assert!(v3 > v2, "After observing v2, clock1 should advance past it");
    }
}
