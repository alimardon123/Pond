// spill.rs — keeping large values out of index leaves.
//
// # The problem, measured
//
// Records are stored as the index's values, which is right for small ones and
// is why point lookups cost 2-3 GETs. It stops being right as values grow,
// because an insert rewrites its whole leaf — and a leaf holds `target`
// entries, so the value is paid for `target` times over:
//
//   value size    bytes rewritten by a one-row insert
//   100 B         404 KB      (4046x)
//   1 KB          3.3 MB      (3310x)
//   10 KB         32 MB       (3236x)
//   100 KB        322 MB      (3229x)
//
// (`cargo run -p pond_bench --bin valuesize`.) A 100 KB row costs a third of a
// gigabyte to update. That is the amplification floor a copy-on-write tree
// cannot escape by tuning, and it is the reason the field's advice is to index
// *descriptors* rather than rows.
//
// # The constraint that shapes the fix
//
// The obvious design — pack many values into one large segment object and
// store (segment, offset, length) in the index — **breaks the property the
// whole system rests on.** Which segment a value lands in would depend on what
// else the writer happened to be writing, so two writers holding identical
// data would produce different descriptors, different node bytes, different
// root hashes. Convergence, structural sharing, and deterministic merge all
// die at once.
//
// So the descriptor must be a pure function of the value and nothing else.
// Content addressing already is one: **spill the value to its own blob and
// store its hash.** Two writers with the same value compute the same hash,
// write the same bytes to the same key, and produce byte-identical index
// nodes. Identical values across rows deduplicate for free, which packing
// would have prevented.
//
// The cost is one extra GET to read a spilled value, which is why small values
// stay inline — see [`SPILL_THRESHOLD`].

use pond_kernel::ObjectStore;

/// Values at or above this size are spilled to their own object.
///
/// The trade is one extra request on read against `target` times the value
/// size on every write to the same leaf, plus — the term that turns out to
/// dominate — the whole leaf crossing the network on every *cold* read.
///
/// Measured, not reasoned. `cargo run --release -p pond_bench --bin spillpoint`
/// runs 40 writes and N reads over 2000 rows and prices requests at 30 ms each
/// (a PUT weighted at 12.5x a GET) plus 20 ms per MiB in either direction. Each
/// row spills at its own size, so each answers "should a value of this size
/// spill?" independently of what this constant currently is:
///
/// | value | reads/write | bytes inline -> spill | cold | warm |
/// |---|---|---|---|---|
/// | 1 KiB  | 1   | 180 MiB -> 14 MiB       | 1.2x slower | 1.2x slower |
/// | 1 KiB  | 10  | 440 MiB -> 35 MiB       | 1.2x slower | 1.3x slower |
/// | 1 KiB  | 100 | 4.1 GiB -> 328 MiB      | 1.1x slower | 2.7x slower |
/// | 4 KiB  | 1   | 690 MiB -> 15 MiB       | 1.0x slower | 1.0x slower |
/// | 4 KiB  | 10  | 1.7 GiB -> 37 MiB       | 1.0x faster | 1.2x slower |
/// | 4 KiB  | 100 | 15.4 GiB -> 340 MiB     | 1.3x faster | 2.3x slower |
/// | 16 KiB | 1   | 3.5 GiB -> 16 MiB       | **1.6x faster** | **1.6x faster** |
/// | 16 KiB | 10  | 7.3 GiB -> 42 MiB       | **1.9x faster** | **2.4x faster** |
/// | 16 KiB | 100 | 61.6 GiB -> 387 MiB     | **3.0x faster** | **6.9x faster** |
/// | 64 KiB | 1   | 13.8 GiB -> 22 MiB      | **4.1x faster** | **4.1x faster** |
/// | 64 KiB | 10  | 29.2 GiB -> 65 MiB      | **5.4x faster** | **6.9x faster** |
/// | 64 KiB | 100 | 245.8 GiB -> 579 MiB    | **9.5x faster** | **24.6x faster** |
///
/// 16 KiB is the smallest size that wins in *every* cell. 4 KiB is genuinely
/// mixed — cold favours spilling, warm opposes it — and 1 KiB loses
/// everywhere. A default should not make some workloads much worse to make
/// others slightly better, so the threshold goes where the sign stops
/// changing.
///
/// # Why the crossover is exactly here
///
/// It is not a coincidence, and it is not a property of spilling. Leaves hold
/// between [`min_entries_for`](pond_index::min_entries_for)`(target)` and
/// `target` entries — 512 to 2048 at the default. The cache admits an entry to
/// memory only if it is at most `CacheConfig::max_memory_entry_bytes`, 8 MiB by
/// default. So the smallest leaf a value of size `v` can produce is `512 * v`,
/// and it stops being cacheable when `512 * v > 8 MiB` — that is, when `v`
/// exceeds **16384 bytes**, this constant, exactly.
///
/// Below it, inline leaves fit in cache, a warm reader serves neighbours of
/// anything it has read for free, and spilling gives that up for a per-value
/// miss — which is why warming makes spilling *worse* at 1 and 4 KiB. Above
/// it, no leaf fits, warming cannot help an inline read at all, and spilled
/// values are small enough to cache on their own — which is why warming makes
/// spilling *better* at 16 and 64 KiB, up to 24.6x.
///
/// An earlier version of this comment claimed a warm cache erases the spill
/// read penalty. It does the opposite below the threshold and much better than
/// that above it; the single-sentence version was wrong in both directions.
///
/// The relationship is asserted by `the_threshold_is_where_leaves_stop_fitting_the_cache`,
/// because it spans three crates: change the chunk target, the minimum leaf
/// fraction, or the cache's entry cap, and this number stops being the right
/// one.
///
/// This is a default, not a law. The value is recorded per collection in its
/// definition, so existing collections keep whatever they were created with,
/// and a lens that knows its own access pattern sets its own — the streaming
/// lens does.
pub const SPILL_THRESHOLD: usize = 16384;

/// Marks an index value as a pointer rather than a record.
///
/// Distinct from the record magic (`PREC`), so a reader can tell them apart by
/// looking at the first four bytes and no version bump is needed for values
/// that stay inline.
const SPILL_MAGIC: &[u8; 4] = b"PSPL";

/// A spilled value: `PSPL` + the hex hash of the bytes.
fn encode_pointer(hash: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + hash.len());
    out.extend_from_slice(SPILL_MAGIC);
    out.extend_from_slice(hash.as_bytes());
    out
}

/// The hash a spill pointer names, or `None` if these are not pointer bytes.
pub fn pointer_target(value: &[u8]) -> Option<&str> {
    if value.len() <= 4 || &value[..4] != SPILL_MAGIC {
        return None;
    }
    std::str::from_utf8(&value[4..]).ok()
}

/// Is this value a spill pointer?
pub fn is_pointer(value: &[u8]) -> bool {
    value.len() > 4 && &value[..4] == SPILL_MAGIC
}

/// Store `value` so it can be retrieved by [`resolve`], spilling it if it is
/// large enough to be worth a round trip.
///
/// Returns what should be written into the index.
pub fn store<S: ObjectStore>(
    backend: &S,
    value: Vec<u8>,
    threshold: usize,
) -> std::io::Result<Vec<u8>> {
    if value.len() < threshold {
        return Ok(value);
    }
    let hash = backend.put_blob(&value)?;
    Ok(encode_pointer(&hash))
}

/// Store many values, spilling the large ones in a single batched write.
///
/// The spills are independent of each other, so there is no reason to pay a
/// round trip each: on S3 the batch goes out 32-wide.
pub fn store_batch<S: ObjectStore>(
    backend: &S,
    values: Vec<Vec<u8>>,
    threshold: usize,
) -> std::io::Result<Vec<Vec<u8>>> {
    let spill_at: Vec<usize> = values
        .iter()
        .enumerate()
        .filter(|(_, v)| v.len() >= threshold)
        .map(|(i, _)| i)
        .collect();

    if spill_at.is_empty() {
        return Ok(values);
    }

    let payloads: Vec<Vec<u8>> = spill_at.iter().map(|i| values[*i].clone()).collect();
    let hashes = backend.put_blob_batch(&payloads)?;

    let mut out = values;
    for (slot, hash) in spill_at.into_iter().zip(hashes) {
        out[slot] = encode_pointer(&hash);
    }
    Ok(out)
}

/// Read back a value written by [`store`], following a pointer if there is one.
pub fn resolve<S: ObjectStore>(backend: &S, value: Vec<u8>) -> std::io::Result<Vec<u8>> {
    match pointer_target(&value) {
        Some(hash) => backend.get_blob(hash),
        None => Ok(value),
    }
}

/// Resolve many values, fetching the spilled ones in one batch.
pub fn resolve_batch<S: ObjectStore>(
    backend: &S,
    values: Vec<Vec<u8>>,
) -> std::io::Result<Vec<Vec<u8>>> {
    let fetch_at: Vec<usize> = values
        .iter()
        .enumerate()
        .filter(|(_, v)| is_pointer(v))
        .map(|(i, _)| i)
        .collect();

    if fetch_at.is_empty() {
        return Ok(values);
    }

    let hashes: Vec<String> = fetch_at
        .iter()
        .map(|i| pointer_target(&values[*i]).unwrap_or_default().to_string())
        .collect();
    let bodies = backend.get_blob_batch(&hashes)?;

    let mut out = values;
    for (slot, body) in fetch_at.into_iter().zip(bodies) {
        out[slot] = body;
    }
    Ok(out)
}

/// Replace every field large enough to be worth it with a pointer, across a
/// batch of records, in one write.
///
/// # Why per field rather than per record
///
/// Spilling a whole record makes a row with one large attachment expensive in
/// both directions, and the sizes are not marginal. Measured over 200 rows
/// with a 256 KiB attachment beside two small columns
/// (`pond_bench --bin fieldspill`):
///
/// | attachment | bytes to change one small field | bytes to scan the small fields |
/// |---|---|---|
/// | 16 KiB  | 32.8 KiB  | 3.2 MiB  |
/// | 64 KiB  | 80.8 KiB  | 12.5 MiB |
/// | 256 KiB | 272.8 KiB | 50.0 MiB |
///
/// The small fields being asked for weigh 2.1 KiB in every row of that table.
/// Editing `status` cost the whole attachment because the record is re-encoded
/// and re-spilled whole; scanning cost every attachment because they are
/// inside the records.
///
/// Per field, a large payload becomes a pointer that a merge carries through
/// untouched, so editing a neighbour rewrites bytes proportional to that
/// neighbour — and a reader can decline to resolve what it did not ask for.
///
/// # Why the batch is across records
///
/// The spills are independent, so there is no reason to pay a round trip per
/// field or per row. One batch covers every large field in the write, which is
/// what keeps a bulk load bounded by bandwidth rather than by latency.
pub fn spill_fields<S: ObjectStore>(
    backend: &S,
    records: &mut [pond_record::Record],
    threshold: usize,
) -> std::io::Result<()> {
    // Where each payload came from, so the hashes can be put back in place.
    let mut sites: Vec<(usize, String)> = Vec::new();
    let mut payloads: Vec<Vec<u8>> = Vec::new();

    for (i, rec) in records.iter().enumerate() {
        for (name, field) in &rec.fields {
            if field.value.is_spilled() {
                // Already a pointer — carried through from a previous write,
                // which is the whole point.
                continue;
            }
            if pond_record::encode::payload_len(&field.value) < threshold {
                continue;
            }
            let (_, bytes) = pond_record::encode::spill_payload(&field.value);
            sites.push((i, name.clone()));
            payloads.push(bytes);
        }
    }

    if payloads.is_empty() {
        return Ok(());
    }

    let hashes = backend.put_blob_batch(&payloads)?;
    for ((i, name), hash) in sites.into_iter().zip(hashes) {
        if let Some(field) = records[i].fields.get_mut(&name) {
            let type_tag = pond_record::encode::type_tag_of(&field.value);
            field.value = pond_record::Value::Spilled { type_tag, hash };
        }
    }
    Ok(())
}

/// Fetch the payloads behind a record's pointers, in one batch.
///
/// `wanted` names the fields to resolve; `None` resolves all of them. A field
/// left unresolved is *removed* rather than handed back as a placeholder, so a
/// `Value::Spilled` never escapes into code that would have to know what one
/// is — the reason projection is worth anything is that it does not fetch, and
/// the reason it is safe is that it does not lie about what it returned.
pub fn resolve_fields<S: ObjectStore>(
    backend: &S,
    records: &mut [pond_record::Record],
    wanted: Option<&[&str]>,
) -> std::io::Result<()> {
    let mut sites: Vec<(usize, String)> = Vec::new();
    let mut hashes: Vec<String> = Vec::new();
    let mut drop_at: Vec<(usize, String)> = Vec::new();

    for (i, rec) in records.iter().enumerate() {
        for (name, field) in &rec.fields {
            let pond_record::Value::Spilled { hash, .. } = &field.value else {
                continue;
            };
            let asked_for = wanted.is_none_or(|w| w.contains(&name.as_str()));
            if asked_for {
                sites.push((i, name.clone()));
                hashes.push(hash.clone());
            } else {
                drop_at.push((i, name.clone()));
            }
        }
    }

    for (i, name) in drop_at {
        records[i].fields.remove(&name);
    }

    if hashes.is_empty() {
        return Ok(());
    }

    let bodies = backend.get_blob_batch(&hashes)?;
    if bodies.len() != sites.len() {
        return Err(std::io::Error::other(format!(
            "backend returned {} payloads for {} spilled fields",
            bodies.len(),
            sites.len()
        )));
    }
    for ((i, name), body) in sites.into_iter().zip(bodies) {
        let value = pond_record::encode::unspill_payload(&body).ok_or_else(|| {
            std::io::Error::other(format!("spilled field '{}' did not decode", name))
        })?;
        if let Some(field) = records[i].fields.get_mut(&name) {
            field.value = value;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pond_kernel::LocalFSObjectStore;

    fn store_dir() -> (tempfile::TempDir, LocalFSObjectStore) {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalFSObjectStore::new(dir.path()).unwrap();
        (dir, s)
    }

    /// The threshold is not a tuned magic number — it is where a leaf stops
    /// fitting in the cache, and that is what the measurement found.
    ///
    /// The smallest leaf a value of size `v` can produce is
    /// `min_entries_for(target) * v`. The cache admits an entry to memory only
    /// up to `max_memory_entry_bytes`. Where those cross is where warming stops
    /// being able to help an inline read at all — and the spillpoint table
    /// changes sign at exactly that value.
    ///
    /// This spans three crates, so nothing else would notice it breaking. If
    /// the chunk target, the minimum-leaf fraction, or the cache's entry cap
    /// moves, this fails and the comment above `SPILL_THRESHOLD` needs redoing
    /// along with the constant.
    #[test]
    fn the_threshold_is_where_leaves_stop_fitting_the_cache() {
        let min_entries = pond_index::min_entries_for(pond_index::DEFAULT_TARGET_ENTRIES);
        let cap = pond_cache::CacheConfig::default().max_memory_entry_bytes as usize;

        assert_eq!(
            cap / min_entries,
            SPILL_THRESHOLD,
            "the smallest cacheable leaf holds {} entries and the cache admits \
             {} bytes, so leaves stop fitting above {} bytes per value — but \
             SPILL_THRESHOLD is {}. One of the three has moved; re-run \
             `pond_bench --bin spillpoint` and redo the table in the doc \
             comment rather than adjusting this assertion.",
            min_entries,
            cap,
            cap / min_entries,
            SPILL_THRESHOLD
        );

        // And the direction that matters: a value at the threshold produces a
        // smallest-leaf that is exactly at the cap, so anything larger cannot
        // be cached inline.
        assert!(
            min_entries * SPILL_THRESHOLD <= cap,
            "a value at the threshold must still be able to sit in a cacheable leaf"
        );
        assert!(
            min_entries * (SPILL_THRESHOLD + 1) > cap,
            "and anything above it must not"
        );
    }

    #[test]
    fn small_values_stay_inline() {
        let (_d, s) = store_dir();
        let small = vec![b'a'; SPILL_THRESHOLD - 1];
        let stored = store(&s, small.clone(), SPILL_THRESHOLD).unwrap();
        assert_eq!(stored, small, "a small value must not become a pointer");
        assert!(!is_pointer(&stored));
        assert_eq!(resolve(&s, stored).unwrap(), small);
    }

    #[test]
    fn large_values_spill_and_come_back() {
        let (_d, s) = store_dir();
        let big = vec![b'b'; SPILL_THRESHOLD * 4];
        let stored = store(&s, big.clone(), SPILL_THRESHOLD).unwrap();
        assert!(is_pointer(&stored));
        assert!(
            stored.len() < 128,
            "the index should hold a pointer, not {} bytes",
            stored.len()
        );
        assert_eq!(resolve(&s, stored).unwrap(), big);
    }

    /// The property everything depends on: the same value always produces the
    /// same index bytes, whoever writes it and whatever else they are writing.
    ///
    /// A design that packed values into shared segments would fail this, and
    /// with it convergence, structural sharing, and deterministic merge.
    #[test]
    fn the_pointer_is_a_pure_function_of_the_value() {
        let (_d1, s1) = store_dir();
        let (_d2, s2) = store_dir();
        let value = vec![b'c'; SPILL_THRESHOLD * 2];

        // Two separate stores, different surrounding writes.
        let _ = store(&s1, vec![b'z'; SPILL_THRESHOLD * 9], SPILL_THRESHOLD).unwrap();
        let a = store(&s1, value.clone(), SPILL_THRESHOLD).unwrap();
        let b = store(&s2, value.clone(), SPILL_THRESHOLD).unwrap();

        assert_eq!(a, b, "the same value must yield byte-identical index bytes");
    }

    /// Identical values share one object — packing would have prevented this.
    #[test]
    fn identical_values_deduplicate() {
        let (_d, s) = store_dir();
        let value = vec![b'd'; SPILL_THRESHOLD * 2];
        let a = store(&s, value.clone(), SPILL_THRESHOLD).unwrap();
        let b = store(&s, value.clone(), SPILL_THRESHOLD).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn batch_matches_one_at_a_time() {
        let (_d, s) = store_dir();
        let values = vec![
            vec![b'a'; 10],
            vec![b'b'; SPILL_THRESHOLD * 2],
            vec![b'c'; 20],
            vec![b'd'; SPILL_THRESHOLD * 3],
        ];
        let batched = store_batch(&s, values.clone(), SPILL_THRESHOLD).unwrap();
        let individually: Vec<Vec<u8>> = values
            .iter()
            .map(|v| store(&s, v.clone(), SPILL_THRESHOLD).unwrap())
            .collect();
        assert_eq!(batched, individually);

        let resolved = resolve_batch(&s, batched).unwrap();
        assert_eq!(resolved, values, "a batch round trip must change nothing");
    }

    /// Ordinary record bytes must never be mistaken for a pointer.
    #[test]
    fn record_bytes_are_not_pointers() {
        let rec = pond_record::encode_record(
            &pond_record::Record::new().with_field(
                "x",
                pond_record::Value::Int(1),
                pond_record::Version::new(1, 0, 1),
            ),
        );
        assert!(!is_pointer(&rec));
        assert!(pointer_target(&rec).is_none());
        // And degenerate inputs do not panic.
        assert!(!is_pointer(b""));
        assert!(!is_pointer(b"PSPL"));
        assert!(pointer_target(b"PSPL").is_none());
    }
}
