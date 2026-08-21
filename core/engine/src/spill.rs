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
/// The trade is one extra GET on read against `target` times the value size on
/// every write to the same leaf. Below the threshold the write cost is small
/// enough that paying a round trip to avoid it is a bad deal; above it, the
/// write cost dominates by orders of magnitude.
///
/// 1 KiB is chosen so that a full leaf of inline values stays inside the size
/// that one GET should carry: at the default target of 2048 entries, 2048 x
/// 1 KiB is a 2 MiB ceiling on a leaf, while the common case — records of a
/// few hundred bytes — stays inline and costs no extra request at all.
pub const SPILL_THRESHOLD: usize = 1024;

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

#[cfg(test)]
mod tests {
    use super::*;
    use pond_kernel::LocalFSObjectStore;

    fn store_dir() -> (tempfile::TempDir, LocalFSObjectStore) {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalFSObjectStore::new(dir.path()).unwrap();
        (dir, s)
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
