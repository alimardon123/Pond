// engine_stream.rs — a stream as an ordered index, not one rewritten blob.
//
// # Why this exists
//
// The original stream is a JSON array of segments stored as a single object.
// Appending means reading every segment written so far, adding one, and
// writing all of them back. Measured, over 40 appends the payload grew from 14
// bytes to 521 — so appending N segments costs O(N²) bytes.
//
// For a *streaming* lens that is the worst characteristic available: appending
// is the only thing a stream does, and it gets slower with every append.
//
// # The shape that fixes it
//
// A stream is already what the index is best at: keys that only ever increase.
// Each segment becomes a record keyed by its start offset, so:
//
//   * appending touches the right-most leaf and its ancestors — `O(log n)`
//     nodes, independent of how much is already stored, and it skips the
//     read-merge entirely because nothing can already be there;
//   * reading `[start, end)` scans only the keys in that range instead of
//     every segment ever written.
//
// # Finding the segment that contains `start`
//
// A segment overlapping `start` may begin before it, and the index can seek to
// a key but not to "the greatest key below one". Bounding the segment size
// solves it: a segment overlapping `start` cannot begin earlier than
// `start - MAX_SEGMENT_BYTES`, so the scan starts there and reads at most one
// segment it does not need.
//
// # Total size
//
// Kept in a metadata record at key -1, which sorts before every offset because
// the key encoding is order-preserving over signed integers. Reading it is one
// point lookup rather than a scan to the end.

use pond_kernel::PondKernel;
use pond_storage::definition::{self, Format};
use pond_storage::engine_path;

/// Largest segment this lens will write.
///
/// It bounds how far back a range read has to look for the segment containing
/// its start, so it is a correctness parameter and not only a tuning one.
/// 8 MiB matches the object size that independent analyses of object-storage
/// economics converge on.
pub const MAX_SEGMENT_BYTES: usize = 8 * 1024 * 1024;

/// Key of the record holding the stream's total size.
///
/// Negative so it sorts before every offset — the key encoding preserves the
/// order of signed integers, so this is a property of the encoding rather than
/// a convention that could drift.
const SIZE_KEY: i64 = -1;

const COL_OFFSET: &str = "offset";
const COL_DATA: &str = "data";
const COL_TOTAL: &str = "total";

/// Is this collection an engine-backed stream?
pub fn is_engine_stream(kernel: &PondKernel, collection: &str) -> bool {
    definition::format_of(kernel, collection) == Format::Engine
}

/// Create an engine-backed stream.
pub fn create(kernel: &PondKernel, collection: &str) -> Result<(), String> {
    engine_path::create(kernel, collection)
}

/// Total bytes in the stream. One point lookup, not a scan.
pub fn size(kernel: &PondKernel, collection: &str) -> Result<usize, String> {
    let rows = engine_path::read_range(kernel, collection, SIZE_KEY, SIZE_KEY + 1)?;
    Ok(rows
        .iter()
        .find_map(|r| match r.get(COL_TOTAL) {
            Some(pond_record::Value::Int(v)) => Some(*v),
            _ => None,
        })
        .unwrap_or(0)
        .max(0) as usize)
}

/// Append bytes to the end of the stream.
///
/// Skips the read-merge that an update needs: the keys are new by
/// construction, because they start where the stream currently ends.
pub fn append(
    kernel: &PondKernel,
    collection: &str,
    data: &[u8],
    segment_size: usize,
    writer_id: u64,
) -> Result<usize, String> {
    if data.is_empty() {
        return size(kernel, collection);
    }
    let seg = segment_size.clamp(1, MAX_SEGMENT_BYTES);
    let start = size(kernel, collection)?;

    let mut offsets: Vec<i64> = Vec::new();
    let mut blobs: Vec<Vec<u8>> = Vec::new();
    let mut at = start;
    for chunk in data.chunks(seg) {
        offsets.push(at as i64);
        blobs.push(chunk.to_vec());
        at += chunk.len();
    }

    engine_path::append_binary_rows(kernel, collection, COL_OFFSET, &offsets, COL_DATA, &blobs, writer_id)?;

    // Record the new total. A separate write, because it is an update rather
    // than an append — it replaces the previous total. Written at an explicit
    // key: `write_rows` would invent a row id and the slot would be
    // unfindable.
    engine_path::put_int_keyed_row(
        kernel,
        collection,
        SIZE_KEY,
        &[
            (COL_OFFSET, pond_record::Value::Int(SIZE_KEY)),
            (COL_TOTAL, pond_record::Value::Int(at as i64)),
        ],
        writer_id,
    )?;
    Ok(at)
}

/// Read `[start, end)` from the stream.
pub fn read(
    kernel: &PondKernel,
    collection: &str,
    start: usize,
    end: Option<usize>,
) -> Result<Vec<u8>, String> {
    let total = size(kernel, collection)?;
    let end = end.unwrap_or(total).min(total);
    if start >= end {
        return Ok(Vec::new());
    }

    // A segment overlapping `start` cannot begin earlier than this.
    let scan_from = start.saturating_sub(MAX_SEGMENT_BYTES) as i64;
    let rows = engine_path::read_range(kernel, collection, scan_from, end as i64)?;

    let mut segments: Vec<(usize, Vec<u8>)> = rows
        .iter()
        .filter_map(|r| {
            let offset = match r.get(COL_OFFSET) {
                Some(pond_record::Value::Int(v)) if *v >= 0 => *v,
                _ => return None, // absent, or the size record
            };
            let bytes = match r.get(COL_DATA) {
                Some(pond_record::Value::Bytes(b)) => b.clone(),
                _ => return None,
            };
            Some((offset as usize, bytes))
        })
        .collect();
    segments.sort_by_key(|(o, _)| *o);

    let mut out = Vec::with_capacity(end - start);
    for (offset, bytes) in segments {
        let seg_end = offset + bytes.len();
        if seg_end <= start || offset >= end {
            continue;
        }
        let from = start.saturating_sub(offset);
        let to = if end < seg_end { end - offset } else { bytes.len() };
        if from < to {
            out.extend_from_slice(&bytes[from..to]);
        }
    }
    Ok(out)
}

/// How many segments the stream holds.
pub fn segment_count(kernel: &PondKernel, collection: &str) -> Result<usize, String> {
    let total = size(kernel, collection)? as i64;
    let rows = engine_path::read_range(kernel, collection, 0, total.max(1))?;
    Ok(rows.len())
}
