// r2_integration.rs — exercise the S3 client against a real endpoint.
//
// These tests need real credentials and are skipped without them, so CI and
// other contributors are unaffected. Run with:
//
//     set -a && . .env && set +a
//     cargo test -p pond_s3 --test r2_integration -- --ignored --test-threads=1
//
// They exist because several code paths cannot be meaningfully covered by unit
// tests, and some of them had never executed anywhere before this file:
//
//   - multipart upload (create / part / complete / abort) — only its part
//     layout was unit-tested; the wire protocol never ran;
//   - ranged GET against a real `Range:` header, including the 416 that gets
//     translated to an empty result;
//   - SigV4 against a non-AWS endpoint with `region=auto`;
//   - the CA-bundle and proxy handling in `build_agent`.
//
// Every test writes under a unique prefix and deletes what it wrote.

use pond_kernel::ObjectStore;
use pond_s3::S3ObjectStore;

/// Build a store rooted at a unique prefix, or return None if unconfigured.
///
/// Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, the
/// location from POND_R2_URL — never from source. A unique per-run prefix
/// keeps concurrent runs and leftover state from interfering.
fn store() -> Option<(S3ObjectStore, String)> {
    let base = std::env::var("POND_R2_URL").ok()?;
    if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        return None;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let run = format!("itest-{:x}", nanos);

    // Insert the run id into the URL's path portion, before the query string.
    let url = match base.split_once('?') {
        Some((path, query)) => format!("{}/{}?{}", path.trim_end_matches('/'), run, query),
        None => format!("{}/{}", base.trim_end_matches('/'), run),
    };
    let s = S3ObjectStore::from_url(&url).expect("from_url should parse POND_R2_URL");
    Some((s, run))
}

macro_rules! require_r2 {
    () => {
        match store() {
            Some(v) => v,
            None => {
                eprintln!("skipping: POND_R2_URL / AWS_ACCESS_KEY_ID not set");
                return;
            }
        }
    };
}

#[test]
#[ignore = "requires real R2 credentials"]
fn blob_roundtrip_and_delete() {
    let (s, _run) = require_r2!();
    let payload = b"pond r2 round trip".to_vec();

    let h = s.put_blob(&payload).expect("put_blob");
    assert_eq!(s.get_blob(&h).expect("get_blob"), payload);
    assert!(s.blob_exists(&h));

    assert!(s.delete_blob(&h).expect("delete_blob"));
    assert_eq!(
        s.get_blob(&h).unwrap_err().kind(),
        std::io::ErrorKind::NotFound,
        "a deleted blob must report NotFound, not a generic failure"
    );
}

/// Ranged GET must behave identically to the local backend, including the
/// edges — that uniformity is the whole point of the four-operation contract.
#[test]
#[ignore = "requires real R2 credentials"]
fn ranged_get_matches_local_semantics() {
    let (s, _run) = require_r2!();
    let data: Vec<u8> = (0..=255u8).cycle().take(10_000).collect();
    let h = s.put_blob(&data).expect("put_blob");

    for (off, len) in [(0u64, 1usize), (0, 10_000), (1, 100), (4096, 4096), (9_999, 1)] {
        let got = s.get_blob_range(&h, off, len).expect("range");
        let want = &data[off as usize..(off as usize + len).min(data.len())];
        assert_eq!(got, want, "range({}, {}) mismatch", off, len);
    }

    // Straddling the end truncates rather than erroring.
    assert_eq!(s.get_blob_range(&h, 9_998, 100).expect("straddle").len(), 2);
    // Starting past the end is empty, not a 416 surfaced to the caller.
    assert!(s
        .get_blob_range(&h, 20_000, 10)
        .expect("past end must not error")
        .is_empty());
    // Zero length never issues a request at all.
    assert!(s.get_blob_range(&h, 0, 0).expect("zero len").is_empty());

    s.delete_blob(&h).ok();
}

/// The one that had never run: a real multipart upload.
///
/// Above MULTIPART_THRESHOLD `put_blob` switches to create/upload/complete.
/// This verifies the whole wire protocol — signed POST with query strings,
/// per-part PUTs, ETag collection, and the completion XML — and that the
/// result reads back byte-identical.
#[test]
#[ignore = "requires real R2 credentials — uploads ~120 MB"]
fn multipart_upload_roundtrip() {
    let (s, _run) = require_r2!();

    // Just over the threshold, so several parts plus a remainder.
    let size = pond_s3::MULTIPART_THRESHOLD + 20 * 1024 * 1024;
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

    let h = s.put_blob(&data).expect("multipart put_blob");
    let got = s.get_blob(&h).expect("get_blob after multipart");
    assert_eq!(got.len(), data.len(), "multipart object size mismatch");
    assert_eq!(got, data, "multipart object content mismatch");

    // A ranged read into the middle of a multipart object must work too —
    // part boundaries are invisible to readers.
    let mid = pond_s3::MULTIPART_PART_SIZE as u64 + 1234;
    let slice = s.get_blob_range(&h, mid, 64).expect("range into multipart");
    assert_eq!(slice, &data[mid as usize..mid as usize + 64]);

    assert!(s.delete_blob(&h).expect("delete multipart object"));
}

/// Refs are the mutable half of the store; last-writer-wins with no CAS.
#[test]
#[ignore = "requires real R2 credentials"]
fn ref_put_get_and_overwrite() {
    let (s, _run) = require_r2!();
    let h1 = s.put_blob(b"first").expect("put");
    let h2 = s.put_blob(b"second").expect("put");

    const REF: &str = "collections/itest/_branches/main/commit";
    s.put_path(REF, &h1).expect("put_path");
    assert_eq!(s.get_path(REF).as_deref(), Some(h1.as_str()));

    s.put_path(REF, &h2).expect("overwrite");
    assert_eq!(s.get_path(REF).as_deref(), Some(h2.as_str()));

    assert!(s.delete_path(REF).expect("delete_path"));
    assert!(s.get_path(REF).is_none());
    s.delete_blob(&h1).ok();
    s.delete_blob(&h2).ok();
}

/// Listing is used for discovery and recovery, never on the hot path — but it
/// still has to be correct, and R2's pagination differs from AWS's.
#[test]
#[ignore = "requires real R2 credentials"]
fn list_paths_finds_written_refs() {
    let (s, _run) = require_r2!();
    let h = s.put_blob(b"listed").expect("put");
    for i in 0..5 {
        s.put_path(&format!("collections/itest/_branches/b{}/commit", i), &h)
            .expect("put_path");
    }
    let found = s.list_paths("collections/itest/").expect("list_paths");
    assert!(
        found.len() >= 5,
        "expected at least 5 refs, found {}: {:?}",
        found.len(),
        found
    );
    for i in 0..5 {
        s.delete_path(&format!("collections/itest/_branches/b{}/commit", i))
            .ok();
    }
    s.delete_blob(&h).ok();
}

/// Content addressing means writing the same bytes twice is idempotent —
/// the second write must not corrupt or duplicate anything.
#[test]
#[ignore = "requires real R2 credentials"]
fn duplicate_writes_are_idempotent() {
    let (s, _run) = require_r2!();
    let payload = b"written twice".to_vec();
    let h1 = s.put_blob(&payload).expect("first put");
    let h2 = s.put_blob(&payload).expect("second put");
    assert_eq!(h1, h2, "same bytes must produce the same address");
    assert_eq!(s.get_blob(&h1).expect("get"), payload);
    s.delete_blob(&h1).ok();
}

/// The same listing contract the local backend is held to, against real S3.
///
/// This is the point of having the check in the kernel rather than in one
/// backend's tests. `list_paths` is specified as a prefix listing; the local
/// backend used to implement a directory listing, and the two only agreed
/// because every caller happened to pass a prefix ending in `/`. A contract
/// that only one backend is checked against is a convention.
#[test]
#[ignore = "requires real R2 credentials"]
fn list_paths_is_a_prefix_listing_on_real_object_storage() {
    // The helper deletes every key it writes, which is the convention the
    // other tests here follow, and the run prefix is unique per run anyway.
    let (s, _run) = require_r2!();
    pond_kernel::assert_list_paths_is_a_prefix_listing(&s, "conformance");
}
