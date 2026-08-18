// r2_pagination.rs — prove ListObjectsV2 pagination and bulk delete against
// real R2.
//
// A bucket with more than 1000 objects is the first place SigV4 signing and
// pagination interact: the `NextContinuationToken` is base64, so it contains
// characters that must be percent-encoded, and the encoding has to be
// identical in the signature and on the wire. It was not — the caller encoded
// the token and the signer encoded it again — so every list past the first
// page failed with `SignatureDoesNotMatch`. That is invisible on any bucket
// small enough to fit in one page, which is every test bucket until it isn't.
//
// The same run measures bulk delete, which is the other half of operating at
// this scale: reclamation is the one operation whose size scales with the data
// rather than with the change.
//
//   set -a && . .env && set +a
//   cargo run --release -p pond_bench --bin r2_pagination
//   cargo run --release -p pond_bench --bin r2_pagination -- --purge

use std::env;
use std::time::Instant;

use pond_kernel::ObjectStore;

/// Enough objects to force at least two pages (S3 caps a page at 1000).
const OBJECTS: usize = 1_200;

/// Every two-hex-character blob shard.
fn all_shards() -> Vec<String> {
    (0u16..256).map(|i| format!("{:02x}", i)).collect()
}

fn main() {
    let url = match env::var("POND_R2_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("POND_R2_URL not set — skipping (this needs real object storage).");
            return;
        }
    };
    let store = pond_s3::S3ObjectStore::from_url(&url).expect("build S3 store from POND_R2_URL");

    if env::args().any(|a| a == "--purge") {
        purge(&store);
        return;
    }

    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    println!("writing {} blobs to cross the 1000-object page boundary…", OBJECTS);
    let t = Instant::now();
    let payloads: Vec<Vec<u8>> = (0..OBJECTS)
        .map(|i| format!("pagination-probe-{}-{}", run, i).into_bytes())
        .collect();
    let hashes = store
        .put_blob_batch(&payloads)
        .expect("batch write must succeed");
    println!("  wrote {} blobs in {:.1}s", hashes.len(), t.elapsed().as_secs_f64());

    // The listing under test. Before the fix this failed with
    // SignatureDoesNotMatch the moment a second page was needed.
    let t = Instant::now();
    let result = store.list_paths("");
    match &result {
        Ok(paths) => println!(
            "list_paths(\"\") succeeded across >{} objects in {:.1}s — {} refs",
            OBJECTS,
            t.elapsed().as_secs_f64(),
            paths.len()
        ),
        Err(e) => println!("list_paths(\"\") FAILED: {}", e),
    }

    // Bulk delete: 1200 keys in two requests instead of 1200.
    let t = Instant::now();
    let removed = store
        .delete_blob_batch(&hashes)
        .expect("bulk delete must succeed");
    let elapsed = t.elapsed().as_secs_f64();
    let requests = hashes.len().div_ceil(pond_s3::DELETE_BATCH_LIMIT);
    println!(
        "delete_blob_batch removed {}/{} blobs in {:.1}s using {} request(s) \
         — {:.0}x fewer round trips than one DELETE per object",
        removed,
        hashes.len(),
        elapsed,
        requests,
        hashes.len() as f64 / requests as f64
    );

    let paths = result.expect("pagination must not fail");
    assert!(
        paths.iter().all(|p| !p.contains("/blobs/")),
        "blob keys must be filtered out of ref listings"
    );
    assert_eq!(removed, hashes.len(), "every written blob must be removed");
    println!("OK");
}

/// Remove everything this harness could have left behind: refs, and blobs in
/// every shard. Used to reset the bucket after an interrupted run.
fn purge(store: &pond_s3::S3ObjectStore) {
    let paths = store.list_paths("").unwrap_or_default();
    for p in &paths {
        let _ = store.delete_path(p);
    }
    println!("purged {} refs", paths.len());

    let mut hashes = Vec::new();
    for shard in all_shards() {
        // Goes through `list_paths("blobs/<shard>/")` — the same call
        // `PondKernel::list_blobs_prefix` makes, which returned nothing on S3
        // whenever a store prefix was configured.
        hashes.extend(
            store
                .list_paths(&format!("blobs/{}/", shard))
                .unwrap_or_default()
                .into_iter()
                .filter_map(|p| p.split('/').nth(2).map(|h| h.to_string())),
        );
    }
    println!("found {} blobs", hashes.len());
    if !hashes.is_empty() {
        let removed = store.delete_blob_batch(&hashes).expect("bulk delete");
        println!("purged {} blobs", removed);
    }
}
