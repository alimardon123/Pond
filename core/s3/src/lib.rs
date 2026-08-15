// S3ObjectStore — S3-compatible content-addressed object store.
//
// Implements pond_kernel::ObjectStore using any S3-compatible API:
//   - AWS S3
//   - Cloudflare R2
//   - MinIO
//   - LocalStack
//   - Wasabi
//   - DigitalOcean Spaces
//   - Any S3-compatible API
//
// PATH LAYOUT (same as Python S3ObjectStore and LocalFSObjectStore):
//   blobs/{hash[:2]}/{hash}        — content-addressed blobs
//   {path}                          — named refs (JSON: {"hash":"..."})
//
// SIGNING: AWS Signature Version 4 (SigV4) — implemented from scratch using
// only sha2 + hmac (HMAC-SHA256 is just sha2 + a wrapper). No AWS SDK
// dependency, no async runtime. Synchronous HTTP via ureq.
//
// USAGE:
//   use pond_s3::S3ObjectStore;
//   use pond_kernel::{PondKernel, ObjectStore};
//
//   let store = S3ObjectStore::new(
//       "my-bucket", "prod", // bucket, prefix
//       "us-east-1",          // region
//       "https://s3.amazonaws.com", // endpoint
//       Some("ACCESS_KEY"),   // access key (or None for env/IMDS)
//       Some("SECRET_KEY"),   // secret key
//   );
//   let kernel = PondKernel::new_with_store(Box::new(store));

use std::io::{self, Read};
use std::sync::Mutex;

use pond_kernel::ObjectStore;
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// HMAC-SHA256 (implemented on top of sha2 — no separate hmac crate needed)
// ---------------------------------------------------------------------------

const SHA256_BLOCK_SIZE: usize = 64;

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut key_block = if key.len() > SHA256_BLOCK_SIZE {
        // Long key: hash first
        let mut h = Sha256::new();
        h.update(key);
        let digest = h.finalize();
        let mut k = vec![0u8; SHA256_BLOCK_SIZE];
        k[..32].copy_from_slice(&digest);
        k
    } else {
        let mut k = vec![0u8; SHA256_BLOCK_SIZE];
        k[..key.len()].copy_from_slice(key);
        k
    };

    let mut ipad = [0u8; SHA256_BLOCK_SIZE];
    let mut opad = [0u8; SHA256_BLOCK_SIZE];
    for i in 0..SHA256_BLOCK_SIZE {
        ipad[i] = key_block[i] ^ 0x36;
        opad[i] = key_block[i] ^ 0x5c;
    }
    // Zero out key_block to avoid leaving key material in memory
    for b in key_block.iter_mut() { *b = 0; }

    // inner = SHA256(ipad || message)
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(message);
    let inner_digest = inner.finalize();

    // outer = SHA256(opad || inner)
    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_digest);
    let outer_digest = outer.finalize();

    let mut out = [0u8; 32];
    out.copy_from_slice(&outer_digest);
    out
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    let digest = h.finalize();
    hex::encode(digest)
}

// ---------------------------------------------------------------------------
// AWS SigV4 signing
// ---------------------------------------------------------------------------

/// AWS credentials for SigV4 signing.
#[derive(Clone)]
pub struct S3Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

impl S3Credentials {
    /// Read credentials from environment variables:
    /// - AWS_ACCESS_KEY_ID (or AWS_ACCESS_KEY)
    /// - AWS_SECRET_ACCESS_KEY (or AWS_SECRET_KEY)
    /// - AWS_SESSION_TOKEN (optional)
    pub fn from_env() -> Option<Self> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("AWS_ACCESS_KEY"))
            .ok()?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("AWS_SECRET_KEY"))
            .ok()?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
        Some(Self { access_key, secret_key, session_token })
    }
}

/// Generate the current timestamp in SigV4 format:
/// - header value: "20260101T120000Z" (YYYYMMDDTHHMMSSZ)
/// - date stamp:   "20260101" (YYYYMMDD)
fn sigv4_timestamp() -> (String, String) {
    use chrono::{Utc, Datelike, Timelike};
    let now = Utc::now();
    let header = format!("{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(), now.month(), now.day(),
        now.hour(), now.minute(), now.second());
    let date_stamp = format!("{:04}{:02}{:02}", now.year(), now.month(), now.day());
    (header, date_stamp)
}

/// Compute the SigV4 signing key for a given date/region/service.
/// Returns 32 bytes.
fn sigv4_signing_key(secret_key: &str, date_stamp: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{}", secret_key).as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Compute the SigV4 signature for a string-to-sign, given the signing key.
fn sigv4_sign(signing_key: &[u8; 32], string_to_sign: &str) -> String {
    let sig = hmac_sha256(signing_key, string_to_sign.as_bytes());
    hex::encode(sig)
}

/// Build the canonical request string for SigV4.
fn build_canonical_request(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    headers: &[(String, String)],
    payload_hash: &str,
) -> String {
    let canonical_headers: String = headers.iter()
        .map(|(k, v)| format!("{}:{}\n", k.to_lowercase(), v.trim()))
        .collect();
    let signed_headers: String = headers.iter()
        .map(|(k, _)| k.to_lowercase())
        .collect::<Vec<_>>()
        .join(";");

    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        canonical_uri,
        canonical_query,
        canonical_headers,
        signed_headers,
        payload_hash
    )
}

/// Canonicalize a query string for SigV4.
///
/// Per AWS spec, the canonical query string is:
/// 1. Split into (key, value) pairs
/// 2. URL-encode each key and value (RFC 3986 — `/` becomes `%2F`)
/// 3. Sort by key name (then by value if keys are equal)
/// 4. Join with `&` and separate key from value with `=`
///
/// Input: `"list-type=2&prefix=pond/"`
/// Output: `"list-type=2&prefix=pond%2F"`
fn canonicalize_query(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = Vec::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        if let Some(eq_pos) = pair.find('=') {
            let key = &pair[..eq_pos];
            let value = &pair[eq_pos + 1..];
            pairs.push((
                urlencoding::encode_query(key),
                urlencoding::encode_query(value),
            ));
        } else {
            // Key with no value
            pairs.push((
                urlencoding::encode_query(pair),
                String::new(),
            ));
        }
    }
    pairs.sort();
    pairs.iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}

/// Build the string-to-sign for SigV4.
fn build_string_to_sign(
    timestamp: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    canonical_request: &str,
) -> String {
    let request_hash = sha256_hex(canonical_request.as_bytes());
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}/{}/{}/aws4_request\n{}",
        timestamp, date_stamp, region, service, request_hash
    )
}

/// Build the Authorization header value for SigV4.
fn build_authorization_header(
    access_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    signed_headers: &str,
    signature: &str,
) -> String {
    let credential = format!("{}/{}/{}/{}/aws4_request",
        access_key, date_stamp, region, service);
    format!(
        "AWS4-HMAC-SHA256 Credential={}, SignedHeaders={}, Signature={}",
        credential, signed_headers, signature
    )
}

// ---------------------------------------------------------------------------
// S3ObjectStore
// ---------------------------------------------------------------------------

/// S3-compatible content-addressed object store.
///
/// Works with AWS S3, Cloudflare R2, MinIO, LocalStack, and any S3-compatible
/// API. Uses SigV4 signing (implemented from scratch — no AWS SDK dependency).
///
/// Thread-safe: the inner HTTP client (ureq) is sync and thread-safe. Stats
/// are behind a Mutex.
pub const MULTIPART_THRESHOLD: usize = 100 * 1024 * 1024;
pub const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;

pub struct S3ObjectStore {
    bucket: String,
    prefix: String,
    region: String,
    endpoint: String,
    credentials: S3Credentials,
    agent: ureq::Agent,
    stats: Mutex<StoreStats>,
}

#[derive(Debug, Default, Clone)]
pub struct StoreStats {
    pub gets: u64,
    pub puts: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl S3ObjectStore {
    /// Create a new S3ObjectStore.
    ///
    /// # Arguments
    /// - `bucket`: S3 bucket name
    /// - `prefix`: key prefix (e.g., "prod" or "pond/v1"). All keys will be
    ///   under this prefix. Empty string for no prefix.
    /// - `region`: AWS region (e.g., "us-east-1"). For R2/MinIO, use "auto".
    /// - `endpoint`: S3 endpoint URL. For AWS S3, use
    ///   `https://s3.{region}.amazonaws.com` or `https://s3.amazonaws.com`.
    ///   For R2: `https://{account_id}.r2.cloudflarestorage.com`.
    ///   For MinIO: `http://localhost:9000`.
    /// - `credentials`: AWS credentials for signing. Use `S3Credentials::from_env()`
    ///   to read from environment variables.
    pub fn new(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl Into<String>,
        credentials: S3Credentials,
    agent: ureq::Agent,
    ) -> Self {
        let prefix = prefix.into();
        let prefix = prefix.trim_matches('/').to_string();
        Self {
            bucket: bucket.into(),
            prefix,
            region: region.into(),
            endpoint: endpoint.into(),
            credentials,
            agent,
            stats: Mutex::new(StoreStats::default()),
        }
    }

    /// Create from a URL like `s3://bucket/prefix?region=us-east-1&endpoint=...`.
    /// Credentials are read from the environment.
    pub fn from_url(url: &str) -> Result<Self, io::Error> {
        let parsed = url::Url::parse(url)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        if parsed.scheme() != "s3" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("expected s3:// URL, got {}", parsed.scheme()),
            ));
        }
        let bucket = parsed.host_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing bucket"))?
            .to_string();
        let prefix = parsed.path().trim_start_matches('/').to_string();

        let query: std::collections::HashMap<_, _> = parsed.query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let region = query.get("region").cloned().unwrap_or_else(|| "us-east-1".to_string());
        let endpoint = query.get("endpoint").cloned().unwrap_or_else(|| {
            format!("https://s3.{}.amazonaws.com", region)
        });

        let credentials = S3Credentials::from_env().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "AWS credentials not found in environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)",
            )
        })?;

        Ok(Self::new(bucket, prefix, region, endpoint, credentials, ureq::AgentBuilder::new().timeout_connect(std::time::Duration::from_secs(10)).timeout_read(std::time::Duration::from_secs(120)).timeout_write(std::time::Duration::from_secs(120)).build()))
    }

    // --- Key helpers ---

    fn blob_key(&self, hash: &str) -> String {
        let shard = &hash[..2];
        if self.prefix.is_empty() {
            format!("blobs/{}/{}", shard, hash)
        } else {
            format!("{}/blobs/{}/{}", self.prefix, shard, hash)
        }
    }

    fn path_key(&self, path: &str) -> String {
        if self.prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.prefix, path)
        }
    }

    // --- HTTP request with SigV4 signing ---

    /// Make a signed S3 request and return the response.
    fn s3_request(
        &self,
        method: &str,
        key: &str,
        query: Option<&str>,
        body: Option<&[u8]>,
        extra_headers: &[(String, String)],
    ) -> Result<ureq::Response, io::Error> {
        let (timestamp, date_stamp) = sigv4_timestamp();
        let payload = body.unwrap_or(&[]);
        let payload_hash = sha256_hex(payload);

        // Build the URL
        let url = if let Some(q) = query {
            format!("{}/{}/{}?{}", self.endpoint, self.bucket, key, q)
        } else {
            format!("{}/{}/{}", self.endpoint, self.bucket, key)
        };

        // Canonical URI is the path (URL-encoded, but for S3 keys we keep them as-is
        // since S3 expects the un-encoded form in the canonical request)
        let canonical_uri = format!("/{}/{}", self.bucket, key);
        // Canonicalize the query string for SigV4 (URL-encode values, sort by key)
        let canonical_query = canonicalize_query(query.unwrap_or(""));

        // Build headers (must include host, x-amz-content-sha256, x-amz-date)
        let host = url::Url::parse(&url)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            .host_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no host"))?
            .to_string();

        let mut headers: Vec<(String, String)> = vec![
            ("host".to_string(), host),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), timestamp.clone()),
        ];
        if let Some(ref token) = self.credentials.session_token {
            headers.push(("x-amz-security-token".to_string(), token.clone()));
        }
        for (k, v) in extra_headers {
            headers.push((k.clone(), v.clone()));
        }
        // Sort headers by lowercase name (required by SigV4)
        headers.sort_by(|a, b| a.0.to_lowercase().cmp(&b.0.to_lowercase()));

        let canonical_request = build_canonical_request(
            method,
            &canonical_uri,
            &canonical_query,
            &headers,
            &payload_hash,
        );

        let string_to_sign = build_string_to_sign(
            &timestamp,
            &date_stamp,
            &self.region,
            "s3",
            &canonical_request,
        );

        let signing_key = sigv4_signing_key(&self.credentials.secret_key, &date_stamp, &self.region, "s3");
        let signature = sigv4_sign(&signing_key, &string_to_sign);

        let signed_headers = headers.iter()
            .map(|(k, _)| k.to_lowercase())
            .collect::<Vec<_>>()
            .join(";");

        let auth_header = build_authorization_header(
            &self.credentials.access_key,
            &date_stamp,
            &self.region,
            "s3",
            &signed_headers,
            &signature,
        );

        // Build the ureq request
        let agent = ureq::AgentBuilder::new()
            .timeout(std::time::Duration::from_secs(30))
            .build();

        let req = match method {
            "GET" => agent.get(&url),
            "PUT" => agent.put(&url),
            "DELETE" => agent.delete(&url),
            "HEAD" => agent.head(&url),
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("unsupported method: {}", method))),
        };

        // Add all signed headers
        let mut req = req.set("Authorization", &auth_header);
        for (k, v) in &headers {
            req = req.set(k, v);
        }

        // Execute
        let response = if method == "PUT" {
            req.send_bytes(payload)
        } else if method == "GET" || method == "HEAD" || method == "DELETE" {
            if payload.is_empty() {
                req.call()
            } else {
                req.send_bytes(payload)
            }
        } else {
            req.call()
        };

        response.map_err(|e| {
            // Convert ureq errors to io::Error
            match e {
                ureq::Error::Status(code, resp) => {
                    let status_text = resp.status_text().to_string();
                    let body = resp.into_string().unwrap_or_default();
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("S3 returned {}: {} — {}", code, status_text, body),
                    )
                }
                ureq::Error::Transport(t) => {
                    io::Error::new(io::ErrorKind::Other, format!("transport error: {}", t))
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// ObjectStore trait implementation
// ---------------------------------------------------------------------------

impl ObjectStore for S3ObjectStore {
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        let h = pond_kernel::hash_bytes(data);
        let key = self.blob_key(&h);
        // S3 PUT is idempotent for same content — no need for HEAD first
        let _resp = self.s3_request("PUT", &key, None, Some(data), &[])?;
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        s.bytes_written += data.len() as u64;
        Ok(h)
    }

    fn get_blob(&self, hash: &str) -> io::Result<Vec<u8>> {
        let key = self.blob_key(hash);
        let resp = self.s3_request("GET", &key, None, None, &[])?;
        let mut body = Vec::new();
        resp.into_reader()
            .read_to_end(&mut body)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        s.bytes_read += body.len() as u64;
        Ok(body)
    }

    fn put_blob_batch(&self, items: &[Vec<u8>]) -> io::Result<Vec<String>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        if items.len() == 1 {
            return self.put_blob(&items[0]).map(|h| vec![h]);
        }

        // Parallel implementation using std::thread::scope (stable since Rust 1.63).
        // No external thread-pool dependency — uses OS threads via scoped threads.
        //
        // S3 PUT latency is ~50-300ms per request. With 32 parallel requests,
        // 100 blobs take ~1 second instead of ~20 seconds sequential.
        //
        // We split the work into chunks of MAX_PARALLEL and process them
        // concurrently. Results are collected in order.
        const MAX_PARALLEL: usize = 32;
        let n = items.len();
        // Pre-compute all hashes (CPU work, no I/O) so we can return them in order
        // even if some PUTs fail.
        let precomputed: Vec<String> = items.iter()
            .map(|data| pond_kernel::hash_bytes(data))
            .collect();

        // Process in batches of MAX_PARALLEL
        for chunk_start in (0..n).step_by(MAX_PARALLEL) {
            let chunk_end = std::cmp::min(chunk_start + MAX_PARALLEL, n);
            let chunk_items = &items[chunk_start..chunk_end];
            let chunk_hashes = &precomputed[chunk_start..chunk_end];

            // Collect errors from threads (first error wins)
            let errors: Vec<Option<io::Error>> = std::thread::scope(|s| {
                let threads: Vec<_> = chunk_items.iter().zip(chunk_hashes.iter())
                    .map(|(data, hash)| {
                        s.spawn(move || {
                            let key = self.blob_key(hash);
                            self.s3_request("PUT", &key, None, Some(data), &[])
                                .err()
                        })
                    })
                    .collect();

                threads.into_iter()
                    .map(|t| t.join().unwrap_or_else(|_| Some(io::Error::new(
                        io::ErrorKind::Other, "thread panicked"
                    ))))
                    .collect()
            });

            // Return the first error if any
            for e in errors {
                if let Some(e) = e {
                    return Err(e);
                }
            }
        }

        // All PUTs succeeded — update stats and return hashes
        let total_bytes: u64 = items.iter().map(|d| d.len() as u64).sum();
        let mut s = self.stats.lock().unwrap();
        s.puts += n as u64;
        s.bytes_written += total_bytes;

        Ok(precomputed)
    }

    /// Override get_blob_batch with a parallel implementation.
    /// This is called by the default trait method, which we override here
    /// for S3 (parallel GETs reduce wall-clock from N×RTT to ~1 RTT).
    fn get_blob_batch(&self, hashes: &[String]) -> io::Result<Vec<Vec<u8>>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        if hashes.len() == 1 {
            return self.get_blob(&hashes[0]).map(|d| vec![d]);
        }

        // Parallel GET — same pattern as put_blob_batch.
        const MAX_PARALLEL: usize = 32;
        let n = hashes.len();
        // Use a slot map so we can place results in order
        let mut slot_map: Vec<Option<Vec<u8>>> = (0..n).map(|_| None).collect();
        let mut first_error: Option<io::Error> = None;

        for chunk_start in (0..n).step_by(MAX_PARALLEL) {
            let chunk_end = std::cmp::min(chunk_start + MAX_PARALLEL, n);
            let chunk_hashes = &hashes[chunk_start..chunk_end];

            let results: Vec<Result<(usize, Vec<u8>), io::Error>> = std::thread::scope(|s| {
                let threads: Vec<_> = chunk_hashes.iter().enumerate()
                    .map(|(i, hash)| {
                        let global_idx = chunk_start + i;
                        s.spawn(move || {
                            let key = self.blob_key(hash);
                            match self.s3_request("GET", &key, None, None, &[]) {
                                Ok(resp) => {
                                    let mut body = Vec::new();
                                    match resp.into_reader().read_to_end(&mut body) {
                                        Ok(_) => Ok((global_idx, body)),
                                        Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
                                    }
                                }
                                Err(e) => Err(e),
                            }
                        })
                    })
                    .collect();

                threads.into_iter()
                    .map(|t| t.join().unwrap_or_else(|_| Err(io::Error::new(
                        io::ErrorKind::Other, "thread panicked"
                    ))))
                    .collect()
            });

            // Place successful results in the slot map; capture first error
            for r in results {
                match r {
                    Ok((idx, body)) => slot_map[idx] = Some(body),
                    Err(e) => {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
            }
        }

        if let Some(e) = first_error {
            return Err(e);
        }

        // Collect in order
        let mut results = Vec::with_capacity(n);
        let mut total_bytes: u64 = 0;
        for opt in slot_map {
            let body = opt.ok_or_else(|| io::Error::new(
                io::ErrorKind::Other,
                "missing result from parallel batch (should not happen)"
            ))?;
            total_bytes += body.len() as u64;
            results.push(body);
        }

        let mut s = self.stats.lock().unwrap();
        s.gets += n as u64;
        s.bytes_read += total_bytes;

        Ok(results)
    }

    fn put_path(&self, path: &str, hash: &str) -> io::Result<()> {
        let key = self.path_key(path);
        let body = format!(r#"{{"hash":"{}"}}"#, hash).into_bytes();
        let _resp = self.s3_request("PUT", &key, None, Some(&body), &[])?;
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        Ok(())
    }

    fn get_path(&self, path: &str) -> Option<String> {
        let key = self.path_key(path);
        match self.s3_request("GET", &key, None, None, &[]) {
            Ok(resp) => {
                let mut body = String::new();
                if resp.into_reader().read_to_string(&mut body).is_err() {
                    return None;
                }
                let mut s = self.stats.lock().unwrap();
                s.gets += 1;
                extract_hash_from_json(&body)
            }
            Err(_) => None,
        }
    }

    fn delete_path(&self, path: &str) -> io::Result<bool> {
        let key = self.path_key(path);
        // S3 DELETE is idempotent — returns 204 even if the key didn't exist
        match self.s3_request("DELETE", &key, None, None, &[]) {
            Ok(_) => Ok(true),
            Err(e) => {
                // Check if it was a 404 (already gone)
                if e.to_string().contains("404") {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    fn list_paths(&self, prefix: &str) -> io::Result<Vec<String>> {
        let list_prefix = self.path_key(prefix);
        let mut all_keys = Vec::new();

        let mut cont: Option<String> = None;
        loop {
            let mut query = format!("list-type=2&prefix={}", urlencoding::encode(&list_prefix));
            if let Some(ref token) = cont {
                query.push_str(&format!("&continuation-token={}", urlencoding::encode(token)));
            }

            let resp = self.s3_request("GET", "", Some(&query), None, &[])?;
            let body = resp.into_string()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            // Simple XML extraction: find all <Key>...</Key> values.
            // S3 ListObjectsV2 XML wraps keys in <Contents><Key>...</Key></Contents>.
            // We use string searching — simpler and more robust than a hand-rolled XML parser.
            let mut search_from = 0;
            let mut next_token: Option<String> = None;
            while search_from < body.len() {
                // Find next <Key> tag
                let key_start = match body[search_from..].find("<Key>") {
                    Some(p) => search_from + p + 5, // skip "<Key>"
                    None => break,
                };
                let key_end = match body[key_start..].find("</Key>") {
                    Some(p) => key_start + p,
                    None => break,
                };
                let key = &body[key_start..key_end];
                // URL-decode the key (S3 may return URL-encoded keys if encoding-type=url was used)
                let key = url_decode(key);
                all_keys.push(key);
                search_from = key_end + 6; // skip "</Key>"
            }

            // Check for pagination — look for <IsTruncated>true</IsTruncated>
            // and <NextContinuationToken>...</NextContinuationToken>
            if let Some(start) = body.find("<IsTruncated>true</IsTruncated>") {
                let _ = start; // IsTruncated is true
                if let Some(tok_start) = body.find("<NextContinuationToken>") {
                    if let Some(tok_end) = body[tok_start..].find("</NextContinuationToken>") {
                        next_token = Some(body[tok_start + 23..tok_start + tok_end].to_string());
                    }
                }
            }

            if next_token.is_none() {
                break;
            }
            cont = next_token;
        }

        // Strip the prefix from keys to return relative paths
        let strip_len = if self.prefix.is_empty() {
            0
        } else {
            self.prefix.len() + 1 // +1 for the '/'
        };
        let mut result: Vec<String> = all_keys.iter()
            .filter_map(|k| {
                // Skip blob keys (they're content-addressed, not named refs)
                if k.contains("/blobs/") {
                    return None;
                }
                let rel = if k.len() > strip_len {
                    &k[strip_len..]
                } else {
                    k
                };
                Some(rel.to_string())
            })
            .collect();
        result.sort();
        result.dedup();
        Ok(result)
    }

    fn blob_exists(&self, hash: &str) -> bool {
        let key = self.blob_key(hash);
        match self.s3_request("HEAD", &key, None, None, &[]) {
            Ok(_) => true,
            Err(e) => {
                // 404 means not found
                if e.to_string().contains("404") {
                    false
                } else {
                    // Other errors — be safe and report false
                    false
                }
            }
        }
    }

    fn delete_blob(&self, hash: &str) -> io::Result<bool> {
        let key = self.blob_key(hash);
        match self.s3_request("DELETE", &key, None, None, &[]) {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.to_string().contains("404") {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }
}

/// Extract the "hash" field from a JSON string like {"hash":"abc123"}.
/// (Same minimal parser as LocalFSObjectStore — copied to avoid a cross-crate dep.)
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

// Minimal URL-encoding helpers (avoid pulling in the `urlencoding` crate)
mod urlencoding {
    /// Encode a string for use in a URL PATH component.
    /// Forward slashes are kept literal (S3 keys contain slashes).
    pub fn encode_path(s: &str) -> String {
        let mut out = String::with_capacity(s.len() * 3);
        for b in s.bytes() {
            match b {
                // Unreserved characters (RFC 3986)
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char);
                }
                // Forward slash is kept literal for S3 path components
                b'/' => out.push('/'),
                _ => {
                    out.push_str(&format!("%{:02X}", b));
                }
            }
        }
        out
    }

    /// Encode a string for use in a URL QUERY parameter value.
    /// Forward slashes ARE encoded as %2F (required by SigV4 canonical query).
    /// This is also used for query parameter VALUES (not the key=value separator).
    pub fn encode_query(s: &str) -> String {
        let mut out = String::with_capacity(s.len() * 3);
        for b in s.bytes() {
            match b {
                // Unreserved characters (RFC 3986)
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char);
                }
                _ => {
                    out.push_str(&format!("%{:02X}", b));
                }
            }
        }
        out
    }

    /// Backward-compat alias: `encode` = `encode_path`.
    /// (Most callers encode path components, not query values.)
    pub fn encode(s: &str) -> String {
        encode_path(s)
    }
}

/// URL-decode a string (e.g., %20 → space, %2F → /).
/// Used to decode S3 object keys returned in XML responses.
fn url_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_digit(bytes[i + 1]);
            let lo = hex_digit(bytes[i + 2]);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as char);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// C ABI — extern "C" wrappers for cross-language SDKs
// ---------------------------------------------------------------------------

use std::ffi::{c_char, CStr};

/// Create an S3ObjectStore from a URL string.
///
/// URL format: `s3://bucket/prefix?region=us-east-1&endpoint=https://s3.amazonaws.com`
///
/// Credentials are read from the environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
///
/// Returns a heap-allocated `*mut S3ObjectStore` (as `*mut std::ffi::c_void`) on success,
/// or NULL on error. The caller MUST free it with `pond_s3_store_free()`.
///
/// The returned pointer can be passed to `pond_kernel_new_with_store()` (in pond_kernel's
/// C ABI) to create a kernel backed by S3.
#[no_mangle]
pub extern "C" fn pond_s3_store_new(url: *const c_char) -> *mut std::ffi::c_void {
    if url.is_null() {
        return std::ptr::null_mut();
    }
    let url_str = match unsafe { CStr::from_ptr(url) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    match S3ObjectStore::from_url(url_str) {
        Ok(store) => {
            let boxed: Box<S3ObjectStore> = Box::new(store);
            Box::into_raw(boxed) as *mut std::ffi::c_void
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Free an S3ObjectStore created by `pond_s3_store_new()`. Safe on NULL.
#[no_mangle]
pub extern "C" fn pond_s3_store_free(store: *mut std::ffi::c_void) {
    if !store.is_null() {
        unsafe {
            drop(Box::from_raw(store as *mut S3ObjectStore));
        }
    }
}

/// Create a UnifiedStorage backed by S3-compatible storage.
///
/// URL format: `s3://bucket/prefix?region=us-east-1&endpoint=https://s3.amazonaws.com`
///
/// Credentials are read from the environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
///
/// Returns a `*mut PondStorageHandle` on success, or NULL on error.
/// The caller MUST free it with `pond_storage_free()`.
///
/// This is the S3 equivalent of `pond_storage_new()` — it produces the same
/// handle type, just backed by S3 instead of local FS. Link against
/// `libpond_s3.a` (in addition to `libpond_storage.a`) to use this.
#[no_mangle]
pub extern "C" fn pond_storage_new_s3(url: *const c_char) -> *mut pond_storage::PondStorageHandle {
    if url.is_null() {
        return std::ptr::null_mut();
    }
    let url_str = match unsafe { CStr::from_ptr(url) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    match S3ObjectStore::from_url(url_str) {
        Ok(store) => {
            let kernel = pond_kernel::PondKernel::new_with_store(Box::new(store));
            let storage = pond_storage::UnifiedStorage::new(kernel);
            Box::into_raw(Box::new(pond_storage::PondStorageHandle::new(storage)))
        }
        Err(_) => std::ptr::null_mut(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmac_sha256_known_vector() {
        // RFC 4231 Test Case 1: key=0x0b*20, data="Hi There"
        let key = [0x0bu8; 20];
        let data = b"Hi There";
        let mac = hmac_sha256(&key, data);
        let expected = "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7";
        assert_eq!(hex::encode(mac), expected);
    }

    #[test]
    fn test_hmac_sha256_known_vector_2() {
        // RFC 4231 Test Case 2: key="Jefe", data="what do ya want for nothing?"
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let expected = "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843";
        assert_eq!(hex::encode(mac), expected);
    }

    #[test]
    fn test_sigv4_signing_key() {
        // From AWS docs:
        // https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
        // secret_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
        // date = "20150830"
        // region = "us-east-1"
        // service = "iam"
        // Expected kSigning hex:
        // c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9
        let key = sigv4_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex::encode(key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    #[test]
    fn test_extract_hash_from_json() {
        assert_eq!(
            extract_hash_from_json(r#"{"hash":"abc123"}"#),
            Some("abc123".to_string())
        );
        assert_eq!(extract_hash_from_json(r#"{"foo":"bar"}"#), None);
        assert_eq!(extract_hash_from_json(""), None);
    }

    #[test]
    fn test_urlencoding() {
        assert_eq!(urlencoding::encode("abc123"), "abc123");
        assert_eq!(urlencoding::encode("a b/c"), "a%20b/c");
        assert_eq!(urlencoding::encode("blobs/ab/abcdef"), "blobs/ab/abcdef");
    }

    #[test]
    fn test_blob_key() {
        let store = S3ObjectStore::new(
            "my-bucket", "prod", "us-east-1", "https://s3.amazonaws.com",
            S3Credentials {
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                session_token: None,
            },
            ureq::Agent::new(),
        );
        assert_eq!(store.blob_key("abcdef1234567890"), "prod/blobs/ab/abcdef1234567890");

        let store_no_prefix = S3ObjectStore::new(
            "my-bucket", "", "us-east-1", "https://s3.amazonaws.com",
            S3Credentials {
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                session_token: None,
            },
            ureq::Agent::new(),
        );
        assert_eq!(store_no_prefix.blob_key("abcdef1234567890"), "blobs/ab/abcdef1234567890");
    }
}
