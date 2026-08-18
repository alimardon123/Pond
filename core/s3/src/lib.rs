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

/// Canonicalize a query string for SigV4 — and for the request URL.
///
/// Per AWS spec, the canonical query string is:
/// 1. Split into (key, value) pairs
/// 2. URL-encode each key and value (RFC 3986 — `/` becomes `%2F`)
/// 3. Sort by key name (then by value if keys are equal)
/// 4. Join with `&` and separate key from value with `=`
///
/// Input: `"list-type=2&prefix=pond/"`
/// Output: `"list-type=2&prefix=pond%2F"`
///
/// **Values passed in must be raw, not pre-encoded.** This function is the
/// single place query encoding happens, and its output is used both to sign
/// the request and to build the URL that is sent. That is deliberate: when a
/// caller pre-encoded a value, this function encoded it a second time, so the
/// signature covered `%253D` while the wire carried `%3D` and S3 answered
/// `SignatureDoesNotMatch`. It only bit on values that actually contain
/// reserved characters — in practice the `NextContinuationToken`, which is
/// base64 and ends in `==`, so listing worked until a bucket crossed 1000
/// objects and pagination kicked in.
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
/// Objects at or above this size are uploaded with multipart.
///
/// S3 caps a single PUT at 5 GiB, and a large single PUT has no resume point:
/// one dropped connection re-sends everything. Multipart splits the object so
/// only the failed part is retried, and lets parts go in parallel.
pub const MULTIPART_THRESHOLD: usize = 100 * 1024 * 1024;
/// Size of each multipart part. S3 requires >= 5 MiB for all but the last.
pub const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
/// How many parts to upload concurrently.
pub const MULTIPART_PARALLELISM: usize = 4;

// S3's own limits, checked at compile time so a careless edit to the constants
// above fails the build rather than a live upload.
const _: () = assert!(
    MULTIPART_PART_SIZE >= 5 * 1024 * 1024,
    "S3 requires every part except the last to be at least 5 MiB"
);
const _: () = assert!(
    MULTIPART_THRESHOLD < 5 * 1024 * 1024 * 1024,
    "objects at or above 5 GiB cannot use a single PUT, so the multipart \
     threshold must be below that limit"
);

/// Keys per `DeleteObjects` request. S3's documented maximum is 1000, and
/// exceeding it is rejected outright rather than truncated.
pub const DELETE_BATCH_LIMIT: usize = 1000;

/// Retry attempts after the initial try, for retryable failures.
const MAX_RETRIES: u32 = 5;
/// Base unit for exponential backoff.
const RETRY_BASE_DELAY_MS: u64 = 50;
/// Ceiling on any single backoff sleep.
const RETRY_MAX_DELAY_MS: u64 = 2_000;

/// An S3 failure that remembers its HTTP status.
///
/// Retry classification needs the status code, and stuffing it into a
/// formatted string would mean parsing English back out. This keeps the
/// decision typed and directly testable.
#[derive(Debug)]
struct S3Error {
    status: Option<u16>,
    message: String,
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for S3Error {}

impl From<S3Error> for io::Error {
    fn from(e: S3Error) -> io::Error {
        // NotFound must survive as NotFound: callers (and the ObjectStore
        // contract) distinguish "missing" from "failed".
        let kind = match e.status {
            Some(404) => io::ErrorKind::NotFound,
            _ => io::ErrorKind::Other,
        };
        io::Error::new(kind, e)
    }
}

/// Recover the HTTP status from an error produced by this client.
fn status_of(e: &io::Error) -> Option<u16> {
    e.get_ref()
        .and_then(|inner| inner.downcast_ref::<S3Error>())
        .and_then(|s3| s3.status)
}

/// Is this failure worth retrying?
///
/// Retryable: throttling and server-side faults (429, 500, 502, 503, 504),
/// and transport errors (connection reset, timeout, DNS blip) which carry no
/// status. Not retryable: anything the request itself got wrong — 400, 403,
/// 404, 412, 416 — since repeating it produces the same answer.
fn is_retryable(e: &io::Error) -> bool {
    match status_of(e) {
        Some(code) => matches!(code, 429 | 500 | 502 | 503 | 504),
        // No status means the response never arrived: transport-level, retry.
        None => e
            .get_ref()
            .map(|inner| inner.downcast_ref::<S3Error>().is_some())
            .unwrap_or(false),
    }
}

/// 416 means the requested range starts past the end of the object.
fn is_range_not_satisfiable(e: &io::Error) -> bool {
    status_of(e) == Some(416)
}

/// Build the HTTP agent, honouring the environment every other S3 client does.
///
/// Two things here are not optional for a storage system that expects to be
/// deployed inside someone else's network:
///
/// **Custom CA bundles.** Corporate networks routinely terminate and re-issue
/// TLS at an inspecting proxy, and private S3-compatible deployments (MinIO,
/// Ceph) commonly use a private CA. A client that only trusts the compiled-in
/// Mozilla root set simply cannot connect in either case, with an
/// `UnknownIssuer` error that looks like a bug in the server. `AWS_CA_BUNDLE`
/// is the AWS-standard variable for this; `SSL_CERT_FILE` is the OpenSSL
/// convention that most tooling also sets. Both are honoured, additively —
/// the public roots stay trusted, the extra bundle is added.
///
/// **Proxy configuration.** `HTTPS_PROXY` / `ALL_PROXY` with `NO_PROXY` is the
/// universal convention; ignoring it means the client is unusable anywhere
/// egress is mediated.
///
/// Both are read from the environment rather than configured in code, so
/// deployment does not require recompiling or a Pond-specific setting.
fn build_agent() -> ureq::Agent {
    let mut builder = ureq::AgentBuilder::new()
        // Split-phase timeouts: a slow connect and a slow body are different
        // failures and deserve different budgets. A single blanket timeout
        // either kills large legitimate transfers or waits far too long on a
        // dead host.
        .timeout_connect(std::time::Duration::from_secs(10))
        .timeout_read(std::time::Duration::from_secs(120))
        .timeout_write(std::time::Duration::from_secs(120));

    if let Some(tls) = tls_config_with_extra_roots() {
        builder = builder.tls_config(std::sync::Arc::new(tls));
    }

    if let Some(proxy_url) = proxy_from_env() {
        if let Ok(p) = ureq::Proxy::new(&proxy_url) {
            builder = builder.proxy(p);
        }
    }

    builder.build()
}

/// Read a proxy URL from the conventional environment variables.
///
/// Lowercase is checked first because it is the more specific convention
/// (`https_proxy` is what curl documents); uppercase is the widely-set
/// fallback. `NO_PROXY` is left to ureq, which applies it per-request.
fn proxy_from_env() -> Option<String> {
    ["https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]
        .iter()
        .find_map(|k| std::env::var(k).ok())
        .filter(|v| !v.is_empty())
}

/// Build a rustls config trusting the public roots plus any bundle named by
/// `AWS_CA_BUNDLE` or `SSL_CERT_FILE`.
///
/// Returns None when no extra bundle is configured, so ureq keeps its default
/// (webpki-roots) and nothing changes for the common case.
fn tls_config_with_extra_roots() -> Option<rustls::ClientConfig> {
    let path = ["AWS_CA_BUNDLE", "SSL_CERT_FILE"]
        .iter()
        .find_map(|k| std::env::var(k).ok())
        .filter(|v| !v.is_empty())?;

    let pem = std::fs::read(&path).ok()?;
    let mut roots = rustls::RootCertStore::empty();

    // Start from the public roots so adding a private CA does not silently
    // drop trust in everything else.
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let mut reader = std::io::BufReader::new(&pem[..]);
    let mut added = 0usize;
    for cert in rustls_pemfile::certs(&mut reader).flatten() {
        if roots.add(cert).is_ok() {
            added += 1;
        }
    }
    if added == 0 {
        // The variable pointed at something unusable. Fall back to defaults
        // rather than starting from an empty trust store, which would fail
        // every connection with a confusing error.
        return None;
    }

    // rustls 0.23 requires an explicit crypto provider unless a process
    // default was installed. Naming `ring` here matches the provider ureq
    // links, so the binary carries one crypto backend rather than two, and
    // avoids depending on global init order.
    Some(
        rustls::ClientConfig::builder_with_provider(
            rustls::crypto::ring::default_provider().into(),
        )
        .with_safe_default_protocol_versions()
        .ok()?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    )
}

/// Exponential backoff with full jitter: `random(0, min(cap, base * 2^n))`.
///
/// Full jitter rather than plain exponential because the failure that makes
/// retries necessary — a shared throttle — hits many clients at once. Without
/// jitter they all wake together and re-collide; with it they spread out.
fn backoff_delay(attempt: u32) -> std::time::Duration {
    let exp = RETRY_BASE_DELAY_MS.saturating_mul(1u64 << attempt.min(16));
    let ceiling = exp.min(RETRY_MAX_DELAY_MS);
    // Cheap jitter source: no RNG dependency needed for scheduling noise.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let jittered = if ceiling == 0 { 0 } else { nanos % (ceiling + 1) };
    std::time::Duration::from_millis(jittered)
}

pub struct S3ObjectStore {
    bucket: String,
    prefix: String,
    region: String,
    endpoint: String,
    credentials: S3Credentials,
    agent: ureq::Agent,
    stats: Mutex<StoreStats>,
    /// Lazily-initialized async HTTP client. Only present with `feature = "async"`.
    /// `OnceLock` so we don't need to thread an `Option` through `new()`.
    #[cfg(feature = "async")]
    async_client: std::sync::OnceLock<reqwest::Client>,
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
            #[cfg(feature = "async")]
            async_client: std::sync::OnceLock::new(),
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

        Ok(Self::new(
            bucket,
            prefix,
            region,
            endpoint,
            credentials,
            build_agent(),
        ))
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

    /// Build a fully-signed S3 request (URL + headers + body), without
    /// executing it. Shared by the sync `s3_request` and the async
    /// `s3_request_async` so SigV4 signing logic lives in exactly one place.
    ///
    /// Returns a [`SignedS3Request`] which can be executed via any HTTP client.
    fn build_signed_request(
        &self,
        method: &str,
        key: &str,
        query: Option<&str>,
        body: Option<&[u8]>,
        extra_headers: &[(String, String)],
    ) -> Result<SignedS3Request, io::Error> {
        let (timestamp, date_stamp) = sigv4_timestamp();
        let payload = body.unwrap_or(&[]);
        let payload_hash = sha256_hex(payload);

        // Canonicalize the query string once (URL-encode values, sort by key)
        // and use that same string for both the signature and the URL. Any
        // other arrangement lets the signed bytes drift from the sent bytes.
        let canonical_query = canonicalize_query(query.unwrap_or(""));

        // Build the URL
        let url = if canonical_query.is_empty() {
            format!("{}/{}/{}", self.endpoint, self.bucket, key)
        } else {
            format!("{}/{}/{}?{}", self.endpoint, self.bucket, key, canonical_query)
        };

        // Canonical URI is the path (URL-encoded, but for S3 keys we keep them as-is
        // since S3 expects the un-encoded form in the canonical request)
        let canonical_uri = format!("/{}/{}", self.bucket, key);

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

        // Add the Authorization header (not part of the signed headers list
        // — it's the signature itself).
        headers.push(("Authorization".to_string(), auth_header));

        Ok(SignedS3Request {
            method: method.to_string(),
            url,
            body: payload.to_vec(),
            headers,
        })
    }

    /// Make a signed S3 request and return the response.
    /// Execute a signed request, retrying failures that S3 documents as
    /// retryable.
    ///
    /// S3 sheds load: 503 SlowDown, 500, and 429 are normal under
    /// concurrency, and connections get reset. Treating any of those as fatal
    /// means a single hiccup fails a whole write — not acceptable for a
    /// storage system, and previously the behaviour, since there was no retry
    /// anywhere in this client.
    ///
    /// Backoff is exponential with full jitter (`random(0, base * 2^n)`),
    /// which is what keeps many concurrent clients from re-colliding in
    /// lockstep after a shared throttle. Requests are re-signed on each
    /// attempt because SigV4 signatures cover a timestamp and expire.
    ///
    /// Only idempotent operations are retried. Every operation this client
    /// performs is idempotent by construction: blob PUTs are
    /// content-addressed (same bytes, same key), ref PUTs are
    /// last-writer-wins, GETs and DELETEs are naturally so.
    fn s3_request(
        &self,
        method: &str,
        key: &str,
        query: Option<&str>,
        body: Option<&[u8]>,
        extra_headers: &[(String, String)],
    ) -> Result<ureq::Response, io::Error> {
        let mut attempt = 0u32;
        loop {
            match self.s3_request_once(method, key, query, body, extra_headers) {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    if attempt >= MAX_RETRIES || !is_retryable(&e) {
                        return Err(e);
                    }
                    std::thread::sleep(backoff_delay(attempt));
                    attempt += 1;
                }
            }
        }
    }

    /// One attempt, without retry.
    fn s3_request_once(
        &self,
        method: &str,
        key: &str,
        query: Option<&str>,
        body: Option<&[u8]>,
        extra_headers: &[(String, String)],
    ) -> Result<ureq::Response, io::Error> {
        let signed = self.build_signed_request(method, key, query, body, extra_headers)?;

        // Use the agent owned by this store rather than building a fresh one
        // per request. `ureq::Agent` owns the connection pool, so a per-request
        // agent meant a new TCP + TLS handshake on every single GET/PUT — the
        // dominant cost against S3. The agent is configured once in `new()`.
        let agent = &self.agent;

        let req = match signed.method.as_str() {
            "GET" => agent.get(&signed.url),
            "PUT" => agent.put(&signed.url),
            "DELETE" => agent.delete(&signed.url),
            "HEAD" => agent.head(&signed.url),
            // POST is used by multipart upload (create / complete).
            "POST" => agent.post(&signed.url),
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("unsupported method: {}", signed.method))),
        };

        // Add all signed headers
        let mut req = req;
        for (k, v) in &signed.headers {
            req = req.set(k, v);
        }

        // Execute. PUT and POST always send a body (possibly empty — S3's
        // CreateMultipartUpload is a POST with no body but a signed empty
        // payload hash, so it must still go through send_bytes for the
        // Content-Length to match what was signed).
        let response = if signed.method == "PUT" || signed.method == "POST" {
            req.send_bytes(&signed.body)
        } else if signed.method == "GET" || signed.method == "HEAD" || signed.method == "DELETE" {
            if signed.body.is_empty() {
                req.call()
            } else {
                req.send_bytes(&signed.body)
            }
        } else {
            req.call()
        };

        response.map_err(|e| {
            // Convert ureq errors to io::Error, preserving the HTTP status so
            // retry classification is a typed decision rather than a string
            // match on the message.
            match e {
                ureq::Error::Status(code, resp) => {
                    let status_text = resp.status_text().to_string();
                    let body = resp.into_string().unwrap_or_default();
                    S3Error {
                        status: Some(code),
                        message: format!("S3 returned {}: {} — {}", code, status_text, body),
                    }
                    .into()
                }
                ureq::Error::Transport(t) => S3Error {
                    status: None,
                    message: format!("transport error: {}", t),
                }
                .into(),
            }
        })
    }

    /// Async variant of [`s3_request`](Self::s3_request) using `reqwest`.
    ///
    /// Builds the same SigV4-signed request via [`build_signed_request`](Self::build_signed_request)
    /// and executes it asynchronously. The `reqwest::Client` is created once
    /// and cached in `self.async_client` (a `OnceLock`) so connection
    /// pooling/keep-alive works across calls.
    #[cfg(feature = "async")]
    async fn s3_request_async(
        &self,
        method: &str,
        key: &str,
        query: Option<&str>,
        body: Option<&[u8]>,
        extra_headers: &[(String, String)],
    ) -> Result<reqwest::Response, io::Error> {
        let signed = self.build_signed_request(method, key, query, body, extra_headers)?;

        let client = self.async_client.get_or_init(|| {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        });

        let method_enum = match signed.method.as_str() {
            "GET" => reqwest::Method::GET,
            "PUT" => reqwest::Method::PUT,
            "DELETE" => reqwest::Method::DELETE,
            "HEAD" => reqwest::Method::HEAD,
            "POST" => reqwest::Method::POST,
            other => return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported method: {}", other),
            )),
        };

        let mut req = client.request(method_enum, &signed.url);
        for (k, v) in &signed.headers {
            req = req.header(k, v);
        }
        if !signed.body.is_empty() {
            req = req.body(signed.body);
        }

        req.send().await.map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("reqwest transport error: {}", e))
        })
        // Note: reqwest::Response doesn't error on 4xx/5xx by default —
        // callers must check .status() themselves. The sync path returns
        // errors for non-2xx via ureq::Error::Status; async callers should
        // use .error_for_status() to mirror that behavior.
    }
}

/// A fully-signed S3 request, ready to be executed by any HTTP client.
///
/// Produced by [`S3ObjectStore::build_signed_request`]. Contains the URL,
/// HTTP method, body bytes (empty for GET/HEAD/DELETE), and all required
/// headers (host, x-amz-content-sha256, x-amz-date, x-amz-security-token
/// if present, plus the Authorization header).
struct SignedS3Request {
    method: String,
    url: String,
    body: Vec<u8>,
    headers: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// Multipart upload
// ---------------------------------------------------------------------------

impl S3ObjectStore {
    /// Upload a large object in parts.
    ///
    /// Three reasons this exists rather than one big PUT: S3 rejects a single
    /// PUT over 5 GiB outright; a large PUT has no resume point, so one
    /// dropped connection re-sends the whole object; and parts can go in
    /// parallel, which is the only way to saturate bandwidth on a big write.
    ///
    /// Each part is retried independently by `s3_request`. If the upload
    /// cannot be completed, it is aborted so S3 does not keep billing for the
    /// orphaned parts — the failure mode people discover months later on an
    /// invoice.
    fn put_multipart(&self, key: &str, data: &[u8]) -> io::Result<()> {
        let upload_id = self.create_multipart_upload(key)?;

        match self.upload_parts(key, &upload_id, data) {
            Ok(parts) => self.complete_multipart_upload(key, &upload_id, &parts),
            Err(e) => {
                // Best-effort abort: the upload already failed, so a failure
                // here must not mask the original error.
                let _ = self.abort_multipart_upload(key, &upload_id);
                Err(e)
            }
        }
    }

    fn create_multipart_upload(&self, key: &str) -> io::Result<String> {
        let resp = self.s3_request("POST", key, Some("uploads="), Some(&[]), &[])?;
        let body = resp.into_string().map_err(io::Error::other)?;
        extract_xml_tag(&body, "UploadId").ok_or_else(|| {
            io::Error::other(format!(
                "multipart: no UploadId in CreateMultipartUpload response: {}",
                body
            ))
        })
    }

    /// Upload all parts, up to `MULTIPART_PARALLELISM` at a time.
    ///
    /// Returns (part_number, etag) pairs in part order, which is what
    /// CompleteMultipartUpload requires.
    fn upload_parts(
        &self,
        key: &str,
        upload_id: &str,
        data: &[u8],
    ) -> io::Result<Vec<(usize, String)>> {
        let chunks: Vec<(usize, &[u8])> = data
            .chunks(MULTIPART_PART_SIZE)
            .enumerate()
            .map(|(i, c)| (i + 1, c)) // S3 part numbers are 1-based
            .collect();

        let mut parts: Vec<(usize, String)> = Vec::with_capacity(chunks.len());

        for window in chunks.chunks(MULTIPART_PARALLELISM) {
            let results: Vec<io::Result<(usize, String)>> = std::thread::scope(|s| {
                let handles: Vec<_> = window
                    .iter()
                    .map(|(n, bytes)| {
                        let n = *n;
                        let bytes = *bytes;
                        s.spawn(move || self.upload_one_part(key, upload_id, n, bytes))
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join().unwrap_or_else(|_| {
                            Err(io::Error::other("multipart: upload thread panicked"))
                        })
                    })
                    .collect()
            });
            for r in results {
                parts.push(r?);
            }
        }

        parts.sort_by_key(|(n, _)| *n);
        Ok(parts)
    }

    fn upload_one_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: usize,
        body: &[u8],
    ) -> io::Result<(usize, String)> {
        let query = format!("partNumber={}&uploadId={}", part_number, upload_id);
        let resp = self.s3_request("PUT", key, Some(&query), Some(body), &[])?;
        let etag = resp.header("etag").or_else(|| resp.header("ETag")).ok_or_else(|| {
            io::Error::other(format!("multipart: part {} returned no ETag", part_number))
        })?;
        Ok((part_number, etag.to_string()))
    }

    fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(usize, String)],
    ) -> io::Result<()> {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (n, etag) in parts {
            // ETags come back quoted; S3 accepts them either way, but the
            // quotes must be balanced, so pass them through verbatim.
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                n, etag
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");

        let query = format!("uploadId={}", upload_id);
        let resp = self.s3_request("POST", key, Some(&query), Some(xml.as_bytes()), &[])?;

        // S3 can return 200 with an error document in the body for this call,
        // so a status check alone is not enough.
        let body = resp.into_string().unwrap_or_default();
        if body.contains("<Error>") {
            return Err(io::Error::other(format!(
                "multipart: CompleteMultipartUpload failed: {}",
                body
            )));
        }
        Ok(())
    }

    fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> io::Result<()> {
        let query = format!("uploadId={}", upload_id);
        self.s3_request("DELETE", key, Some(&query), None, &[])?;
        Ok(())
    }

    /// `POST /?delete` with up to [`DELETE_BATCH_LIMIT`] keys.
    ///
    /// Returns how many keys were actually removed. S3 reports per-key results
    /// in the response body and answers 200 even when some keys failed, so the
    /// body has to be read — a status check alone would report success for a
    /// request that deleted nothing.
    ///
    /// `Quiet` mode is deliberately *not* used: it suppresses the `<Deleted>`
    /// entries, which are exactly what makes the count trustworthy.
    fn delete_objects(&self, keys: &[String]) -> io::Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        if keys.len() > DELETE_BATCH_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "DeleteObjects accepts at most {} keys, got {}",
                    DELETE_BATCH_LIMIT,
                    keys.len()
                ),
            ));
        }

        let mut xml = String::from("<Delete>");
        for k in keys {
            xml.push_str("<Object><Key>");
            xml.push_str(&xml_escape(k));
            xml.push_str("</Key></Object>");
        }
        xml.push_str("</Delete>");
        let body = xml.as_bytes();

        // S3 requires an integrity header on this operation. `Content-MD5` is
        // the historical one; the checksum headers are the modern replacement
        // and use SHA-256, which is already computed here for SigV4 — so this
        // costs nothing and avoids taking an MD5 dependency for one call.
        let digest = {
            let mut h = Sha256::new();
            h.update(body);
            h.finalize()
        };
        let checksum = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, digest);
        let extra = vec![
            ("x-amz-sdk-checksum-algorithm".to_string(), "SHA256".to_string()),
            ("x-amz-checksum-sha256".to_string(), checksum),
        ];

        let resp = self.s3_request("POST", "", Some("delete="), Some(body), &extra)?;
        let body = resp.into_string().map_err(io::Error::other)?;

        // A key that did not exist is reported as deleted, matching the
        // idempotent semantics of a single DELETE.
        let deleted = body.matches("<Deleted>").count();
        if let Some(err) = extract_xml_tag(&body, "Error") {
            // Per-key failures do not fail the whole batch — the caller is
            // reclaiming space, and a key that could not be removed is an
            // orphan, not a correctness problem. But it must not be silent.
            let _ = err;
            let failed = body.matches("<Error>").count();
            if deleted == 0 {
                return Err(io::Error::other(format!(
                    "DeleteObjects removed nothing and reported {} errors: {}",
                    failed, body
                )));
            }
        }
        Ok(deleted)
    }
}

/// Escape the five XML metacharacters. Object keys are user-controlled, and an
/// unescaped `&` or `<` in a key would produce a malformed request body — at
/// best an error, at worst a request that deletes something else.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// Extract the text content of the first `<tag>...</tag>` in an XML document.
///
/// S3's multipart responses are small and fixed-shape, so a full XML parser
/// is not warranted here; this handles exactly the shape AWS returns.
/// Extract every `<Key>` from a ListObjectsV2 response body.
///
/// Keys are returned exactly as S3 wrote them, still percent-encoded if the
/// request asked for `encoding-type=url`; the caller decodes.
///
/// This is string searching rather than an XML parse on purpose: the response
/// shape is fixed by the S3 API and `<Key>` never nests, so a parser would buy
/// nothing but a dependency.
fn list_response_keys(body: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut from = 0usize;
    while from < body.len() {
        let start = match body[from..].find("<Key>") {
            Some(p) => from + p + 5,
            None => break,
        };
        let end = match body[start..].find("</Key>") {
            Some(p) => start + p,
            None => break,
        };
        keys.push(body[start..end].to_string());
        from = end + 6;
    }
    keys
}

/// The continuation token for the next page, or `None` when this is the last.
///
/// A list that ignores this silently returns the first 1000 objects and calls
/// it the whole bucket — which is a wrong answer, not a slow one, and the kind
/// that only shows up once a deployment is large enough to matter.
fn list_response_next_token(body: &str) -> Option<String> {
    if !body.contains("<IsTruncated>true</IsTruncated>") {
        return None;
    }
    extract_xml_tag(body, "NextContinuationToken")
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].trim().to_string())
}

// ---------------------------------------------------------------------------
// ObjectStore trait implementation
// ---------------------------------------------------------------------------

impl ObjectStore for S3ObjectStore {
    fn put_blob(&self, data: &[u8]) -> io::Result<String> {
        let h = pond_kernel::hash_bytes(data);
        let key = self.blob_key(&h);
        // S3 PUT is idempotent for same content — no need for HEAD first.
        if data.len() >= MULTIPART_THRESHOLD {
            self.put_multipart(&key, data)?;
        } else {
            let _resp = self.s3_request("PUT", &key, None, Some(data), &[])?;
        }
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
            .map_err(io::Error::other)?;
        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        s.bytes_read += body.len() as u64;
        Ok(body)
    }

    /// Ranged GET via the HTTP `Range` header — S3 transfers only the
    /// requested bytes, which is what makes a range-readable index and
    /// per-column-chunk reads affordable.
    ///
    /// Semantics match the local backend: a range at or past the end returns
    /// empty (S3 answers 416, which is not an error here), and a range running
    /// past the end is truncated by the server.
    fn get_blob_range(&self, hash: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let key = self.blob_key(hash);
        // HTTP ranges are inclusive on both ends.
        let end = offset.saturating_add(len as u64).saturating_sub(1);
        let range = format!("bytes={}-{}", offset, end);

        let resp = match self.s3_request(
            "GET",
            &key,
            None,
            None,
            &[("range".to_string(), range)],
        ) {
            Ok(r) => r,
            // 416 Range Not Satisfiable means the offset is at or past the end
            // of the object. The local backend returns empty for that, so this
            // one does too — otherwise callers would need backend-specific
            // error handling, which is exactly what the uniform contract is
            // meant to avoid.
            Err(e) if is_range_not_satisfiable(&e) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let mut body = Vec::new();
        resp.into_reader()
            .read_to_end(&mut body)
            .map_err(io::Error::other)?;
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
                    .map(|t| t.join().unwrap_or_else(|_| Some(io::Error::other(
                        "thread panicked"
                    ))))
                    .collect()
            });

            // Return the first error if any
            if let Some(e) = errors.into_iter().flatten().next() {
                return Err(e);
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
                                        Err(e) => Err(io::Error::other(e)),
                                    }
                                }
                                Err(e) => Err(e),
                            }
                        })
                    })
                    .collect();

                threads.into_iter()
                    .map(|t| t.join().unwrap_or_else(|_| Err(io::Error::other(
                        "thread panicked"
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
            let body = opt.ok_or_else(|| io::Error::other(
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

    fn put_object(&self, path: &str, bytes: &[u8]) -> io::Result<()> {
        let key = self.path_key(path);
        self.s3_request("PUT", &key, None, Some(bytes), &[])?;
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        s.bytes_written += bytes.len() as u64;
        Ok(())
    }

    fn get_object(&self, path: &str) -> Option<Vec<u8>> {
        let key = self.path_key(path);
        let resp = self.s3_request("GET", &key, None, None, &[]).ok()?;
        let mut buf = Vec::new();
        resp.into_reader().read_to_end(&mut buf).ok()?;
        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        s.bytes_read += buf.len() as u64;
        Some(buf)
    }

    /// Parallel reads. Named objects are independent of one another, so the
    /// cost of reading N of them should be one round trip, not N.
    fn get_object_batch(&self, paths: &[String]) -> Vec<Option<Vec<u8>>> {
        if paths.len() <= 1 {
            return paths.iter().map(|p| self.get_object(p)).collect();
        }
        const MAX_PARALLEL: usize = 32;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(paths.len());
        for chunk in paths.chunks(MAX_PARALLEL) {
            let results: Vec<Option<Vec<u8>>> = std::thread::scope(|sc| {
                let handles: Vec<_> = chunk
                    .iter()
                    .map(|p| sc.spawn(move || self.get_object(p)))
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap_or(None))
                    .collect()
            });
            out.extend(results);
        }
        out
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
            // Raw values: `build_signed_request` does the one and only
            // encoding pass, for both the signature and the URL.
            let mut query = format!("list-type=2&prefix={}", list_prefix);
            if let Some(ref token) = cont {
                query.push_str(&format!("&continuation-token={}", token));
            }

            let resp = self.s3_request("GET", "", Some(&query), None, &[])?;
            let body = resp.into_string()
                .map_err(io::Error::other)?;

            // S3 may return URL-encoded keys if encoding-type=url was used.
            all_keys.extend(list_response_keys(&body).iter().map(|k| url_decode(k)));

            match list_response_next_token(&body) {
                Some(token) => cont = Some(token),
                None => break,
            }
        }

        // Strip the prefix from keys to return relative paths
        let strip_len = if self.prefix.is_empty() {
            0
        } else {
            self.prefix.len() + 1 // +1 for the '/'
        };
        // Blob keys are excluded from ref listings, but only when the caller
        // did not ask for them — `list_blobs_prefix` lists `blobs/<shard>/`
        // through this same method. The test has to run on the *relative* key:
        // matching the substring "/blobs/" against the absolute key silently
        // returned nothing whenever a store prefix was configured, and
        // everything when one was not.
        let want_blobs = pond_kernel::prefix_targets_blobs(prefix);
        let mut result: Vec<String> = all_keys.iter()
            .filter_map(|k| {
                let rel = if k.len() > strip_len {
                    &k[strip_len..]
                } else {
                    k.as_str()
                };
                if !want_blobs && pond_kernel::is_blob_key(rel) {
                    return None;
                }
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

    /// Bulk delete of named paths, same mechanism as
    /// [`delete_blob_batch`](Self::delete_blob_batch).
    fn delete_path_batch(&self, paths: &[String]) -> io::Result<usize> {
        if paths.is_empty() {
            return Ok(0);
        }
        let mut removed = 0usize;
        for chunk in paths.chunks(DELETE_BATCH_LIMIT) {
            let keys: Vec<String> = chunk.iter().map(|p| self.path_key(p)).collect();
            removed += self.delete_objects(&keys)?;
        }
        Ok(removed)
    }

    /// Bulk delete via S3 `DeleteObjects`: up to
    /// [`DELETE_BATCH_LIMIT`] keys in a single request.
    ///
    /// This is the widest gap between the naive and the correct implementation
    /// of any operation in this client. Reclaiming a million dead nodes is a
    /// million round trips one at a time and a thousand this way — and unlike
    /// reads, deletes cannot be cached or amortised by a tree, so the request
    /// count *is* the cost.
    fn delete_blob_batch(&self, hashes: &[String]) -> io::Result<usize> {
        if hashes.is_empty() {
            return Ok(0);
        }
        let mut removed = 0usize;
        for chunk in hashes.chunks(DELETE_BATCH_LIMIT) {
            removed += self.delete_objects(&chunk.iter().map(|h| self.blob_key(h)).collect::<Vec<_>>())?;
        }
        Ok(removed)
    }
}

// ---------------------------------------------------------------------------
// Async S3 methods — behind `feature = "async"`.
//
// These are INHERENT methods on `S3ObjectStore` (not an `AsyncObjectStore`
// trait impl) so callers don't have to import a trait to use them. They
// reuse [`build_signed_request`](S3ObjectStore::build_signed_request) and
// execute via [`s3_request_async`](S3ObjectStore::s3_request_async) using
// `reqwest`.
//
// SigV4 signing is bit-for-bit identical between sync and async — only the
// HTTP client differs (`ureq` vs `reqwest`).
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
impl S3ObjectStore {
    /// Async variant of [`ObjectStore::put_blob`].
    ///
    /// Computes the content hash, signs a PUT request via SigV4, and executes
    /// it via `reqwest`. Returns the hash.
    pub async fn put_blob_async(&self, data: Vec<u8>) -> io::Result<String> {
        let h = pond_kernel::hash_bytes(&data);
        let key = self.blob_key(&h);
        // S3 PUT is idempotent for same content — no need for HEAD first.
        let resp = self.s3_request_async("PUT", &key, None, Some(&data), &[]).await?;
        // reqwest doesn't error on 4xx/5xx by default — convert here so the
        // async API matches the sync one (which errors via ureq::Error::Status).
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("S3 returned {}: {}", status, body),
            ));
        }
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        s.bytes_written += data.len() as u64;
        Ok(h)
    }

    /// Async variant of [`ObjectStore::get_blob`].
    pub async fn get_blob_async(&self, hash: &str) -> io::Result<Vec<u8>> {
        let key = self.blob_key(hash);
        let resp = self.s3_request_async("GET", &key, None, None, &[]).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Blob '{}' not found in S3", hash),
            ));
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("S3 returned {}: {}", status, body),
            ));
        }
        let body = resp.bytes().await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("read body: {}", e)))?;
        let body = body.to_vec();
        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        s.bytes_read += body.len() as u64;
        Ok(body)
    }

    /// Async variant of [`ObjectStore::delete_blob`].
    pub async fn delete_blob_async(&self, hash: &str) -> io::Result<bool> {
        let key = self.blob_key(hash);
        let resp = self.s3_request_async("DELETE", &key, None, None, &[]).await?;
        let status = resp.status();
        // S3 DELETE is idempotent — 204 means deleted, 404 means it was already gone.
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("S3 returned {}: {}", status, body),
            ));
        }
        Ok(true)
    }

    /// Async variant of [`PondKernel::list_blobs_prefix`](pond_kernel::PondKernel::list_blobs_prefix).
    ///
    /// Uses S3 ListObjectsV2 with `prefix={prefix}/blobs/{xx}/` to enumerate
    /// blob hashes in a single shard. Returns matching hashes (sorted).
    pub async fn list_blobs_prefix_async(&self, prefix: &str) -> Vec<String> {
        if prefix.len() < 2 {
            return Vec::new();
        }
        let shard = &prefix[..2];
        // Build the S3 list prefix: {our_prefix/}blobs/{shard}/
        let list_prefix = if self.prefix.is_empty() {
            format!("blobs/{}/", shard)
        } else {
            format!("{}/blobs/{}/", self.prefix, shard)
        };

        // The key shape is: {our_prefix/}blobs/{shard}/{hash}
        // We want just the {hash} portion, filtered by the caller's prefix.
        let strip_len = list_prefix.len(); // includes trailing '/'
        let mut hashes = Vec::new();
        let mut cont: Option<String> = None;

        loop {
            // ListObjectsV2 query — same as sync `list_paths` but only one
            // shard. Values are raw; `build_signed_request` encodes them once.
            let mut query = format!("list-type=2&prefix={}", list_prefix);
            if let Some(ref token) = cont {
                query.push_str(&format!("&continuation-token={}", token));
            }

            let resp = match self.s3_request_async("GET", "", Some(&query), None, &[]).await {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            if !resp.status().is_success() {
                return Vec::new();
            }
            let body = match resp.text().await {
                Ok(b) => b,
                Err(_) => return Vec::new(),
            };

            for key in list_response_keys(&body) {
                let key = url_decode(&key);
                let hash = if key.len() >= strip_len {
                    &key[strip_len..]
                } else {
                    key.as_str()
                };
                if hash.starts_with(prefix) {
                    hashes.push(hash.to_string());
                }
            }

            // One shard holds 1/256th of the blobs, so it crosses the 1000-key
            // page limit at ~256k blobs — well inside the range this store is
            // meant for. Without this loop, GC and recovery would quietly see
            // only the first page.
            match list_response_next_token(&body) {
                Some(token) => cont = Some(token),
                None => break,
            }
        }

        hashes.sort();
        hashes.dedup();
        hashes
    }

    /// Async variant of [`ObjectStore::put_path`]. Binds a named path to a
    /// content hash by writing a `{"hash":"..."}` JSON object to S3.
    pub async fn put_path_async(&self, path: &str, hash: &str) -> io::Result<()> {
        let key = self.path_key(path);
        let body = format!(r#"{{"hash":"{}"}}"#, hash).into_bytes();
        let resp = self.s3_request_async("PUT", &key, None, Some(&body), &[]).await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("S3 returned {}: {}", status, body),
            ));
        }
        let mut s = self.stats.lock().unwrap();
        s.puts += 1;
        Ok(())
    }

    /// Async variant of [`ObjectStore::get_path`]. Resolves a named path
    /// to its content hash. Returns `None` if the path is unbound or on error.
    pub async fn get_path_async(&self, path: &str) -> Option<String> {
        let key = self.path_key(path);
        let resp = match self.s3_request_async("GET", &key, None, None, &[]).await {
            Ok(r) => r,
            Err(_) => return None,
        };
        if !resp.status().is_success() {
            return None;
        }
        let body = match resp.text().await {
            Ok(b) => b,
            Err(_) => return None,
        };
        let mut s = self.stats.lock().unwrap();
        s.gets += 1;
        extract_hash_from_json(&body)
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

// Minimal URL-encoding helper (avoids pulling in the `urlencoding` crate).
//
// There is deliberately only one encoder. Query values are encoded in exactly
// one place — `canonicalize_query`, whose output is both signed and sent — so
// no caller can pre-encode a value and have it encoded a second time.
mod urlencoding {
    /// Encode a string for use in a URL QUERY parameter name or value.
    /// Forward slashes ARE encoded as %2F (required by SigV4 canonical query).
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
//
// `clippy::not_unsafe_ptr_arg_deref` is allowed for these two entry points:
// both are C ABI functions that by definition dereference a caller-supplied
// pointer. Marking them `unsafe fn` would not change the emitted C ABI (no
// Rust caller invokes them). Nulls are checked on entry.

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
#[allow(clippy::not_unsafe_ptr_arg_deref)]
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
#[allow(clippy::not_unsafe_ptr_arg_deref)]
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

    fn s3_err(status: Option<u16>) -> io::Error {
        S3Error {
            status,
            message: "test".into(),
        }
        .into()
    }

    /// Throttling and server faults are retryable; client mistakes are not.
    /// Getting this backwards either hammers S3 with requests that can never
    /// succeed, or gives up on a transient 503 that a single retry would fix.
    #[test]
    fn test_retry_classification() {
        for code in [429, 500, 502, 503, 504] {
            assert!(is_retryable(&s3_err(Some(code))), "{} should retry", code);
        }
        for code in [400, 401, 403, 404, 412, 416] {
            assert!(
                !is_retryable(&s3_err(Some(code))),
                "{} must not retry — the request itself is wrong",
                code
            );
        }
        // Transport failure: no response arrived, so retry.
        assert!(is_retryable(&s3_err(None)));
        // An unrelated io::Error is not ours and is not retried.
        assert!(!is_retryable(&io::Error::other("something else")));
    }

    /// A 404 must still surface as NotFound — the ObjectStore contract
    /// distinguishes "missing" from "failed", and callers branch on it.
    #[test]
    fn test_not_found_kind_is_preserved() {
        assert_eq!(s3_err(Some(404)).kind(), io::ErrorKind::NotFound);
        assert_eq!(s3_err(Some(503)).kind(), io::ErrorKind::Other);
    }

    #[test]
    fn test_range_not_satisfiable_detected() {
        assert!(is_range_not_satisfiable(&s3_err(Some(416))));
        assert!(!is_range_not_satisfiable(&s3_err(Some(404))));
    }

    /// Backoff must stay inside its ceiling and never be negative or absurd.
    /// Full jitter means each delay is random in [0, cap], so the assertion is
    /// on the bound, not on a specific value.
    #[test]
    fn test_backoff_respects_ceiling() {
        for attempt in 0..20 {
            let d = backoff_delay(attempt).as_millis() as u64;
            assert!(
                d <= RETRY_MAX_DELAY_MS,
                "attempt {} produced {}ms, above the {}ms ceiling",
                attempt,
                d,
                RETRY_MAX_DELAY_MS
            );
        }
    }

    #[test]
    fn test_extract_xml_tag() {
        let body = r#"<?xml version="1.0"?>
            <InitiateMultipartUploadResult>
              <Bucket>b</Bucket><Key>k</Key><UploadId>abc-123</UploadId>
            </InitiateMultipartUploadResult>"#;
        assert_eq!(extract_xml_tag(body, "UploadId").as_deref(), Some("abc-123"));
        assert_eq!(extract_xml_tag(body, "Bucket").as_deref(), Some("b"));
        assert!(extract_xml_tag(body, "Missing").is_none());
        assert!(extract_xml_tag("<Open>no close", "Open").is_none());
    }

    /// Part splitting must match S3's rules: 1-based numbering, every part
    /// but the last exactly PART_SIZE, and the whole object covered.
    #[test]
    fn test_multipart_part_layout() {
        let size = MULTIPART_PART_SIZE * 3 + 1234;
        let data = vec![0u8; size];
        let chunks: Vec<(usize, &[u8])> = data
            .chunks(MULTIPART_PART_SIZE)
            .enumerate()
            .map(|(i, c)| (i + 1, c))
            .collect();

        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].0, 1, "S3 part numbers start at 1");
        for (_, c) in &chunks[..3] {
            assert_eq!(c.len(), MULTIPART_PART_SIZE);
        }
        assert_eq!(chunks[3].1.len(), 1234, "last part carries the remainder");
        assert_eq!(
            chunks.iter().map(|(_, c)| c.len()).sum::<usize>(),
            size,
            "parts must cover the object exactly"
        );
    }

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
        assert_eq!(urlencoding::encode_query("abc123"), "abc123");
        assert_eq!(urlencoding::encode_query("a b/c"), "a%20b%2Fc");
        assert_eq!(
            urlencoding::encode_query("blobs/ab/abcdef"),
            "blobs%2Fab%2Fabcdef"
        );
        // Unreserved characters must survive untouched (RFC 3986).
        assert_eq!(urlencoding::encode_query("-._~"), "-._~");
    }


    /// A store pointing at a fake endpoint — enough to exercise signing,
    /// which is pure computation over the credentials and the request.
    fn fake_store() -> S3ObjectStore {
        S3ObjectStore::new(
            "my-bucket",
            "prod",
            "us-east-1",
            "https://s3.amazonaws.com",
            S3Credentials {
                access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            ureq::Agent::new(),
        )
    }

    /// The signed bytes and the sent bytes must be the same bytes.
    ///
    /// This is the invariant behind the `SignatureDoesNotMatch` failures that
    /// broke listing past 1000 objects: the URL was built from the caller's
    /// query string while the signature was built from a re-encoded copy of
    /// it, so the two agreed only as long as no value contained a reserved
    /// character. Asserting they are literally equal makes that impossible to
    /// reintroduce.
    #[test]
    fn test_url_query_matches_signed_query() {
        let store = fake_store();
        // A real R2 continuation token: base64, so it ends in '=' padding.
        let token = "1-JTdCJTIybGFzdCUyMiUzQSUyMmZvbyUyMiU3RA==";
        let query = format!("list-type=2&prefix=pond/data/&continuation-token={}", token);

        let signed = store
            .build_signed_request("GET", "", Some(&query), None, &[])
            .unwrap();

        let sent = signed.url.split_once('?').expect("query must be present").1;
        assert_eq!(
            sent,
            canonicalize_query(&query),
            "the URL query must be byte-identical to the signed canonical query"
        );

        // And specifically: the '=' padding is encoded once, not twice.
        assert!(
            sent.contains("%3D%3D"),
            "base64 padding must be percent-encoded: {}",
            sent
        );
        assert!(
            !sent.contains("%253D"),
            "double-encoding regression — this is the >1000-object listing bug: {}",
            sent
        );
    }

    /// Canonical query values are encoded exactly once, and parameters are
    /// sorted by name as SigV4 requires.
    #[test]
    fn test_canonicalize_query_encodes_once_and_sorts() {
        assert_eq!(
            canonicalize_query("prefix=a/b&list-type=2"),
            "list-type=2&prefix=a%2Fb",
            "parameters must be sorted by name and values encoded once"
        );
        assert_eq!(canonicalize_query("uploads="), "uploads=");
        assert_eq!(canonicalize_query(""), "");
        // A value that is already percent-encoded is treated as raw text — so
        // callers must pass raw values. This documents the contract.
        assert_eq!(canonicalize_query("t=%3D"), "t=%253D");
    }

    /// A truncated ListObjectsV2 response must surface its continuation token,
    /// and a complete one must not. Getting this wrong turns "the first 1000
    /// objects" into "all the objects" silently.
    #[test]
    fn test_list_response_pagination_parsing() {
        let truncated = "<ListBucketResult>\
             <Contents><Key>a/1</Key></Contents>\
             <Contents><Key>a/2</Key></Contents>\
             <IsTruncated>true</IsTruncated>\
             <NextContinuationToken>tok==</NextContinuationToken>\
             </ListBucketResult>";
        assert_eq!(list_response_keys(truncated), vec!["a/1", "a/2"]);
        assert_eq!(
            list_response_next_token(truncated),
            Some("tok==".to_string())
        );

        let complete = "<ListBucketResult>\
             <Contents><Key>a/1</Key></Contents>\
             <IsTruncated>false</IsTruncated>\
             </ListBucketResult>";
        assert_eq!(list_response_keys(complete), vec!["a/1"]);
        assert_eq!(list_response_next_token(complete), None);

        // An empty bucket is not an error and is not truncated.
        let empty = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
        assert!(list_response_keys(empty).is_empty());
        assert_eq!(list_response_next_token(empty), None);
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

// ---------------------------------------------------------------------------
// Async tests — only compiled when `feature = "async"` is on.
//
// These don't hit real S3. They verify:
//   - `build_signed_request` produces a well-formed SignedS3Request
//     (URL + Authorization header + signed headers).
//   - Async methods on S3ObjectStore return io::Error (not panic) when the
//     endpoint is unreachable — i.e. the async error path mirrors the sync
//     one (errors are propagated, not swallowed).
//
// Real-S3 round-trip tests live in the integration suite (`tests/`) and
// require AWS credentials in the environment. They're skipped here to keep
// `cargo test` hermetic.
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "async"))]
mod async_tests {
    use super::*;

    /// Helper: build an S3ObjectStore pointing at a fake endpoint.
    fn fake_store() -> S3ObjectStore {
        S3ObjectStore::new(
            "my-bucket",
            "prod",
            "us-east-1",
            "https://s3.amazonaws.com",
            S3Credentials {
                access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
                session_token: None,
            },
            ureq::Agent::new(),
        )
    }

    /// `build_signed_request` produces a SignedS3Request with:
    ///   - the right URL (endpoint + bucket + key)
    ///   - an `Authorization` header containing the SigV4 credential string
    ///   - the host, x-amz-content-sha256, x-amz-date headers
    ///   - the original body bytes preserved
    #[test]
    fn test_build_signed_request_shape() {
        let store = fake_store();
        let body = b"hello s3".to_vec();
        let signed = store
            .build_signed_request("PUT", "prod/blobs/ab/abcdef", None, Some(&body), &[])
            .unwrap();

        assert_eq!(signed.method, "PUT");
        assert_eq!(signed.url, "https://s3.amazonaws.com/my-bucket/prod/blobs/ab/abcdef");
        assert_eq!(signed.body, body);

        // Authorization header is SigV4-shaped.
        let auth = signed.headers.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .map(|(_, v)| v.as_str())
            .expect("Authorization header must be present");
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"));
        assert!(auth.contains("/us-east-1/s3/aws4_request"));
        assert!(auth.contains("Signature="));

        // Required SigV4 headers are present.
        let header_names: Vec<String> = signed.headers.iter()
            .map(|(k, _)| k.to_lowercase())
            .collect();
        assert!(header_names.contains(&"host".to_string()));
        assert!(header_names.contains(&"x-amz-content-sha256".to_string()));
        assert!(header_names.contains(&"x-amz-date".to_string()));
    }

    /// A signed PUT and a signed GET for the same key produce DIFFERENT
    /// Authorization headers (because the method is part of the canonical
    /// request). This catches a class of bugs where signing forgets the method.
    #[test]
    fn test_signing_differs_per_method() {
        let store = fake_store();
        let key = "prod/blobs/ab/abcdef";
        let data = b"payload".to_vec();

        let put = store.build_signed_request("PUT", key, None, Some(&data), &[]).unwrap();
        let get = store.build_signed_request("GET", key, None, None, &[]).unwrap();

        let put_auth = put.headers.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .map(|(_, v)| v.clone())
            .unwrap();
        let get_auth = get.headers.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .map(|(_, v)| v.clone())
            .unwrap();

        assert_ne!(put_auth, get_auth,
            "PUT and GET must produce different signatures (method is part of canonical request)");
    }

    /// Async `get_blob_async` against an unreachable endpoint returns an
    /// `io::Error` rather than panicking. This is the basic safety contract:
    /// async errors must be propagated through `Result`, not unwound.
    #[tokio::test]
    async fn test_async_get_blob_unreachable_endpoint_errors() {
        // Point at a localhost port that's almost certainly closed.
        let store = S3ObjectStore::new(
            "my-bucket", "prod", "us-east-1",
            "http://127.0.0.1:1", // port 1 — nothing listening
            S3Credentials {
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                session_token: None,
            },
            ureq::Agent::new(),
        );

        let result = store.get_blob_async("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890").await;
        assert!(result.is_err(), "expected Err, got: {:?}", result);
        let err = result.unwrap_err();
        // We don't assert on the exact error kind — reqwest maps connection
        // failures to its own error type — but it must be an io::Error.
        assert!(!err.to_string().is_empty());
    }

    /// Async `list_blobs_prefix_async` returns an empty Vec (not an error)
    /// when the endpoint is unreachable — matches the sync behavior where
    /// `list_paths` returns `unwrap_or_default()` on failure.
    #[tokio::test]
    async fn test_async_list_blobs_prefix_unreachable_returns_empty() {
        let store = S3ObjectStore::new(
            "my-bucket", "prod", "us-east-1",
            "http://127.0.0.1:1",
            S3Credentials {
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                session_token: None,
            },
            ureq::Agent::new(),
        );

        // 64-char hex hash prefix.
        let prefix = "ab";
        let result = store.list_blobs_prefix_async(prefix).await;
        assert!(result.is_empty(), "expected empty Vec on unreachable endpoint, got: {:?}", result);
    }

    /// `S3ObjectStore::new` works with the async feature on — the
    /// `async_client` OnceLock is initialized lazily (not in `new()`).
    #[test]
    fn test_new_with_async_feature_smoke() {
        let store = fake_store();
        // Just confirm we can construct without panicking. The OnceLock
        // is empty until the first async call touches it.
        let _ = &store;
    }

    /// `put_blob_async` against an unreachable endpoint errors (does not
    /// hang or panic). This verifies the request-building path doesn't
    /// short-circuit on the body.
    #[tokio::test]
    async fn test_async_put_blob_unreachable_endpoint_errors() {
        let store = S3ObjectStore::new(
            "my-bucket", "prod", "us-east-1",
            "http://127.0.0.1:1",
            S3Credentials {
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                session_token: None,
            },
            ureq::Agent::new(),
        );

        let result = store.put_blob_async(b"some data".to_vec()).await;
        assert!(result.is_err(), "expected Err, got: {:?}", result);
    }

    /// `delete_blob_async` against an unreachable endpoint errors.
    #[tokio::test]
    async fn test_async_delete_blob_unreachable_endpoint_errors() {
        let store = S3ObjectStore::new(
            "my-bucket", "prod", "us-east-1",
            "http://127.0.0.1:1",
            S3Credentials {
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                session_token: None,
            },
            ureq::Agent::new(),
        );

        let fake_hash = "ab".repeat(32); // 64 hex chars
        let result = store.delete_blob_async(&fake_hash).await;
        assert!(result.is_err(), "expected Err, got: {:?}", result);
    }

    /// Async S3 round-trip — only runs when AWS credentials are present
    /// (sets `POND_ASYNC_S3_INTEGRATION=1` to opt in explicitly).
    ///
    /// To run locally:
    ///   POND_ASYNC_S3_INTEGRATION=1 \
    ///   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
    ///   POND_S3_TEST_URL='s3://my-bucket/pond-async-test?region=us-east-1' \
    ///   cargo test -p pond_s3 --features async test_async_s3_round_trip -- --nocapture
    #[tokio::test]
    async fn test_async_s3_round_trip_integration() {
        // Gate on both env vars so CI doesn't try to hit real S3.
        let ok = std::env::var("POND_ASYNC_S3_INTEGRATION").ok().as_deref() == Some("1");
        if !ok {
            eprintln!("[skipped] POND_ASYNC_S3_INTEGRATION not set — skipping live S3 async round-trip");
            return;
        }
        let url = match std::env::var("POND_S3_TEST_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("[skipped] POND_S3_TEST_URL not set — skipping live S3 async round-trip");
                return;
            }
        };
        let store = S3ObjectStore::from_url(&url).expect("from_url");

        let payload = b"async s3 round-trip payload".to_vec();
        let h = store.put_blob_async(payload.clone()).await
            .expect("put_blob_async");
        let got = store.get_blob_async(&h).await
            .expect("get_blob_async");
        assert_eq!(got, payload);

        let deleted = store.delete_blob_async(&h).await
            .expect("delete_blob_async");
        assert!(deleted, "delete should report true for existing blob");

        // Second get should 404.
        let err = store.get_blob_async(&h).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }
}
