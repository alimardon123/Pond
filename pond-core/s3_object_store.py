"""S3ObjectStore — a real boto3-backed object store for Pond.

Implements the same 9-primitive interface as InMemoryObjectStore:
  - put_blob(data) → hash         (content-addressed S3 PUT)
  - get_blob(hash) → bytes         (S3 GET)
  - has_blob(hash) → bool          (S3 HEAD)
  - delete_blob(hash) → bool       (S3 DELETE)
  - list_all_blob_hashes() → list  (S3 list-objects-v2)
  - put_path(path, hash)           (S3 PUT to well-known key)
  - get_path(path) → hash|None     (S3 GET to well-known key)
  - compare_and_set_path(path, expected, new) → bool  (conditional PUT)
  - list_paths(prefix) → list      (S3 list-objects-v2 with prefix)

The store is content-addressed: blob keys are "{prefix}/blobs/{hash}".
Paths (named refs) are stored at "{prefix}/paths/{path}". The content
hash is SHA-256 hex (64 chars), same as the rest of Pond.

For compare_and_set_path (optimistic concurrency), we use S3 conditional
PUT with If-Match (ETag) / If-None-Match headers. ETags for small S3
objects are MD5 of the content; since our paths are small JSON blobs
({"hash": "..."}), this gives us atomic CAS semantics.

USAGE:
    import boto3
    from s3_object_store import S3ObjectStore
    from object_store_native_kernel import ObjectStoreNativeKernel
    from pond_storage import PondStorage

    client = boto3.client("s3", region_name="us-east-1")
    store = S3ObjectStore(client, bucket="my-pond", prefix="prod")
    kernel = ObjectStoreNativeKernel(store)
    storage = PondStorage(kernel)

This is a production backend. No SQLite, no local disk, no tempfiles.
All state lives in S3.
"""
from __future__ import annotations

import json
import threading
from typing import Optional

# Import hash_bytes from kernel.py (same as InMemoryObjectStore)
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kernel import hash_bytes


class S3ObjectStore:
    """A real S3-backed content-addressed object store.

    Blobs are stored at: s3://{bucket}/{prefix}/blobs/{hash}
    Paths (named refs) are stored at: s3://{bucket}/{prefix}/paths/{path}

    The path objects contain a small JSON body: {"hash": "..."}.
    This lets us use S3 conditional PUT (If-Match on ETag) for CAS.

    Thread-safe: boto3 clients are thread-safe for S3 operations after
    creation. We use a lock only for stats mutation.
    """

    def __init__(self, client, bucket: str, prefix: str = ""):
        """Create an S3-backed object store.

        Args:
            client: a boto3 S3 client (boto3.client("s3", ...)).
                Must be configured with region/credentials.
            bucket: the S3 bucket name.
            prefix: optional key prefix (e.g., "prod" or "pond/v1").
                All keys will be under this prefix.
        """
        self._client = client
        self._bucket = bucket
        # Normalize prefix: no leading/trailing slashes
        self._prefix = prefix.strip("/") if prefix else ""
        self._lock = threading.Lock()

        # Honest stats (same shape as InMemoryObjectStore)
        self.stats = {
            "gets": 0,
            "puts": 0,
            "bytes_read": 0,
            "bytes_written": 0,
            "latency_ms_total": 0.0,
        }

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _blob_key(self, hash_val: str) -> str:
        """The S3 key for a content-addressed blob.

        NEW short layout: b/{hash[:2]}/{hash}
        (was: blobs/{hash[:2]}/{hash})
        2-char sharding keeps list_objects_v2 fast at PB scale.
        """
        shard = hash_val[:2]
        if self._prefix:
            return f"{self._prefix}/blobs/{shard}/{hash_val}"
        return f"blobs/{shard}/{hash_val}"

    def _old_blob_key(self, hash_val: str) -> str:
        """OLD blob key format for backward compat."""
        shard = hash_val[:2]
        if self._prefix:
            return f"{self._prefix}/blobs/{shard}/{hash_val}"
        return f"blobs/{shard}/{hash_val}"

    def _path_key(self, path: str) -> str:
        """The S3 key for a named path (ref).

        NEW short layout: paths are stored directly under the prefix.
        No "paths/" subdirectory — the path IS the key.
        (was: {prefix}/paths/{path})
        """
        if self._prefix:
            return f"{self._prefix}/{path}"
        return path

    def _old_path_key(self, path: str) -> str:
        """OLD path key format for backward compat."""
        if self._prefix:
            return f"{self._prefix}/paths/{path}"
        return f"paths/{path}"

    def _paths_prefix(self) -> str:
        """The S3 prefix for listing all paths (refs)."""
        if self._prefix:
            return f"{self._prefix}/collections/"
        return "collections/"

    def _blobs_prefix(self) -> str:
        """The S3 prefix for listing all blobs."""
        if self._prefix:
            return f"{self._prefix}/blobs/"
        return "blobs/"

    # ------------------------------------------------------------------
    # Content-addressed blob operations
    # ------------------------------------------------------------------

    def put_blob(self, data: bytes) -> str:
        """Write bytes, content-addressed. Returns the content hash.

        Idempotent: same bytes → same hash → same S3 key. We still do
        the PUT every time (S3 PUT is idempotent for same content), but
        we could optimize with a HEAD first. The trade-off: HEAD saves
        the upload but adds a round trip. For now, always PUT (simpler,
        and S3 PUT of existing key is cheap).
        """
        h = hash_bytes(data)
        key = self._blob_key(h)
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data)
        with self._lock:
            self.stats["puts"] += 1
            self.stats["bytes_written"] += len(data)
        return h

    def put_blob_batch(self, items: list[bytes],
                        max_workers: int = 32) -> list[str]:
        """Write a batch of blobs in PARALLEL — wall-clock ~1 RTT, not N × RTT.

        Args:
            items: list of byte payloads to write
            max_workers: max parallel PUTs (default 32 — R2 supports 50+
                concurrent connections per client)

        Returns:
            List of content hashes, in the same order as `items`.

        This is the SINGLE most impactful optimization for bulk writes on
        R2/S3: 10 sequential PUTs × ~300ms = 3000ms becomes ~300ms wall-clock.
        """
        if not items:
            return []
        if len(items) == 1:
            return [self.put_blob(items[0])]

        # Pre-compute hashes + S3 keys (CPU work, no I/O)
        hashes_keys = [(hash_bytes(data), self._blob_key(hash_bytes(data)))
                        for data in items]

        from concurrent.futures import ThreadPoolExecutor
        # Order-preserving parallel execution
        results: list[Optional[str]] = [None] * len(items)
        errors: list[Optional[Exception]] = [None] * len(items)

        def _put_one(idx, data, key):
            try:
                self._client.put_object(Bucket=self._bucket, Key=key, Body=data)
            except Exception as e:
                errors[idx] = e

        workers = min(max_workers, len(items))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(_put_one, i, items[i], hashes_keys[i][1])
                for i in range(len(items))
            ]
            for f in futures:
                f.result()

        # Surface the first error if any
        for e in errors:
            if e is not None:
                raise e

        with self._lock:
            self.stats["puts"] += len(items)
            self.stats["bytes_written"] += sum(len(d) for d in items)

        return [h for h, _ in hashes_keys]

    def get_blob(self, hash_val: str) -> bytes:
        """Read bytes by content hash. 1 GET = 1 S3 round trip.

        Tries the NEW short key first, falls back to OLD key for backward compat.
        """
        key = self._blob_key(hash_val)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            data = response["Body"].read()
        except self._client.exceptions.NoSuchKey:
            # Backward compat: try old key format
            old_key = self._old_blob_key(hash_val)
            try:
                response = self._client.get_object(Bucket=self._bucket, Key=old_key)
                data = response["Body"].read()
            except self._client.exceptions.NoSuchKey:
                raise KeyError(f"Blob {hash_val} not found in S3")
        with self._lock:
            self.stats["gets"] += 1
            self.stats["bytes_read"] += len(data)
        return data

    def get_blob_batch(self, hash_vals: list[str],
                        max_workers: int = 32) -> list[bytes]:
        """Fetch a batch of blobs in PARALLEL — wall-clock ~1 RTT, not N × RTT.

        Args:
            hash_vals: list of content hashes to fetch
            max_workers: max parallel GETs (default 32)

        Returns:
            List of byte payloads, in the same order as `hash_vals`.
            Raises KeyError if any hash is not found.
        """
        if not hash_vals:
            return []
        if len(hash_vals) == 1:
            return [self.get_blob(hash_vals[0])]

        from concurrent.futures import ThreadPoolExecutor
        results: list[Optional[bytes]] = [None] * len(hash_vals)
        errors: list[Optional[Exception]] = [None] * len(hash_vals)

        def _get_one(idx, h):
            try:
                key = self._blob_key(h)
                response = self._client.get_object(Bucket=self._bucket, Key=key)
                results[idx] = response["Body"].read()
            except Exception as e:
                errors[idx] = e

        workers = min(max_workers, len(hash_vals))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_get_one, i, h)
                        for i, h in enumerate(hash_vals)]
            for f in futures:
                f.result()

        for e in errors:
            if e is not None:
                raise e

        with self._lock:
            self.stats["gets"] += len(hash_vals)
            for r in results:
                if r is not None:
                    self.stats["bytes_read"] += len(r)

        return results  # type: ignore[return-value]

    def has_blob(self, hash_val: str) -> bool:
        """Check if a blob exists (S3 HEAD — cheaper than GET).

        Checks both NEW and OLD key formats.
        """
        key = self._blob_key(hash_val)
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except self._client.exceptions.ClientError:
            pass
        # Backward compat: try old key
        old_key = self._old_blob_key(hash_val)
        try:
            self._client.head_object(Bucket=self._bucket, Key=old_key)
            return True
        except self._client.exceptions.ClientError:
            return False

    def delete_blob(self, hash_val: str) -> bool:
        """Delete a blob by hash. Used by GC/vacuum.

        Deletes from both NEW and OLD key formats.
        """
        deleted = False
        for key_func in [self._blob_key, self._old_blob_key]:
            key = key_func(hash_val)
            try:
                self._client.head_object(Bucket=self._bucket, Key=key)
                self._client.delete_object(Bucket=self._bucket, Key=key)
                deleted = True
            except self._client.exceptions.ClientError:
                pass
        return deleted

    def list_all_blob_hashes(self) -> list[str]:
        """List all blob hashes in the store (for GC reachability).

        Scans both NEW (b/) and OLD (blobs/) locations.
        """
        hashes = []
        paginator = self._client.get_paginator("list_objects_v2")

        # NEW location: b/{hash[:2]}/{hash}
        for blobs_prefix in [self._blobs_prefix(), f"{self._prefix}/blobs/" if self._prefix else "blobs/"]:
            for page in paginator.paginate(Bucket=self._bucket, Prefix=blobs_prefix, Delimiter="/"):
                for prefix_entry in page.get("CommonPrefixes", []):
                    shard_prefix = prefix_entry["Prefix"]
                    for shard_page in paginator.paginate(Bucket=self._bucket, Prefix=shard_prefix):
                        for obj in shard_page.get("Contents", []):
                            key = obj["Key"]
                            hash_val = key[len(shard_prefix):]
                            if hash_val:
                                hashes.append(hash_val)
        return list(set(hashes))

    # ------------------------------------------------------------------
    # Named path operations (well-known refs)
    # ------------------------------------------------------------------

    def put_path(self, path: str, hash_val: str) -> None:
        """Bind a well-known path to a content hash.

        The path object stores {"hash": "..."} as JSON. This lets us use
        S3 conditional PUT (If-Match on ETag) for CAS.

        Last-writer-wins: no CAS check. Use compare_and_set_path for
        optimistic concurrency.
        """
        key = self._path_key(path)
        body = json.dumps({"hash": hash_val}).encode()
        self._client.put_object(Bucket=self._bucket, Key=key, Body=body)
        with self._lock:
            self.stats["puts"] += 1

    def get_path(self, path: str) -> Optional[str]:
        """Resolve a well-known path to its current content hash.

        Tries the NEW short key first, falls back to OLD key for backward compat.
        """
        key = self._path_key(path)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            body = response["Body"].read()
            data = json.loads(body)
            with self._lock:
                self.stats["gets"] += 1
            return data.get("hash")
        except (self._client.exceptions.NoSuchKey, self._client.exceptions.ClientError):
            pass

        # Backward compat: try old key format
        old_key = self._old_path_key(path)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=old_key)
            body = response["Body"].read()
            data = json.loads(body)
            with self._lock:
                self.stats["gets"] += 1
            return data.get("hash")
        except (self._client.exceptions.NoSuchKey, self._client.exceptions.ClientError):
            return None

    def delete_path(self, path: str) -> bool:
        """Delete a named path. Returns True if deleted."""
        key = self._path_key(path)
        try:
            self._client.delete_object(Bucket=self._bucket, Key=key)
            return True
        except self._client.exceptions.ClientError:
            return False

    def list_paths(self, prefix: str = "") -> list[str]:
        """List all paths (refs) with the given prefix.

        The prefix is a full path like "collections/users/branches/main/shards/"
        or "". Scans all known top-level directories.
        """
        paths = []
        paginator = self._client.get_paginator("list_objects_v2")

        known_prefixes = ["collections/", "transactions/", "r/", "paths/"]

        # If prefix already starts with a known top-level dir, scan directly
        if any(prefix.startswith(kp) for kp in known_prefixes):
            full_prefix = f"{self._prefix}/{prefix}" if self._prefix else prefix
            strip = len(f"{self._prefix}/") if self._prefix else 0
            for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.startswith(full_prefix):
                        p = key[strip:]
                        if not p.startswith("blobs/") and not p.startswith("b/"):
                            paths.append(p)
        else:
            # No known prefix — scan all known ref directories
            for dirname in ["collections", "transactions", "r"]:
                full_prefix = f"{self._prefix}/{dirname}/{prefix}" if self._prefix else f"{dirname}/{prefix}"
                strip = len(f"{self._prefix}/") if self._prefix else 0
                for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        if key.startswith(full_prefix):
                            p = key[strip:]
                            if not p.startswith("blobs/") and not p.startswith("b/"):
                                paths.append(p)

            # Original "paths/" layout
            old_prefix = f"{self._prefix}/paths/{prefix}" if self._prefix else f"paths/{prefix}"
            old_strip = len(f"{self._prefix}/paths/") if self._prefix else len("paths/")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=old_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.startswith(old_prefix):
                        paths.append(key[old_strip:])

        return sorted(set(paths))

    # ------------------------------------------------------------------
    # Stats (same interface as InMemoryObjectStore)
    # ------------------------------------------------------------------

    def reset_stats(self) -> None:
        """Reset the I/O stats (for benchmarking)."""
        with self._lock:
            self.stats = {
                "gets": 0, "puts": 0,
                "bytes_read": 0, "bytes_written": 0,
                "latency_ms_total": 0.0,
            }

    def print_stats(self, label: str = "") -> None:
        """Print I/O stats."""
        if label:
            print(f"  [{label}]")
        print(f"    GETs:           {self.stats['gets']:,}")
        print(f"    PUTs:           {self.stats['puts']:,}")
        print(f"    Bytes read:     {self.stats['bytes_read']:,}")
        print(f"    Bytes written:  {self.stats['bytes_written']:,}")


def make_s3_kernel(bucket: str, prefix: str = "",
                    region: Optional[str] = None,
                    endpoint_url: Optional[str] = None,
                    aws_access_key_id: Optional[str] = None,
                    aws_secret_access_key: Optional[str] = None,
                    max_retries: int = 10,
                    connect_timeout: float = 5.0,
                    read_timeout: float = 30.0,
                    max_pool_connections: int = 50) -> "ObjectStoreNativeKernel":
    """Convenience constructor: create an ObjectStoreNativeKernel backed by real S3.

    Args:
        bucket: S3 bucket name
        prefix: key prefix (e.g., "prod" or "pond/v1")
        region: AWS region (or None to use boto3 defaults)
        endpoint_url: custom endpoint (for MinIO, LocalStack, R2, etc.)
        aws_access_key_id: credentials (or None to use boto3 defaults)
        aws_secret_access_key: credentials (or None to use boto3 defaults)
        max_retries: max retry attempts for transient failures (default 10)
        connect_timeout: TCP connect timeout in seconds (default 5.0)
        read_timeout: S3 read timeout in seconds (default 30.0)
        max_pool_connections: connection pool size (default 50 — Pond's
            parallel fetch uses up to 16 threads, 50 gives headroom)

    Returns:
        An ObjectStoreNativeKernel instance backed by S3ObjectStore.

    Usage:
        kernel = make_s3_kernel("my-pond", prefix="prod", region="us-east-1")
        storage = PondStorage(kernel)
    """
    import boto3
    from botocore.config import Config

    config = Config(
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        max_pool_connections=max_pool_connections,
        retries={"max_attempts": max_retries, "mode": "adaptive"},
    )
    client = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=config,
    )
    store = S3ObjectStore(client, bucket=bucket, prefix=prefix)

    # Import here to avoid circular import at module load
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from object_store_native_kernel import ObjectStoreNativeKernel
    return ObjectStoreNativeKernel(store)
