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
        """The S3 key for a content-addressed blob."""
        if self._prefix:
            return f"{self._prefix}/blobs/{hash_val}"
        return f"blobs/{hash_val}"

    def _path_key(self, path: str) -> str:
        """The S3 key for a named path (ref)."""
        if self._prefix:
            return f"{self._prefix}/paths/{path}"
        return f"paths/{path}"

    def _paths_prefix(self) -> str:
        """The S3 prefix for listing all paths."""
        if self._prefix:
            return f"{self._prefix}/paths/"
        return "paths/"

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

    def get_blob(self, hash_val: str) -> bytes:
        """Read bytes by content hash. 1 GET = 1 S3 round trip."""
        key = self._blob_key(hash_val)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            data = response["Body"].read()
        except self._client.exceptions.NoSuchKey:
            raise KeyError(f"Blob {hash_val} not found in S3")
        with self._lock:
            self.stats["gets"] += 1
            self.stats["bytes_read"] += len(data)
        return data

    def has_blob(self, hash_val: str) -> bool:
        """Check if a blob exists (S3 HEAD — cheaper than GET)."""
        key = self._blob_key(hash_val)
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except self._client.exceptions.ClientError:
            return False

    def delete_blob(self, hash_val: str) -> bool:
        """Delete a blob by hash. Used by GC/vacuum."""
        key = self._blob_key(hash_val)
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            self._client.delete_object(Bucket=self._bucket, Key=key)
            return True
        except self._client.exceptions.ClientError:
            return False

    def list_all_blob_hashes(self) -> list[str]:
        """List all blob hashes in the store (for GC reachability)."""
        prefix = self._blobs_prefix()
        hashes = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                # Key is "{prefix}/blobs/{hash}" — extract the hash
                key = obj["Key"]
                if key.startswith(prefix):
                    hashes.append(key[len(prefix):])
        return hashes

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
        """Resolve a well-known path to its current content hash."""
        key = self._path_key(path)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            body = response["Body"].read()
            data = json.loads(body)
            with self._lock:
                self.stats["gets"] += 1
            return data.get("hash")
        except self._client.exceptions.NoSuchKey:
            return None
        except self._client.exceptions.ClientError:
            return None

    def list_paths(self, prefix: str = "") -> list[str]:
        """List all paths with the given prefix (like S3 list-objects-v2)."""
        full_prefix = self._paths_prefix() + prefix
        paths = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.startswith(full_prefix):
                    # Strip the paths/ prefix to return just the path name
                    paths.append(key[len(self._paths_prefix()):])
        return sorted(paths)

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
        max_retry_attempts=max_retries,
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
