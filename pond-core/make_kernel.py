"""Unified kernel factory — one entry point for all storage backends.

Switching between local FS and S3 is ONE line:

    # Local filesystem (pure files, no SQLite):
    kernel = make_kernel("file:///path/to/.pond")

    # S3:
    kernel = make_kernel("s3://my-bucket/prod", region="us-east-1")

Both return an ObjectStoreNativeKernel backed by the appropriate
store. The kernel code, SDK, lenses — everything else is identical.

URL schemes:
  file://      — LocalFSObjectStore (pure files, no SQLite)
  s3://        — S3ObjectStore (boto3)

For tests, use file:// with a tempdir — local FS is fast enough and
exercises the real on-disk code path (catches layout bugs, validates
restart persistence).

For S3, credentials are picked up from the environment (AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_REGION) or boto3's default credential chain.
You can override with explicit kwargs.
"""
from __future__ import annotations

import os
import sys
from typing import Optional
from urllib.parse import urlparse

# Make pond-core importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def make_kernel(url: str, **kwargs) -> "ObjectStoreNativeKernel":
    """Create a Pond kernel backed by the storage backend identified by the URL.

    Args:
        url: storage URL. Supported schemes:
            "file:///path/to/.pond"  — local filesystem (pure files, no SQLite)
            "s3://bucket/prefix"    — S3 (boto3)
            "memory://"             — in-memory (for tests)
        **kwargs: backend-specific options:
            For S3: region, endpoint_url, aws_access_key_id, aws_secret_access_key
            For local/memory: ignored

    Returns:
        An ObjectStoreNativeKernel instance backed by the appropriate store.

    Examples:
        # Local FS (pure files, no SQLite):
        kernel = make_kernel("file:///var/lib/pond")

        # S3:
        kernel = make_kernel("s3://my-pond/prod", region="us-east-1")

        # Then use PondStorage as usual:
        from pond_storage import PondStorage
        storage = PondStorage(kernel)
    """
    from object_store_native_kernel import ObjectStoreNativeKernel

    parsed = urlparse(url)
    scheme = parsed.scheme

    if scheme == "file" or (not scheme and parsed.path):
        # Local filesystem
        from local_fs_object_store import LocalFSObjectStore
        base_dir = parsed.path if parsed.path else url
        store = LocalFSObjectStore(base_dir)

    elif scheme == "s3":
        # S3
        from s3_object_store import S3ObjectStore
        import boto3
        from botocore.config import Config
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        region = kwargs.get("region") or os.environ.get("AWS_REGION", "us-east-1")
        endpoint_url = kwargs.get("endpoint_url")
        aws_access_key_id = kwargs.get("aws_access_key_id")
        aws_secret_access_key = kwargs.get("aws_secret_access_key")
        # Production retry/timeout config (overrides boto3 defaults)
        max_retries = kwargs.get("max_retries", 10)
        connect_timeout = kwargs.get("connect_timeout", 5.0)
        read_timeout = kwargs.get("read_timeout", 30.0)
        max_pool_connections = kwargs.get("max_pool_connections", 50)
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

    else:
        raise ValueError(
            f"Unsupported storage URL scheme '{scheme}'. "
            f"Use 'file://' or 's3://'."
        )

    return ObjectStoreNativeKernel(store)
