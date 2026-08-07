"""Shared configuration for R2/S3 benchmark scripts.

All benchmark and demo scripts that need Cloudflare R2 (or any S3-compatible
backend) MUST load credentials from environment variables — never hardcode
them. Hardcoding credentials in source files is a security incident.

Required environment variables:
  R2_ENDPOINT   — e.g. https://<account>.r2.cloudflarestorage.com
  R2_ACCESS_KEY — the access key ID
  R2_SECRET_KEY — the secret access key
  R2_BUCKET     — the bucket name

Optional:
  R2_PREFIX     — key prefix (defaults to a timestamped bench dir)

Usage:
    from scripts._r2_config import get_r2_client, get_r2_bucket

    client = get_r2_client()
    bucket = get_r2_bucket()
    prefix = get_r2_prefix()

If the environment variables are not set, the script exits with a clear
error message (no fallback to hardcoded values).
"""
import os
import sys
import time


_MISSING_HELP = """
ERROR: Required R2 environment variables are not set.

This script needs S3-compatible storage credentials. Set them via
environment variables before running:

    export R2_ENDPOINT="https://<account>.r2.cloudflarestorage.com"
    export R2_ACCESS_KEY="<your-access-key>"
    export R2_SECRET_KEY="<your-secret-key>"
    export R2_BUCKET="<your-bucket-name>"
    # optional:
    export R2_PREFIX="bench-$(date +%s)"

Never hardcode credentials in source files. If you previously found
hardcoded credentials in this repo, treat them as compromised and
rotate them immediately.
"""


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        sys.stderr.write(_MISSING_HELP)
        sys.exit(2)
    return val


def get_r2_endpoint() -> str:
    return _require_env("R2_ENDPOINT")


def get_r2_access_key() -> str:
    return _require_env("R2_ACCESS_KEY")


def get_r2_secret_key() -> str:
    return _require_env("R2_SECRET_KEY")


def get_r2_bucket() -> str:
    return _require_env("R2_BUCKET")


def get_r2_prefix(default: str = None) -> str:
    if default is None:
        default = f"qbench-{int(time.time())}"
    return os.environ.get("R2_PREFIX", default)


def get_r2_client():
    """Build a boto3 S3 client configured for R2.

    Returns a boto3.client("s3", ...) with sensible defaults for
    benchmark scripts (5s connect timeout, 60s read timeout, 50 pool
    connections, adaptive retries).
    """
    import boto3
    from botocore.config import Config

    config = Config(
        connect_timeout=5.0,
        read_timeout=60.0,
        max_pool_connections=50,
        retries={"max_attempts": 3, "mode": "adaptive"},
    )
    return boto3.client(
        "s3",
        endpoint_url=get_r2_endpoint(),
        aws_access_key_id=get_r2_access_key(),
        aws_secret_access_key=get_r2_secret_key(),
        region_name="auto",
        config=config,
    )
