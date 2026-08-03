"""Pond Core — the frozen 3-primitive kernel + storage backends."""

# Re-export everything from pond-core/ source files
import os, sys
_syspath = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "pond-core")
if _syspath not in sys.path:
    sys.path.insert(0, _syspath)

from kernel import PondMinimal, hash_bytes
from object_store_native_kernel import ObjectStoreNativeKernel, InMemoryObjectStore, make_object_store_native_kernel
from local_fs_object_store import LocalFSObjectStore, make_local_kernel
from s3_object_store import S3ObjectStore, make_s3_kernel
from make_kernel import make_kernel

__all__ = [
    "PondMinimal", "hash_bytes",
    "ObjectStoreNativeKernel", "InMemoryObjectStore", "make_object_store_native_kernel",
    "LocalFSObjectStore", "make_local_kernel",
    "S3ObjectStore", "make_s3_kernel",
    "make_kernel",
]
