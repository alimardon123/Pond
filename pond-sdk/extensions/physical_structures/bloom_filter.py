"""
Bloom Filter — probabilistic membership test Physical Structure.

A bloom filter answers "is X in the set?" in O(1), with a configurable
false positive rate. It NEVER has false negatives.

Use cases:
  - Skip unnecessary blob reads (if key is definitely not in this collection)
  - Pre-filter before expensive lookups
  - Cross-Lens membership test (Track 2: built by FeatureStore, used by Lakehouse)

Storage:
  - Bit array stored as a kernel blob
  - Referenced by __bloom/{collection}
  - Any Lens can build or query it

Usage:
    from extensions.physical_structures import BloomFilter

    # Build from a list of items
    BloomFilter.build(kernel, "users", ["user_1", "user_2", "user_3"])

    # Query (any Lens can do this)
    BloomFilter.query(kernel, "users", "user_2")  # → True
    BloomFilter.query(kernel, "users", "user_999")  # → False (or True with low probability)
"""

from __future__ import annotations
import json
import hashlib
import math
from typing import Optional
from extensions.physical_structures.base import PhysicalStructure


class BloomFilter(PhysicalStructure):
    """Probabilistic membership test. O(1) query, configurable false positive rate."""

    type_name = "bloom"

    @staticmethod
    def build(kernel, collection: str, source_data: list, **kwargs) -> str:
        """Build a bloom filter from a list of items.

        Args:
            kernel: PondMinimal instance
            collection: collection name
            source_data: list of items (strings or stringifiable)
            **kwargs:
                false_positive_rate: float (default 0.01)

        Returns:
            Blob hash of the stored bloom filter.
        """
        fpr = kwargs.get("false_positive_rate", 0.01)
        capacity = max(len(source_data) * 2, 100)

        num_bits = int(-capacity * math.log(fpr) / (math.log(2) ** 2))
        num_hashes = max(1, int(num_bits / capacity * math.log(2)))
        bits = [False] * num_bits

        for item in source_data:
            for i in range(num_hashes):
                h = int(hashlib.sha256(f"{i}:{item}".encode()).hexdigest(), 16) % num_bits
                bits[h] = True

        # Pack bits into bytes
        packed = bytearray()
        for i in range(0, len(bits), 8):
            byte = 0
            for j in range(8):
                if i + j < len(bits) and bits[i + j]:
                    byte |= (1 << j)
            packed.append(byte)

        data = {
            "capacity": capacity,
            "num_bits": num_bits,
            "num_hashes": num_hashes,
            "false_positive_rate": fpr,
            "packed_bits": list(packed),
            "item_count": len(source_data),
        }
        blob_hash = kernel.write(json.dumps(data, sort_keys=True).encode())
        kernel.reference(BloomFilter.ref_name(collection), blob_hash)
        return blob_hash

    @classmethod
    def load(cls, kernel, collection: str) -> Optional[dict]:
        """Load the bloom filter data from the kernel."""
        h = kernel.resolve(cls.ref_name(collection))
        if h is None:
            return None
        return json.loads(kernel.read_blob(h))

    @staticmethod
    def _contains(data: dict, item: str) -> bool:
        """Check if item MIGHT be in the set (true = maybe, false = definitely not)."""
        num_bits = data["num_bits"]
        num_hashes = data["num_hashes"]
        packed = bytes(data["packed_bits"])

        for i in range(num_hashes):
            h = int(hashlib.sha256(f"{i}:{item}".encode()).hexdigest(), 16) % num_bits
            byte_idx = h // 8
            bit_idx = h % 8
            if byte_idx >= len(packed):
                return False
            if not (packed[byte_idx] & (1 << bit_idx)):
                return False
        return True

    @staticmethod
    def query(kernel, collection: str, item: str, **kwargs) -> bool:
        """Query the bloom filter.

        Returns True if the item MIGHT be in the set (could be false positive).
        Returns False if the item is DEFINITELY NOT in the set (no false negatives).
        """
        data = BloomFilter.load(kernel, collection)
        if data is None:
            return True  # No filter = assume everything might be present
        return BloomFilter._contains(data, str(item))

    @classmethod
    def verify(cls, kernel, collection: str) -> bool:
        """Verify the bloom filter exists and is well-formed."""
        data = cls.load(kernel, collection)
        if data is None:
            return False
        return all(k in data for k in ("num_bits", "num_hashes", "packed_bits"))
