"""
StreamingLens — chunked storage for large objects (video, music, logs, streams).

RESOLUTION TO ARCHITECT ISSUE #4:
The architect noted "kernel has no range-read primitive; music/video
impossible." This lens resolves that concern WITHOUT adding a 4th
kernel primitive.

DESIGN GOALS COMPLIANCE:
- Principle 1 (Simple): Kernel stays FROZEN at 3 primitives (Write, Read, Ref).
  Range-read is NOT a kernel primitive — it's a Lens pattern.
- Principle 2 (Powerful): Range-read emerges from composition:
  ProllyTreeIndex (segment index) + multiple kernel blobs (segments).
- Principle 4 (Scalable): Any lens can implement the same pattern.
- Principle 7 (Functional): Video, music, logs, streaming — all possible.

HOW IT WORKS:
A large object (video, music, log file) is split into fixed-size segments.
Each segment is stored as a separate kernel blob (content-addressed).
The ProllyTreeIndex maps segment_number → blob_hash.

  write_stream(name, data, segment_size):
    1. Split data into segments of segment_size bytes
    2. Write each segment as a kernel blob
    3. Stage segment_0 → blob_hash_0, segment_1 → blob_hash_1, ...
    4. Commit to ProllyTreeIndex

  read_stream(name, start_byte, end_byte):
    1. Compute which segments overlap [start_byte, end_byte]
    2. Look up those segments in ProllyTreeIndex (O(log N))
    3. Read only those blobs from the kernel
    4. Concatenate + slice to exact [start_byte, end_byte]

  append_stream(name, data):
    1. Find the last segment number
    2. Stage new segments after it
    3. Commit (structural sharing — old segments unchanged)

This is the SAME pattern as LakehouseLens.range_write/range_read:
  - Lakehouse: table → row groups → Parquet blobs → ProllyTreeIndex
  - Streaming: stream → segments → raw bytes blobs → ProllyTreeIndex

The kernel doesn't need to know about ranges. The Lens composes
existing primitives (Write for segments, Read for individual blobs,
Ref for the ProllyTreeIndex commit chain) to provide range-read.

GENERIC: works for any large-blob workload:
  - Video: segment_size = 10MB (one segment per video chunk)
  - Music: segment_size = 1MB (one segment per audio chunk)
  - Logs: segment_size = 64KB (one segment per log block)
  - Any future streaming workload

VERSIONING: each commit creates a new snapshot. Old segments are
content-addressed (deduped). Time-travel: read_stream at a specific
commit hash reads the segment index at that commit.
"""

from __future__ import annotations

import os
import sys
from typing import Optional

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk"))

from kernel import PondMinimal
from base_lens import PondLens
from prolly_tree import ProllyLensBase

# Segment key prefix in the ProllyTreeIndex (like _RG_PREFIX in Lakehouse)
_SEG_PREFIX = "seg/"


class StreamingLens(PondLens):
    """Streaming/media lens — chunked storage for large objects.

    Splits large objects (video, music, logs) into fixed-size segments.
    Each segment is a separate kernel blob. The ProllyTreeIndex maps
    segment_number → blob_hash, enabling O(log N) range reads.

    COLLECTION-AGNOSTIC: pass the collection name to each operation.

        lens = StreamingLens(kernel)
        lens.write_stream("video_1", video_bytes, segment_size=10_000_000)
        chunk = lens.read_stream("video_1", start_byte=50_000_000, end_byte=60_000_000)
        lens.append_stream("video_1", more_bytes)
    """

    def __init__(self, kernel: PondMinimal, use_unified_storage: bool = True):
        """Create a StreamingLens.

        Args:
            kernel: the PondMinimal kernel instance
            use_unified_storage: IGNORED (kept for backward compat).
                There is now only ONE storage path — the unified
                manifest-based architecture. Segments stored as BINARY
                column values with offset as INT64 key_col.
        """
        super().__init__(kernel)
        self._bases: dict[str, ProllyLensBase] = {}
        self._unified_storage = None
        try:
            from unified_storage import UnifiedStorage
            self._unified_storage = UnifiedStorage(kernel)
        except ImportError:
            pass

    def _get_base(self, collection: str) -> ProllyLensBase:
        if collection not in self._bases:
            self._bases[collection] = ProllyLensBase(self.kernel, collection)
        return self._bases[collection]

    def write_stream(self, collection: str, data: bytes,
                     segment_size: int = 1_000_000) -> str:
        """Write a stream as chunked segments.

        Unified path: stores segments as rows {'offset': int, 'segment': bytes}
        via PondStorage. PND2 BINARY column for segment data, INT64 for offset.
        """
        # Unified storage path
        if self._unified_storage is not None:
            if not data:
                commit_hash = self._unified_storage.write(collection, [],
                    key_col="offset", message="write_stream: empty")
            else:
                rows = []
                n_segments = (len(data) + segment_size - 1) // segment_size
                for i in range(n_segments):
                    start = i * segment_size
                    end = min(start + segment_size, len(data))
                    rows.append({"offset": start, "segment": data[start:end]})
                commit_hash = self._unified_storage.write(
                    collection, rows, key_col="offset",
                    row_group_size=max(1, 10_000_000 // segment_size),
                    message=f"write_stream: {len(data)} bytes in {n_segments} segments")
            # Stamp cross-lens metadata so other lenses know this is a
            # streaming collection with key_col="offset".
            self.stamp_collection_metadata(
                collection, lens_type="streaming", key_col="offset",
                schema_hint={"offset": "int64", "segment": "bytes"},
                extra={"segment_size": segment_size, "total_bytes": len(data)})
            return commit_hash

        # Legacy path
        base = self._get_base(collection)
        existing = base.read_all()
        for k in existing.keys():
            if k.startswith(_SEG_PREFIX):
                base.stage_delete(k)
        if not data:
            return base.commit("write_stream: empty")
        n_segments = (len(data) + segment_size - 1) // segment_size
        for i in range(n_segments):
            start = i * segment_size
            end = min(start + segment_size, len(data))
            segment = data[start:end]
            blob_hash = self.kernel.write(segment)
            seg_key = f"{_SEG_PREFIX}{i:010d}"
            base.stage(seg_key, blob_hash)
        return base.commit(
            f"write_stream: {len(data)} bytes in {n_segments} segments "
            f"(segment_size={segment_size})")

    def read_stream(self, collection: str,
                    start_byte: Optional[int] = None,
                    end_byte: Optional[int] = None,
                    commit_hash: Optional[str] = None) -> bytes:
        """Read a range of bytes from a stream.

        Unified path: uses PondStorage.read with start_key/end_key for
        range scan. Only fetches segments that overlap the byte range.

        CROSS-LENS: works on any collection. If the collection has
        "offset" and "segment" columns (streaming-native), uses range
        scan. Otherwise reads all rows and concatenates any bytes-typed
        column values it can find (ugly shape, but full visibility).
        """
        # Unified storage path
        if self._unified_storage is not None:
            # Inspect metadata — is this a streaming-native collection?
            md = self.get_collection_metadata(collection)
            is_streaming = md.get("lens_type") == "streaming"
            if is_streaming:
                rows = self._unified_storage.read(collection,
                    start_key=start_byte, end_key=end_byte,
                    columns=["offset", "segment"])
                result = b""
                for row in rows:
                    seg_offset = row.get("offset", 0)
                    seg_data = row.get("segment", b"")
                    if seg_data is None:
                        continue
                    # Slice the segment to the requested range
                    seg_start = max(0, (start_byte or 0) - seg_offset)
                    seg_end = min(len(seg_data),
                                  (end_byte or float('inf')) - seg_offset)
                    if seg_end > seg_start:
                        result += seg_data[seg_start:int(seg_end)]
                return result
            # Cross-lens: not a streaming collection. Best-effort read:
            # concatenate any bytes-valued columns from all rows.
            try:
                rows = self._unified_storage.read(collection)
            except Exception:
                return b""
            result = b""
            for row in rows:
                for v in row.values():
                    if isinstance(v, (bytes, bytearray)):
                        result += bytes(v)
            # Apply byte range if requested
            if start_byte is not None and end_byte is not None:
                return result[start_byte:end_byte]
            elif start_byte is not None:
                return result[start_byte:]
            return result

        # Legacy path
        base = self._get_base(collection)
        if commit_hash:
            state = base.read_state_at_commit(commit_hash)
        else:
            state = base.read_all()

        # Find segment keys (sorted)
        seg_keys = sorted(k for k in state.keys() if k.startswith(_SEG_PREFIX))
        if not seg_keys:
            return b""

        # Determine segment size from the first segment
        first_blob = self.kernel.read_blob(state[seg_keys[0]])
        segment_size = len(first_blob)

        # Compute which segments overlap [start_byte, end_byte]
        if start_byte is None:
            start_byte = 0
        if end_byte is None:
            # Read all segments
            result = b""
            for k in seg_keys:
                result += self.kernel.read_blob(state[k])
            return result[start_byte:]

        # Calculate segment indices that overlap the range
        start_seg = start_byte // segment_size
        end_seg = (end_byte - 1) // segment_size if end_byte > 0 else 0

        # Read only the overlapping segments
        result = b""
        for i in range(start_seg, end_seg + 1):
            seg_key = f"{_SEG_PREFIX}{i:010d}"
            if seg_key in state:
                result += self.kernel.read_blob(state[seg_key])

        # Slice to exact byte range
        # The first segment may start before start_byte
        offset_in_first = start_byte - (start_seg * segment_size)
        # The total bytes we need from the concatenated result
        total_needed = end_byte - start_byte
        return result[offset_in_first:offset_in_first + total_needed]

    def append_stream(self, collection: str, data: bytes,
                      segment_size: int = 1_000_000) -> str:
        """Append data to an existing stream.

        Uses the unified storage path (append_shard) — each segment
        becomes a row {offset, segment}. CRDT-safe: multiple producers
        can append to the same partition concurrently.

        Args:
            collection: collection name
            data: bytes to append
            segment_size: bytes per segment

        Returns:
            The shard manifest hash.
        """
        if self._unified_storage is not None:
            # Use unified storage: each segment = one row
            if not data:
                return ""
            # Get the current segment count (offset for new segments)
            current_count = self.segment_count(collection)
            rows = []
            for i in range(0, len(data), segment_size):
                segment = data[i:i + segment_size]
                rows.append({
                    "offset": current_count + i // segment_size,
                    "segment": segment,
                })
            return self._unified_storage.append_shard(
                collection, rows, key_col="offset", row_group_size=1000)

        # Legacy path (ProllyLensBase)
        base = self._get_base(collection)
        state = base.read_all()

        seg_keys = sorted(k for k in state.keys() if k.startswith(_SEG_PREFIX))
        if not seg_keys:
            return self.write_stream(collection, data, segment_size)

        last_seg_num = int(seg_keys[-1].replace(_SEG_PREFIX, ""))
        last_blob = self.kernel.read_blob(state[seg_keys[-1]])
        if len(last_blob) < segment_size:
            remaining = segment_size - len(last_blob)
            fill_data = data[:remaining]
            data = data[remaining:]
            combined = last_blob + fill_data
            new_blob_hash = self.kernel.write(combined)
            base.stage(seg_keys[-1], new_blob_hash)

        if data:
            n_new = (len(data) + segment_size - 1) // segment_size
            for i in range(n_new):
                start = i * segment_size
                end = min(start + segment_size, len(data))
                segment = data[start:end]
                blob_hash = self.kernel.write(segment)
                seg_num = last_seg_num + 1 + i
                seg_key = f"{_SEG_PREFIX}{seg_num:010d}"
                base.stage(seg_key, blob_hash)

        return base.commit(
            f"append_stream: {len(data)} bytes appended")

    def stream_size(self, collection: str,
                    commit_hash: Optional[str] = None) -> int:
        """Get the total size of a stream in bytes."""
        base = self._get_base(collection)
        if commit_hash:
            state = base.read_state_at_commit(commit_hash)
        else:
            state = base.read_all()

        seg_keys = sorted(k for k in state.keys() if k.startswith(_SEG_PREFIX))
        if not seg_keys:
            return 0

        total = 0
        for k in seg_keys:
            blob = self.kernel.read_blob(state[k])
            total += len(blob)
        return total

    def segment_count(self, collection: str) -> int:
        """Get the number of segments in a stream (total rows across all shards)."""
        if self._unified_storage is not None:
            # Count rows (segments) across HEAD + all shards
            # Each row is one segment — sum n_rows from all row groups
            manifest = self._unified_storage._load_manifest(collection)
            if manifest is None:
                return 0
            total = sum(rg.n_rows for rg in manifest.scan_with_pruning())
            # Also count shards
            shard_hashes = self._unified_storage._read_shard_index(collection)
            for sh in shard_hashes:
                try:
                    from collection_manifest import CollectionManifest
                    sm = CollectionManifest.load(self.kernel, sh)
                    total += sum(rg.n_rows for rg in sm.scan_with_pruning())
                except (ValueError, KeyError):
                    pass
            return total
        # Legacy path
        base = self._get_base(collection)
        state = base.read_all()
        return sum(1 for k in state.keys() if k.startswith(_SEG_PREFIX))

    # ==================================================================
    # KAFKA-LIKE FEATURES: partitions, consumer groups, offsets
    #
    # UNIFIED DESIGN: topic = collection. Partitions = branches.
    #
    # A streaming topic IS a Pond collection. Each partition is a branch
    # within that collection (branch "p0", "p1", ...). This follows our
    # architecture: ONE collection, MANY branches, unified storage.
    #
    #   create_topic(collection, n_partitions): create collection + N branch partitions
    #   produce(collection, partition, data): append_shard to branch "p{partition}"
    #   consume(collection, partition, group, n): read_with_shards from branch
    #   commit_offset(group, collection, partition, offset): at-least-once
    #   replay_from(collection, partition, offset): time-travel read
    #
    # How it maps to our architecture (NO new primitives):
    #   - topic = a collection (just like any other Pond collection)
    #   - partition = a branch within the collection (p0, p1, ...)
    #   - consumer group = a ref tracking the last-read offset
    #   - offset = segment number (implicit, sequential)
    #
    # This is the SAME pattern as Kafka-on-S3 (WarpStream):
    #   - producers write directly to object storage
    #   - consumers read directly from object storage
    #   - offset tracking via small metadata objects
    # ==================================================================

    def create_topic(self, collection: str, n_partitions: int = 1) -> list[str]:
        """Create a streaming topic (collection) with N partitions.

        The topic IS the collection. Partitions are branches within it
        (p0, p1, ...). This follows Pond's unified architecture: ONE
        collection, MANY branches.

        Args:
            collection: collection name (the topic)
            n_partitions: number of partitions (parallelism)

        Returns:
            List of partition branch names.
        """
        # Initialize the collection
        self.write_stream(collection, b"init", segment_size=1_000_000)
        # Create partition branches
        partitions = []
        for i in range(n_partitions):
            p = f"p{i}"
            if self._unified_storage is not None:
                self._unified_storage.branch(collection, p)
            partitions.append(p)
        return partitions

    def list_partitions(self, collection: str) -> list[str]:
        """List all partitions (branches) for a topic (collection)."""
        if self._unified_storage is not None:
            branches = self._unified_storage.list_branches(collection)
            return [b for b in branches if b.startswith("p") and b[1:].isdigit()]
        return []

    def produce(self, collection: str, partition: int, data: bytes,
                segment_size: int = 1_000_000) -> str:
        """Produce (append) data to a specific partition.

        Args:
            collection: collection name (the topic)
            partition: partition number (0-indexed)
            data: bytes to append
            segment_size: segment size for chunking

        Returns:
            The shard manifest hash.
        """
        if self._unified_storage is not None:
            # Checkout the partition branch, then append_shard
            self._unified_storage.checkout(collection, f"p{partition}")
            return self.append_stream(collection, data, segment_size)
        return self.append_stream(collection, data, segment_size)

    def produce_round_robin(self, collection: str, data: bytes,
                             n_partitions: int = 1) -> tuple[int, str]:
        """Produce to the next partition (round-robin).

        Returns:
            (partition_number, shard_hash)
        """
        if not hasattr(self, '_rr_counter'):
            self._rr_counter: dict[str, int] = {}
        p = self._rr_counter.get(collection, 0)
        self._rr_counter[collection] = (p + 1) % n_partitions
        commit = self.produce(collection, p, data)
        return p, commit

    def get_latest_offset(self, collection: str, partition: int) -> int:
        """Get the latest offset (segment count) for a partition."""
        if self._unified_storage is not None:
            self._unified_storage.checkout(collection, f"p{partition}")
        return self.segment_count(collection)

    def consume(self, collection: str, partition: int,
                group: Optional[str] = None,
                max_messages: int = 100,
                timeout_ms: int = 0) -> list[dict]:
        """Consume messages from a partition starting from the group's offset.

        If a consumer group is specified, reads from the group's last-committed
        offset. If no group, reads from the beginning.

        Args:
            collection: collection name (the topic)
            partition: partition number
            group: consumer group name (for offset tracking)
            max_messages: max messages to return
            timeout_ms: (unused — kept for Kafka API compat)

        Returns:
            List of {offset, data, partition} dicts.
        """
        if self._unified_storage is not None:
            self._unified_storage.checkout(collection, f"p{partition}")
        start_offset = 0
        if group:
            start_offset = self._get_offset(group, collection, partition)

        latest = self.segment_count(collection)
        end_offset = min(start_offset + max_messages, latest)

        messages = []
        for offset in range(start_offset, end_offset):
            data = self._read_segment_by_offset(collection, offset)
            if data is not None:
                messages.append({
                    "collection": collection,
                    "partition": partition,
                    "offset": offset,
                    "data": data,
                })

        return messages

    def commit_offset(self, group: str, collection: str,
                      partition: int, offset: int) -> str:
        """Commit a consumer offset (at-least-once semantics).

        Stores the offset as a ref: consumer_offsets/{group}/{collection}/p{partition}
        → offset (encoded as a blob).

        Args:
            group: consumer group name
            collection: collection name (the topic)
            partition: partition number
            offset: the offset to commit (next message to read)

        Returns:
            The offset blob hash.
        """
        ref = f"consumer_offsets/{group}/{collection}/p{partition}"
        offset_bytes = str(offset).encode()
        h = self.kernel.write(offset_bytes)
        self.kernel.reference(ref, h)
        return h

    def _get_offset(self, group: str, collection: str, partition: int) -> int:
        """Get the committed offset for a consumer group (0 if none)."""
        ref = f"consumer_offsets/{group}/{collection}/p{partition}"
        h = self.kernel.resolve(ref)
        if h is None:
            return 0
        try:
            data = self.kernel.read_blob(h)
            return int(data.decode())
        except (ValueError, KeyError):
            return 0

    def replay_from(self, collection: str, partition: int,
                    offset: int, max_messages: int = 100) -> list[dict]:
        """Replay messages from a specific offset (time-travel read).

        Like Kafka's seek() — reads from any offset, not just the
        consumer group's committed offset.

        Args:
            collection: collection name (the topic)
            partition: partition number
            offset: starting offset (0 = beginning)
            max_messages: max messages to return

        Returns:
            List of {offset, data, partition} dicts.
        """
        if self._unified_storage is not None:
            self._unified_storage.checkout(collection, f"p{partition}")
        latest = self.segment_count(collection)
        end_offset = min(offset + max_messages, latest)

        messages = []
        for off in range(offset, end_offset):
            data = self._read_segment_by_offset(collection, off)
            if data is not None:
                messages.append({
                    "collection": collection,
                    "partition": partition,
                    "offset": off,
                    "data": data,
                })
        return messages

    def _read_segment_by_offset(self, collection: str, offset: int) -> Optional[bytes]:
        """Read a single segment by its offset number.

        Uses read_with_shards to merge HEAD + all shards, then finds the
        segment with the matching offset.
        """
        if self._unified_storage is not None:
            # Read all rows (HEAD + shards merged) and find the one with this offset
            rows = self._unified_storage.read_with_shards(collection)
            for row in rows:
                if row.get("offset") == offset:
                    return row.get("segment")
            return None
        # Legacy path
        base = self._get_base(collection)
        seg_key = f"{_SEG_PREFIX}{offset:010d}"
        h = base.lookup(seg_key)
        if h is None:
            return None
        return self.kernel.read_blob(h)

    def list_consumer_groups(self) -> list[str]:
        """List all consumer groups."""
        prefix = "consumer_offsets/"
        groups = set()
        for name in self.kernel.list_names():
            if name.startswith(prefix):
                # consumer_offsets/{group}/{topic}/p{partition}
                parts = name[len(prefix):].split("/")
                if parts:
                    groups.add(parts[0])
        return sorted(groups)

    def get_consumer_group_offsets(self, group: str) -> dict:
        """Get all offsets for a consumer group.

        Returns:
            { "topic/p0": offset, "topic/p1": offset, ... }
        """
        prefix = f"consumer_offsets/{group}/"
        offsets = {}
        for name in self.kernel.list_names():
            if name.startswith(prefix):
                rest = name[len(prefix):]
                # rest = {topic}/p{partition}
                h = self.kernel.resolve(name)
                if h:
                    try:
                        data = self.kernel.read_blob(h)
                        offsets[rest] = int(data.decode())
                    except (ValueError, KeyError):
                        pass
        return offsets

    # ==================================================================
    # Version control (delegated to ProllyLensBase)
    # ==================================================================

    def create_branch(self, collection: str, branch_name: str) -> str:
        return self._get_base(collection).branch(branch_name)

    def merge_branch(self, collection: str, branch_name: str) -> str:
        return self._get_base(collection).merge(branch_name)

    def get_history(self, collection: str, limit: int = 20) -> list[dict]:
        return self._get_base(collection).history(limit)
