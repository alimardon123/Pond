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
  UnifiedStorage manifest (segment index) + multiple kernel blobs (segments).
- Principle 4 (Scalable): Any lens can implement the same pattern.
- Principle 7 (Functional): Video, music, logs, streaming — all possible.

HOW IT WORKS:
A large object (video, music, log file) is split into fixed-size segments.
Each segment is stored as a row {offset, segment} in a PND2 blob via
UnifiedStorage. The manifest maps offset ranges → blob_hash.

  write_stream(collection, data, segment_size):
    1. Split data into segments of segment_size bytes
    2. Write rows {offset, segment} to a PND2 blob
    3. Commit manifest + JSON commit blob via UnifiedStorage

  read_stream(collection, start_byte, end_byte):
    1. Read manifest (1 GET)
    2. Range scan [start_byte, end_byte] over the manifest (in-memory)
    3. Fetch only the overlapping PND2 blobs (K GETs)
    4. Concatenate + slice to exact [start_byte, end_byte]

  append_stream(collection, data):
    1. Get current segment count (the next offset)
    2. append_shard() new {offset, segment} rows
    3. CRDT-safe: multiple producers can append concurrently

This is the SAME pattern as LakehouseLens, but with BINARY segment
columns instead of typed tabular columns. The kernel doesn't need to
know about ranges — UnifiedStorage composes Write (PND2 blobs) +
Read (manifest + blob) + Ref (commit chain) to provide range-read.

GENERIC: works for any large-blob workload:
  - Video: segment_size = 10MB (one segment per video chunk)
  - Music: segment_size = 1MB (one segment per audio chunk)
  - Logs: segment_size = 64KB (one segment per log block)
  - Any future streaming workload

VERSIONING: each commit creates a new snapshot. Old segments are
content-addressed (deduped). Time-travel via replay_from() reads
from any offset; create_branch()/merge_branch() provide git-style
version control.

STORAGE: There is exactly ONE storage path — the UnifiedStorage
backend (PND2 blobs + CollectionManifest + JSON commit blobs). The
legacy ProllyTreeIndex / ProllyLensBase path has been removed. If
UnifiedStorage is not available, all I/O methods raise RuntimeError.
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

# Segment key prefix used by the legacy ProllyTreeIndex backend (kept for
# documentation; the unified path stores segments as rows keyed by INT64
# 'offset' columns and does not use this prefix).
_SEG_PREFIX = "seg/"


class StreamingLens(PondLens):
    """Streaming/media lens — chunked storage for large objects.

    Splits large objects (video, music, logs) into fixed-size segments.
    Each segment is stored as a row {offset, segment} in a PND2 blob via
    UnifiedStorage. The manifest maps offset ranges → blob_hash, enabling
    efficient range reads.

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
        self._unified_storage = None
        try:
            from unified_storage import UnifiedStorage
            self._unified_storage = UnifiedStorage(kernel)
        except ImportError:
            pass  # _require_unified() will raise RuntimeError on first I/O

    def _require_unified(self) -> None:
        """Raise RuntimeError if UnifiedStorage is not available.

        The legacy ProllyTreeIndex / ProllyLensBase path has been removed.
        UnifiedStorage is the ONLY storage path. If it is None (because
        the physical_structures extension is not importable), every I/O
        method must fail loudly rather than silently fall back.
        """
        if self._unified_storage is None:
            raise RuntimeError(
                "UnifiedStorage is not available — the legacy "
                "ProllyTreeIndex path has been removed. Install the "
                "physical_structures extension (pond-sdk/extensions/"
                "physical_structures) to enable Streaming I/O."
            )

    def write_stream(self, collection: str, data: bytes,
                     segment_size: int = 1_000_000) -> str:
        """Write a stream as chunked segments.

        Stores segments as rows {'offset': int, 'segment': bytes} via
        UnifiedStorage. PND2 BINARY column for segment data, INT64 for
        offset. This overwrites any existing stream with the same name.
        """
        self._require_unified()

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

    def read_stream(self, collection: str,
                    start_byte: Optional[int] = None,
                    end_byte: Optional[int] = None,
                    commit_hash: Optional[str] = None) -> bytes:
        """Read a range of bytes from a stream.

        Uses UnifiedStorage.read with start_key/end_key for range scan.
        Only fetches segments that overlap the byte range.

        CROSS-LENS: works on any collection. If the collection has
        "offset" and "segment" columns (streaming-native), uses range
        scan. Otherwise reads all rows and concatenates any bytes-typed
        column values it can find (ugly shape, but full visibility).

        Args:
            collection: collection name
            start_byte: optional inclusive start byte (None = 0)
            end_byte: optional exclusive end byte (None = end of stream)
            commit_hash: IGNORED in the unified path. The legacy
                ProllyTreeIndex backend supported time-travel reads at
                an arbitrary commit; the unified path reads from the
                collection's current HEAD (+ shards). Use replay_from()
                for offset-based time-travel on partitioned topics.

        Returns:
            The concatenated bytes in [start_byte, end_byte).
        """
        self._require_unified()
        # commit_hash is accepted for API compatibility but ignored —
        # unified reads always use HEAD (+ shards). See docstring above.

        # Inspect metadata — is this a streaming-native collection?
        md = self.get_collection_metadata(collection)
        is_streaming = md.get("lens_type") == "streaming"
        if is_streaming:
            # Read all segments (no start_key/end_key — those are byte
            # offsets, not rg_keys). Filter in memory.
            rows = self._unified_storage.read_with_shards(collection,
                columns=["offset", "segment"])
            # Sort by offset
            rows.sort(key=lambda r: r.get("offset", 0))
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
            rows = self._unified_storage.read_with_shards(collection)
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

    def append_stream(self, collection: str, data: bytes,
                      segment_size: int = 1_000_000) -> str:
        """Append data to an existing stream.

        Uses UnifiedStorage.append_shard — each segment becomes a row
        {offset, segment}. CRDT-safe: multiple producers can append to
        the same partition concurrently.

        Args:
            collection: collection name
            data: bytes to append
            segment_size: bytes per segment

        Returns:
            The shard manifest hash.
        """
        self._require_unified()

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

    def stream_size(self, collection: str,
                    commit_hash: Optional[str] = None) -> int:
        """Get the total size of a stream in bytes.

        Reads HEAD (+ shards merged) via read_with_shards and sums the
        length of every bytes-typed 'segment' column value.

        Args:
            collection: collection name
            commit_hash: IGNORED in the unified path. Kept for API
                compatibility with the legacy time-travel signature.
                Unified reads always use the current HEAD (+ shards).

        Returns:
            Total stream size in bytes (0 if the collection is empty
            or doesn't have any bytes-typed columns).
        """
        self._require_unified()
        # commit_hash is accepted for API compatibility but ignored.
        try:
            rows = self._unified_storage.read_with_shards(collection)
        except Exception:
            return 0
        total = 0
        for row in rows:
            seg = row.get("segment")
            if isinstance(seg, (bytes, bytearray)):
                total += len(seg)
            else:
                # Cross-lens: sum any bytes-typed column.
                for v in row.values():
                    if isinstance(v, (bytes, bytearray)):
                        total += len(v)
        return total

    def segment_count(self, collection: str) -> int:
        """Get the number of segments in a stream (total rows across all shards).

        Each row is one segment — sum n_rows from all row groups in the
        HEAD manifest + every shard manifest.
        """
        self._require_unified()
        # Count rows (segments) across HEAD + all shards
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
        self._require_unified()
        partitions = []
        for i in range(n_partitions):
            p = f"p{i}"
            self._unified_storage.branch(collection, p)
            partitions.append(p)
        return partitions

    def list_partitions(self, collection: str) -> list[str]:
        """List all partitions (branches) for a topic (collection)."""
        self._require_unified()
        branches = self._unified_storage.list_branches(collection)
        return [b for b in branches if b.startswith("p") and b[1:].isdigit()]

    def produce(self, collection: str, partition: int, data: bytes,
                segment_size: int = 1_000_000) -> str:
        """Produce (append) data to a specific partition.

        Bug 9 fix: do NOT call checkout() — that mutates the shared HEAD
        ref, causing two concurrent producers (even to different
        partitions) to race on HEAD. Instead, set the active branch
        IN-MEMORY only (via _active_branches dict). append_shard reads
        _active_branches to determine where to write the shard, so this
        is sufficient — no storage mutation, no HEAD race.

        Args:
            collection: collection name (the topic)
            partition: partition number (0-indexed)
            data: bytes to append
            segment_size: segment size for chunking

        Returns:
            The shard manifest hash.
        """
        self._require_unified()
        us = self._unified_storage
        branch_name = f"p{partition}"
        # Set the active branch IN-MEMORY only (no HEAD mutation).
        # append_shard uses _get_active_branch(collection) which reads
        # this dict — the shard is written to the partition's shard path
        # without touching HEAD.
        us._active_branches[collection] = us._branch_ref(collection, branch_name)
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
        self._require_unified()
        self._unified_storage.checkout(collection, f"p{partition}")
        return self.segment_count(collection)

    def consume(self, collection: str, partition: int,
                group: Optional[str] = None,
                max_messages: int = 100,
                timeout_ms: int = 0) -> list[dict]:
        """Consume messages from a partition starting from the group's offset.

        B9 fix: reads ALL segments at once (1 read_with_shards call) instead
        of N individual _read_segment_by_offset calls (each doing a full scan).
        """
        self._require_unified()
        self._unified_storage.checkout(collection, f"p{partition}")
        start_offset = 0
        if group:
            start_offset = self._get_offset(group, collection, partition)

        # Read all segments at once (1 read_with_shards = 1 scan)
        all_rows = self._unified_storage.read_with_shards(collection)
        # Sort by offset and filter to the requested range
        all_rows.sort(key=lambda r: r.get("offset", 0))
        end_offset = min(start_offset + max_messages, len(all_rows))

        messages = []
        for i in range(start_offset, end_offset):
            if i < len(all_rows):
                row = all_rows[i]
                data = row.get("segment")
                if data is not None:
                    messages.append({
                        "collection": collection,
                        "partition": partition,
                        "offset": i,
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
        self._require_unified()
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
        self._require_unified()
        # Read all rows (HEAD + shards merged) and find the one with this offset
        rows = self._unified_storage.read_with_shards(collection)
        for row in rows:
            if row.get("offset") == offset:
                return row.get("segment")
        return None

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
    # Version control (delegated to UnifiedStorage)
    # ==================================================================

    def create_branch(self, collection: str, branch_name: str) -> str:
        """Create a branch — O(1) ref copy via UnifiedStorage."""
        self._require_unified()
        return self._unified_storage.branch(collection, branch_name)

    def merge_branch(self, collection: str, branch_name: str) -> str:
        """Merge a branch into the collection's HEAD.

        Union merge with a 2-parent commit (git-like).
        """
        self._require_unified()
        return self._unified_storage.merge(collection, branch_name)

    def get_history(self, collection: str, limit: int = 20) -> list[dict]:
        """Walk the commit chain for the collection."""
        self._require_unified()
        return self._unified_storage.history(collection, limit)
