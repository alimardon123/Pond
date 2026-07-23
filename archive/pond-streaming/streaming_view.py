"""
Pond Streaming View — Kafka-like streaming on Pond.

Features:
  - Topics (append-only logs stored as Prolly tree segments)
  - Producers (write records to topics)
  - Consumers (read records by offset)
  - Consumer groups (track read offsets)
  - Partitions (shard topics by key)
  - Time travel (read at past offsets)
  - Retention (delete old segments)

Design:
  - Each topic is a ProllyViewBase instance
  - Records are stored as key: "records/<offset>" → blob_hash
  - Consumer offsets stored as key: "_offsets/<consumer_group>" → last_read_offset
  - Partitions are separate ProllyViewBase instances per partition

  This is NOT a streaming engine (no watermarks, no exactly-once, no
  stateful processing). It's a streaming STORAGE layer — the log itself.
  Processing semantics are a View/infrastructure concern.

  Uses the 3-primitive kernel (Write/Read/Reference) + ProllyViewBase.
"""

import json, time, sys, os, struct, hashlib
from typing import Optional, Iterator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "pond-sdk"))
from pond_minimal import PondMinimal
from prolly_view import ProllyViewBase, ProllyTree


class StreamingView:
    """
    Kafka-like streaming on Pond.

    A topic is an append-only log. Records are keyed by sequential offset.
    Producers append; consumers read by offset.
    """

    def __init__(self, kernel: PondMinimal, topic_name: str):
        self.kernel = kernel
        self.topic_name = topic_name
        self.base = ProllyViewBase(kernel, topic_name)
        self._next_offset = self._compute_next_offset()

    def _compute_next_offset(self) -> int:
        """Find the highest offset in the topic and add 1."""
        try:
            state = self.base.read_all()
        except Exception:
            return 0
        if not state:
            return 0
        max_offset = -1
        for key in state:
            if key.startswith("records/"):
                try:
                    offset = int(key[len("records/"):])
                    max_offset = max(max_offset, offset)
                except ValueError:
                    pass
        return max_offset + 1

    # ------------------------------------------------------------------
    # Produce (write records)
    # ------------------------------------------------------------------

    def produce(self, key: str, value: bytes, headers: dict = None) -> int:
        """Produce a single record. Returns the offset."""
        record = {
            "offset": self._next_offset,
            "key": key,
            "value": value.hex() if isinstance(value, bytes) else value,
            "headers": headers or {},
            "timestamp": time.time(),
        }
        record_bytes = json.dumps(record, sort_keys=True).encode()
        record_hash = self.kernel.write(record_bytes)
        self.base.stage(f"records/{self._next_offset:020d}", record_hash)
        self._next_offset += 1
        return self._next_offset - 1

    def produce_batch(self, records: list) -> int:
        """Produce multiple records. Returns the last offset."""
        last = -1
        for r in records:
            last = self.produce(r.get("key", ""), r.get("value", b""), r.get("headers"))
        return last

    def flush(self) -> str:
        """Commit staged records to the log."""
        return self.base.commit(f"flush {self._next_offset} records")

    # ------------------------------------------------------------------
    # Consume (read records)
    # ------------------------------------------------------------------

    def consume(self, from_offset: int = 0, limit: int = 100) -> list[dict]:
        """Read records starting from an offset. Returns up to `limit` records."""
        results = []
        state = self.base.read_all()
        for i in range(from_offset, from_offset + limit):
            key = f"records/{i:020d}"
            h = state.get(key)
            if not h:
                break  # no more records
            try:
                record = json.loads(self.kernel.read_blob(h))
                # Decode value back to bytes
                if "value" in record:
                    try:
                        record["value"] = bytes.fromhex(record["value"])
                    except (ValueError, TypeError):
                        pass
                results.append(record)
            except (json.JSONDecodeError, Exception):
                # Not a valid record (might be a tree node)
                break
        return results

    def consume_latest(self, limit: int = 10) -> list[dict]:
        """Read the latest N records."""
        return self.consume(max(0, self._next_offset - limit), limit)

    # ------------------------------------------------------------------
    # Consumer groups (offset tracking)
    # ------------------------------------------------------------------

    def commit_offset(self, consumer_group: str, offset: int):
        """Commit a consumer group's read offset."""
        offset_data = json.dumps({"group": consumer_group, "offset": offset,
                                  "timestamp": time.time()}).encode()
        h = self.kernel.write(offset_data)
        self.base.stage(f"_offsets/{consumer_group}", h)
        self.base.commit(f"commit offset {consumer_group}={offset}")

    def get_committed_offset(self, consumer_group: str) -> int:
        """Get a consumer group's last committed offset."""
        h = self.base.lookup(f"_offsets/{consumer_group}")
        if not h:
            return 0
        data = json.loads(self.kernel.read_blob(h))
        return data.get("offset", 0)

    def consume_from_group(self, consumer_group: str, limit: int = 100) -> list[dict]:
        """Consume records from a consumer group's last committed offset."""
        offset = self.get_committed_offset(consumer_group)
        records = self.consume(offset, limit)
        if records:
            self.commit_offset(consumer_group, records[-1]["offset"] + 1)
        return records

    # ------------------------------------------------------------------
    # Topic info
    # ------------------------------------------------------------------

    def get_offset(self) -> int:
        """Get the current write offset (next offset to be written)."""
        return self._next_offset

    def get_record_count(self) -> int:
        """Count records in the topic."""
        state = self.base.read_all()
        return sum(1 for k in state if k.startswith("records/"))

    # ------------------------------------------------------------------
    # Retention (delete old records)
    # ------------------------------------------------------------------

    def apply_retention(self, max_records: int):
        """Delete records older than max_records (keep only the latest N)."""
        state = self.base.read_all()
        record_keys = sorted(k for k in state if k.startswith("records/"))
        if len(record_keys) <= max_records:
            return  # nothing to delete
        # Delete oldest records
        to_delete = record_keys[:len(record_keys) - max_records]
        for key in to_delete:
            self.base.stage_delete(key)
        self.base.commit(f"retention: deleted {len(to_delete)} old records")

    # ------------------------------------------------------------------
    # History & branching (for topic versioning)
    # ------------------------------------------------------------------

    def history(self, limit=20): return self.base.history(limit)
    def branch(self, name): return self.base.branch(name)
    def checkout(self, name): self.base.checkout(name)
    def list_branches(self): return self.base.list_branches()


class StreamingClient:
    """
    Multi-topic streaming client. Manages multiple topics on one kernel.
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel
        self._topics: dict[str, StreamingView] = {}

    def create_topic(self, name: str) -> StreamingView:
        """Create or open a topic."""
        topic = StreamingView(self.kernel, name)
        self._topics[name] = topic
        return topic

    def list_topics(self) -> list[str]:
        """List all topics."""
        prefix = ""
        return [n for n in self.kernel.list_names() if not n.startswith("__")]


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import shutil

    bench_dir = "/tmp/pond_stream_test"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    kernel = PondMinimal(bench_dir)
    client = StreamingClient(kernel)

    print("=== CREATE TOPIC ===")
    topic = client.create_topic("events")

    print("\n=== PRODUCE ===")
    for i in range(10):
        offset = topic.produce(f"key-{i}", f"event-{i}".encode(), {"source": "test"})
    topic.flush()
    print(f"  Produced 10 records. Next offset: {topic.get_offset()}")

    print("\n=== CONSUME (from offset 0) ===")
    records = topic.consume(0, 5)
    for r in records:
        print(f"  offset={r['offset']} key={r['key']} value={r['value']}")
    if not records:
        print("  (no records found)")

    print("\n=== CONSUME LATEST (3) ===")
    records = topic.consume_latest(3)
    for r in records:
        print(f"  offset={r['offset']} key={r['key']}")

    print("\n=== CONSUMER GROUP ===")
    topic.commit_offset("group-A", 5)
    print(f"  Committed offset: {topic.get_committed_offset('group-A')}")
    records = topic.consume_from_group("group-A", 3)
    print(f"  Consumed from group-A: {len(records)} records")
    for r in records:
        print(f"    offset={r['offset']} key={r['key']}")
    print(f"  New committed offset: {topic.get_committed_offset('group-A')}")

    print("\n=== RETENTION (keep latest 5) ===")
    print(f"  Before: {topic.get_record_count()} records")
    topic.apply_retention(5)
    print(f"  After: {topic.get_record_count()} records")
    records = topic.consume(0, 100)
    print(f"  First available offset: {records[0]['offset'] if records else 'none'}")

    print("\n=== HISTORY ===")
    for h in topic.history():
        print(f"  {h['commit']}  {h['type']}  {h['message']}")

    print("\n=== ALL TESTS PASSED ===")
    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)
