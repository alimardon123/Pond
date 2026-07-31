"""Streaming Kafka-like features tests — partitions, consumer groups, offsets."""
import os, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "streaming"))

from object_store_native_kernel import make_object_store_native_kernel
from streaming_lens import StreamingLens


def test_create_topic_partitions():
    """Create topic with N partitions (topic = collection, partitions = branches)."""
    kernel, _ = make_object_store_native_kernel()
    sl = StreamingLens(kernel)
    partitions = sl.create_topic("events", n_partitions=3)
    assert len(partitions) == 3
    assert "p0" in partitions
    assert "p1" in partitions
    assert "p2" in partitions
    listed = sl.list_partitions("events")
    assert len(listed) == 3
    print(f"PASS: test_create_topic_partitions — {len(partitions)} partitions (branches)")
    return True


def test_produce_consume():
    """Produce messages and consume them."""
    kernel, _ = make_object_store_native_kernel()
    sl = StreamingLens(kernel)
    sl.create_topic("t", n_partitions=2)

    for i in range(10):
        sl.produce_round_robin("t", f"msg_{i}".encode(), n_partitions=2)

    # Each partition should have messages
    total = sum(sl.get_latest_offset("t", p) for p in range(2))
    assert total >= 10, f"Expected >=10 messages, got {total}"

    # Consume without group (from beginning)
    msgs = sl.consume("t", 0, max_messages=100)
    assert len(msgs) > 0
    print(f"PASS: test_produce_consume — {total} messages across 2 partitions")
    return True


def test_consumer_group_offsets():
    """Consumer group tracks offsets — consume, commit, consume again."""
    kernel, _ = make_object_store_native_kernel()
    sl = StreamingLens(kernel)
    sl.create_topic("t", n_partitions=1)

    for i in range(5):
        sl.produce("t", 0, f"msg_{i}".encode())

    # Consume with group
    msgs1 = sl.consume("t", 0, group="g1", max_messages=10)
    assert len(msgs1) >= 5
    sl.commit_offset("g1", "t", 0, msgs1[-1]["offset"] + 1)

    # Consume again — should get 0 new
    msgs2 = sl.consume("t", 0, group="g1", max_messages=10)
    assert len(msgs2) == 0, f"Expected 0 new, got {len(msgs2)}"

    # Produce more
    for i in range(5, 8):
        sl.produce("t", 0, f"msg_{i}".encode())

    # Consume again — should get 3 new
    msgs3 = sl.consume("t", 0, group="g1", max_messages=10)
    assert len(msgs3) == 3, f"Expected 3 new, got {len(msgs3)}"
    print(f"PASS: test_consumer_group_offsets — commit + resume works")
    return True


def test_replay_from_offset():
    """Replay messages from any offset (time-travel)."""
    kernel, _ = make_object_store_native_kernel()
    sl = StreamingLens(kernel)
    sl.create_topic("t", n_partitions=1)

    for i in range(10):
        sl.produce("t", 0, f"msg_{i}".encode())

    # Replay from offset 5
    msgs = sl.replay_from("t", 0, offset=5, max_messages=100)
    assert len(msgs) >= 5, f"Expected >=5 messages from offset 5, got {len(msgs)}"
    assert msgs[0]["offset"] == 5
    print(f"PASS: test_replay_from_offset — {len(msgs)} messages from offset 5")
    return True


def test_multiple_consumer_groups():
    """Multiple groups track offsets independently."""
    kernel, _ = make_object_store_native_kernel()
    sl = StreamingLens(kernel)
    sl.create_topic("t", n_partitions=1)

    for i in range(5):
        sl.produce("t", 0, f"msg_{i}".encode())

    # Group A consumes and commits
    msgs_a = sl.consume("t", 0, group="A", max_messages=10)
    sl.commit_offset("A", "t", 0, msgs_a[-1]["offset"] + 1)

    # Group B should still see all messages (independent offset)
    msgs_b = sl.consume("t", 0, group="B", max_messages=10)
    assert len(msgs_b) >= 5, f"Group B expected >=5, got {len(msgs_b)}"
    sl.commit_offset("B", "t", 0, msgs_b[-1]["offset"] + 1)

    groups = sl.list_consumer_groups()
    assert "A" in groups and "B" in groups
    print(f"PASS: test_multiple_consumer_groups — {len(groups)} groups independent")
    return True


def main():
    tests = [
        test_create_topic_partitions,
        test_produce_consume,
        test_consumer_group_offsets,
        test_replay_from_offset,
        test_multiple_consumer_groups,
    ]
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{passed}/{len(tests)} tests passed")
    if passed == len(tests):
        print("=== ALL STREAMING TESTS PASS ===")
    return 0 if passed == len(tests) else 1


if __name__ == "__main__":
    sys.exit(main())
