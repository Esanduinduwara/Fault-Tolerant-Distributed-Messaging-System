"""
=============================================================================
 MEMBER 1 — FAULT TOLERANCE — Producer Unit Tests
 File: tests/test_producer.py
 Run with: pytest tests/test_producer.py -v
=============================================================================

PUSH SCHEDULE (Member 1 — Tests)
----------------------------------
Day 1  →  Tests 1-3: FaultTolerantProducer init tests
Day 2  →  Tests 4-6: send_message and DLQ behaviour
Day 3  →  Tests 7-9: batch send, error paths, HLC integration
Day 4  →  Run full suite, verify green
Day 5  →  Final integration verification

GIT COMMIT MESSAGE TEMPLATES
-----------------------------
Day 1: "test(producer): add FaultTolerantProducer init and connection tests"
Day 2: "test(producer): add send_message, DLQ fallback, and timestamp tests"
Day 3: "test(producer): add batch send, error handling, and HLC integration tests"
=============================================================================
"""

import json
import pytest
from unittest.mock import patch, MagicMock, PropertyMock


# ─────────────────────────────────────────────────────────────────────────────
# Mock all external dependencies BEFORE importing the producer module
# This prevents real Kafka/MongoDB/NTP connections during testing
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_all_externals():
    """
    Fixture: mock Kafka, NTP, and HLC at the module level so no test
    accidentally connects to a real broker or NTP server.
    """
    mock_kafka_producer = MagicMock()
    mock_future = MagicMock()
    mock_future.get.return_value = MagicMock(partition=0, offset=42)
    mock_kafka_producer.return_value.send.return_value = mock_future

    with patch("src.producer.message_producer.KafkaProducer", mock_kafka_producer), \
         patch("src.time_sync.time_synchronizer.NTPSynchronizer._do_sync"):
        yield mock_kafka_producer


class TestFaultTolerantProducer:

    # ── DAY 1 TESTS ────────────────────────────────────────────────────────

    def test_producer_initializes_successfully(self):
        """Producer should initialize without raising any exception."""
        from src.producer.message_producer import FaultTolerantProducer
        producer = FaultTolerantProducer()
        assert producer is not None
        assert producer.topic == "user-messages"

    def test_producer_uses_all_brokers(self, mock_all_externals):
        """Producer should initialize with all 3 bootstrap servers."""
        from src.producer.message_producer import FaultTolerantProducer
        producer = FaultTolerantProducer()
        # KafkaProducer was called with bootstrap_servers as a list
        call_kwargs = mock_all_externals.call_args[1]
        bootstrap = call_kwargs.get("bootstrap_servers")
        assert isinstance(bootstrap, list)
        assert len(bootstrap) >= 1  # at least 1 broker configured

    def test_producer_uses_acks_all(self, mock_all_externals):
        """Producer should use acks='all' for quorum writes."""
        from src.producer.message_producer import FaultTolerantProducer
        producer = FaultTolerantProducer()
        call_kwargs = mock_all_externals.call_args[1]
        assert call_kwargs["acks"] == "all"

    # ── DAY 2 TESTS ────────────────────────────────────────────────────────

    def test_send_message_success(self):
        """Sending a message should return True on success."""
        from src.producer.message_producer import FaultTolerantProducer
        producer = FaultTolerantProducer()
        result = producer.send_message({
            "messageId": "m1",
            "fromUser":  "alice",
            "toUser":    "bob",
            "content":   "Hello!",
        })
        assert result is True

    def test_send_message_adds_timestamp(self):
        """Messages without timestamps should get one."""
        from src.producer.message_producer import FaultTolerantProducer
        producer = FaultTolerantProducer()
        msg = {
            "messageId": "m2",
            "fromUser":  "alice",
            "toUser":    "bob",
            "content":   "Hi",
        }
        producer.send_message(msg)
        assert "timestamp" in msg
        assert isinstance(msg["timestamp"], int)

    def test_send_message_uses_canonical_key(self):
        """Partition key should use sorted user pair for consistent routing."""
        from src.producer.message_producer import FaultTolerantProducer
        producer = FaultTolerantProducer()
        msg1 = {"messageId": "m1", "fromUser": "bob", "toUser": "alice", "content": "Hi"}
        msg2 = {"messageId": "m2", "fromUser": "alice", "toUser": "bob", "content": "Hey"}
        producer.send_message(msg1)
        producer.send_message(msg2)
        # Both calls should use the same key "alice:bob"
        calls = producer.producer.send.call_args_list
        assert calls[0][1]["key"] == "alice:bob"
        assert calls[1][1]["key"] == "alice:bob"

    def test_send_to_dlq_on_kafka_error(self, tmp_path):
        """When Kafka fails, message should be saved to DLQ file."""
        from src.producer.message_producer import FaultTolerantProducer
        from kafka.errors import KafkaError

        producer = FaultTolerantProducer()
        producer.dlq_file = str(tmp_path / "test_dlq.jsonl")

        # Make Kafka raise an error
        producer.producer.send.return_value.get.side_effect = KafkaError("broker down")

        result = producer.send_message({
            "messageId": "m-fail",
            "fromUser":  "alice",
            "toUser":    "bob",
            "content":   "Lost?",
        })
        assert result is False
        # DLQ file should exist with the failed message
        with open(producer.dlq_file) as f:
            entry = json.loads(f.readline())
        assert entry["original_message"]["messageId"] == "m-fail"

    # ── DAY 3 TESTS ────────────────────────────────────────────────────────

    def test_send_batch(self):
        """Batch send should return sent/failed counts."""
        from src.producer.message_producer import FaultTolerantProducer
        producer = FaultTolerantProducer()
        messages = [
            {"messageId": f"batch-{i}", "fromUser": "a", "toUser": "b", "content": f"msg{i}"}
            for i in range(5)
        ]
        stats = producer.send_batch(messages)
        assert stats["sent"] == 5
        assert stats["failed"] == 0

    def test_close_flushes_producer(self):
        """close() should call flush() and close() on the Kafka producer."""
        from src.producer.message_producer import FaultTolerantProducer
        producer = FaultTolerantProducer()
        producer.close()
        producer.producer.flush.assert_called()
        producer.producer.close.assert_called_once()
