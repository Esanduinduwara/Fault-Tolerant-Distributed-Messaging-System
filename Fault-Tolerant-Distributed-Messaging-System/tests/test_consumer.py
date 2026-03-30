"""
=============================================================================
 MEMBER 2 — DATA REPLICATION & MEMBER 4 — CONSENSUS — Consumer Unit Tests
 File: tests/test_consumer.py
 Run with: pytest tests/test_consumer.py -v
=============================================================================

PUSH SCHEDULE
--------------
Member 2:
Day 2  →  Tests 1-3: Consumer init, _process_message, partition assignment
Day 3  →  Tests 4-5: auto-reconnect behaviour, stop lifecycle
Member 4:
Day 3  →  Test 6: leader lock acquisition success
Day 4  →  Test 7: leader lock contention (DuplicateKeyError)
Day 5  →  Test 8: leader stats job updates system_stats
=============================================================================
"""

import json
import time
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime, timezone


@pytest.fixture
def mock_mongodb():
    """Mock the MongoDBHandler singleton to avoid real DB connections."""
    mock_db = MagicMock()
    mock_db.save_message.return_value = True
    mock_db.log_event.return_value = None
    mock_db.db = {
        "messages":     MagicMock(),
        "users":        MagicMock(),
        "system_stats": MagicMock(),
        "leader_lock":  MagicMock(),
    }
    return mock_db


@pytest.fixture
def mock_consumer_env(mock_mongodb):
    """Mock all externals: Kafka, MongoDB, NTP, Heartbeat."""
    with patch("src.consumer.message_consumer.MongoDBHandler", return_value=mock_mongodb), \
         patch("src.time_sync.time_synchronizer.NTPSynchronizer._do_sync"), \
         patch("src.fault_detection.heartbeat_monitor.HeartbeatMonitor.start"), \
         patch("src.fault_detection.heartbeat_monitor.HeartbeatMonitor._ping_node", return_value=True):
        yield mock_mongodb


class TestFaultTolerantConsumer:

    # ── MEMBER 2  DAY 2 ─────────────────────────────────────────────────────

    def test_consumer_init(self, mock_consumer_env):
        """Consumer should initialize without error."""
        from src.consumer.message_consumer import FaultTolerantConsumer
        consumer = FaultTolerantConsumer(node_id="test-node")
        assert consumer.node_id == "test-node"
        assert consumer.running is False

    def test_process_message_success(self, mock_consumer_env):
        """_process_message should save to DB and return True."""
        from src.consumer.message_consumer import FaultTolerantConsumer
        consumer = FaultTolerantConsumer(node_id="test-node")

        msg = {
            "messageId": "m1",
            "fromUser":  "alice",
            "toUser":    "bob",
            "content":   "Hello!",
            "timestamp": int(time.time() * 1000),
        }
        result = consumer._process_message(msg, offset=0, partition=0)
        assert result is True
        mock_consumer_env.save_message.assert_called_once_with(msg)
        mock_consumer_env.log_event.assert_called_once()

    def test_process_message_db_failure(self, mock_consumer_env):
        """_process_message should return False if DB save raises."""
        from src.consumer.message_consumer import FaultTolerantConsumer
        mock_consumer_env.save_message.side_effect = Exception("DB down")
        consumer = FaultTolerantConsumer(node_id="test-node")

        msg = {"messageId": "m-fail", "fromUser": "a", "toUser": "b", "content": "x"}
        result = consumer._process_message(msg, offset=0, partition=0)
        assert result is False

    # ── MEMBER 2  DAY 3 ─────────────────────────────────────────────────────

    def test_build_consumer_config(self, mock_consumer_env):
        """_build_consumer should configure consumer group and manual commit."""
        from src.consumer.message_consumer import FaultTolerantConsumer

        with patch("src.consumer.message_consumer.KafkaConsumer") as mock_kc:
            consumer = FaultTolerantConsumer(node_id="test-node")
            consumer._build_consumer()
            call_kwargs = mock_kc.call_args[1]
            assert call_kwargs["enable_auto_commit"] is False
            assert call_kwargs["group_id"] == "streamflow-consumers"

    def test_stop_sets_running_false(self, mock_consumer_env):
        """stop() should set running=False and close DB."""
        from src.consumer.message_consumer import FaultTolerantConsumer
        consumer = FaultTolerantConsumer(node_id="test-node")
        consumer.running = True
        consumer.stop()
        assert consumer.running is False
