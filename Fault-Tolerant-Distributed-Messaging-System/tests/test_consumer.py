"""
 MEMBER 2 — DATA REPLICATION & MEMBER 4 — CONSENSUS — Consumer Unit Tests
 Run with: pytest tests/test_consumer.py -v
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
        
        
    # ── MEMBER 4  DAY 3 ─────────────────────────────────────────────────────

    def test_leader_lock_acquisition(self, mock_mongodb):
        """try_acquire_leader_lock should return True on successful insert."""
        from src.consumer.message_consumer import try_acquire_leader_lock

        mock_mongodb.db["leader_lock"].create_index = MagicMock()
        mock_mongodb.db["leader_lock"].insert_one = MagicMock(return_value=None)

        result = try_acquire_leader_lock(mock_mongodb, "node-1", ttl_seconds=30)
        assert result is True

    def test_leader_lock_contention(self, mock_mongodb):
        """Second lock attempt should return False (DuplicateKeyError)."""
        from src.consumer.message_consumer import try_acquire_leader_lock
        from pymongo.errors import DuplicateKeyError

        mock_mongodb.db["leader_lock"].create_index = MagicMock()
        mock_mongodb.db["leader_lock"].insert_one = MagicMock(
            side_effect=DuplicateKeyError("duplicate")
        )

        result = try_acquire_leader_lock(mock_mongodb, "node-2", ttl_seconds=30)
        assert result is False

    def test_leader_stats_job(self, mock_mongodb):
        """run_leader_stats_job should aggregate counts into system_stats."""
        from src.consumer.message_consumer import run_leader_stats_job

        mock_mongodb.db["messages"].count_documents.return_value = 42
        mock_mongodb.db["users"].count_documents.return_value = 3

        run_leader_stats_job(mock_mongodb)

        mock_mongodb.db["system_stats"].replace_one.assert_called_once()
        call_args = mock_mongodb.db["system_stats"].replace_one.call_args
        assert call_args[0][1]["message_count"] == 42
        assert call_args[0][1]["user_count"] == 3        
