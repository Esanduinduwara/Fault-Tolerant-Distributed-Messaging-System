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