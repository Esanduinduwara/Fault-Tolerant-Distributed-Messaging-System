"""
=============================================================================
 MEMBER 1 - FAULT TOLERANCE - DLQ Replayer Unit Tests
 File: tests/test_dlq_replayer.py
 Run with: pytest tests/test_dlq_replayer.py -v
=============================================================================

PUSH SCHEDULE (Member 1 - DLQ Replayer Tests)
-----------------------------------------------
Day 3  -  test_no_dlq_file + test_replay_success
Day 4  -  test_partial_replay + test_archive_created
Day 5  -  Run full suite, verify green


=============================================================================
"""

import json
import os
import pytest
from unittest.mock import patch, MagicMock

from src.producer.dlq_replayer import DLQReplayer


class TestDLQReplayer:

    def test_no_dlq_file(self, tmp_path):
        """Replaying with no DLQ file should return zero counts."""
        replayer = DLQReplayer(dlq_file=str(tmp_path / "nonexistent.jsonl"))
        stats = replayer.replay()
        assert stats["total"] == 0
        assert stats["replayed"] == 0
        assert stats["still_failed"] == 0

    def test_empty_dlq_file(self, tmp_path):
        """Replaying an empty DLQ file should return zero counts."""
        dlq_path = str(tmp_path / "empty.jsonl")
        open(dlq_path, "w").close()
        replayer = DLQReplayer(dlq_file=dlq_path)
        stats = replayer.replay()
        assert stats["total"] == 0

    def test_replay_success(self, tmp_path):
        """All messages should be replayed when Kafka is available."""
        dlq_path = str(tmp_path / "test.jsonl")

        # Write 2 DLQ entries
        entries = [
            {
                "failed_at": "2024-01-01T00:00:00Z",
                "error": "broker down",
                "original_message": {
                    "messageId": "m1", "fromUser": "alice",
                    "toUser": "bob", "content": "hello"
                }
            },
            {
                "failed_at": "2024-01-01T00:01:00Z",
                "error": "broker down",
                "original_message": {
                    "messageId": "m2", "fromUser": "bob",
                    "toUser": "alice", "content": "hi"
                }
            }
        ]
        with open(dlq_path, "w") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")

        # Mock the producer to succeed
        mock_producer = MagicMock()
        mock_producer.send_message.return_value = True
        mock_producer.close.return_value = None

        with patch("src.producer.message_producer.KafkaProducer"), \
             patch("src.time_sync.time_synchronizer.NTPSynchronizer._do_sync"), \
             patch("src.producer.message_producer.FaultTolerantProducer") as MockProducer:
            mock_instance = MockProducer.return_value
            mock_instance.send_message.return_value = True
            mock_instance.close.return_value = None
            replayer = DLQReplayer(dlq_file=dlq_path)
            stats = replayer.replay()

        assert stats["total"] == 2
        assert stats["replayed"] == 2
        assert stats["still_failed"] == 0
        # DLQ file should be removed after successful replay
        assert not os.path.exists(dlq_path)

    def test_partial_replay(self, tmp_path):
        """Some messages fail so they should stay in the DLQ file."""
        dlq_path = str(tmp_path / "partial.jsonl")

        entries = [
            {
                "failed_at": "2024-01-01T00:00:00Z",
                "error": "broker down",
                "original_message": {
                    "messageId": "m1", "fromUser": "alice",
                    "toUser": "bob", "content": "hello"
                }
            },
            {
                "failed_at": "2024-01-01T00:01:00Z",
                "error": "broker down",
                "original_message": {
                    "messageId": "m2", "fromUser": "bob",
                    "toUser": "alice", "content": "hi"
                }
            }
        ]
        with open(dlq_path, "w") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")

        with patch("src.producer.message_producer.KafkaProducer"), \
             patch("src.time_sync.time_synchronizer.NTPSynchronizer._do_sync"), \
             patch("src.producer.message_producer.FaultTolerantProducer") as MockProducer:
            mock_instance = MockProducer.return_value
            mock_instance.send_message.side_effect = [True, False]
            mock_instance.close.return_value = None
            replayer = DLQReplayer(dlq_file=dlq_path)
            stats = replayer.replay()

        assert stats["total"] == 2
        assert stats["replayed"] == 1
        assert stats["still_failed"] == 1
        # DLQ file should still exist with the 1 failed message
        assert os.path.exists(dlq_path)

    def test_archive_created_on_success(self, tmp_path):
        """Successfully replayed messages should be archived."""
        dlq_path = str(tmp_path / "archive_test.jsonl")
        done_path = str(tmp_path / "archive_test_replayed.jsonl")

        entry = {
            "failed_at": "2024-01-01",
            "error": "down",
            "original_message": {
                "messageId": "m1", "fromUser": "a",
                "toUser": "b", "content": "c"
            }
        }
        with open(dlq_path, "w") as f:
            f.write(json.dumps(entry) + "\n")

        with patch("src.producer.message_producer.KafkaProducer"), \
             patch("src.time_sync.time_synchronizer.NTPSynchronizer._do_sync"), \
             patch("src.producer.message_producer.FaultTolerantProducer") as MockProducer:
            mock_instance = MockProducer.return_value
            mock_instance.send_message.return_value = True
            mock_instance.close.return_value = None
            replayer = DLQReplayer(dlq_file=dlq_path)
            replayer.replay()

        assert os.path.exists(done_path)
        with open(done_path) as f:
            archived = json.loads(f.readline())
        assert archived["original_message"]["messageId"] == "m1"
        assert "replayed_at" in archived
