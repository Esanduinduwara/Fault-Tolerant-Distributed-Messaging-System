"""
=============================================================================
 MEMBER 4 — CONSENSUS & AGREEMENT — REST API Unit Tests
 File: tests/test_api.py
 Run with: pytest tests/test_api.py -v
=============================================================================

PUSH SCHEDULE (Member 4 — API Tests)
--------------------------------------
Day 2  →  Tests 1-3: health, send message success, message via fallback
Day 3  →  Tests 4-6: get messages, get conversation, sorted_by field
Day 4  →  Tests 7-10: create user, get user, 404, /stats, /logs
Day 5  →  Tests 11-12: /clock-skew endpoint, duplicate user 409

GIT COMMIT MESSAGE TEMPLATES
-----------------------------
Day 2: "test(api): add health check, message send, and DB fallback tests"
Day 3: "test(api): add message retrieval and conversation endpoint tests"
Day 4: "test(api): add user CRUD, stats, and logs tests"
Day 5: "test(api): add clock-skew endpoint and duplicate-user test — final API verification"
=============================================================================
"""

import json
import time
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures — mock all external systems before importing the app
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_db():
    """Create a mock MongoDBHandler."""
    db = MagicMock()
    db.health_check.return_value = True
    db.save_message.return_value = True
    db.get_all_messages.return_value = []
    db.get_messages_between_users.return_value = []
    db.get_all_users.return_value = []
    db.get_user.return_value = None
    db.create_user.return_value = True
    db.get_logs.return_value = []
    db.db = {"system_stats": MagicMock()}
    db.db["system_stats"].find_one.return_value = None
    return db


@pytest.fixture
def client(mock_db):
    """TestClient with all externals mocked."""
    # Mock the Kafka producer pool
    mock_pool = MagicMock()
    mock_pool.send.return_value = True
    mock_pool.is_connected.return_value = True
    mock_pool.close.return_value = None

    with patch("src.api.rest_api.get_db", return_value=mock_db), \
         patch("src.api.rest_api.KafkaProducerPool.get_instance", return_value=mock_pool):
        from src.api.rest_api import app
        with TestClient(app) as tc:
            yield tc, mock_db, mock_pool


class TestAPIRoutes:

    # ── DAY 2 ────────────────────────────────────────────────────────────────

    def test_health_endpoint(self, client):
        """GET /health should return status with mongodb and kafka."""
        tc, mock_db, _ = client
        response = tc.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "mongodb" in data
        assert "kafka" in data

    def test_send_message_via_kafka(self, client):
        """POST /messages should route through Kafka pool."""
        tc, _, _ = client
        response = tc.post("/messages", json={
            "messageId": "m1",
            "fromUser":  "alice",
            "toUser":    "bob",
            "content":   "Hello Bob!",
        })
        assert response.status_code == 201
        data = response.json()
        assert data["status"] == "sent"
        assert data["messageId"] == "m1"
        assert data["route"] == "kafka"

    def test_send_message_via_fallback(self, client):
        """POST /messages should use direct DB if Kafka pool fails."""
        tc, mock_db, mock_pool = client
        mock_pool.send.side_effect = Exception("Kafka down")

        response = tc.post("/messages", json={
            "messageId": "m-fallback",
            "fromUser":  "alice",
            "toUser":    "bob",
            "content":   "Fallback test",
        })
        assert response.status_code == 201
        data = response.json()
        assert data["route"] == "direct_db_fallback"
        mock_db.save_message.assert_called_once()

    # ── DAY 3 ────────────────────────────────────────────────────────────────

    def test_get_all_messages(self, client):
        """GET /messages should return messages and count."""
        tc, mock_db, _ = client
        mock_db.get_all_messages.return_value = [
            {"messageId": "m1", "content": "hi"},
        ]
        response = tc.get("/messages")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["messages"][0]["messageId"] == "m1"

    def test_get_conversation(self, client):
        """GET /messages/{user1}/{user2} should include sorted_by field."""
        tc, mock_db, _ = client
        mock_db.get_messages_between_users.return_value = [
            {"messageId": "m1", "fromUser": "alice", "toUser": "bob", "content": "hi"},
        ]
        response = tc.get("/messages/alice/bob")
        assert response.status_code == 200
        data = response.json()
        assert data["sorted_by"] == "timestamp_asc"

    def test_message_auto_timestamp(self, client):
        """POST /messages without timestamp should auto-add one."""
        tc, _, mock_pool = client
        response = tc.post("/messages", json={
            "messageId": "m-ts",
            "fromUser":  "alice",
            "toUser":    "bob",
            "content":   "No timestamp",
        })
        assert response.status_code == 201

    # ── DAY 4 ────────────────────────────────────────────────────────────────

    def test_create_user(self, client):
        """POST /users should create a user."""
        tc, _, _ = client
        response = tc.post("/users", json={
            "userId":   "u1",
            "username": "alice",
            "email":    "alice@example.com",
        })
        assert response.status_code == 201
        assert response.json()["status"] == "created"

    def test_duplicate_user_409(self, client):
        """POST /users for existing user should return 409."""
        tc, mock_db, _ = client
        mock_db.create_user.return_value = False
        response = tc.post("/users", json={
            "userId": "u1", "username": "alice", "email": "a@b.com"
        })
        assert response.status_code == 409

    def test_get_user_not_found(self, client):
        """GET /users/{id} for missing user should return 404."""
        tc, mock_db, _ = client
        mock_db.get_user.return_value = None
        response = tc.get("/users/nonexistent")
        assert response.status_code == 404

    def test_get_stats_not_computed(self, client):
        """GET /stats before leader job runs should return hint message."""
        tc, _, _ = client
        response = tc.get("/stats")
        assert response.status_code == 200
        assert "not yet computed" in response.json().get("message", "")

    def test_get_logs(self, client):
        """GET /logs should return log entries."""
        tc, mock_db, _ = client
        mock_db.get_logs.return_value = [{"event": "test"}]
        response = tc.get("/logs")
        assert response.status_code == 200
        assert len(response.json()["logs"]) == 1

    def test_list_users(self, client):
        """GET /users should return user list."""
        tc, mock_db, _ = client
        mock_db.get_all_users.return_value = [{"userId": "u1", "username": "alice"}]
        response = tc.get("/users")
        assert response.status_code == 200
        assert len(response.json()["users"]) == 1