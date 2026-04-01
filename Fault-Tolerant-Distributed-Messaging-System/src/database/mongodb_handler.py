"""
=============================================================================
 MEMBER 3 — TIME SYNCHRONIZATION & MESSAGE ORDERING
=============================================================================

=============================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# DAY 1  
# Imports + Singleton class skeleton + __new__ + __init__ with WriteConcern
# ─────────────────────────────────────────────────────────────────────────────
import logging
import time
import threading
from datetime import datetime, timezone

from pymongo import MongoClient, ASCENDING, errors
from pymongo.write_concern import WriteConcern

from src.config.settings import (
    MONGODB_URI, MONGODB_DATABASE,
    COL_MESSAGES, COL_USERS, COL_CHATROOMS, COL_LOGS,
)

logger = logging.getLogger(__name__)


class MongoDBHandler:
    """
    Thread-safe Singleton MongoDB connection wrapper.
    Implements Part 3 (Time & Order) guarantees:
      - All messages stored with NTP-corrected millisecond timestamps
      - Hybrid Logical Clock (HLC) timestamps for causal ordering
      - Reads always sorted: timestamp ASC, HLC tie-breaker, messageId fallback
      - Unique index on messageId → idempotent deduplication
      - WriteConcern majority+journal → strong write durability
      - Timestamp correction via NTP offset for stored messages
    """

    _instance     = None
    _initialised  = False
    _init_lock    = threading.Lock()  # Thread-safe singleton initialisation

    def __new__(cls):
        # Thread-safe singleton: use a lock to prevent two threads from
        # creating separate instances simultaneously.
        with cls._init_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialised = False
            return cls._instance

    def __init__(self):
        if self._initialised:
            return    # already connected — do nothing
        logger.info("Connecting to MongoDB …")
        self.client = MongoClient(MONGODB_URI)
        # WriteConcern(w="majority"): write confirmed only after majority of
        # replica-set members acknowledge it → prevents data loss on node crash.
        # j=True (journaled): must flush to on-disk journal → survives power loss.
        self.db = self.client[MONGODB_DATABASE].with_options(
            write_concern=WriteConcern(w="majority", j=True)
        )
        self._ensure_indexes()
        self._initialised = True
        logger.info("MongoDB connected and indexes verified.")
# ─────────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # DAY 2  
    # Index creation — the foundation of time ordering and deduplication
    # ─────────────────────────────────────────────────────────────────────────
    def _ensure_indexes(self):
        """
        Create all required indexes on first startup.
        These indexes implement the Part 3 (Time & Order) requirements:
          - Unique messageId → deduplication (exactly-once storage)
          - Compound (fromUser, toUser, timestamp) → fast sorted reads
          - HLC encoded index → causal ordering with logical clock tie-breaking
          - Timestamp index → global message ordering
        """
        msgs = self.db[COL_MESSAGES]

        # UNIQUE index on messageId — THE deduplication mechanism.
        # Kafka at-least-once delivery may redeliver messages.
        # Second insert raises DuplicateKeyError → caught → treated as success.
        msgs.create_index(
            [("messageId", ASCENDING)], unique=True, background=True
        )

        # Compound index: fast conversation reads sorted by timestamp.
        # Without this, MongoDB scans ALL messages for each conversation query.
        msgs.create_index(
            [("fromUser", ASCENDING), ("toUser", ASCENDING), ("timestamp", ASCENDING)],
            background=True,
        )

        # Simple timestamp index for global message ordering.
        msgs.create_index([("timestamp", ASCENDING)], background=True)

        # ── HLC index (Part 3: Hybrid Logical Clock ordering) ─────────────
        # The hlc_encoded field is a single 64-bit int that encodes both
        # NTP-corrected physical time and Lamport logical counter.
        # Sorting by this field gives causal ordering even when physical
        # timestamps are identical (the logical counter breaks ties).
        msgs.create_index([("hlc_encoded", ASCENDING)], background=True, sparse=True)

        # User indexes (unique userId and username)
        self.db[COL_USERS].create_index(
            [("userId", ASCENDING)], unique=True, background=True
        )
        self.db[COL_USERS].create_index(
            [("username", ASCENDING)], unique=True, background=True
        )

        # Logs index for fast time-based retrieval
        self.db[COL_LOGS].create_index(
            [("timestamp", ASCENDING)], background=True
        )
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # DAY 3  
    # Message persistence and retrieval — core Part 3 methods
    # ─────────────────────────────────────────────────────────────────────────
    def save_message(self, message_data: dict) -> bool:
        """
        Persist a message. Idempotent — safe to call multiple times.
        Returns True on success AND on duplicate (no error raised for dupes).
        Stores stored_at timestamp in milliseconds for audit trail.

        TIMESTAMP CORRECTION (Part 3):
          If the message has an NTP-corrected timestamp from the producer,
          we store it as-is.  We also record stored_at (the time the DB
          actually wrote this record) so we can calculate the end-to-end
          delivery latency: delivery_latency = stored_at - timestamp.
        """
        try:
            if "timestamp" not in message_data:
                # Fallback: set timestamp at storage time if producer missed it
                message_data["timestamp"] = int(time.time() * 1000)

            # stored_at = when the DB actually wrote this record (audit trail)
            message_data["stored_at"]      = int(datetime.now(timezone.utc).timestamp() * 1000)
            message_data["deliveryStatus"] = message_data.get("deliveryStatus", "delivered")

            self.db[COL_MESSAGES].insert_one(message_data)
            logger.info(f"Message saved: {message_data.get('messageId')}")
            return True

        except errors.DuplicateKeyError:
            # Duplicate messageId → message already in DB from earlier delivery.
            # This is expected behaviour with at-least-once Kafka delivery.
            logger.info(f"Duplicate message ignored (idempotent): {message_data.get('messageId')}")
            return True

        except Exception as exc:
            logger.error(f"Failed to save message: {exc}")
            raise

    def get_messages_between_users(self, user1: str, user2: str, limit: int = 50) -> list:
        """
        Fetch full conversation between two users.
        ALWAYS sorted: timestamp ASC (oldest first), then messageId tie-breaker.
        This is the core Part 3 (Time & Order) read guarantee.

        ORDERING STRATEGY:
          Primary sort:  timestamp ASC (NTP-corrected physical time)
          Secondary sort: hlc_encoded ASC (HLC causal order, if available)
          Tertiary sort:  messageId ASC (final tie-breaker for truly concurrent messages)
        """
        query = {
            "$or": [
                {"fromUser": user1, "toUser": user2},
                {"fromUser": user2, "toUser": user1},
            ]
        }
        cursor = (
            self.db[COL_MESSAGES]
            .find(query, {"_id": 0})           # exclude internal MongoDB _id
            .sort([("timestamp", ASCENDING), ("hlc_encoded", ASCENDING), ("messageId", ASCENDING)])
            .limit(limit)
        )
        return list(cursor)

    def get_all_messages(self, limit: int = 100) -> list:
        """Return all messages sorted by time — used by GET /messages."""
        return list(
            self.db[COL_MESSAGES]
            .find({}, {"_id": 0})
            .sort([("timestamp", ASCENDING), ("hlc_encoded", ASCENDING), ("messageId", ASCENDING)])
            .limit(limit)
        )

    def update_message_status(self, message_id: str, status: str) -> bool:
        result = self.db[COL_MESSAGES].update_one(
            {"messageId": message_id},
            {"$set": {"deliveryStatus": status}},
        )
        return result.modified_count > 0
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # DAY 4  
    # User management + chat rooms + system audit logging
    # ─────────────────────────────────────────────────────────────────────────
    def create_user(self, user_data: dict) -> bool:
        """Create a user. Returns False (not error) if userId/username exists."""
        try:
            if "joinDate" not in user_data:
                user_data["joinDate"] = int(datetime.now(timezone.utc).timestamp() * 1000)
            self.db[COL_USERS].insert_one(user_data)
            logger.info(f"User created: {user_data.get('username')}")
            return True
        except errors.DuplicateKeyError:
            logger.warning(f"User already exists: {user_data.get('userId')}")
            return False

    def get_user(self, user_id: str):
        return self.db[COL_USERS].find_one({"userId": user_id}, {"_id": 0})

    def get_all_users(self) -> list:
        return list(self.db[COL_USERS].find({}, {"_id": 0}))

    def update_last_seen(self, user_id: str):
        """Update lastSeen timestamp in milliseconds when user is fetched."""
        self.db[COL_USERS].update_one(
            {"userId": user_id},
            {"$set": {"lastSeen": int(datetime.now(timezone.utc).timestamp() * 1000)}},
        )

    def create_chatroom(self, room_data: dict) -> bool:
        try:
            if "createdDate" not in room_data:
                room_data["createdDate"] = int(datetime.now(timezone.utc).timestamp() * 1000)
            self.db[COL_CHATROOMS].insert_one(room_data)
            return True
        except errors.DuplicateKeyError:
            return False

    def get_chatroom(self, room_id: str):
        return self.db[COL_CHATROOMS].find_one({"roomId": room_id}, {"_id": 0})

    def log_event(self, message_id: str, event: str, server_node: str = "node-1"):
        """
        Append a system log entry — called by the consumer after each message.
        Provides full audit trail: which node processed which message and when.
        Timestamps stored in milliseconds (Part 3 requirement).
        """
        self.db[COL_LOGS].insert_one({
            "messageId":  message_id,
            "event":      event,
            "timestamp":  int(datetime.now(timezone.utc).timestamp() * 1000),
            "serverNode": server_node,
        })

    def get_logs(self, limit: int = 50) -> list:
        """Return most recent log entries (newest first) for GET /logs."""
        return list(
            self.db[COL_LOGS]
            .find({}, {"_id": 0})
            .sort("timestamp", -1)     # -1 = descending (newest first)
            .limit(limit)
        )
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # DAY 5  
    # Health check + connection teardown + singleton reset + timestamp correction
    # ─────────────────────────────────────────────────────────────────────────
    def correct_timestamps(self, ntp_offset_ms: float, batch_size: int = 100) -> int:
        """
        Timestamp correction technique (Part 3 requirement).

        Retroactively adjusts stored message timestamps based on the current
        NTP offset.  This is useful when:
          - A node's clock was significantly skewed when it stored messages
          - NTP sync reveals the offset after messages are already persisted

        Args:
            ntp_offset_ms: the NTP offset in milliseconds (+ means local was behind)
            batch_size: number of messages to correct per batch

        Returns:
            Number of messages corrected.

        NOTE: This operation is append-only — the original timestamp is
        preserved in 'original_timestamp' so corrections are reversible.
        """
        corrected = 0
        cursor = self.db[COL_MESSAGES].find(
            {"timestamp_corrected": {"$ne": True}},
            {"_id": 1, "timestamp": 1}
        ).limit(batch_size)

        for doc in cursor:
            original_ts = doc["timestamp"]
            corrected_ts = original_ts + int(ntp_offset_ms)

            self.db[COL_MESSAGES].update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "timestamp":            corrected_ts,
                        "original_timestamp":   original_ts,
                        "timestamp_corrected":  True,
                        "correction_offset_ms": int(ntp_offset_ms),
                    }
                },
            )
            corrected += 1

        if corrected > 0:
            logger.info(
                f"[TIMESTAMP CORRECTION] Corrected {corrected} messages "
                f"by {ntp_offset_ms:+.2f}ms"
            )
        return corrected

    def health_check(self) -> bool:
        """Ping MongoDB server. Used by GET /health and Docker healthcheck."""
        try:
            self.client.admin.command("ping")
            return True
        except Exception:
            return False

    def close(self):
        """Close MongoDB connection and reset singleton so it can reconnect."""
        self.client.close()
        with MongoDBHandler._init_lock:
            MongoDBHandler._instance    = None
            MongoDBHandler._initialised = False
        self._initialised = False
    # ─────────────────────────────────────────────────────────────────────────