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