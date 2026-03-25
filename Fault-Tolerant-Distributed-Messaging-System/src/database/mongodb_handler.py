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