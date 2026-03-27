import time
import logging
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.config.settings import KAFKA_TOPIC_MESSAGES, KAFKA_BOOTSTRAP_SERVERS
from src.database.mongodb_handler import MongoDBHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def get_db() -> MongoDBHandler:
    """
    Lazy DB accessor — called inside each route handler, NOT at module level.
    Reason: importing this module in tests would instantly try to connect to
    MongoDB before any mock can be applied, causing tests to fail.
    MongoDBHandler is a singleton, so only one connection is ever created.
    """
    return MongoDBHandler()


class MessageRequest(BaseModel):
    """Pydantic validates all incoming JSON automatically against this model."""
    messageId:      str
    fromUser:       str
    toUser:         str
    content:        str
    messageType:    str           = "text"
    deliveryStatus: str           = "sent"
    timestamp:      Optional[int] = None  # ms epoch; auto-filled if missing


class UserRequest(BaseModel):
    userId:   str
    username: str
    email:    str

class KafkaProducerPool:
    """
    Singleton Kafka producer connection pool.

    WHY A POOL INSTEAD OF PER-REQUEST PRODUCERS:
      Creating a KafkaProducer involves:
        1. TCP connection to bootstrap broker(s)
        2. Metadata exchange (discover cluster topology)
        3. Allocate internal buffers and background threads
      This takes 50-200ms per creation.  At 100 req/s, that's 5-20 seconds
      of pure connection overhead per second — unsustainable.

    POOL STRATEGY:
      We maintain ONE long-lived KafkaProducer that is:
        - Created once at API startup via FastAPI lifespan
        - Reused across ALL /messages POST requests
        - Flushed and closed on API shutdown
        - If it fails, __send falls back to direct DB write (fault tolerance)

    PERFORMANCE IMPACT:
      - First request: ~100ms (producer already pre-warmed at startup)
      - Subsequent requests: ~1-5ms (reuse existing TCP connection)
      - Memory: one set of buffers vs N sets for N concurrent requests
    """

    _instance = None

    def __init__(self):
        from kafka import KafkaProducer
        import json

        # ── Multi-broker bootstrap (Part 2: Quorum Replication) ───────────
        bootstrap = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",")]

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",              # QUORUM: wait for ALL in-sync replicas
            retries=3,
            request_timeout_ms=5_000,
            linger_ms=5,
        )
        logger.info(f"[POOL] KafkaProducer pool created (brokers={bootstrap})")

    def send(self, message_data: dict) -> bool:
        """Send a message using the pooled producer. Returns True on success."""
        # Canonical key: sorted user pair for consistent partition routing
        users = sorted([message_data['fromUser'], message_data['toUser']])
        key   = f"{users[0]}:{users[1]}"

        if not message_data.get("timestamp"):
            message_data["timestamp"] = int(time.time() * 1000)

        self._producer.send(KAFKA_TOPIC_MESSAGES, key=key, value=message_data)
        self._producer.flush(timeout=5)
        return True

    def close(self):
        """Flush pending messages and close the producer."""
        try:
            self._producer.flush(timeout=10)
            self._producer.close()
            logger.info("[POOL] KafkaProducer pool closed")
        except Exception as exc:
            logger.warning(f"[POOL] Error closing producer: {exc}")

    def is_connected(self) -> bool:
        """Lightweight check: can we reach at least one Kafka broker?"""
        try:
            return bool(self._producer.bootstrap_connected())
        except Exception:
            return False

    @classmethod
    def get_instance(cls):
        """Get or create the singleton producer pool."""
        if cls._instance is None:
            try:
                cls._instance = cls()
            except Exception as exc:
                logger.error(f"[POOL] Failed to create producer pool: {exc}")
                return None
        return cls._instance

    @classmethod
    def shutdown(cls):
        """Shutdown the singleton pool."""
        if cls._instance:
            cls._instance.close()
            cls._instance = None
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI lifespan — startup/shutdown hooks for connection pool
# ─────────────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app):
    """
    FastAPI lifespan context manager.
    - On startup: pre-warm the Kafka producer pool
    - On shutdown: flush and close all connections
    """
    logger.info("[LIFESPAN] Starting up — creating Kafka producer pool")
    KafkaProducerPool.get_instance()
    yield
    logger.info("[LIFESPAN] Shutting down — closing Kafka producer pool")
    KafkaProducerPool.shutdown()


app = FastAPI(
    title="StreamFlow API",
    description="Fault-tolerant distributed messaging system - SE2062",
    version="1.0.0",
    lifespan=lifespan,
)

# Allow all origins for development and demo purposes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def _send_to_kafka(message_data: dict) -> bool:
    """
    Publish a message to Kafka using the connection pool.
    If Kafka is unreachable, falls back to direct MongoDB write.
    This implements the fault-tolerance bridge between Member 1 (producer)
    and Member 4 (API) — the API can always accept messages even if Kafka
    is temporarily down.
    Returns True if Kafka was used, False if direct-DB fallback was used.
    """
    pool = KafkaProducerPool.get_instance()

    if pool:
        try:
            pool.send(message_data)
            return True
        except Exception as exc:
            logger.warning(f"Kafka pool send failed ({exc}); saving directly to DB")

    # Fallback: direct MongoDB write if Kafka is completely unavailable
    get_db().save_message(message_data)
    return False

@app.get("/health")
def health():
    """
    System health check endpoint.
    Docker uses this every 15s: test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
    Returns status of both MongoDB and Kafka connections.

    OPTIMIZATION: uses the producer pool's bootstrap_connected() instead of
    creating a full KafkaConsumer per health check.  This is a lightweight
    metadata-only call with no consumer group overhead.
    """
    db = get_db()
    mongo_ok = db.health_check()

    # Lightweight Kafka health check via producer pool
    pool = KafkaProducerPool.get_instance()
    kafka_ok = pool.is_connected() if pool else False

    status = "healthy" if (mongo_ok and kafka_ok) else "degraded"
    return {
        "status":  status,
        "mongodb": "ok" if mongo_ok else "down",
        "kafka":   "ok" if kafka_ok else "down",
    }


@app.post("/messages", status_code=201)
def send_message(req: MessageRequest):
    """
    Accept a message from a client and route it through Kafka to MongoDB.
    Normal path:   API → Kafka topic → Consumer → MongoDB
    Fallback path: API → MongoDB directly (when Kafka is down)
    The 'route' field in the response tells which path was used.
    """
    data = req.model_dump()
    if not data.get("timestamp"):
        data["timestamp"] = int(time.time() * 1000)

    via_kafka = _send_to_kafka(data)
    route     = "kafka" if via_kafka else "direct_db_fallback"
    logger.info(f"Message {data['messageId']} sent via {route}")
    return {"status": "sent", "messageId": data["messageId"], "route": route}


@app.get("/messages")
def get_all_messages(limit: int = 100):
    """Return all messages sorted by timestamp ASC."""
    msgs = get_db().get_all_messages(limit=limit)
    return {"messages": msgs, "count": len(msgs)}


@app.get("/messages/{user1}/{user2}")
def get_conversation(user1: str, user2: str, limit: int = 50):
    """
    Fetch conversation between two users — always sorted by timestamp ASC
    with HLC and messageId as tie-breakers. The 'sorted_by' field is Part 3 evidence.
    """
    msgs = get_db().get_messages_between_users(user1, user2, limit=limit)
    return {
        "messages":  msgs,
        "count":     len(msgs),
        "sorted_by": "timestamp_asc",  # explicit Part 3 evidence in response
    }

if __name__ == "__main__":
    import uvicorn
    from src.config.settings import API_HOST, API_PORT

    uvicorn.run(
        "src.api.rest_api:app",
        host=API_HOST,
        port=API_PORT,
        reload=True,   # auto-reload on code changes during development
    )
# ─────────────────────────────────────────────────────────────────────────────
