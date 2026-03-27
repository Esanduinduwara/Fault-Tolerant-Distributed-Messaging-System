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
    return MongoDBHandler()


class MessageRequest(BaseModel):
    messageId: str
    fromUser: str
    toUser: str
    content: str
    messageType: str = "text"
    deliveryStatus: str = "sent"
    timestamp: Optional[int] = None


class UserRequest(BaseModel):
    userId: str
    username: str
    email: str


class KafkaProducerPool:
    _instance = None

    def __init__(self):
        from kafka import KafkaProducer
        import json

        bootstrap = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",")]

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
            request_timeout_ms=5000,
            linger_ms=5,
        )

    def send(self, message_data: dict) -> bool:
        users = sorted([message_data['fromUser'], message_data['toUser']])
        key = f"{users[0]}:{users[1]}"

        if not message_data.get("timestamp"):
            message_data["timestamp"] = int(time.time() * 1000)

        self._producer.send(KAFKA_TOPIC_MESSAGES, key=key, value=message_data)
        self._producer.flush(timeout=5)
        return True

    def close(self):
        try:
            self._producer.flush(timeout=10)
            self._producer.close()
        except Exception:
            pass

    def is_connected(self) -> bool:
        try:
            return bool(self._producer.bootstrap_connected())
        except Exception:
            return False

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            try:
                cls._instance = cls()
            except Exception:
                return None
        return cls._instance

    @classmethod
    def shutdown(cls):
        if cls._instance:
            cls._instance.close()
            cls._instance = None


@asynccontextmanager
async def lifespan(app):
    KafkaProducerPool.get_instance()
    yield
    KafkaProducerPool.shutdown()


app = FastAPI(
    title="StreamFlow API",
    description="Fault-tolerant distributed messaging system - SE2062",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _send_to_kafka(message_data: dict) -> bool:
    pool = KafkaProducerPool.get_instance()

    if pool:
        try:
            pool.send(message_data)
            return True
        except Exception:
            pass

    get_db().save_message(message_data)
    return False


@app.get("/health")
def health():
    db = get_db()
    mongo_ok = db.health_check()

    pool = KafkaProducerPool.get_instance()
    kafka_ok = pool.is_connected() if pool else False

    status = "healthy" if (mongo_ok and kafka_ok) else "degraded"
    return {
        "status": status,
        "mongodb": "ok" if mongo_ok else "down",
        "kafka": "ok" if kafka_ok else "down",
    }


@app.post("/messages", status_code=201)
def send_message(req: MessageRequest):
    data = req.model_dump()
    if not data.get("timestamp"):
        data["timestamp"] = int(time.time() * 1000)

    via_kafka = _send_to_kafka(data)
    route = "kafka" if via_kafka else "direct_db_fallback"
    return {"status": "sent", "messageId": data["messageId"], "route": route}


@app.get("/messages")
def get_all_messages(limit: int = 100):
    msgs = get_db().get_all_messages(limit=limit)
    return {"messages": msgs, "count": len(msgs)}


@app.get("/messages/{user1}/{user2}")
def get_conversation(user1: str, user2: str, limit: int = 50):
    msgs = get_db().get_messages_between_users(user1, user2, limit=limit)
    return {
        "messages": msgs,
        "count": len(msgs),
        "sorted_by": "timestamp_asc",
    }