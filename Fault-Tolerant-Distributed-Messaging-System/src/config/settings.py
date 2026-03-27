import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092,kafka2:29093,kafka3:29094"
)

MONGODB_URI = os.getenv(
    "MONGODB_URI",
    "mongodb://admin:password123@mongodb:27017/messaging_app?authSource=admin"
)

MONGODB_DATABASE = os.getenv(
    "MONGODB_DATABASE",
    "messaging_app"
)

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))