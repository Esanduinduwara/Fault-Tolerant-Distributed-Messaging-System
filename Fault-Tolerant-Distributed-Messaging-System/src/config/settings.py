# ─────────────────────────────────────────────────────────────────────────────
# Central config: all connection strings and constants live here.
# ─────────────────────────────────────────────────────────────────────────────
import os
from dotenv import load_dotenv

load_dotenv()  # reads .env file; os.getenv() calls below then work anywhere

# ── Kafka ──────────────────────────────────────────────────────────────────
# Multi-broker cluster: all 3 brokers listed for high-availability failover.
# If one broker goes down, the producer/consumer automatically connects
# to the surviving brokers — no code change or restart needed.
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
)
KAFKA_TOPIC_MESSAGES    = os.getenv("KAFKA_TOPIC_NAME", "user-messages")
KAFKA_CONSUMER_GROUP    = "streamflow-consumers"

# ── Kafka Quorum Replication (Part 2: Data Replication) ────────────────────
# replication.factor = 3 → every topic partition is copied to ALL 3 brokers.
# min.insync.replicas = 2 → a write is only confirmed when at least 2 of the
# 3 replicas have acknowledged it.  Combined with producer acks="all", this
# forms a QUORUM-BASED replication strategy:
#   Write succeeds ⟺ majority (2/3) of replicas confirm
#   Read is always from the elected partition leader
# Trade-off: higher latency per write (wait for 2 acks) but zero data loss
# even if 1 broker crashes immediately after the write.
KAFKA_REPLICATION_FACTOR    = int(os.getenv("KAFKA_REPLICATION_FACTOR", "3"))
KAFKA_MIN_INSYNC_REPLICAS   = int(os.getenv("KAFKA_MIN_INSYNC_REPLICAS", "2"))

# ── MongoDB ────────────────────────────────────────────────────────────────
MONGODB_URI      = os.getenv(
    "MONGODB_URI",
    "mongodb://admin:password123@localhost:27017/messaging_app?authSource=admin"
)
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "messaging_app")

# Collections (like table names in SQL)
COL_MESSAGES  = "messages"
COL_USERS     = "users"
COL_CHATROOMS = "chatrooms"
COL_LOGS      = "system_logs"

# ── API ────────────────────────────────────────────────────────────────────
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# ── App ────────────────────────────────────────────────────────────────────
LOG_LEVEL   = os.getenv("LOG_LEVEL", "INFO")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BATCH_SIZE  = int(os.getenv("BATCH_SIZE", "100"))

# ── Time Synchronization (Part 3: Time & Order) ───────────────────────────
# NTP servers used by NTPSynchronizer to calculate the clock offset between
# this node's local clock and a reference time source.  Multiple servers
# listed for redundancy — if the first is unreachable, the next is tried.
NTP_SERVERS = os.getenv("NTP_SERVERS", "pool.ntp.org,time.google.com,time.windows.com").split(",")

# How often (seconds) each node re-syncs with the NTP server.
# Shorter = more accurate but higher network overhead.
NTP_SYNC_INTERVAL_S = int(os.getenv("NTP_SYNC_INTERVAL_S", "60"))

# Reorder buffer window: messages are held for this many milliseconds before
# being released in timestamp-sorted order.  Compensates for network jitter
# and clock skew that cause messages to arrive out of sequence.
# Trade-off: larger window = better ordering but higher end-to-end latency.
REORDER_BUFFER_WINDOW_MS = int(os.getenv("REORDER_BUFFER_WINDOW_MS", "500"))

# ── Fault Detection (Part 1: Fault Tolerance) ─────────────────────────────
# Heartbeat-based failure detection: each consumer pings known peers via
# HTTP /health.  If a peer misses HEARTBEAT_TIMEOUT_S consecutive seconds
# of heartbeats, it is marked as SUSPECTED → then DEAD.
HEARTBEAT_INTERVAL_S = int(os.getenv("HEARTBEAT_INTERVAL_S", "5"))
HEARTBEAT_TIMEOUT_S  = int(os.getenv("HEARTBEAT_TIMEOUT_S", "15"))
# ─────────────────────────────────────────────────────────────────────────────
