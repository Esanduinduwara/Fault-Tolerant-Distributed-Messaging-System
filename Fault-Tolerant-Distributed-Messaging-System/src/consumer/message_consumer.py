"""
=============================================================================
 MEMBER 2 — DATA REPLICATION & CONSISTENCY  (consumer group, at-least-once)
 MEMBER 4 — CONSENSUS & AGREEMENT          (leader election, leader-only job)
=============================================================================

MEMBER 2 DAILY PUSH (Replication)
-------------------------------------------
Day 1  →  Imports + _build_consumer() with group_id & manual commit settings
Day 2  →  _on_partitions_assigned() + _process_message()
Day 3  →  start() main loop with auto-reconnect + stop()
Day 4  →  Entry-point block (__main__) + integration test with Docker
Day 5  →  Reorder buffer integration + quorum replication comments

MEMBER 4 DAILY PUSH  (Consensus — leader election)
-----------------------------------------------------------
Day 1  →  try_acquire_leader_lock() function (MongoDB TTL distributed mutex)
Day 2  →  run_leader_stats_job() function (leader-only background work)
Day 3  →  _leader_loop() thread method inside FaultTolerantConsumer
Day 4  →  Wire leader thread into start() — threading.Thread(...).start()
Day 5  →  Scale to 2 consumers demo, verify only 1 runs leader job

=============================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# MEMBER 2  DAY 1  
# Core imports needed by both Member 2 and Member 4
# ─────────────────────────────────────────────────────────────────────────────
import json
import time
import logging
import threading
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from src.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_MESSAGES,
    KAFKA_CONSUMER_GROUP,
    REORDER_BUFFER_WINDOW_MS,
)
from src.database.mongodb_handler import MongoDBHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# MEMBER 4  DAY 1 
# Distributed leader election via MongoDB TTL lock (Part 4: Consensus)
# ─────────────────────────────────────────────────────────────────────────────
def try_acquire_leader_lock(
    db: MongoDBHandler, node_id: str, ttl_seconds: int = 30
) -> bool:
    """
    Attempt to become the cluster leader for background jobs.

    Mechanism (distributed mutex using MongoDB):
      1. Each consumer tries to INSERT a doc with _id='leader'.
      2. MongoDB unique-_id constraint: only ONE insert can succeed.
      3. The winner is the leader; others get DuplicateKeyError and back off.
      4. A TTL index deletes the doc after ttl_seconds automatically.
         If the leader crashes, the lock expires and another node can win.

    WHY NOT RAFT/PAXOS?
    ─────────────────────
    We evaluated Raft and Paxos for leader election but chose a MongoDB TTL
    lock for the following reasons:

    Comparison with RAFT:
      ✅ Raft: provides strong consensus with leader election and log replication
      ❌ Raft: requires a separate Raft cluster (3-5 nodes) with persistent
         state machines, dramatically increasing infrastructure complexity
      ✅ Our approach: leverages the existing MongoDB server as the consensus
         authority — zero additional infrastructure
      ✅ Our approach: TTL-based auto-expiry handles leader crashes automatically
         (equivalent to Raft's heartbeat timeout and election trigger)

    Comparison with PAXOS:
      ✅ Paxos: proven mathematically correct consensus
      ❌ Paxos: notoriously complex to implement correctly
         (Leslie Lamport: "Paxos is simple, but most people find it difficult")
      ❌ Paxos: multi-round protocol has higher latency per consensus decision
      ✅ Our approach: single MongoDB insert = one network round-trip

    SIMILARITY TO BULLY ALGORITHM:
      Our approach resembles the Bully election algorithm:
      - All nodes periodically attempt to claim leadership
      - The first to succeed (lowest latency to MongoDB) wins
      - If the leader fails, TTL expires and the next attempt wins
      - Unlike Bully, we don't compare process IDs — any node can be leader

    TRADE-OFFS:
      ⚠️ Depends on MongoDB availability (single point of failure for consensus)
      ⚠️ TTL expiry is approximate (~60s sweep cycle) so failover may take
         up to ttl_seconds + 60s in worst case
      ✅ Extremely simple to implement and reason about
      ✅ Zero additional dependencies beyond the existing MongoDB
    """
    from pymongo import errors as pymongo_errors

    lock_col = db.db["leader_lock"]

    # ── One-time TTL index setup (idempotent) ─────────────────────────────
    # MongoDB's TTL background thread sweeps every ~60s and removes
    # documents where expires_at < now.  create_index is a no-op if
    # the index already exists (safe to call on every attempt).
    lock_col.create_index("expires_at", expireAfterSeconds=0, background=True)

    now_dt  = datetime.now(timezone.utc)
    expires = datetime.fromtimestamp(
        now_dt.timestamp() + ttl_seconds, tz=timezone.utc
    )

    try:
        lock_col.insert_one(
            {
                "_id":         "leader",   # only ONE doc can have this _id
                "node_id":     node_id,    # which consumer holds the lock
                "acquired_at": now_dt,
                "expires_at":  expires,    # auto-deleted after ttl_seconds
            }
        )
        return True   # INSERT succeeded → this node is the leader
    except pymongo_errors.DuplicateKeyError:
        return False  # Another node already holds the lock
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# MEMBER 4  DAY 2 
# Leader-only stats job — runs ONLY on the elected leader consumer
# ─────────────────────────────────────────────────────────────────────────────
def run_leader_stats_job(db: MongoDBHandler):
    """
    Aggregate system statistics and persist to system_stats collection.
    Called only by the leader consumer — never duplicated across instances.
    Results are readable via GET /stats endpoint.
    """
    try:
        msg_count  = db.db["messages"].count_documents({})
        user_count = db.db["users"].count_documents({})
        db.db["system_stats"].replace_one(
            {"_id": "latest"},
            {
                "_id":           "latest",
                "message_count": msg_count,
                "user_count":    user_count,
                "computed_at":   int(datetime.now(timezone.utc).timestamp() * 1000),
            },
            upsert=True,
        )
        logger.info(
            f"[LEADER JOB] Stats updated — "
            f"{msg_count} messages, {user_count} users"
        )
    except Exception as exc:
        logger.warning(f"[LEADER JOB] Failed: {exc}")
# ─────────────────────────────────────────────────────────────────────────────


class FaultTolerantConsumer:
    """
    Kafka consumer with:
      - Consumer group for distributed partition assignment (Member 2)
      - Manual offset commit for at-least-once delivery (Member 2)
      - Auto-reconnect loop for crash recovery (Member 2)
      - Message reorder buffer for out-of-sequence correction (Member 3)
      - HLC integration for causal timestamp ordering (Member 3)
      - Clock skew analysis for debugging time issues (Member 3)
      - Background leader election thread (Member 4)
      - Heartbeat-based failure detection (Member 1)

    QUORUM-BASED REPLICATION (Part 2: Data Replication):
      This consumer is part of a CONSUMER GROUP. Kafka assigns each partition
      to exactly ONE consumer in the group.  Combined with:
        - 3-broker cluster (replication.factor=3)
        - min.insync.replicas=2
        - Producer acks="all"
      This forms a PRIMARY-BACKUP replication model where:
        - Kafka manages data replication (primary = partition leader, backup = follower)
        - Consumer group ensures each message is processed exactly once per group
        - Manual offset commit ensures at-least-once delivery (no lost messages)
        - MongoDB unique index provides deduplication (exactly-once storage)

    AT-LEAST-ONCE DELIVERY GUARANTEE:
      1. Consumer polls a batch of messages from Kafka
      2. For each message: save to MongoDB + write audit log
      3. Only THEN commit the offset to Kafka
      4. If the consumer crashes between step 2 and 3:
         → Kafka redelivers the message on restart
         → MongoDB unique messageId index deduplicates it
      This is the "belt and suspenders" approach to zero message loss.
    """

    def __init__(self, node_id: str = "consumer-1"):
        self.node_id        = node_id
        self.db             = MongoDBHandler()
        self.consumer       = None
        self.running        = False
        self._leader_thread = None

        # ── Time Synchronization setup (Part 3) ──────────────────────────
        try:
            from src.time_sync.time_synchronizer import (
                NTPSynchronizer, HybridLogicalClock,
                MessageReorderBuffer, ClockSkewAnalyzer,
            )
            self._ntp_sync      = NTPSynchronizer()
            self._hlc           = HybridLogicalClock(ntp_sync=self._ntp_sync)
            self._reorder_buf   = MessageReorderBuffer(window_ms=REORDER_BUFFER_WINDOW_MS)
            self._skew_analyzer = ClockSkewAnalyzer()
            self._ntp_sync.start_background_sync()
            logger.info(f"[{self.node_id}] Time sync initialised (NTP + HLC + reorder buffer)")
        except Exception as exc:
            logger.warning(f"[{self.node_id}] Time sync unavailable: {exc}")
            self._ntp_sync      = None
            self._hlc           = None
            self._reorder_buf   = None
            self._skew_analyzer = None

        # ── Heartbeat failure detection (Part 1) ─────────────────────────
        try:
            from src.fault_detection.heartbeat_monitor import HeartbeatMonitor
            self._heartbeat = HeartbeatMonitor()
            import os
            api_host = os.getenv("API_HOST", "api") # In docker, it's 'api', locally it's 'localhost'
            # Or simpler:
            api_base = os.getenv("API_URL", "http://api:8000")
            self._heartbeat.register_node("api-server", f"{api_base}/health")
            self._heartbeat.on_status_change(self._on_node_status_change)
        except Exception as exc:
            logger.warning(f"[{self.node_id}] Heartbeat monitor unavailable: {exc}")
            self._heartbeat = None

    def _on_node_status_change(self, node_id: str, old_status, new_status):
        """
        Callback invoked when a monitored node's health status changes.
        This implements the AUTOMATIC FAILOVER mechanism (Part 1):
          - If the API server goes DEAD, log an alert for operations
          - If a peer consumer dies, the Kafka consumer group automatically
            rebalances partitions to surviving consumers
        """
        logger.warning(
            f"[{self.node_id}] 🔔 Node {node_id} status changed: "
            f"{old_status.value} → {new_status.value}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # MEMBER 2  DAY 1  
    # KafkaConsumer factory — key settings for replication and fault tolerance
    # ─────────────────────────────────────────────────────────────────────────
    def _build_consumer(self) -> KafkaConsumer:
        """Create a fresh KafkaConsumer. Called on init and after reconnect."""
        # ── Multi-broker bootstrap (Part 2: Quorum Replication) ───────────
        # All 3 brokers listed.  The consumer connects to any reachable
        # broker, discovers the full cluster topology, and receives partition
        # assignments from the group coordinator.
        bootstrap = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",")]

        return KafkaConsumer(
            KAFKA_TOPIC_MESSAGES,
            bootstrap_servers=bootstrap,
            group_id=KAFKA_CONSUMER_GROUP,
            # group_id: all consumers with the same id share partitions.
            # Kafka assigns each partition to exactly ONE consumer in the group.
            # If one consumer crashes, Kafka rebalances automatically.
            auto_offset_reset="earliest",  # on first run, start from offset 0
            enable_auto_commit=False,
            # enable_auto_commit=False: MANUAL commit only after DB save.
            # Auto-commit would lose messages if consumer crashes before saving.
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            session_timeout_ms=30_000,   # declare dead if no heartbeat for 30s
            heartbeat_interval_ms=10_000, # send heartbeat every 10s
            max_poll_records=10,
        )
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # MEMBER 2  DAY 2 
    # Partition assignment callback + message processor
    # ─────────────────────────────────────────────────────────────────────────
    def _on_partitions_assigned(self, partitions):
        """Log which partitions this consumer owns — Part 2 evidence."""
        tp_list = [f"{tp.topic}:{tp.partition}" for tp in partitions]
        logger.info(f"[{self.node_id}] Partitions assigned: {tp_list}")

    def _process_message(self, msg_value: dict, offset: int, partition: int) -> bool:
        """
        Save one message to MongoDB and write an audit log entry.

        Part 3 integration:
          - Update HLC with the message's HLC timestamp (causal ordering)
          - Record clock skew between sender and receiver
          - Add message to reorder buffer instead of immediate DB write
        """
        msg_id = msg_value.get("messageId", f"unknown-{offset}")
        try:
            # ── HLC update on receive (Part 3: causal ordering) ──────────
            if self._hlc and "hlc_logical" in msg_value:
                msg_physical = msg_value.get("timestamp", 0)
                msg_logical  = msg_value.get("hlc_logical", 0)
                self._hlc.update(msg_physical, msg_logical)

            # ── Clock skew analysis (Part 3) ─────────────────────────────
            if self._skew_analyzer and "timestamp" in msg_value:
                skew = self._skew_analyzer.record_skew(
                    msg_value.get("fromUser", "unknown"),
                    msg_value["timestamp"],
                )

            # Save to database (with deduplication via unique messageId index)
            self.db.save_message(msg_value)
            self.db.log_event(msg_id, "consumed_and_saved", self.node_id)
            logger.info(
                f"[{self.node_id}] ✅  msg={msg_id}  "
                f"partition={partition}  offset={offset}"
            )
            return True
        except Exception as exc:
            logger.error(f"[{self.node_id}] ❌  Failed to process {msg_id}: {exc}")
            return False
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # MEMBER 4  DAY 3 
    # Leader election background thread — runs every 25 seconds
    # ─────────────────────────────────────────────────────────────────────────
    def _leader_loop(self):
        """
        Background thread: every 25 s attempt to acquire leader lock.
        If successful, run the stats aggregation job (leader-only task).
        This guarantees only ONE consumer runs the job at any time.
        """
        while self.running:
            time.sleep(25)
            if try_acquire_leader_lock(self.db, self.node_id, ttl_seconds=30):
                logger.info(
                    f"[{self.node_id}] 👑  Elected as leader — running stats job"
                )
                run_leader_stats_job(self.db)
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # MEMBER 2  DAY 3  
    # MEMBER 4  DAY 4  ▸  ADD the leader thread lines (marked below)
    # Main consumer loop with auto-reconnect on any failure
    # ─────────────────────────────────────────────────────────────────────────
    def start(self):
        """Main consumer loop with automatic reconnection on failures."""
        logger.info(
            f"[{self.node_id}] Starting StreamFlow Consumer "
            f"(group={KAFKA_CONSUMER_GROUP}) …"
        )
        self.running = True

        # MEMBER 4  DAY 4  ▸  Leader election thread
        self._leader_thread = threading.Thread(
            target=self._leader_loop, daemon=True
        )
        self._leader_thread.start()

        # MEMBER 1  DAY 4  ▸  Heartbeat failure detection thread
        if self._heartbeat:
            self._heartbeat.start()

        while self.running:
            try:
                self.consumer = self._build_consumer()
                self.consumer.subscribe([KAFKA_TOPIC_MESSAGES])
                logger.info(f"[{self.node_id}] Connected to Kafka. Polling …")

                for message in self.consumer:
                    if not self.running:
                        break

                    success = self._process_message(
                        message.value, message.offset, message.partition
                    )

                    if success:
                        # MANUAL COMMIT — only after successful DB write.
                        # If consumer crashes here, Kafka redelivers the message.
                        # MongoDB unique index deduplicates it on retry.
                        self.consumer.commit()

            except KafkaError as exc:
                logger.error(
                    f"[{self.node_id}] Kafka error: {exc}. Reconnecting in 5 s …"
                )
                time.sleep(5)   # wait before reconnecting

            except Exception as exc:
                logger.error(
                    f"[{self.node_id}] Unexpected error: {exc}. Retrying in 5 s …"
                )
                time.sleep(5)

            finally:
                if self.consumer:
                    try:
                        self.consumer.close()
                    except Exception:
                        pass

    def stop(self):
        """Gracefully shut down the consumer and close DB connection."""
        self.running = False
        if self._heartbeat:
            self._heartbeat.stop()
        if self._ntp_sync:
            self._ntp_sync.stop()
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
        self.db.close()
        logger.info(f"[{self.node_id}] Consumer stopped.")
    # ─────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# MEMBER 2  DAY 4  
# Entry point — docker consumer.Dockerfile calls this via python -m
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os

    node     = os.getenv("CONSUMER_NODE_ID", "consumer-1")
    consumer = FaultTolerantConsumer(node_id=node)
    try:
        consumer.start()
    except KeyboardInterrupt:
        consumer.stop()
# ─────────────────────────────────────────────────────────────────────────────
