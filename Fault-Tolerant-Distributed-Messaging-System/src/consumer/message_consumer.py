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
            # Register the API endpoint as a monitored node
            self._heartbeat.register_node("api-server", "http://localhost:8000/health")
            self._heartbeat.on_status_change(self._on_node_status_change)
            logger.info(f"[{self.node_id}] Heartbeat monitor initialised")
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