"""
=============================================================================
 MEMBER 1 — FAULT TOLERANCE
=============================================================================

DAILY PUSH SCHEDULE
-------------------
Day 1  →  Class skeleton + KafkaProducer init block  (lines marked DAY-1)
Day 2  →  send_to_dlq() Dead Letter Queue method      (lines marked DAY-2)
Day 3  →  send_message() single-message send          (lines marked DAY-3)
Day 4  →  send_batch() + close()                      (lines marked DAY-4)
Day 5  →  __main__ smoke-test block + HLC integration (lines marked DAY-5)

=============================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# DAY 1  
# Imports and logging setup — shared foundation for all days
# ─────────────────────────────────────────────────────────────────────────────
import json
import time
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer          # kafka-python-ng 2.2.3+
from kafka.errors import KafkaError

from src.config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_MESSAGES

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
# ─────────────────────────────────────────────────────────────────────────────


class FaultTolerantProducer:
    """
    Kafka producer with built-in fault tolerance:
      - acks='all'  →  waits for ALL in-sync replicas before confirming
      - retries=10  →  automatically retries transient failures
      - DLQ file    →  saves messages locally when Kafka is completely down

    QUORUM WRITE GUARANTEE (Part 2: Data Replication):
      With a 3-broker cluster configured as:
        replication.factor    = 3  (each partition replicated to all 3 brokers)
        min.insync.replicas   = 2  (at least 2 replicas must confirm)
        acks                  = "all" (producer waits for ALL in-sync replicas)
      A message is confirmed ONLY when ≥ 2 of 3 brokers have persisted it.
      This is a QUORUM: majority (2/3) must agree before the write is durable.
      Even if 1 broker crashes immediately after confirming, the message
      survives on the other 2 replicas.

    HLC INTEGRATION (Part 3: Time Synchronization):
      Each message is stamped with a Hybrid Logical Clock timestamp that
      combines NTP-corrected physical time with a Lamport logical counter.
      This ensures both wall-clock accuracy AND causal ordering.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # DAY 1 
    # KafkaProducer initialisation with all fault-tolerance settings
    # ─────────────────────────────────────────────────────────────────────────
    def __init__(self):
        logger.info("Initialising Fault-Tolerant StreamFlow Producer ...")
        self.dlq_file = "failed_messages_dlq.jsonl"
        self.topic    = KAFKA_TOPIC_MESSAGES

        # ── Multi-broker bootstrap (Part 2: Quorum Replication) ────────────
        # All 3 brokers listed.  KafkaProducer connects to any reachable
        # broker first, then discovers the rest via metadata exchange.
        # If broker 1 is down, it seamlessly connects to broker 2 or 3.
        bootstrap = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",")]

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",       # QUORUM: wait for ALL in-sync replicas (min 2/3)
            retries=10,       # retry transient failures automatically
            max_in_flight_requests_per_connection=5,
            linger_ms=5,      # small batching window for throughput
        )

        # ── HLC integration (Part 3: Time Synchronization) ────────────────
        # Hybrid Logical Clock combines NTP-corrected physical time with
        # a logical Lamport counter for causal ordering across nodes.
        try:
            from src.time_sync.time_synchronizer import (
                NTPSynchronizer, HybridLogicalClock,
            )
            self._ntp_sync = NTPSynchronizer()
            self._hlc      = HybridLogicalClock(ntp_sync=self._ntp_sync)
            logger.info("HLC time synchronization initialised for producer")
        except Exception as exc:
            logger.warning(f"HLC init failed, falling back to system time: {exc}")
            self._ntp_sync = None
            self._hlc      = None
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # DAY 2 
    # Dead Letter Queue — the core message-recovery mechanism
    # When Kafka is completely unreachable, messages are written to a JSONL
    # file so they can be replayed later. No message is ever silently dropped.
    #
    # RECOVERY: Use `python -m src.producer.dlq_replayer` to replay the DLQ
    # file once Kafka is back online.  The replayer re-publishes each message
    # through the normal pipeline, and MongoDB's unique messageId index
    # guarantees idempotent deduplication on the storage side.
    # ─────────────────────────────────────────────────────────────────────────
    def send_to_dlq(self, message_data: dict, error_reason):
        """
        Persist a failed message to a local JSONL file (Dead Letter Queue).
        A recovery script can read this file later and re-publish the messages.
        Fulfils the 'message recovery mechanism' requirement from the scenario.
        """
        entry = {
            "failed_at":        datetime.now(timezone.utc).isoformat(),
            "error":            str(error_reason),
            "original_message": message_data,
        }
        with open(self.dlq_file, "a") as fh:
            fh.write(json.dumps(entry) + "\n")
        logger.warning(
            "Message saved to DLQ (%s): %s",
            self.dlq_file, message_data.get("messageId"),
        )
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # DAY 3  
    # send_message() — single message publish with HLC timestamp + partition key
    # ─────────────────────────────────────────────────────────────────────────
    def send_message(self, message_data: dict) -> bool:
        """Send one message. Returns True on success, False on failure."""
        # ── HLC timestamp (Part 3: Time Synchronization) ──────────────────
        # If HLC is available, stamp with NTP-corrected + logically-ordered
        # timestamp.  Falls back to raw system time if HLC is unavailable.
        if self._hlc:
            physical_ms, logical = self._hlc.tick()
            message_data["timestamp"]  = physical_ms
            message_data["hlc_logical"] = logical
            message_data["hlc_encoded"] = self._hlc.encode(physical_ms, logical)
        elif "timestamp" not in message_data:
            message_data["timestamp"] = int(time.time() * 1000)

        # ── Canonical conversation key for partition ordering ─────────────
        # Sort the user pair so alice→bob and bob→alice use the SAME key.
        # This guarantees all messages in a conversation go to the SAME
        # Kafka partition → preserving per-conversation order.
        users = sorted([message_data['fromUser'], message_data['toUser']])
        key   = f"{users[0]}:{users[1]}"

        try:
            future = self.producer.send(self.topic, key=key, value=message_data)
            meta   = future.get(timeout=10)
            logger.info(
                "Sent %s -> partition=%s offset=%s",
                message_data.get("messageId"), meta.partition, meta.offset,
            )
            return True

        except KafkaError as exc:
            logger.error("Kafka error for %s: %s", message_data.get("messageId"), exc)
            self.send_to_dlq(message_data, exc)
            return False
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # DAY 4 
    # send_batch() + close() — scalability and safe teardown
    # ─────────────────────────────────────────────────────────────────────────
    def send_batch(self, messages: list) -> dict:
        """Send multiple messages. Returns stats dict. Demonstrates scalability."""
        results = {"sent": 0, "failed": 0}
        for msg in messages:
            if self.send_message(msg):
                results["sent"]   += 1
            else:
                results["failed"] += 1
        self.producer.flush()
        logger.info("Batch complete: %s", results)
        return results

    def close(self):
        """Flush all pending messages and cleanly shut down the producer."""
        self.producer.flush()
        self.producer.close()
        if self._ntp_sync:
            self._ntp_sync.stop()
        logger.info("Producer closed safely.")
    # ─────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# DAY 5  
# Smoke-test entry point — run with: python -m src.producer.message_producer
# Sends a sample batch to prove the full producer pipeline works end-to-end
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from src.utils.helpers import create_sample_message

    producer = FaultTolerantProducer()

    messages = (
        [create_sample_message("alice",   "bob",   f"Hello Bob, message {i}!")  for i in range(3)]
      + [create_sample_message("bob",     "alice", f"Hey Alice, reply {i}!")    for i in range(3)]
      + [create_sample_message("charlie", "alice", "Hi Alice from Charlie!")]
    )

    print(f"\nSending batch of {len(messages)} messages to Kafka ...\n")
    stats = producer.send_batch(messages)
    print(f"\nResults: {stats}")
    producer.close()
# ─────────────────────────────────────────────────────────────────────────────