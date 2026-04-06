"""
=============================================================================
 MEMBER 1 — FAULT TOLERANCE — Dead Letter Queue Replay
=============================================================================

DAILY PUSH SCHEDULE
-------------------
Day 3  →  DLQReplayer class + replay() method
Day 4  →  __main__ entry point for standalone replay
Day 5  →  Demo: stop Kafka, send messages → DLQ, restart Kafka, replay DLQ

=============================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# DAY 3  
# DLQ Replayer — reads failed messages and re-publishes them to Kafka
# ─────────────────────────────────────────────────────────────────────────────
import json
import os
import logging
import shutil
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class DLQReplayer:
    """
    Dead Letter Queue (DLQ) replay mechanism for message recovery.

    PURPOSE (Part 1: "Propose a message recovery mechanism for nodes
    that rejoin the system"):
      When Kafka is completely unreachable, the FaultTolerantProducer saves
      failed messages to a local JSONL file (the DLQ).  This replayer reads
      that file and re-publishes each message to Kafka once it's back online.

    RECOVERY WORKFLOW:
      1. Kafka goes down → producer writes to failed_messages_dlq.jsonl
      2. Kafka comes back online
      3. Operator (or scheduled job) runs: python -m src.producer.dlq_replayer
      4. Replayer reads each line from the DLQ file
      5. For each message: attempt to re-send via Kafka
         - Success → message is now in Kafka pipeline → consumer will process it
         - Failure → message stays in a "still failed" file for next attempt
      6. Successfully replayed messages are moved to a .done archive file

    IDEMPOTENCY:
      MongoDB's unique messageId index ensures that if a message was already
      delivered by another path (e.g. direct DB fallback), the duplicate insert
      is silently ignored.  So replaying is always SAFE — no double-delivery.
    """

    def __init__(self, dlq_file: str = "failed_messages_dlq.jsonl"):
        self.dlq_file  = dlq_file
        self.done_file = dlq_file.replace(".jsonl", "_replayed.jsonl")
        self.fail_file = dlq_file.replace(".jsonl", "_still_failed.jsonl")

    def replay(self) -> dict:
        """
        Read all messages from the DLQ and attempt to re-send each via Kafka.

        Returns a stats dict:
          {"total": N, "replayed": M, "still_failed": K}
        """
        if not os.path.exists(self.dlq_file):
            logger.info("[DLQ REPLAY] No DLQ file found — nothing to replay")
            return {"total": 0, "replayed": 0, "still_failed": 0}

        # Read all DLQ entries
        entries = []
        with open(self.dlq_file, "r") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        logger.warning(f"[DLQ REPLAY] Skipping malformed entry: {exc}")

        if not entries:
            logger.info("[DLQ REPLAY] DLQ file is empty — nothing to replay")
            return {"total": 0, "replayed": 0, "still_failed": 0}

        logger.info(f"[DLQ REPLAY] Found {len(entries)} messages to replay")

        # Create a Kafka producer for replaying
        from src.producer.message_producer import FaultTolerantProducer

        try:
            producer = FaultTolerantProducer()
        except Exception as exc:
            logger.error(f"[DLQ REPLAY] Cannot connect to Kafka: {exc}")
            return {"total": len(entries), "replayed": 0, "still_failed": len(entries)}

        stats = {"total": len(entries), "replayed": 0, "still_failed": 0}
        still_failed = []

        for entry in entries:
            original_msg = entry.get("original_message", {})
            msg_id       = original_msg.get("messageId", "unknown")

            try:
                # Re-send the original message through the normal pipeline
                success = producer.send_message(original_msg)
                if success:
                    stats["replayed"] += 1
                    logger.info(f"[DLQ REPLAY] ✅ Replayed: {msg_id}")
                    # Archive successfully replayed message
                    self._archive_entry(entry)
                else:
                    stats["still_failed"] += 1
                    still_failed.append(entry)
                    logger.warning(f"[DLQ REPLAY] ❌ Still failing: {msg_id}")

            except Exception as exc:
                stats["still_failed"] += 1
                still_failed.append(entry)
                logger.error(f"[DLQ REPLAY] ❌ Error replaying {msg_id}: {exc}")

        # Write still-failed messages to a new file
        if still_failed:
            with open(self.fail_file, "w") as fh:
                for entry in still_failed:
                    fh.write(json.dumps(entry) + "\n")
            logger.info(
                f"[DLQ REPLAY] {len(still_failed)} messages still failing → "
                f"saved to {self.fail_file}"
            )

        # Clean up original DLQ file
        if stats["still_failed"] == 0:
            # All messages replayed successfully — delete the DLQ file
            os.remove(self.dlq_file)
            logger.info(f"[DLQ REPLAY] All messages replayed — DLQ file removed")
        else:
            # Replace DLQ with only the still-failed messages
            shutil.move(self.fail_file, self.dlq_file)

        producer.close()

        logger.info(f"[DLQ REPLAY] Results: {stats}")
        return stats

    def _archive_entry(self, entry: dict):
        """Append a successfully replayed entry to the .done archive file."""
        entry["replayed_at"] = datetime.now(timezone.utc).isoformat()
        with open(self.done_file, "a") as fh:
            fh.write(json.dumps(entry) + "\n")
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# DAY 4 
# Entry point — run with: python -m src.producer.dlq_replayer
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    replayer = DLQReplayer()
    stats    = replayer.replay()

    print(f"\n{'='*50}")
    print(f"DLQ Replay Complete")
    print(f"  Total messages:   {stats['total']}")
    print(f"  Replayed:         {stats['replayed']}")
    print(f"  Still failed:     {stats['still_failed']}")
    print(f"{'='*50}\n")
# ─────────────────────────────────────────────────────────────────────────────