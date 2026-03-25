"""
=============================================================================
 MEMBER 3 — TIME SYNCHRONIZATION (shared utilities)
 File: src/utils/helpers.py
=============================================================================

PUSH SCHEDULE (Member 3)
-------------------------
Day 1  ▸  ID generators (generate_message_id, generate_user_id)
Day 2  ▸  Timestamp helpers (current_timestamp_ms, ms_to_iso)
Day 3  ▸  Validators (validate_message, validate_user)
Day 4  ▸  Logging setup + create_sample_message factory
Day 5  ▸  Code review + docstring improvements

GIT COMMIT MESSAGE TEMPLATES
-----------------------------
Day 1: "feat(utils): add UUID-based message and user ID generators"
Day 2: "feat(utils): add millisecond timestamp helpers for Part 3 time ordering"
Day 3: "feat(utils): add message and user validators with field checks"
Day 4: "feat(utils): add logging setup and create_sample_message factory"
Day 5: "refactor(utils): improve docstrings and finalize shared utilities"
=============================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# DAY 1  ▸  PUSH THIS BLOCK
# UUID-based ID generators — ensure globally unique IDs without a central counter
# ─────────────────────────────────────────────────────────────────────────────
import uuid
import time
import logging
from datetime import datetime, timezone


def generate_message_id() -> str:
    """Generate a unique message ID. Format: msg_<8-char-uuid>"""
    return f"msg_{uuid.uuid4().hex[:8]}"


def generate_user_id() -> str:
    """Generate a unique user ID. Format: usr_<8-char-uuid>"""
    return f"usr_{uuid.uuid4().hex[:8]}"
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# DAY 2  ▸  PUSH THIS BLOCK
# Timestamp utilities — millisecond precision for Part 3 ordering guarantee
# ─────────────────────────────────────────────────────────────────────────────
def current_timestamp_ms() -> int:
    """
    Return current UTC time as milliseconds since Unix epoch.
    Used consistently everywhere: int(time.time() * 1000).
    Milliseconds chosen to match Kafka's internal timestamp format.
    """
    return int(time.time() * 1000)


def ms_to_iso(timestamp_ms: int) -> str:
    """Convert a millisecond timestamp to ISO 8601 string for display."""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# DAY 3  ▸  PUSH THIS BLOCK
# Input validators — check required fields before touching Kafka or MongoDB
# ─────────────────────────────────────────────────────────────────────────────
def validate_message(data: dict) -> tuple:
    """
    Validate a message dict before sending.
    Returns (True, "") on success, (False, reason) on failure.
    FastAPI's Pydantic models already validate HTTP requests;
    this is used for programmatic calls (e.g., from the producer directly).
    """
    required = ["messageId", "fromUser", "toUser", "content"]
    for field in required:
        if field not in data or not data[field]:
            return False, f"Missing required field: {field}"
    if len(data["content"]) > 10_000:
        return False, "Content exceeds 10,000 character limit"
    return True, ""


def validate_user(data: dict) -> tuple:
    """Validate a user dict. Returns (True, "") or (False, reason)."""
    required = ["userId", "username", "email"]
    for field in required:
        if field not in data or not data[field]:
            return False, f"Missing required field: {field}"
    return True, ""
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# DAY 4  ▸  PUSH THIS BLOCK
# Logging setup + sample message factory
# ─────────────────────────────────────────────────────────────────────────────
def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure root logger. Call once at application startup."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("streamflow")


def create_sample_message(from_user: str, to_user: str, content: str) -> dict:
    """
    Convenience factory used by the producer smoke-test and test helpers.
    Always includes a millisecond timestamp (Part 3 requirement).
    """
    return {
        "messageId":      generate_message_id(),
        "fromUser":       from_user,
        "toUser":         to_user,
        "content":        content,
        "messageType":    "text",
        "deliveryStatus": "sent",
        "timestamp":      current_timestamp_ms(),
    }
# ─────────────────────────────────────────────────────────────────────────────
