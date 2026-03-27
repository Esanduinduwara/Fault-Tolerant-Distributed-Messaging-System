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