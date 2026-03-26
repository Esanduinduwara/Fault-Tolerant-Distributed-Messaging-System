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