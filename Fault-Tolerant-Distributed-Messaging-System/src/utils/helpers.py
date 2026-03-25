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
