"""
=============================================================================
 MEMBER 1 — FAULT TOLERANCE — Heartbeat-Based Failure Detection
=============================================================================

DAILY PUSH SCHEDULE
-------------------
Day 1  →  NodeStatus enum + HeartbeatMonitor class skeleton
Day 2  →  register_node() + _ping_node() + _monitor_loop()
Day 3  →  start() / stop() lifecycle + get_status_report()
Day 4  →  Integration with consumer start() — heartbeat thread wiring
Day 5  →  Final testing with Docker scaled consumers

=============================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# DAY 1 
# Imports + NodeStatus enum + HeartbeatMonitor skeleton
# ─────────────────────────────────────────────────────────────────────────────
import time
import logging
import threading
from enum import Enum
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class NodeStatus(Enum):
    """
    Node health status — modelled after the Phi Accrual Failure Detector
    (Hayashibara et al., 2004) with simplified thresholds.

    ALIVE:     Node responded to the last heartbeat within timeout
    SUSPECTED: Node missed 1-2 heartbeat cycles — may be slow or restarting
    DEAD:      Node missed 3+ heartbeat cycles — considered permanently down
               → triggers failover / message rerouting

    This graduated approach avoids false positives caused by temporary
    network congestion or garbage collection pauses.
    """
    ALIVE     = "alive"
    SUSPECTED = "suspected"
    DEAD      = "dead"
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# DAY 2  
# HeartbeatMonitor — pings registered nodes and tracks their health
# ─────────────────────────────────────────────────────────────────────────────
class HeartbeatMonitor:
    """
    Heartbeat-based failure detection system for distributed messaging nodes.

    PURPOSE (Part 1: "Design a failure detection system to identify when
    a messaging node becomes unavailable"):
      Each consumer/server registers its peers' health endpoints.
      A background thread periodically pings each peer via HTTP GET /health.
      If a peer fails to respond within the timeout, it transitions:
        ALIVE → SUSPECTED → DEAD

    MECHANISM:
      1. Nodes register peers via register_node(node_id, health_url)
      2. Background thread pings each peer every heartbeat_interval_s seconds
      3. Successful ping → status = ALIVE, last_seen updated
      4. Failed ping → check time since last successful ping:
         - < timeout_s     → ALIVE (still within grace period)
         - < timeout_s * 2 → SUSPECTED (missed 1-2 cycles)
         - ≥ timeout_s * 2 → DEAD (node is down)

    TRADE-OFFS:
      - Short interval → faster failure detection but more network overhead
      - Long timeout  → fewer false positives but slower failover
      - Default: ping every 5s, timeout after 15s (3 missed heartbeats)
    """

    def __init__(self, heartbeat_interval_s=5, timeout_s=15):
        from src.config.settings import HEARTBEAT_INTERVAL_S, HEARTBEAT_TIMEOUT_S

        self._interval   = heartbeat_interval_s or HEARTBEAT_INTERVAL_S
        self._timeout    = timeout_s or HEARTBEAT_TIMEOUT_S
        self._nodes      = {}    # node_id → {url, status, last_seen, last_checked}
        self._lock       = threading.Lock()
        self._running    = False
        self._thread     = None
        self._callbacks  = []    # list of (status, callback_fn) for notifications

    # ─────────────────────────────────────────────────────────────────────────
    # Node registration and management
    # ─────────────────────────────────────────────────────────────────────────
    def register_node(self, node_id: str, health_url: str):
        """
        Register a peer node to monitor.
        Args:
            node_id:    unique identifier (e.g. "consumer-2")
            health_url: HTTP endpoint to ping (e.g. "http://api:8000/health")
        """
        with self._lock:
            self._nodes[node_id] = {
                "url":          health_url,
                "status":       NodeStatus.ALIVE,
                "last_seen":    time.time(),
                "last_checked": 0,
                "fail_count":   0,
            }
        logger.info(f"[HEARTBEAT] Registered node: {node_id} → {health_url}")

    def on_status_change(self, callback):
        """
        Register a callback to be invoked when a node's status changes.
        Callback signature: callback(node_id: str, old_status, new_status)
        Used to trigger automatic failover when a node becomes DEAD.
        """
        self._callbacks.append(callback)

    # ─────────────────────────────────────────────────────────────────────────
    # Core ping logic
    # ─────────────────────────────────────────────────────────────────────────
    def _ping_node(self, node_id: str, health_url: str) -> bool:
        """
        Send an HTTP GET to the node's health endpoint.
        Returns True if the response status is 200, False otherwise.
        Uses a short timeout (3s) to avoid blocking the monitor thread.
        """
        try:
            import requests
            response = requests.get(health_url, timeout=3)
            return response.status_code == 200
        except Exception:
            return False

    def _check_node(self, node_id: str):
        """
        Ping a single node and update its status based on the result.
        Implements the ALIVE → SUSPECTED → DEAD state machine.
        """
        with self._lock:
            node = self._nodes.get(node_id)
            if not node:
                return

        alive   = self._ping_node(node_id, node["url"])
        now     = time.time()

        with self._lock:
            node["last_checked"] = now
            old_status           = node["status"]

            if alive:
                node["status"]     = NodeStatus.ALIVE
                node["last_seen"]  = now
                node["fail_count"] = 0
            else:
                node["fail_count"] += 1
                elapsed = now - node["last_seen"]

                if elapsed < self._timeout:
                    # Still within grace period — keep ALIVE
                    node["status"] = NodeStatus.ALIVE
                elif elapsed < self._timeout * 2:
                    # Missed 1-2 heartbeat cycles — mark SUSPECTED
                    node["status"] = NodeStatus.SUSPECTED
                    logger.warning(
                        f"[HEARTBEAT] Node {node_id} SUSPECTED "
                        f"(no response for {elapsed:.0f}s)"
                    )
                else:
                    # Missed 3+ cycles — mark DEAD
                    node["status"] = NodeStatus.DEAD
                    logger.error(
                        f"[HEARTBEAT] Node {node_id} declared DEAD "
                        f"(no response for {elapsed:.0f}s)"
                    )

            new_status = node["status"]

        # Fire callbacks on status change (outside lock to avoid deadlocks)
        if old_status != new_status:
            for callback in self._callbacks:
                try:
                    callback(node_id, old_status, new_status)
                except Exception as exc:
                    logger.error(f"[HEARTBEAT] Callback error: {exc}")

    # ─────────────────────────────────────────────────────────────────────────
    # DAY 3  
    # Background monitor thread + lifecycle + reporting
    # ─────────────────────────────────────────────────────────────────────────
    def _monitor_loop(self):
        """Background thread: periodically ping all registered nodes."""
        while self._running:
            with self._lock:
                node_ids = list(self._nodes.keys())

            for node_id in node_ids:
                if not self._running:
                    break
                self._check_node(node_id)

            time.sleep(self._interval)

    def start(self):
        """Start the heartbeat monitor background thread."""
        self._running = True
        self._thread  = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        logger.info(
            f"[HEARTBEAT] Monitor started "
            f"(interval={self._interval}s, timeout={self._timeout}s)"
        )

    def stop(self):
        """Stop the heartbeat monitor."""
        self._running = False
        logger.info("[HEARTBEAT] Monitor stopped")

    def get_node_status(self, node_id: str) -> NodeStatus:
        """Get the current status of a specific node."""
        with self._lock:
            node = self._nodes.get(node_id)
            if node:
                return node["status"]
        return NodeStatus.DEAD  # unknown node treated as dead

    def get_status_report(self) -> dict:
        """
        Return a full status report of all monitored nodes.
        Used by GET /health or for operational dashboards.
        """
        with self._lock:
            report = {}
            for node_id, info in self._nodes.items():
                report[node_id] = {
                    "status":       info["status"].value,
                    "last_seen":    info["last_seen"],
                    "fail_count":   info["fail_count"],
                    "url":          info["url"],
                }
        return report

    def get_alive_nodes(self) -> list:
        """Return list of node IDs that are currently ALIVE."""
        with self._lock:
            return [
                nid for nid, info in self._nodes.items()
                if info["status"] == NodeStatus.ALIVE
            ]

    def get_dead_nodes(self) -> list:
        """Return list of node IDs that are currently DEAD."""
        with self._lock:
            return [
                nid for nid, info in self._nodes.items()
                if info["status"] == NodeStatus.DEAD
            ]
    # ─────────────────────────────────────────────────────────────────────────