"""
=============================================================================
 MEMBER 1 — FAULT TOLERANCE — Heartbeat Monitor Unit Tests
=============================================================================

PUSH SCHEDULE (Member 1 — Heartbeat Tests)
--------------------------------------------
Day 2  ▸  test_register_node + test_alive_on_successful_ping
Day 3  ▸  test_suspected_on_timeout + test_dead_on_extended_timeout
Day 4  ▸  test_status_report + test_callback_on_status_change
Day 5  ▸  Run full suite, verify green
=============================================================================
"""

import time
import pytest
from unittest.mock import patch, MagicMock

from src.fault_detection.heartbeat_monitor import HeartbeatMonitor, NodeStatus


class TestHeartbeatMonitor:

    def test_register_node(self):
        """Registering a node should add it to the monitor."""
        monitor = HeartbeatMonitor(heartbeat_interval_s=5, timeout_s=15)
        monitor.register_node("node-1", "http://localhost:8000/health")
        assert monitor.get_node_status("node-1") == NodeStatus.ALIVE

    def test_alive_on_successful_ping(self):
        """Node should be ALIVE when ping succeeds."""
        monitor = HeartbeatMonitor(heartbeat_interval_s=5, timeout_s=15)
        monitor.register_node("node-1", "http://localhost:8000/health")

        with patch.object(monitor, '_ping_node', return_value=True):
            monitor._check_node("node-1")

        assert monitor.get_node_status("node-1") == NodeStatus.ALIVE

    def test_suspected_on_timeout(self):
        """Node should be SUSPECTED when ping fails and timeout < 2x."""
        monitor = HeartbeatMonitor(heartbeat_interval_s=5, timeout_s=10)
        monitor.register_node("node-1", "http://localhost:8000/health")

        # Set last_seen to be beyond timeout but within 2x timeout
        monitor._nodes["node-1"]["last_seen"] = time.time() - 12  # > 10s but < 20s

        with patch.object(monitor, '_ping_node', return_value=False):
            monitor._check_node("node-1")

        assert monitor.get_node_status("node-1") == NodeStatus.SUSPECTED

    def test_dead_on_extended_timeout(self):
        """Node should be DEAD when ping fails and timeout >= 2x."""
        monitor = HeartbeatMonitor(heartbeat_interval_s=5, timeout_s=10)
        monitor.register_node("node-1", "http://localhost:8000/health")

        # Set last_seen to be beyond 2x timeout
        monitor._nodes["node-1"]["last_seen"] = time.time() - 25  # > 20s

        with patch.object(monitor, '_ping_node', return_value=False):
            monitor._check_node("node-1")

        assert monitor.get_node_status("node-1") == NodeStatus.DEAD

    def test_recovery_from_suspected_to_alive(self):
        """Node should recover from SUSPECTED to ALIVE when ping succeeds."""
        monitor = HeartbeatMonitor(heartbeat_interval_s=5, timeout_s=10)
        monitor.register_node("node-1", "http://localhost:8000/health")

        # First make it suspected
        monitor._nodes["node-1"]["last_seen"] = time.time() - 12
        with patch.object(monitor, '_ping_node', return_value=False):
            monitor._check_node("node-1")
        assert monitor.get_node_status("node-1") == NodeStatus.SUSPECTED

        # Then ping succeeds — should recover
        with patch.object(monitor, '_ping_node', return_value=True):
            monitor._check_node("node-1")
        assert monitor.get_node_status("node-1") == NodeStatus.ALIVE

    def test_status_report(self):
        """get_status_report should return all registered nodes."""
        monitor = HeartbeatMonitor(heartbeat_interval_s=5, timeout_s=15)
        monitor.register_node("node-1", "http://host1:8000/health")
        monitor.register_node("node-2", "http://host2:8000/health")

        report = monitor.get_status_report()
        assert "node-1" in report
        assert "node-2" in report
        assert report["node-1"]["status"] == "alive"

    def test_get_alive_nodes(self):
        """get_alive_nodes should return only ALIVE node IDs."""
        monitor = HeartbeatMonitor(heartbeat_interval_s=5, timeout_s=10)
        monitor.register_node("node-1", "http://host1:8000/health")
        monitor.register_node("node-2", "http://host2:8000/health")

        # Make node-2 dead
        monitor._nodes["node-2"]["last_seen"] = time.time() - 25
        with patch.object(monitor, '_ping_node', return_value=False):
            monitor._check_node("node-2")

        alive = monitor.get_alive_nodes()
        assert "node-1" in alive
        assert "node-2" not in alive

    def test_callback_on_status_change(self):
        """Callback should fire when a node's status changes."""
        monitor = HeartbeatMonitor(heartbeat_interval_s=5, timeout_s=10)
        callback = MagicMock()
        monitor.on_status_change(callback)
        monitor.register_node("node-1", "http://localhost:8000/health")

        # Trigger ALIVE → DEAD
        monitor._nodes["node-1"]["last_seen"] = time.time() - 25
        with patch.object(monitor, '_ping_node', return_value=False):
            monitor._check_node("node-1")

        callback.assert_called_once()
        args = callback.call_args[0]
        assert args[0] == "node-1"
        assert args[1] == NodeStatus.ALIVE
        assert args[2] == NodeStatus.DEAD

    def test_unknown_node_returns_dead(self):
        """Getting status of an unregistered node should return DEAD."""
        monitor = HeartbeatMonitor(heartbeat_interval_s=5, timeout_s=15)
        assert monitor.get_node_status("nonexistent") == NodeStatus.DEAD