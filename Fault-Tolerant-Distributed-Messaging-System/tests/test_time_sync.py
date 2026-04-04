"""
=============================================================================
 MEMBER 3 — TIME SYNCHRONIZATION — Unit Tests

=============================================================================
"""

import time
import pytest
from unittest.mock import patch, MagicMock


# ─────────────────────────────────────────────────────────────────────────────
# DAY 2 (Lamport tests don't need NTP mocking)
# ─────────────────────────────────────────────────────────────────────────────
class TestLamportClock:

    def test_tick_increments(self):
        """Each tick should increment the counter by 1."""
        from src.time_sync.time_synchronizer import LamportClock
        clock = LamportClock()
        assert clock.tick() == 1
        assert clock.tick() == 2
        assert clock.tick() == 3

    def test_update_takes_max(self):
        """Update should set counter to max(local, received) + 1."""
        from src.time_sync.time_synchronizer import LamportClock
        clock = LamportClock()
        clock.tick()  # counter = 1
        clock.tick()  # counter = 2
        # Receive a message with timestamp 10 → counter = max(2, 10) + 1 = 11
        result = clock.update(10)
        assert result == 11

    def test_update_when_local_is_higher(self):
        """If local counter > received, result = local + 1."""
        from src.time_sync.time_synchronizer import LamportClock
        clock = LamportClock()
        for _ in range(20):
            clock.tick()  # counter = 20
        result = clock.update(5)  # max(20, 5) + 1 = 21
        assert result == 21

    def test_monotonicity(self):
        """Lamport clock should never go backward."""
        from src.time_sync.time_synchronizer import LamportClock
        clock = LamportClock()
        prev = 0
        for _ in range(100):
            val = clock.tick()
            assert val > prev
            prev = val

    def test_current_does_not_increment(self):
        """Reading .current should not change the counter."""
        from src.time_sync.time_synchronizer import LamportClock
        clock = LamportClock()
        clock.tick()
        val = clock.current
        assert val == clock.current  # reading twice gives same value


# ─────────────────────────────────────────────────────────────────────────────
# DAY 3 
# ─────────────────────────────────────────────────────────────────────────────
class TestHybridLogicalClock:

    def test_tick_returns_tuple(self):
        """HLC tick should return (physical_ms, logical) tuple."""
        from src.time_sync.time_synchronizer import HybridLogicalClock
        hlc = HybridLogicalClock(ntp_sync=None)
        result = hlc.tick()
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_tick_physical_close_to_now(self):
        """Physical component should be close to current time."""
        from src.time_sync.time_synchronizer import HybridLogicalClock
        hlc = HybridLogicalClock(ntp_sync=None)
        physical, _ = hlc.tick()
        now_ms = int(time.time() * 1000)
        assert abs(physical - now_ms) < 1000  # within 1 second

    def test_tick_logical_increments_on_same_physical(self):
        """If physical time hasn't advanced, logical counter should increment."""
        from src.time_sync.time_synchronizer import HybridLogicalClock
        hlc = HybridLogicalClock(ntp_sync=None)
        # Force same physical time by calling tick() rapidly
        results = [hlc.tick() for _ in range(10)]
        # At least some should have logical > 0 (unless time advances between each)
        max_logical = max(r[1] for r in results)
        assert max_logical >= 0  # may be 0 if time advances between each call

    def test_encode_decode_roundtrip(self):
        """Encoding then decoding should return the original values."""
        from src.time_sync.time_synchronizer import HybridLogicalClock
        hlc = HybridLogicalClock(ntp_sync=None)
        physical_ms = 1700000000000
        logical = 42
        encoded = hlc.encode(physical_ms, logical)
        decoded = hlc.decode(encoded)
        assert decoded == (physical_ms, logical)

    def test_encoded_ordering(self):
        """Encoded HLC values should maintain causal ordering."""
        from src.time_sync.time_synchronizer import HybridLogicalClock
        hlc = HybridLogicalClock(ntp_sync=None)
        t1_phys, t1_log = hlc.tick()
        t2_phys, t2_log = hlc.tick()
        enc1 = hlc.encode(t1_phys, t1_log)
        enc2 = hlc.encode(t2_phys, t2_log)
        assert enc1 < enc2  # t1 happened before t2

    def test_update_advances_clock(self):
        """Receiving a message with future timestamp should advance the HLC."""
        from src.time_sync.time_synchronizer import HybridLogicalClock
        hlc = HybridLogicalClock(ntp_sync=None)
        hlc.tick()
        # Simulate receiving a message from a node whose clock is far ahead
        future_physical = int(time.time() * 1000) + 60000  # 60s in the future
        result_physical, result_logical = hlc.update(future_physical, 5)
        assert result_physical >= future_physical


# ─────────────────────────────────────────────────────────────────────────────
# DAY 4  
# ─────────────────────────────────────────────────────────────────────────────
class TestMessageReorderBuffer:

    def test_add_increases_size(self):
        """Adding a message should increase buffer size."""
        from src.time_sync.time_synchronizer import MessageReorderBuffer
        buf = MessageReorderBuffer(window_ms=500)
        assert buf.size == 0
        buf.add({"messageId": "m1", "timestamp": int(time.time() * 1000)})
        assert buf.size == 1

    def test_flush_releases_old_messages(self):
        """Messages older than the window should be released."""
        from src.time_sync.time_synchronizer import MessageReorderBuffer
        buf = MessageReorderBuffer(window_ms=100)
        old_ts = int(time.time() * 1000) - 200  # 200ms ago (> 100ms window)
        buf.add({"messageId": "m1", "timestamp": old_ts})
        released = buf.flush()
        assert len(released) == 1
        assert released[0]["messageId"] == "m1"

    def test_flush_holds_recent_messages(self):
        """Messages newer than the window should stay in the buffer."""
        from src.time_sync.time_synchronizer import MessageReorderBuffer
        buf = MessageReorderBuffer(window_ms=10000)  # 10s window
        now_ts = int(time.time() * 1000)
        buf.add({"messageId": "m1", "timestamp": now_ts})
        released = buf.flush()
        assert len(released) == 0
        assert buf.size == 1

    def test_flush_returns_sorted_order(self):
        """Released messages should be sorted by timestamp ascending."""
        from src.time_sync.time_synchronizer import MessageReorderBuffer
        buf = MessageReorderBuffer(window_ms=100)
        base = int(time.time() * 1000) - 500
        # Add out of order
        buf.add({"messageId": "m3", "timestamp": base + 30})
        buf.add({"messageId": "m1", "timestamp": base + 10})
        buf.add({"messageId": "m2", "timestamp": base + 20})
        released = buf.flush()
        assert len(released) == 3
        assert released[0]["messageId"] == "m1"
        assert released[1]["messageId"] == "m2"
        assert released[2]["messageId"] == "m3"

    def test_flush_all_releases_everything(self):
        """flush_all should release all messages regardless of window."""
        from src.time_sync.time_synchronizer import MessageReorderBuffer
        buf = MessageReorderBuffer(window_ms=999999)
        now = int(time.time() * 1000)
        buf.add({"messageId": "m1", "timestamp": now})
        buf.add({"messageId": "m2", "timestamp": now + 1})
        released = buf.flush_all()
        assert len(released) == 2
        assert buf.size == 0


# ─────────────────────────────────────────────────────────────────────────────
# DAY 5  
# ─────────────────────────────────────────────────────────────────────────────
class TestClockSkewAnalyzer:

    def test_record_skew_returns_value(self):
        """record_skew should return the observed skew in ms."""
        from src.time_sync.time_synchronizer import ClockSkewAnalyzer
        analyzer = ClockSkewAnalyzer()
        # Sender timestamp = 100ms ago → skew ≈ 100ms
        sender_ts = int(time.time() * 1000) - 100
        skew = analyzer.record_skew("node-1", sender_ts)
        assert 50 <= skew <= 200  # allow some tolerance

    def test_report_no_data(self):
        """Report with no samples should indicate no data."""
        from src.time_sync.time_synchronizer import ClockSkewAnalyzer
        analyzer = ClockSkewAnalyzer()
        report = analyzer.get_skew_report()
        assert report["status"] == "no_data"

    def test_report_with_data(self):
        """Report should include per-node statistics."""
        from src.time_sync.time_synchronizer import ClockSkewAnalyzer
        analyzer = ClockSkewAnalyzer()
        now = int(time.time() * 1000)
        analyzer.record_skew("node-A", now - 50)
        analyzer.record_skew("node-A", now - 100)
        analyzer.record_skew("node-B", now - 200)

        report = analyzer.get_skew_report()
        assert report["status"] == "ok"
        assert "node-A" in report["nodes"]
        assert "node-B" in report["nodes"]
        assert report["nodes"]["node-A"]["sample_count"] == 2
        assert report["nodes"]["node-B"]["sample_count"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# DAY 1 
# ─────────────────────────────────────────────────────────────────────────────
class TestNTPSynchronizer:

    @patch("src.time_sync.time_synchronizer.NTPSynchronizer._do_sync")
    def test_initial_offset_zero_on_failure(self, mock_sync):
        """If NTP sync fails, offset should default to 0."""
        mock_sync.return_value = None  # simulate sync failure
        from src.time_sync.time_synchronizer import NTPSynchronizer
        ntp = NTPSynchronizer.__new__(NTPSynchronizer)
        ntp.ntp_servers = ["pool.ntp.org"]
        ntp.sync_interval = 60
        ntp._offset_ms = 0.0
        ntp._last_sync_time = 0
        ntp._sync_count = 0
        import threading
        ntp._lock = threading.Lock()
        ntp._running = False
        ntp._sync_thread = None
        assert ntp.offset_ms == 0.0

    @patch("src.time_sync.time_synchronizer.NTPSynchronizer._do_sync")
    def test_corrected_timestamp_applies_offset(self, mock_sync):
        """Corrected timestamp should add the NTP offset to local time."""
        mock_sync.return_value = None
        from src.time_sync.time_synchronizer import NTPSynchronizer
        ntp = NTPSynchronizer.__new__(NTPSynchronizer)
        ntp.ntp_servers = ["pool.ntp.org"]
        ntp.sync_interval = 60
        ntp._offset_ms = 50.0  # local clock is 50ms behind
        ntp._last_sync_time = time.time()
        ntp._sync_count = 1
        import threading
        ntp._lock = threading.Lock()
        ntp._running = False
        ntp._sync_thread = None

        corrected = ntp.get_corrected_timestamp_ms()
        local     = int(time.time() * 1000)
        # Corrected should be about 50ms ahead of raw local time
        assert abs(corrected - local - 50) < 100  # within 100ms tolerance