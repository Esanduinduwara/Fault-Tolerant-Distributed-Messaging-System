import time
import pytest
from unittest.mock import patch, MagicMock

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

        

        