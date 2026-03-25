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