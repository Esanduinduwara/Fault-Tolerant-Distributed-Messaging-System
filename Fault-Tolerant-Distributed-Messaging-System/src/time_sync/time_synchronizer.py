import time
import logging
import threading
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class NTPSynchronizer:
    """
    Network Time Protocol (NTP) synchronizer for distributed clock alignment.

    PURPOSE (Part 3 requirement: "Implement a time synchronization protocol"):
      Each messaging server has its own local clock, which may drift by
      milliseconds or even seconds relative to other servers.  NTP corrects
      this by querying a reference time server and calculating the OFFSET
      between the local clock and the reference clock.

    ALGORITHM (simplified NTP):
      1. Record local time T1
      2. Send request to NTP server
      3. NTP server responds with its time T_server
      4. Record local time T4 when response arrives
      5. Round-trip delay = (T4 - T1)
      6. Offset = T_server - (T1 + delay/2)
         → positive offset means local clock is BEHIND
         → negative offset means local clock is AHEAD

    TRADE-OFFS:
      - Accuracy: NTP achieves ~1-10ms accuracy over the internet
      - Network overhead: one UDP packet per sync (negligible)
      - Sync frequency: configurable via NTP_SYNC_INTERVAL_S
        - Frequent syncs → better accuracy, more network traffic
        - Infrequent syncs → lower overhead, more potential drift
    """

    def __init__(self, ntp_servers=None, sync_interval_s=60):
        """
        Args:
            ntp_servers: list of NTP server hostnames to query (tried in order)
            sync_interval_s: how often to re-sync with the NTP server
        """
        from src.config.settings import NTP_SERVERS, NTP_SYNC_INTERVAL_S

        self.ntp_servers     = ntp_servers or NTP_SERVERS
        self.sync_interval   = sync_interval_s or NTP_SYNC_INTERVAL_S
        self._offset_ms      = 0.0        # current offset in milliseconds
        self._last_sync_time = 0           # epoch when last synced
        self._sync_count     = 0           # total successful syncs
        self._lock           = threading.Lock()
        self._running        = False
        self._sync_thread    = None

        # Perform initial sync
        self._do_sync()

    # ─────────────────────────────────────────────────────────────────────────
    # Core NTP sync method
    # ─────────────────────────────────────────────────────────────────────────
    def _do_sync(self):
        """
        Query NTP servers and calculate the clock offset.
        Tries each server in order; uses the first successful response.
        Falls back to zero offset if all servers are unreachable.
        """
        try:
            import ntplib
            client = ntplib.NTPClient()

            for server in self.ntp_servers:
                try:
                    # T1: local time before request
                    t1 = time.time()

                    # Query NTP server (timeout 2s to avoid blocking)
                    response = client.request(server, version=3, timeout=2)

                    # T4: local time after response
                    t4 = time.time()

                    # NTP offset in seconds (server_time - local_time)
                    # response.offset is already calculated by ntplib using
                    # the standard NTP formula:
                    #   offset = ((T2 - T1) + (T3 - T4)) / 2
                    # where T2/T3 are server receive/transmit timestamps
                    offset_s = response.offset

                    # Round-trip delay tells us about network quality
                    delay_s = response.delay

                    with self._lock:
                        self._offset_ms      = offset_s * 1000.0
                        self._last_sync_time = time.time()
                        self._sync_count    += 1

                    logger.info(
                        f"[NTP] Synced with {server}: "
                        f"offset={self._offset_ms:+.2f}ms, "
                        f"delay={delay_s*1000:.2f}ms, "
                        f"syncs={self._sync_count}"
                    )
                    return  # success — stop trying other servers

                except Exception as exc:
                    logger.warning(f"[NTP] Failed to reach {server}: {exc}")
                    continue

            # All servers failed — keep previous offset
            logger.warning("[NTP] All NTP servers unreachable, keeping previous offset")

        except ImportError:
            # ntplib not installed — fall back to zero offset
            logger.warning("[NTP] ntplib not installed, using zero offset")

    def get_corrected_timestamp_ms(self) -> int:
        """
        Return the current time in milliseconds, CORRECTED for clock offset.
        This is the "timestamp correction technique" required by Part 3.

        Formula: corrected_time = local_time + ntp_offset
        - If local clock is behind by 50ms, offset = +50ms → time moves forward
        - If local clock is ahead  by 50ms, offset = -50ms → time moves backward
        """
        local_ms = int(time.time() * 1000)
        with self._lock:
            corrected = local_ms + int(self._offset_ms)
        return corrected

    @property
    def offset_ms(self) -> float:
        """Current NTP offset in milliseconds."""
        with self._lock:
            return self._offset_ms

    @property
    def sync_count(self) -> int:
        """Number of successful NTP syncs performed."""
        with self._lock:
            return self._sync_count

    # ─────────────────────────────────────────────────────────────────────────
    # Background sync thread — periodically re-syncs to track drift
    # ─────────────────────────────────────────────────────────────────────────
    def start_background_sync(self):
        """
        Start a daemon thread that re-syncs with NTP every sync_interval seconds.
        Continuous re-sync is needed because local clocks DRIFT over time:
        - Crystal oscillators in commodity hardware drift ~10-100 ppm
        - 100ppm drift = 8.64 seconds per day!
        - Periodic NTP re-sync keeps the offset accurate
        """
        self._running = True
        self._sync_thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._sync_thread.start()

    def _sync_loop(self):
        """Background loop: sleep → sync → repeat."""
        while self._running:
            time.sleep(self.sync_interval)
            self._do_sync()

    def stop(self):
        """Stop the background sync thread."""
        self._running = False