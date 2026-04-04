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

class LamportClock:
    """
        Lamport Logical Clock (Leslie Lamport, 1978).

        PURPOSE (Part 3: ensuring correct message ordering):
      Physical clocks can never be perfectly synchronised.  Lamport clocks
      provide a LOGICAL ordering guarantee:
        If event A causally precedes event B → L(A) < L(B)
      This means we can always determine the causal order of events,
      even when physical timestamps are unreliable due to clock skew.

    RULES:
      1. Each process maintains a counter C, initially 0
      2. Before each local event:  C = C + 1
      3. When sending a message:   attach C to the message
      4. When receiving a message with timestamp T_msg:
            C = max(C, T_msg) + 1

    TRADE-OFF vs vector clocks:
      - Lamport clocks are simpler (single integer vs vector of integers)
      - But they cannot detect CONCURRENT events (where neither caused the other)
      - For a messaging app this is acceptable: we only need total ordering
    """

    def __init__(self):
        self._counter = 0
        self._lock    = threading.Lock()

    def tick(self) -> int:
        """
        Increment the clock for a local event (e.g. sending a message).
        Returns the new timestamp.
        """
        with self._lock:
            self._counter += 1
            return self._counter

    def update(self, received_timestamp: int) -> int:
        """
        Update the clock when a message is received.
        Takes the maximum of local clock and received timestamp, then +1.
        This ensures causal ordering is preserved across nodes.
        """
        with self._lock:
            self._counter = max(self._counter, received_timestamp) + 1
            return self._counter

    @property
    def current(self) -> int:
        """Get the current Lamport timestamp without incrementing."""
        with self._lock:
            return self._counter

class HybridLogicalClock:
    """
    Hybrid Logical Clock (HLC), inspired by CockroachDB's implementation.

    PURPOSE:
      Combines NTP-corrected physical time with a logical counter to get:
        ✅ Wall-clock accuracy from NTP (useful for display, TTL, debugging)
        ✅ Causal ordering guarantee from Lamport clock
        ✅ Monotonicity — HLC never goes backward, even if NTP adjusts

    TIMESTAMP FORMAT:
      (physical_ms, logical_counter)
      - physical_ms: NTP-corrected wall-clock time in milliseconds
      - logical_counter: incremented when physical time hasn't advanced

    ENCODING (for storage and comparison):
      We encode as a single 64-bit integer:
        encoded = (physical_ms << 16) | (logical_counter & 0xFFFF)
      This allows simple integer comparison for ordering while preserving
      both physical and logical components.

    ALGORITHM:
      On local event or send:
        new_physical = max(old_physical, NTP_corrected_now)
        if new_physical == old_physical:
            logical += 1   # physical didn't advance, use logical tie-breaker
        else:
            logical = 0    # physical advanced, reset logical
        timestamp = (new_physical, logical)

      On receive (with message timestamp (msg_physical, msg_logical)):
        new_physical = max(old_physical, NTP_corrected_now, msg_physical)
        if new_physical == old_physical == msg_physical:
            logical = max(old_logical, msg_logical) + 1
        elif new_physical == old_physical:
            logical = old_logical + 1
        elif new_physical == msg_physical:
            logical = msg_logical + 1
        else:
            logical = 0
        timestamp = (new_physical, logical)
    """

    def __init__(self, ntp_sync: NTPSynchronizer = None):
        self._ntp_sync    = ntp_sync
        self._physical_ms = 0
        self._logical     = 0
        self._lock        = threading.Lock()

    def _now_ms(self) -> int:
        """Get NTP-corrected current time, or fallback to system time."""
        if self._ntp_sync:
            return self._ntp_sync.get_corrected_timestamp_ms()
        return int(time.time() * 1000)

    def tick(self) -> tuple:
        """
        Generate an HLC timestamp for a local event (sending a message).
        Returns (physical_ms, logical_counter) tuple.
        """
        with self._lock:
            now = self._now_ms()
            if now > self._physical_ms:
                self._physical_ms = now
                self._logical     = 0
            else:
                self._logical += 1
            return (self._physical_ms, self._logical)

    def update(self, msg_physical: int, msg_logical: int) -> tuple:
        """
        Update HLC when receiving a message with its HLC timestamp.
        Returns updated (physical_ms, logical_counter) tuple.
        """
        with self._lock:
            now = self._now_ms()
            old_physical = self._physical_ms
            old_logical  = self._logical

            self._physical_ms = max(old_physical, now, msg_physical)

            if self._physical_ms == old_physical == msg_physical:
                self._logical = max(old_logical, msg_logical) + 1
            elif self._physical_ms == old_physical:
                self._logical = old_logical + 1
            elif self._physical_ms == msg_physical:
                self._logical = msg_logical + 1
            else:
                self._logical = 0

            return (self._physical_ms, self._logical)

    def encode(self, physical_ms: int, logical: int) -> int:
        """
        Encode HLC tuple into a single 64-bit integer for storage.
        Allows simple integer comparison: if encoded(A) < encoded(B),
        then event A happened before event B.
        """
        return (physical_ms << 16) | (logical & 0xFFFF)

    def decode(self, encoded: int) -> tuple:
        """Decode a stored HLC integer back to (physical_ms, logical)."""
        physical_ms = encoded >> 16
        logical     = encoded & 0xFFFF
        return (physical_ms, logical)

    @property
    def current(self) -> tuple:
        """Get current HLC value without incrementing."""
        with self._lock:
            return (self._physical_ms, self._logical)            
        
#Day04
class MessageReorderBuffer:
    """
    Reorder buffer that holds messages for a configurable time window
    and releases them in timestamp-sorted order.

    PURPOSE (Part 3: "Develop a mechanism to reorder messages that arrive
    out of sequence due to network delays"):
      In a distributed system, messages sent at times T1 < T2 may arrive
      in reverse order (T2 arrives before T1) due to:
        - Different network paths with varying latency
        - Kafka partition rebalancing
        - Consumer restarts processing from an earlier offset

    MECHANISM:
      1. Incoming messages are added to a buffer (sorted list)
      2. A message is "ready" to be released when:
         current_time - message_timestamp > window_ms
      3. When flushed, all ready messages are returned sorted by timestamp
      4. Messages not yet ready stay in the buffer for the next flush

    TRADE-OFFS:
      - Larger window → better ordering but higher delivery latency
      - Smaller window → lower latency but more risk of out-of-order
      - Default 500ms is a practical balance for LAN-connected servers
    """

    def __init__(self, window_ms: int = 500):
        self._window_ms = window_ms
        self._buffer    = []     # list of (timestamp_ms, message_dict)
        self._lock      = threading.Lock()

    def add(self, message: dict) -> None:
        """Add a message to the reorder buffer."""
        ts = message.get("timestamp", 0)
        with self._lock:
            self._buffer.append((ts, message))
            # Keep buffer sorted by timestamp for efficient flush
            self._buffer.sort(key=lambda x: x[0])

    def flush(self) -> list:
        """
        Release all messages whose timestamp is older than the window.
        Returns a list of message dicts, sorted by timestamp ascending.
        Messages newer than (now - window_ms) stay in the buffer.
        """
        now_ms    = int(time.time() * 1000)
        cutoff_ms = now_ms - self._window_ms

        with self._lock:
            ready   = [msg for ts, msg in self._buffer if ts <= cutoff_ms]
            self._buffer = [(ts, msg) for ts, msg in self._buffer if ts > cutoff_ms]

        return ready

    def flush_all(self) -> list:
        """
        Force-release ALL messages (used during shutdown or when latency
        doesn't matter). Returns messages sorted by timestamp.
        """
        with self._lock:
            ready = [msg for _, msg in self._buffer]
            self._buffer = []
        return ready

    @property
    def size(self) -> int:
        """Number of messages currently in the buffer."""
        with self._lock:
            return len(self._buffer)
        
class ClockSkewAnalyzer:
    """
    Analyzes clock skew between nodes by comparing message timestamps
    with local receive timestamps.

    PURPOSE (Part 3: "Analyze the impact of clock skew on message ordering"):
      When sender's clock is ahead of receiver's clock by Δ ms:
        - Messages appear to arrive "from the future"
        - Sorting by timestamp places them AFTER messages that were
          actually sent later — breaking real-time ordering

      When sender's clock is behind receiver's clock by Δ ms:
        - Messages appear older than they are
        - May be incorrectly placed before more recent messages

    HOW IT WORKS:
      1. When a message arrives, record (sender_timestamp, local_receive_time)
      2. The difference (local_receive - sender_timestamp) is the observed skew
         (includes both clock difference AND network latency)
      3. Over many messages, the MINIMUM observed difference approximates
         the true clock skew (minimum latency is closest to just the skew)

    IMPACT ON THIS SYSTEM:
      - Clock skew < 500ms: reorder buffer handles it (window = 500ms)
      - Clock skew > 500ms: messages may be released in wrong order
        → solution: increase REORDER_BUFFER_WINDOW_MS or sync more often
    """

    def __init__(self):
        self._samples     = []     # list of (sender_id, skew_ms) tuples
        self._lock        = threading.Lock()
        self._max_samples = 1000   # keep last 1000 samples to limit memory

    def record_skew(self, sender_node: str, sender_timestamp_ms: int) -> float:
        """
        Record the clock skew observed when receiving a message.
        Returns the observed skew in milliseconds.
        Positive = sender clock is behind, Negative = sender clock is ahead.
        """
        local_now_ms = int(time.time() * 1000)
        skew_ms      = local_now_ms - sender_timestamp_ms

        with self._lock:
            self._samples.append((sender_node, skew_ms))
            # Trim old samples to prevent memory growth
            if len(self._samples) > self._max_samples:
                self._samples = self._samples[-self._max_samples:]

        # Log warning if skew exceeds the reorder buffer window
        if abs(skew_ms) > 500:
            logger.warning(
                f"[CLOCK SKEW] High skew detected from {sender_node}: "
                f"{skew_ms:+.0f}ms — messages may arrive out of order"
            )

        return skew_ms

    def get_skew_report(self) -> dict:
        """
        Generate a report of observed clock skew across all nodes.
        Returns per-node statistics: min, max, average, and sample count.
        """
        with self._lock:
            samples_copy = list(self._samples)

        if not samples_copy:
            return {"status": "no_data", "nodes": {}}

        # Group by sender node
        per_node = {}
        for node, skew in samples_copy:
            per_node.setdefault(node, []).append(skew)

        report = {"status": "ok", "nodes": {}}
        for node, skews in per_node.items():
            report["nodes"][node] = {
                "sample_count": len(skews),
                "min_skew_ms":  min(skews),
                "max_skew_ms":  max(skews),
                "avg_skew_ms":  sum(skews) / len(skews),
                "estimated_clock_offset_ms": min(skews),  # minimum ≈ true skew
            }

        return report        