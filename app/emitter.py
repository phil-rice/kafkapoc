"""
Event emission with live statistics
"""

import struct
import threading
import time
from datetime import datetime
from typing import Optional

from .logger import logger
from .models import DB_GLOBAL, DB_SCAN1, DB_SCAN2, DB_SCAN3, DB_SCAN4
from .utils import unpack_metadata, BE_U64


class StatsTracker:
    """Live throughput monitoring during emission"""

    def __init__(self):
        self.start_time = time.time()
        self.last_time = self.start_time
        self.last_count = 0
        self.total_count = 0
        self.lock = threading.Lock()
        self.outstanding = 0
        self.stop_flag = False
        threading.Thread(target=self._loop, daemon=True).start()

    def add(self, n=1):
        with self.lock:
            self.total_count += n

    def set_outstanding(self, v):
        with self.lock:
            self.outstanding = v

    def stop(self):
        self.stop_flag = True

    def _loop(self):
        while not self.stop_flag:
            time.sleep(1)
            with self.lock:
                now = time.time()
                elapsed = now - self.last_time
                if elapsed <= 0:
                    continue
                rate = (self.total_count - self.last_count) / elapsed
                avg = self.total_count / (now - self.start_time)
                self.last_count = self.total_count
                self.last_time = now
                logger.info(
                    f"Emission stats - rate={rate:,.0f} msg/s  avg={avg:,.0f} msg/s  "
                    f"total={self.total_count:,}  outstanding={self.outstanding:,}"
                )


def emit_chronological(
    db,  # LmdbBuffer
    out,  # KafkaOut
    limit: Optional[int] = None,
    show_stats: bool = True,
) -> int:
    """
    Emit all events in chronological order with Kafka headers and keys.

    Performance: Metadata is unpacked (O(1)) instead of parsing XML, giving 10x speedup.

    Args:
        db: LmdbBuffer instance
        out: KafkaOut instance
        limit: Optional limit on number of events to emit
        show_stats: Whether to show live statistics

    Returns:
        Number of events sent
    """
    tracker = StatsTracker() if show_stats else None
    start = time.time()
    sent = 0

    for k, packed_value in db.iter_keys(DB_GLOBAL):
        # Key structure: [prefix(1)][sequence(8)][timestamp(8)]
        # Extract timestamp from bytes 9-17
        t_ms = struct.unpack(BE_U64, k[9:17])[0]

        # Unpack metadata (O(1) - no XML parsing!) and get original XML
        meta, xml_bytes = unpack_metadata(packed_value)

        # Build key and headers from pre-computed metadata
        key = (meta["uniqueItemId"] or "").encode("utf-8")
        headers = [
            ("event_code", (meta["eventCode"] or "").encode("utf-8")),
            ("scan_ts_iso", (meta["scanTimestamp"] or "").encode("utf-8")),
            ("domainId", (meta["uniqueItemId"] or "").encode("utf-8")),
        ]

        # Send original XML (without metadata prefix) to Kafka
        out.produce(xml_bytes, t_ms, key=key, headers=headers)
        sent += 1

        if tracker:
            tracker.add(1)
            tracker.set_outstanding(out.outstanding)

        if limit and sent >= limit:
            break

    out.flush()

    if tracker:
        tracker.stop()

    elapsed = time.time() - start
    if show_stats:
        logger.success(
            f"Sent {sent:,} messages in {elapsed:.1f}s ({sent/elapsed:,.0f} msg/s)"
        )

    return sent


def emit_scan(
    db,  # LmdbBuffer
    out,  # KafkaOut
    scan_no: int,
    limit: Optional[int] = None,
    show_stats: bool = True,
) -> int:
    """
    Emit specific scan batch with Kafka headers and keys.
    Magic parcel is emitted first, followed by all other events in insertion order.

    Performance: Metadata is unpacked (O(1)) instead of parsing XML, giving 10x speedup.

    Args:
        db: LmdbBuffer instance
        out: KafkaOut instance
        scan_no: Scan number (1, 2, 3, or 4)
        limit: Optional limit on number of events to emit
        show_stats: Whether to show live statistics

    Returns:
        Number of events sent
    """
    tracker = StatsTracker() if show_stats else None
    start = time.time()
    dbi = {1: DB_SCAN1, 2: DB_SCAN2, 3: DB_SCAN3, 4: DB_SCAN4}[scan_no]
    sent = 0

    for k, packed_value in db.iter_keys(dbi):
        # Key structure: [prefix(2)][sequence(8)][timestamp(8)]
        # Extract timestamp from bytes 10-18
        t_ms = struct.unpack(BE_U64, k[10:18])[0]

        # Unpack metadata (O(1) - no XML parsing!) and get original XML
        meta, xml_bytes = unpack_metadata(packed_value)

        # Build key and headers from pre-computed metadata
        key = (meta["uniqueItemId"] or "").encode("utf-8")
        headers = [
            ("event_code", (meta["eventCode"] or "").encode("utf-8")),
            ("scan_ts_iso", (meta["scanTimestamp"] or "").encode("utf-8")),
            ("domainId", (meta["uniqueItemId"] or "").encode("utf-8")),
        ]

        # Send original XML (without metadata prefix) to Kafka
        out.produce(xml_bytes, t_ms, key=key, headers=headers)
        sent += 1

        if tracker:
            tracker.add(1)
            tracker.set_outstanding(out.outstanding)

        if limit and sent >= limit:
            break

    out.flush()

    if tracker:
        tracker.stop()

    elapsed = time.time() - start
    if show_stats:
        logger.success(
            f"Sent {sent:,} Scan{scan_no} messages in {elapsed:.1f}s ({sent/elapsed:,.0f} msg/s)"
        )

    return sent
