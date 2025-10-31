"""
Streaming emitter - emits events directly from generator to Kafka
"""

import time
from typing import Optional

from .logger import logger
from .config_store import GenerationConfig
from .streaming_generator import stream_scan_events
from .emitter import StatsTracker


def emit_scan_streaming(
    out,  # KafkaOut instance
    config: GenerationConfig,
    scan_no: int,
    limit: Optional[int] = None,
    show_stats: bool = True,
) -> int:
    """
    Stream events directly from generator to Kafka without storage

    Args:
        out: KafkaOut instance
        config: Generation configuration
        scan_no: Scan number (1, 2, 3, or 4)
        limit: Optional limit on number of events to emit
        show_stats: Whether to show live statistics

    Returns:
        Number of events sent
    """
    tracker = StatsTracker() if show_stats else None
    start = time.time()
    sent = 0

    logger.info(f"Starting stream for scan {scan_no}")

    # Pre-encode event_code header (constant per scan)
    event_code_header = None

    # Stream events directly from generator
    for xml_bytes, metadata, t_ms in stream_scan_events(config, scan_no):
        # Build key
        unique_id = metadata["uniqueItemId"] or ""
        key = unique_id.encode("utf-8")

        # Pre-encode event code on first iteration
        if event_code_header is None:
            event_code_header = (metadata["eventCode"] or "").encode("utf-8")

        # Build headers (reuse pre-encoded event_code)
        headers = [
            ("event_code", event_code_header),
            ("scan_ts_iso", (metadata["scanTimestamp"] or "").encode("utf-8")),
            ("domainId", key),  # Reuse encoded key
        ]

        # Send directly to Kafka (no storage)
        out.produce(xml_bytes, t_ms, key=key, headers=headers)
        sent += 1

        if tracker:
            tracker.add(1)
            tracker.set_outstanding(out.outstanding)

        if limit and sent >= limit:
            break

    # Flush remaining messages
    out.flush()

    if tracker:
        tracker.stop()

    elapsed = time.time() - start
    if show_stats:
        logger.success(
            f"Sent {sent:,} Scan{scan_no} messages in {elapsed:.1f}s ({sent/elapsed:,.0f} msg/s)"
        )

    return sent
