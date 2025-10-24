#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
produce_inputs_with_delays.py

Reads the generated INPUT ZIPs (from generate_rm_pairs.py) and publishes the
manual scan XML events to a Kafka/Redpanda topic in chronological order.

Replay strategy (per your requirement):
- Process scan index 1 for ALL parcels:
    - Gather all first-scan files, sort by scanTimestamp, publish in order,
      sleeping between events to preserve (compressed) inter-event gaps.
- Then repeat for scan index 2, 3, and 4.

Features:
- Creates the Kafka topic if missing (partitions/retention configurable).
- Uses an on-disk index CSV per scan index to avoid high memory.
- Time compression factor (e.g., 3600 means 1 hour replayed as 1 second).
- Publishes the full XML as message value; message key = uniqueItemId.
- Adds useful Kafka headers: event_code, scan_index, product_id, upu, scan_ts_iso.

Usage:
  python produce_inputs_with_delays.py \
    --inputs-dir ./out_pairs/inputs \
    --bootstrap-servers localhost:8080 \
    --topic mper-input-events \
    --create-topics \
    --partitions 12 \
    --retention-ms 604800000 \
    --time-compression 3600 \
    --linger-ms 5 \
    --compression zstd
"""

import argparse
import csv
import os
import sys
import time
import zipfile
from datetime import datetime, timezone
from typing import Dict, Tuple

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# --- XML parsing ---
import xml.etree.ElementTree as ET


def parse_args():
    p = argparse.ArgumentParser(
        description="Publish MPEr input XML events to Kafka in time order with delays."
    )
    p.add_argument(
        "--inputs-dir",
        type=str,
        required=True,
        help="Directory containing input ZIPs (mp_inputs_*.zip).",
    )
    p.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:8080",
        help="Kafka/Redpanda bootstrap servers.",
    )
    p.add_argument(
        "--topic", type=str, default="mper-input-events", help="Topic to publish to."
    )
    p.add_argument(
        "--create-topics",
        action="store_true",
        help="Create the topic if it does not exist.",
    )
    p.add_argument(
        "--partitions",
        type=int,
        default=12,
        help="Partitions for topic creation (single-node RF=1).",
    )
    p.add_argument(
        "--retention-ms",
        type=int,
        default=7 * 24 * 3600 * 1000,
        help="Topic retention in ms.",
    )
    p.add_argument(
        "--replication-factor",
        type=int,
        default=1,
        help="Replication factor (1 for single-node dev).",
    )
    p.add_argument("--linger-ms", type=int, default=5, help="Producer linger.ms.")
    p.add_argument(
        "--compression",
        type=str,
        default="zstd",
        choices=["none", "gzip", "snappy", "lz4", "zstd"],
        help="Kafka message compression.type.",
    )
    p.add_argument(
        "--time-compression",
        type=float,
        default=3600.0,
        help="Divide real gaps by this factor. 3600 => 1 hour gap becomes 1 second sleep.",
    )
    p.add_argument(
        "--flush-every",
        type=int,
        default=50000,
        help="Flush producer after this many messages.",
    )
    p.add_argument(
        "--index-dir",
        type=str,
        default="./.replay_index",
        help="Where to store temporary index CSVs.",
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Do everything except sending to Kafka."
    )
    return p.parse_args()


def ensure_topic(
    admin: AdminClient, topic: str, partitions: int, rf: int, retention_ms: int
):
    """Create topic if it doesn't exist."""
    md = admin.list_topics(timeout=10)
    if topic in md.topics and not md.topics[topic].error:
        print(f"[topic] '{topic}' already exists.")
        return

    print(
        f"[topic] Creating '{topic}' with partitions={partitions}, rf={rf}, retention.ms={retention_ms}"
    )
    new_topic = NewTopic(
        topic=topic,
        num_partitions=partitions,
        replication_factor=rf,
        config={"retention.ms": str(retention_ms)},
    )
    fs = admin.create_topics([new_topic])
    try:
        fs[topic].result()
        print(f"[topic] Created '{topic}'.")
    except KafkaException as e:
        # If it's "Topic already exists", that's fine (race)
        print(f"[topic] Create result: {e}")


def build_producer(bootstrap: str, linger_ms: int, compression: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": linger_ms,
        "compression.type": compression,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 200000,
        "message.max.bytes": 10 * 1024 * 1024,
        "retries": 1000000,
        "request.timeout.ms": 30000,
        "delivery.timeout.ms": 120000,
    }
    return Producer(conf)


def list_input_zips(inputs_dir: str):
    files = [
        os.path.join(inputs_dir, f)
        for f in os.listdir(inputs_dir)
        if f.startswith("mp_inputs_") and f.endswith(".zip")
    ]
    return sorted(files)


def extract_scan_index_from_name(inner_name: str) -> int:
    # filename pattern: tracking_<uniqueItemId>_<scanIndex>.xml
    # e.g., tracking_110999991111001091111_1.xml
    base = os.path.basename(inner_name)
    if "_" not in base:
        return -1
    try:
        scan_idx = int(base.split("_")[-1].split(".")[0])
        return scan_idx
    except Exception:
        return -1


def parse_xml_metadata(xml_bytes: bytes) -> Dict[str, str]:
    """
    Returns dict with:
      uniqueItemId, upu, product_id, event_code, scan_ts_iso
    """
    # NB: the input uses ptp:* namespaces, but ElementTree can find by localname with .//tag
    root = ET.fromstring(xml_bytes)

    # uniqueItemId
    uid_el = root.find(".//uniqueItemId")
    uniqueItemId = uid_el.text.strip() if uid_el is not None else ""

    # upu
    upu_el = root.find(".//UPUTrackingNumber")
    upu = upu_el.text.strip() if upu_el is not None else ""

    # product_id
    pid_el = root.find(".//productId")
    product_id = pid_el.text.strip() if pid_el is not None else ""

    # event_code
    ev_el = root.find(".//manualScan/trackedEventCode")
    event_code = ev_el.text.strip() if ev_el is not None else ""

    # scan_ts
    ts_el = root.find(".//manualScan/scanTimestamp")
    scan_ts_iso = ts_el.text.strip() if ts_el is not None else ""

    return {
        "uniqueItemId": uniqueItemId,
        "UPUTrackingNumber": upu,
        "productId": product_id,
        "eventCode": event_code,
        "scanTimestamp": scan_ts_iso,
    }


def iso_to_epoch_ms(iso_ts: str) -> int:
    # example: 2025-10-07T03:00:09+01:00
    # Python 3.11's fromisoformat supports this directly
    dt = datetime.fromisoformat(iso_ts)
    # Normalize to UTC epoch ms
    return int(dt.timestamp() * 1000)


def build_index_for_scan(inputs_dir: str, scan_index: int, index_dir: str) -> str:
    """
    Walk all ZIPs, pick files where scan_index matches, parse minimal metadata,
    and write a CSV index with columns:
      epoch_ms, iso_ts, zip_path, inner_name, uniqueItemId, upu, event_code, product_id
    Returns the index CSV path.
    """
    os.makedirs(index_dir, exist_ok=True)
    idx_path = os.path.join(index_dir, f"scan{scan_index}_index.csv")
    count = 0

    with open(idx_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "epoch_ms",
                "iso_ts",
                "zip_path",
                "inner_name",
                "uniqueItemId",
                "upu",
                "event_code",
                "product_id",
            ]
        )

        for zpath in list_input_zips(inputs_dir):
            with zipfile.ZipFile(zpath, "r") as zf:
                for inner in zf.namelist():
                    if not inner.endswith(".xml"):
                        continue
                    # shortcut: most reliable to use scan index in filename
                    si = extract_scan_index_from_name(inner)
                    if si != scan_index:
                        continue
                    xml_bytes = zf.read(inner)
                    meta = parse_xml_metadata(xml_bytes)
                    epoch_ms = iso_to_epoch_ms(meta["scanTimestamp"])
                    w.writerow(
                        [
                            epoch_ms,
                            meta["scanTimestamp"],
                            zpath,
                            inner,
                            meta["uniqueItemId"],
                            meta["UPUTrackingNumber"],
                            meta["eventCode"],
                            meta["productId"],
                        ]
                    )
                    count += 1

    print(f"[index] scan {scan_index}: indexed {count} events -> {idx_path}")
    return idx_path


def sort_index_csv(idx_path: str) -> str:
    """
    Loads the index CSV, sorts by epoch_ms asc. For simplicity, we load in memory.
    If your dataset is huge, replace with a chunked external sort (left as exercise).
    Returns path to sorted CSV.
    """
    sorted_path = idx_path.replace("_index.csv", "_sorted.csv")
    rows = []
    with open(idx_path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # keep as strings; epoch_ms used for sort
            rows.append(row)
    rows.sort(key=lambda r: int(r["epoch_ms"]))
    with open(sorted_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            w.writeheader()
            for row in rows:
                w.writerow(row)
    print(f"[index] sorted -> {sorted_path}")
    return sorted_path


def produce_scan_index(
    producer: Producer,
    topic: str,
    sorted_idx_csv: str,
    time_compression: float,
    flush_every: int,
    dry_run: bool,
):
    """
    Publish events in the order given by sorted_idx_csv.
    Sleep between consecutive events by (delta_real / time_compression).
    """
    prev_epoch_ms = None
    sent = 0

    def delivery_report(err, msg):
        if err is not None:
            print(
                f"[deliver] FAILED: {err} (topic={msg.topic()} partition={msg.partition()})",
                file=sys.stderr,
            )

    with open(sorted_idx_csv, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        open_zip: Tuple[str, zipfile.ZipFile] = (None, None)
        for row in r:
            zip_path = row["zip_path"]
            inner = row["inner_name"]
            epoch_ms = int(row["epoch_ms"])

            # controlled sleep to replicate delays
            if prev_epoch_ms is not None:
                delta_ms = epoch_ms - prev_epoch_ms
                if delta_ms > 0 and time_compression > 0:
                    sleep_s = (delta_ms / 1000.0) / time_compression
                    if sleep_s > 0:
                        time.sleep(sleep_s)
            prev_epoch_ms = epoch_ms

            # open/reuse zip
            if open_zip[0] != zip_path:
                if open_zip[1] is not None:
                    open_zip[1].close()
                open_zip = (zip_path, zipfile.ZipFile(zip_path, "r"))

            xml_bytes = open_zip[1].read(inner)
            meta = parse_xml_metadata(xml_bytes)

            key = meta["uniqueItemId"].encode("utf-8")
            headers = [
                ("event_code", meta["eventCode"].encode("utf-8")),
                (
                    "scan_index",
                    str(extract_scan_index_from_name(inner)).encode("utf-8"),
                ),
                ("product_id", meta["productId"].encode("utf-8")),
                ("upu", meta["UPUTrackingNumber"].encode("utf-8")),
                ("scan_ts_iso", meta["scanTimestamp"].encode("utf-8")),
                ("domainId", meta["uniqueItemId"].encode("utf-8")),
            ]

            if dry_run:
                # print a tiny dot now and then
                if (sent % 50000) == 0:
                    print(
                        f"[dryrun] would send: key={meta['uniqueItemId']} ts={meta['scanTimestamp']} file={inner}"
                    )
            else:
                producer.produce(
                    topic=topic,
                    key=key,
                    value=xml_bytes,
                    headers=headers,
                    timestamp=epoch_ms,
                    on_delivery=delivery_report,
                )

                if (sent % flush_every) == 0 and sent > 0:
                    producer.flush()

            sent += 1

        # close last zip
        if open_zip[1] is not None:
            open_zip[1].close()

    if not dry_run:
        producer.flush()
    print(
        f"[produce] Published {sent} messages for this scan index from {sorted_idx_csv}"
    )


def main():
    args = parse_args()

    # Admin setup
    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    if args.create_topics:
        ensure_topic(
            admin,
            topic=args.topic,
            partitions=args.partitions,
            rf=args.replication_factor,
            retention_ms=args.retention_ms,
        )

    # Producer
    prod = (
        None
        if args.dry_run
        else build_producer(args.bootstrap_servers, args.linger_ms, args.compression)
    )

    # Process scans 1..4 in order
    for scan_idx in (1, 2, 3, 4):
        idx_csv = build_index_for_scan(args.inputs_dir, scan_idx, args.index_dir)
        sorted_csv = sort_index_csv(idx_csv)
        produce_scan_index(
            producer=prod,
            topic=args.topic,
            sorted_idx_csv=sorted_csv,
            time_compression=args.time_compression,
            flush_every=args.flush_every,
            dry_run=args.dry_run,
        )

    print("[done] All scan indices processed.")


if __name__ == "__main__":
    main()
