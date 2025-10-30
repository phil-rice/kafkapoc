#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
produce_inputs_with_delays_fast_bench.py

Optimized high-throughput XML replay to Kafka/Redpanda with live benchmarking.
"""

import argparse
import os
import sys
import time
import zipfile
import sqlite3
import threading
from datetime import datetime, timezone
from collections import OrderedDict
from typing import Dict

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


# --- XML fast parsing ---
def _extract_between_bytes(b: bytes, start_tag: bytes, end_tag: bytes) -> str:
    i = b.find(start_tag)
    if i == -1:
        return ""
    j = b.find(end_tag, i + len(start_tag))
    if j == -1:
        return ""
    return b[i + len(start_tag) : j].decode("utf-8", errors="ignore").strip()


def parse_xml_metadata_fast(xml_bytes: bytes) -> Dict[str, str]:
    uniqueItemId = _extract_between_bytes(
        xml_bytes, b"<uniqueItemId>", b"</uniqueItemId>"
    )
    upu = _extract_between_bytes(
        xml_bytes, b"<UPUTrackingNumber>", b"</UPUTrackingNumber>"
    )
    product_id = _extract_between_bytes(xml_bytes, b"<productId>", b"</productId>")
    event_code = _extract_between_bytes(
        xml_bytes, b"<trackedEventCode>", b"</trackedEventCode>"
    )
    scan_ts_iso = _extract_between_bytes(
        xml_bytes, b"<scanTimestamp>", b"</scanTimestamp>"
    )
    return {
        "uniqueItemId": uniqueItemId,
        "UPUTrackingNumber": upu,
        "productId": product_id,
        "eventCode": event_code,
        "scanTimestamp": scan_ts_iso,
    }


def iso_to_epoch_ms(iso_ts: str) -> int:
    dt = datetime.fromisoformat(iso_ts)
    return int(dt.timestamp() * 1000)


# --- Zip cache ---
class ZipCache:
    def __init__(self, max_open=32):
        self.cache = OrderedDict()
        self.max_open = max_open

    def get(self, path):
        if path in self.cache:
            zf = self.cache.pop(path)
            self.cache[path] = zf
            return zf
        zf = zipfile.ZipFile(path, "r")
        self.cache[path] = zf
        if len(self.cache) > self.max_open:
            _, old = self.cache.popitem(last=False)
            try:
                old.close()
            except:
                pass
        return zf

    def close_all(self):
        for z in self.cache.values():
            try:
                z.close()
            except:
                pass
        self.cache.clear()


# --- Kafka helpers ---
def ensure_topic(
    admin: AdminClient, topic: str, partitions: int, rf: int, retention_ms: int
):
    md = admin.list_topics(timeout=10)
    if topic in md.topics and not md.topics[topic].error:
        print(f"[topic] '{topic}' already exists.")
        return
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
    except Exception as e:
        print(f"[topic] Create result: {e}")


def build_producer(bootstrap: str, linger_ms: int, compression: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": linger_ms,
        "compression.type": compression,
        "batch.num.messages": 20000,
        "queue.buffering.max.messages": 2097152,
        "queue.buffering.max.kbytes": 1048576,
        "message.max.bytes": 10 * 1024 * 1024,
        "max.in.flight.requests.per.connection": 5,
    }
    return Producer(conf)


# --- CLI ---
def parse_args():
    p = argparse.ArgumentParser(description="Optimized replay with live benchmark.")
    p.add_argument("--inputs-dir", required=True)
    p.add_argument("--bootstrap-servers", default="localhost:8080")
    p.add_argument("--topic", default="mper-input-events")
    p.add_argument("--create-topics", action="store_true")
    p.add_argument("--partitions", type=int, default=12)
    p.add_argument("--retention-ms", type=int, default=7 * 24 * 3600 * 1000)
    p.add_argument("--replication-factor", type=int, default=1)
    p.add_argument("--linger-ms", type=int, default=200)
    p.add_argument("--compression", default="zstd")
    p.add_argument("--time-compression", type=float, default=3600.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--zip-cache-size", type=int, default=32)
    p.add_argument("--max-outstanding", type=int, default=200000)
    p.add_argument("--min-sleep-ms", type=float, default=1.0)
    p.add_argument("--index-db-dir", default="./.replay_index_sqlite")
    return p.parse_args()


# --- Index building (SQLite) ---
def list_input_files(inputs_dir: str):
    """Return list of XML files (flat) or fallback to zip files if present."""
    xml_files = sorted(
        os.path.join(inputs_dir, f)
        for f in os.listdir(inputs_dir)
        if f.lower().endswith(".xml")
    )
    if xml_files:
        return xml_files

    # fallback if someone gives zip-based inputs
    return sorted(
        os.path.join(inputs_dir, f)
        for f in os.listdir(inputs_dir)
        if f.startswith("mp_inputs_") and f.endswith(".zip")
    )


def extract_scan_index_from_name(inner_name: str) -> int:
    try:
        return int(inner_name.split("_")[-1].split(".")[0])
    except Exception:
        return -1


def build_index_for_scan_sqlite(
    inputs_dir: str, scan_index: int, index_dir: str
) -> str:
    """Build a SQLite index for fast replay. Supports both ZIP and flat XML folders."""
    os.makedirs(index_dir, exist_ok=True)
    db_path = os.path.join(index_dir, f"scan{scan_index}_index.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path, timeout=30)
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE idx(
            epoch_ms INTEGER,
            iso_ts TEXT,
            zip_path TEXT,
            inner_name TEXT,
            uniqueItemId TEXT,
            upu TEXT,
            event_code TEXT,
            product_id TEXT
        )"""
    )
    cur.execute("PRAGMA synchronous=OFF")
    cur.execute("PRAGMA journal_mode=MEMORY")

    insert_sql = "INSERT INTO idx VALUES (?,?,?,?,?,?,?,?)"
    batch = []
    BATCH_SZ = 5000
    count = 0

    input_files = list_input_files(inputs_dir)
    for path in input_files:
        if path.lower().endswith(".zip"):
            with zipfile.ZipFile(path, "r") as zf:
                for inner in zf.namelist():
                    if not inner.endswith(".xml"):
                        continue
                    si = extract_scan_index_from_name(inner)
                    if si != scan_index:
                        continue
                    xml_bytes = zf.read(inner)
                    meta = parse_xml_metadata_fast(xml_bytes)
                    if not meta["scanTimestamp"]:
                        continue
                    epoch_ms = iso_to_epoch_ms(meta["scanTimestamp"])
                    batch.append(
                        (
                            epoch_ms,
                            meta["scanTimestamp"],
                            path,
                            inner,
                            meta["uniqueItemId"],
                            meta["UPUTrackingNumber"],
                            meta["eventCode"],
                            meta["productId"],
                        )
                    )
        else:
            # Plain XML file
            si = extract_scan_index_from_name(os.path.basename(path))
            if si != scan_index:
                continue
            try:
                with open(path, "rb") as f:
                    xml_bytes = f.read()
            except Exception as e:
                print(f"[warn] could not read {path}: {e}")
                continue
            meta = parse_xml_metadata_fast(xml_bytes)
            if not meta["scanTimestamp"]:
                continue
            epoch_ms = iso_to_epoch_ms(meta["scanTimestamp"])
            batch.append(
                (
                    epoch_ms,
                    meta["scanTimestamp"],
                    path,
                    os.path.basename(path),
                    meta["uniqueItemId"],
                    meta["UPUTrackingNumber"],
                    meta["eventCode"],
                    meta["productId"],
                )
            )

        if len(batch) >= BATCH_SZ:
            cur.executemany(insert_sql, batch)
            conn.commit()
            count += len(batch)
            batch.clear()

    if batch:
        cur.executemany(insert_sql, batch)
        conn.commit()
        count += len(batch)

    cur.execute("CREATE INDEX idx_epoch ON idx(epoch_ms)")
    conn.commit()
    conn.close()

    print(f"[index] scan {scan_index}: indexed {count} events -> {db_path}")
    return db_path


# --- Live benchmarking thread ---
class StatsTracker:
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
                print(
                    f"[stats] {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}  "
                    f"rate={rate:8.0f} msg/s  avg={avg:8.0f} msg/s  "
                    f"total={self.total_count:,}  out={self.outstanding:,}"
                )


# --- Produce ---
def produce_scan_index_parallel(
    bootstrap,
    topic,
    db_path,
    num_workers=4,
    linger_ms=200,
    compression="zstd",
    max_outstanding=200000,
):
    import queue
    from concurrent.futures import ThreadPoolExecutor

    # Preload DB rows into memory (optional for performance)
    conn = sqlite3.connect(db_path, timeout=30)
    cur = conn.cursor()
    cur.execute(
        "SELECT epoch_ms, iso_ts, zip_path, inner_name, uniqueItemId, upu, event_code, product_id "
        "FROM idx ORDER BY epoch_ms ASC"
    )
    rows = cur.fetchall()
    conn.close()
    total = len(rows)
    if total == 0:
        print(f"[parallel] No messages found in {db_path}")
        return

    # Split into equal chunks for workers
    chunks = [rows[i::num_workers] for i in range(num_workers)]
    tracker = StatsTracker()

    def worker(i, rows):
        producer = build_producer(bootstrap, linger_ms, compression)
        outstanding = 0

        def delivery(err, msg):
            nonlocal outstanding
            outstanding -= 1
            tracker.set_outstanding(outstanding)

        for r in rows:
            epoch_ms, iso_ts, zpath, inner, uid, upu, event_code, product_id = r
            try:
                with open(zpath, "rb") as f:
                    xml_bytes = f.read()
            except:
                continue
            headers = [
                ("event_code", (event_code or "").encode()),
                ("scan_ts_iso", (iso_ts or "").encode()),
                ("domainId", (uid or "").encode()),
            ]
            key = (uid or "").encode()
            while True:
                try:
                    producer.produce(
                        topic=topic,
                        key=key,
                        value=xml_bytes,
                        headers=headers,
                        timestamp=epoch_ms,
                        on_delivery=delivery,
                    )
                    outstanding += 1
                    tracker.add(1)
                    break
                except BufferError:
                    producer.poll(0.05)

            if outstanding > max_outstanding:
                while outstanding > max_outstanding // 2:
                    producer.poll(0.1)

            producer.poll(0)

        while outstanding > 0:
            producer.poll(0.1)
        producer.flush()

    print(f"[parallel] Starting {num_workers} workers for {total:,} messages")
    start = time.time()
    with ThreadPoolExecutor(max_workers=num_workers) as ex:
        for i, chunk in enumerate(chunks):
            ex.submit(worker, i, chunk)
    tracker.stop()
    elapsed = time.time() - start
    print(
        f"[summary] Parallel total {total:,} messages in {elapsed:.1f}s  ({total/elapsed:,.0f} msg/s)"
    )

# --- main ---
def main():
    args = parse_args()
    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    if args.create_topics:
        ensure_topic(
            admin,
            args.topic,
            args.partitions,
            args.replication_factor,
            args.retention_ms,
        )
    prod = (
        None
        if args.dry_run
        else build_producer(args.bootstrap_servers, args.linger_ms, args.compression)
    )
    zip_cache = ZipCache(args.zip_cache_size)
    for scan_idx in (1, 2, 3, 4):
        print(f"[main] Processing scan index {scan_idx}")
        db_path = build_index_for_scan_sqlite(args.inputs_dir, scan_idx, args.index_db_dir)
        produce_scan_index_parallel(
            bootstrap=args.bootstrap_servers,
            topic=args.topic,
            db_path=db_path,
            num_workers=8,  # tweak this based on CPU cores
            linger_ms=args.linger_ms,
            compression=args.compression,
            max_outstanding=args.max_outstanding,
        )
    zip_cache.close_all()


if __name__ == "__main__":
    main()
