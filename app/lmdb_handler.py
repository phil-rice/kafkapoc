"""
LMDB database handler for sorted event storage
"""

import lmdb
import os
from typing import Iterator, Tuple

from .models import Event, DB_GLOBAL, DB_SCAN1, DB_SCAN2, DB_SCAN3, DB_SCAN4
from .utils import be_u64, ms


class LmdbBuffer:
    """LMDB-based event buffer with multi-DB support"""

    def __init__(self, path: str, map_size: int = 32 * 1024 * 1024 * 1024):
        """
        Initialize LMDB buffer

        Args:
            path: Directory path for LMDB database
            map_size: Maximum size of database in bytes (default 32GB)
        """
        os.makedirs(path, exist_ok=True)
        self.env = lmdb.open(
            path,
            map_size=map_size,
            max_dbs=8,
            subdir=True,
            lock=True,
            create=True,
            writemap=False,
        )
        self.dbi_global = self.env.open_db(DB_GLOBAL, create=True)
        self.dbi_s1 = self.env.open_db(DB_SCAN1, create=True)
        self.dbi_s2 = self.env.open_db(DB_SCAN2, create=True)
        self.dbi_s3 = self.env.open_db(DB_SCAN3, create=True)
        self.dbi_s4 = self.env.open_db(DB_SCAN4, create=True)

    def put_event(self, e: Event, seq: int):
        """
        Store event in LMDB (both global and scan-specific DBs)

        Key structure: [prefix][sequence][timestamp] for insertion order

        Args:
            e: Event to store
            seq: Sequence number for uniqueness
        """
        tms = ms(e.event_time)
        # Changed key structure: sequence first, then timestamp (for insertion order)
        k_global = b"G" + be_u64(seq) + be_u64(tms)
        with self.env.begin(write=True) as txn:
            txn.put(k_global, e.xml_value, db=self.dbi_global)
            if e.scan_no == 1:
                k_scan = b"S1" + be_u64(seq) + be_u64(tms)
                txn.put(k_scan, e.xml_value, db=self.dbi_s1)
            elif e.scan_no == 2:
                k_scan = b"S2" + be_u64(seq) + be_u64(tms)
                txn.put(k_scan, e.xml_value, db=self.dbi_s2)
            elif e.scan_no == 3:
                k_scan = b"S3" + be_u64(seq) + be_u64(tms)
                txn.put(k_scan, e.xml_value, db=self.dbi_s3)
            elif e.scan_no == 4:
                k_scan = b"S4" + be_u64(seq) + be_u64(tms)
                txn.put(k_scan, e.xml_value, db=self.dbi_s4)

    def iter_keys(self, dbi_name: bytes) -> Iterator[Tuple[bytes, bytes]]:
        """
        Iterate over keys in specified database

        Args:
            dbi_name: Database name (DB_GLOBAL, DB_SCAN1, DB_SCAN2, DB_SCAN3, or DB_SCAN4)

        Yields:
            Tuples of (key, value)
        """
        dbi = {
            DB_GLOBAL: self.dbi_global,
            DB_SCAN1: self.dbi_s1,
            DB_SCAN2: self.dbi_s2,
            DB_SCAN3: self.dbi_s3,
            DB_SCAN4: self.dbi_s4,
        }[dbi_name]
        with self.env.begin() as txn:
            with txn.cursor(dbi) as cur:
                if cur.first():
                    yield cur.key(), cur.value()
                    while cur.next():
                        yield cur.key(), cur.value()

    def delete(self, dbi_name: bytes, key: bytes):
        """
        Delete key from specified database

        Args:
            dbi_name: Database name
            key: Key to delete
        """
        dbi = {
            DB_GLOBAL: self.dbi_global,
            DB_SCAN1: self.dbi_s1,
            DB_SCAN2: self.dbi_s2,
            DB_SCAN3: self.dbi_s3,
            DB_SCAN4: self.dbi_s4,
        }[dbi_name]
        with self.env.begin(write=True) as txn:
            txn.delete(key, db=dbi)
