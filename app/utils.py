"""
Utility functions for event generation
"""

import json
import os
import random
import struct
import uuid
from datetime import datetime, timezone
from typing import Optional, Tuple

from .models import ACCOUNT_RATIO, CONTACT_MIX, DEFAULT_POSTCODES, INPUT_NS_HEADER

# Binary encoding
BE_U64 = ">Q"  # big-endian unsigned long long


def ms(dt: datetime) -> int:
    """Convert datetime to milliseconds since epoch"""
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def be_u64(n: int) -> bytes:
    """Encode integer as big-endian 64-bit unsigned"""
    return struct.pack(BE_U64, n)


def choose_weighted(options):
    """Choose from weighted options"""
    r = random.random()
    cumulative = 0.0
    for value, weight in options:
        cumulative += weight
        if r <= cumulative:
            return value
    return options[-1][0]


def load_postcodes(path: str):
    """Load postcodes from file or return defaults"""
    if path and os.path.isfile(path):
        pcs = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip().replace(" ", "").upper()
                if s:
                    pcs.append(s)
        if pcs:
            return pcs
    return DEFAULT_POSTCODES[:]


def make_unique_item_id() -> Tuple[str, Optional[str]]:
    """Generate unique item ID (30% account-based, 70% online)"""
    if random.random() < ACCOUNT_RATIO:
        acct = "".join(str(random.randint(0, 9)) for _ in range(10))
        uid = f"11{acct}001091111"
        return uid, acct
    else:
        uid = "".join(str(random.randint(0, 9)) for _ in range(11))
        return uid, None


def make_upu_tracking() -> str:
    """Generate UPU tracking number"""
    digits = "".join(str(random.randint(0, 9)) for _ in range(9))
    return f"YA{digits}GB"


def iso_date(dt: datetime) -> str:
    """Format date as ISO string"""
    return dt.strftime("%Y-%m-%d")


def iso_datetime_with_tz(dt: datetime) -> str:
    """Format datetime with timezone"""
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + "+01:00"


def random_email(unique_item_id: str) -> str:
    """Generate random email"""
    return f"user{unique_item_id[:6]}@example.com"


def random_mobile() -> str:
    """Generate random UK mobile number"""
    return "07" + "".join(str(random.randint(0, 9)) for _ in range(9))


def pick_contact_mix() -> Tuple[bool, bool]:
    """Pick contact mix (has_email, has_mobile)"""
    r = random.random()
    cumulative = 0.0
    for has_email, has_mobile, share in CONTACT_MIX:
        cumulative += share
        if r <= cumulative:
            return has_email, has_mobile
    return CONTACT_MIX[-1][0], CONTACT_MIX[-1][1]


def random_address_line(street_words: int = 3) -> str:
    """Generate random UK address line"""
    streets = [
        "High",
        "Station",
        "Church",
        "Green",
        "Park",
        "Mill",
        "Grove",
        "Oak",
        "King",
        "Queen",
        "Victoria",
        "George",
        "Bridge",
    ]
    name = " ".join(random.choice(streets) for _ in range(street_words))
    number = str(random.randint(1, 200))
    return f"{number} {name} St"


def make_po_suffix() -> str:
    """Generate postcode suffix (two uppercase letters)"""
    return "".join(chr(random.randint(65, 90)) for _ in range(2))


# XML building functions
def build_mailpiece_input_xml(
    unique_item_id: str,
    upu_tracking: str,
    postcode: str,
    barcode_creation_date: str,
    product_id: str,
) -> str:
    """Build mailPiece XML fragment"""
    return (
        "<mailPiece>"
        "<mailPieceBarcode>"
        "<royalMailSegment>"
        "<UPUCountry>JGB </UPUCountry>"
        "<informationCode>8</informationCode>"
        "<versionId>2</versionId>"
        "<mailItemFormatCode>15</mailItemFormatCode>"
        "<mailClassCode>F</mailClassCode>"
        "<mailTypeCode>A</mailTypeCode>"
        "</royalMailSegment>"
        "<channelSegment>"
        f"<uniqueItemId>{unique_item_id}</uniqueItemId>"
        "<mailPieceWeight>1</mailPieceWeight>"
        "<weightCode>1</weightCode>"
        "<pricePaid>123</pricePaid>"
        f"<barcodeCreationDate>{barcode_creation_date}</barcodeCreationDate>"
        f"<productId>{product_id}</productId>"
        f"<UPUTrackingNumber>{upu_tracking}</UPUTrackingNumber>"
        "<address><buildingNumber>1</buildingNumber></address>"
        "<destinationPostcodeDPS>"
        f"<postcode>{postcode}</postcode>"
        "</destinationPostcodeDPS>"
        "<destinationCountry>GB </destinationCountry>"
        "<requiredAtDeliveryCode>S</requiredAtDeliveryCode>"
        "</channelSegment>"
        "</mailPieceBarcode>"
        "</mailPiece>"
    )


def build_manualscan_input_xml(
    event_code: str,
    scan_ts: str,
    transmission_ts: str,
    functional_location_id: str,
    site_id: str,
    device_id: str,
    user_id: str,
    include_route: bool = False,
    route: Optional[str] = None,
    include_aux: bool = False,
    email: Optional[str] = None,
    mobile: Optional[str] = None,
) -> str:
    """Build manualScan XML fragment"""
    parts = ["<manualScan>"]
    if include_route and route:
        parts.append(f"<routeOrWalkNumber>{route}</routeOrWalkNumber>")
    parts.append(f"<messageId>{uuid.uuid4()}</messageId>")
    parts.append(f"<trackEventId>{random.randint(10**15, 10**16-1)}</trackEventId>")
    parts.append(f"<deviceId>{device_id}</deviceId>")
    parts.append(f"<userId>{user_id}</userId>")
    parts.append("<RMGLocation>")
    parts.append(
        f"<functionalLocationId>{functional_location_id}</functionalLocationId>"
    )
    parts.append(f"<siteId>{site_id}</siteId>")
    parts.append("</RMGLocation>")
    parts.append(
        "<scanLocation><altitude>0.0</altitude><longitude>0.0</longitude>"
        "<latitude>0.0</latitude></scanLocation>"
    )
    parts.append(f"<trackedEventCode>{event_code}</trackedEventCode>")
    parts.append(f"<scanTimestamp>{scan_ts}</scanTimestamp>")
    parts.append(f"<eventTimestamp>{scan_ts}</eventTimestamp>")
    parts.append(f"<transmissionTimestamp>{transmission_ts}</transmissionTimestamp>")
    parts.append(
        f"<transmissionCompleteTimestamp>{transmission_ts}</transmissionCompleteTimestamp>"
    )
    parts.append(f"<eventReceivedTimestamp>{transmission_ts}</eventReceivedTimestamp>")
    parts.append(f"<eventReason>{random.choice([11,22,33,44])}</eventReason>")
    parts.append("<manualScanIndicator>false</manualScanIndicator>")
    work_process_code = {"EVDAV": 100, "EVIMC": 200, "EVGPD": 300, "ENKDN": 400}[
        event_code
    ]
    parts.append(f"<workProcessCode>{work_process_code}</workProcessCode>")
    if include_aux:
        parts.append("<auxiliaryData>")
        if email:
            parts.append(
                f"<data><name>RECIPIENT_EMAILID</name><value>{email}</value></data>"
            )
        if mobile:
            parts.append(
                f"<data><name>RECIPIENT_MOBILENO</name><value>{mobile}</value></data>"
            )
        parts.append("</auxiliaryData>")
    parts.append("</manualScan>")
    return "".join(parts)


# XML metadata extraction and packing
METADATA_DELIMITER = b"|||META|||"


def _extract_between_bytes(b: bytes, start_tag: bytes, end_tag: bytes) -> str:
    """Fast XML tag extraction without full parser"""
    i = b.find(start_tag)
    if i == -1:
        return ""
    j = b.find(end_tag, i + len(start_tag))
    if j == -1:
        return ""
    return b[i + len(start_tag) : j].decode("utf-8", errors="ignore").strip()


def extract_metadata_for_kafka(xml_bytes: bytes) -> dict:
    """Extract metadata needed for Kafka headers and key"""
    return {
        "uniqueItemId": _extract_between_bytes(
            xml_bytes, b"<uniqueItemId>", b"</uniqueItemId>"
        ),
        "eventCode": _extract_between_bytes(
            xml_bytes, b"<trackedEventCode>", b"</trackedEventCode>"
        ),
        "scanTimestamp": _extract_between_bytes(
            xml_bytes, b"<scanTimestamp>", b"</scanTimestamp>"
        ),
    }


def pack_with_metadata(xml_bytes: bytes, metadata: dict) -> bytes:
    """Pack XML with metadata for efficient storage and retrieval"""
    meta_json = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    return meta_json + METADATA_DELIMITER + xml_bytes


def unpack_metadata(packed_bytes: bytes) -> Tuple[dict, bytes]:
    """Unpack metadata and XML (O(1) operation, no XML parsing)"""
    idx = packed_bytes.find(METADATA_DELIMITER)
    if idx == -1:
        # Fallback for old format (backward compatibility)
        meta = extract_metadata_for_kafka(packed_bytes)
        return meta, packed_bytes

    meta_json = packed_bytes[:idx]
    xml_bytes = packed_bytes[idx + len(METADATA_DELIMITER) :]
    metadata = json.loads(meta_json.decode("utf-8"))
    return metadata, xml_bytes
