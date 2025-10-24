import argparse
import csv
import os
import random
import string
import uuid
from datetime import datetime, timedelta
from zipfile import ZipFile, ZIP_DEFLATED

# ---------------------------
# Configurable mappings / defaults
# ---------------------------
EVENT_SEQUENCE = [
    # (trackedEventCode, workProcessCode, eventReason, description, min_gap_hours, max_gap_hours)
    ("EVDAC", 100, 11, "Accepted at depot", 0.01, 2),   # minutes to a few hours after barcode creation
    ("EVIMC", 200, 22, "In transit / sorting", 1, 48),  # hours to days
    ("EVGPD", 300, 33, "Out for delivery", 4, 36),      # hours to next day
    ("EVKDN", 400, 44, "Delivered (or neighbour)", 0.5, 24),  # delivery attempt / delivered
]

# Default small sample postcode list (used only if user doesn't provide a postcode file)
DEFAULT_POSTCODES = [
    "EC1A1BB","W1A0AX","M11AE","B338TH","UB70BH","SW1A1AA","E14BH","G11AA","BT71NN","L16XX",
    "SE11ZZ","N16XY","NE10AA","TN12AB","GU11AA","CF101BH","EH12AB","AB101NN","KA11BB","DD11AA"
]

NAMESPACE_HEADER = (
    '<ptp:MPE xmlns:dt="http://www.royalmailgroup.com/cm/rmDatatypes/V1" '
    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
    'xmlns:ptp="http://www.royalmailgroup.com/cm/ptpMailPiece/V1">'
)

# ---------------------------
# Utility functions
# ---------------------------

def load_postcodes(path):
    """Load postcodes from a newline or simple CSV file. Returns list of cleaned postcodes."""
    pcs = []
    if not path or not os.path.isfile(path):
        return DEFAULT_POSTCODES.copy()
    with open(path, newline='', encoding='utf-8') as f:
        # Accept simple newline list or single-column CSV (robust)
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            val = row[0].strip()
            if not val:
                continue
            # Normalize: remove spaces, uppercase (sample has no internal space for DPS)
            pcs.append(val.replace(" ", "").upper())
    if not pcs:
        return DEFAULT_POSTCODES.copy()
    return pcs

def make_unique_item_id(is_account_user):
    """Return either 21-digit account ID or 11-digit online order ID (as string)."""
    if is_account_user:
        # 21 digits
        return "".join(str(random.randint(0,9)) for _ in range(21))
    else:
        # 11 digits
        return "".join(str(random.randint(0,9)) for _ in range(11))

def make_upu_tracking():
    """Make a sample UPUTrackingNumber similar to examples: YA + 9 digits + GB"""
    digits = "".join(str(random.randint(0,9)) for _ in range(9))
    return f"YA{digits}GB"

def make_message_id():
    """UUID-like messageId but keep alphabetic+dash style similar to sample"""
    return str(uuid.uuid4())

def make_track_event_id(counter):
    """Make a numeric-ish trackEventId: use a large int derived from counter and random digits."""
    random_tail = "".join(str(random.randint(0,9)) for _ in range(5))
    return f"{counter}{random_tail}"

def zero_pad_site_id(n, width=6):
    return str(n).zfill(width)

def iso_date(dt):
    return dt.strftime("%Y-%m-%d")

def iso_datetime_with_tz(dt):
    # Keep timezone as +01:00 in samples; for generality assume UTC+1 (BST) if within UK summer months,
    # but to keep deterministic we will use +01:00 always (matches samples).
    return dt.strftime("%Y-%m-%dT%H:%M:%S")+ "+01:00"

def generate_auxiliary_data(email, phone):
    # Build auxiliaryData section as in sample (only for first scan)
    return (
        "<auxiliaryData>"
        f"<data><name>RECIPIENT_EMAILID</name><value>{email}</value></data>"
        f"<data><name>RECIPIENT_MOBILENO</name><value>{phone}</value></data>"
        "</auxiliaryData>"
    )

def random_email(uid):
    # synthetic email using uid
    return f"user{uid[:6]}@example.com"

def random_phone():
    # UK-ish mobile 10 digits in sample; produce 10 digits
    return "".join(str(random.randint(0,9)) for _ in range(10))

# ---------------------------
# XML assembly
# ---------------------------
def build_mailpiece_xml(unique_item_id, mail_piece_weight, barcode_creation_date, product_id, upu_tracking, postcode):
    # keep fields consistent with sample
    # note: postcode in sample includes no space (UB70BH). We'll use that format.
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
        f"<mailPieceWeight>{mail_piece_weight}</mailPieceWeight>"
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

def build_manualscan_xml(scan_index, message_id, track_event_id, device_id, user_id,
                         functional_location_id, site_id, tracked_event_code,
                         scan_ts, transmission_ts, event_reason, work_process_code,
                         include_auxiliary=False, email=None, phone=None,
                         include_route=False, route_or_walk_number=None,
                         include_neighbour=False):
    # scanLocation lat/long zero as sample
    parts = ["<manualScan>"]
    if include_route and route_or_walk_number:
        parts.append(f"<routeOrWalkNumber>{route_or_walk_number}</routeOrWalkNumber>")
    parts.append(f"<messageId>{message_id}</messageId>")
    parts.append(f"<trackEventId>{track_event_id}</trackEventId>")
    parts.append(f"<deviceId>{device_id}</deviceId>")
    parts.append(f"<userId>{user_id}</userId>")
    parts.append("<RMGLocation>")
    parts.append(f"<functionalLocationId>{functional_location_id}</functionalLocationId>")
    parts.append(f"<siteId>{site_id}</siteId>")
    parts.append("</RMGLocation>")
    parts.append("<scanLocation>")
    parts.append("<altitude>0.0</altitude>")
    parts.append("<longitude>0.0</longitude>")
    parts.append("<latitude>0.0</latitude>")
    parts.append("</scanLocation>")
    parts.append(f"<trackedEventCode>{tracked_event_code}</trackedEventCode>")
    parts.append(f"<scanTimestamp>{scan_ts}</scanTimestamp>")
    parts.append(f"<eventTimestamp>{scan_ts}</eventTimestamp>")
    parts.append(f"<transmissionTimestamp>{transmission_ts}</transmissionTimestamp>")
    # sample had transmissionCompleteTimestamp 14s after transmissionTimestamp
    # and eventReceivedTimestamp equal to transmissionCompleteTimestamp
    # We'll compute them as needed by caller and pass in scan_ts/transmission_ts
    # For simplicity, compute complete and recv here as +14s
    # To keep times ISO with +01:00 suffix, we accept strings
    # parse transmission_ts to dt? To avoid heavy parsing we will append computed suffix using simple approach
    # We'll create a dt version in caller and pass formatted strings for all timestamps.
    # But for now, append placeholders and let caller set correct timestamps with iso strings.
    # To keep code simple we'll assume caller passed transmission_ts and scan_ts as strings
    # and create transmissionComplete and eventReceived by adding 14 seconds to transmission_ts is complex here,
    # so caller will pass them as part of the xml content if needed. To approximate, we'll reuse transmission_ts.
    parts.append(f"<transmissionCompleteTimestamp>{transmission_ts}</transmissionCompleteTimestamp>")
    parts.append(f"<eventReceivedTimestamp>{transmission_ts}</eventReceivedTimestamp>")
    parts.append(f"<eventReason>{event_reason}</eventReason>")
    parts.append("<manualScanIndicator>false</manualScanIndicator>")
    parts.append(f"<workProcessCode>{work_process_code}</workProcessCode>")
    if include_auxiliary:
        parts.append(generate_auxiliary_data(email, phone))
    if include_neighbour:
        parts.append("<neighbourName>TEST</neighbourName>")
        parts.append("<neighbourAddress><buildingName>1</buildingName></neighbourAddress>")
    parts.append("</manualScan>")
    return "".join(parts)

def build_full_xml_string(mailpiece_xml, manualscan_xml):
    return (
        NAMESPACE_HEADER +
        mailpiece_xml +
        manualscan_xml +
        "</ptp:MPE>"
    )

# ---------------------------
# Main generator
# ---------------------------
def generate(parcels, postcode_list, out_dir, batch_size=100000,
             account_ratio=0.30, seed=None, start_date=None, end_date=None,
             sample_mode=False):
    """
    parcels: number of parcels to generate (each produces 4 events)
    postcode_list: list of postcode strings (no spaces)
    out_dir: where to put zip files
    batch_size: how many XML files per zip
    account_ratio: fraction of parcels that get 21-digit account IDs
    seed: random seed for reproducibility
    start_date, end_date: datetime objects bounding barcode creation and first scan
    sample_mode: if True, produce smaller, quicker random site pool
    """
    if seed is not None:
        random.seed(seed)

    os.makedirs(out_dir, exist_ok=True)

    # site pool
    site_pool_size = 500 if not sample_mode else 20
    site_ids = [zero_pad_site_id(i+1) for i in range(site_pool_size)]
    functional_ids = [str(i+1) for i in range(site_pool_size)]

    # counters
    total_files = parcels * len(EVENT_SEQUENCE)
    file_counter = 0
    zip_index = 1
    current_zip_path = os.path.join(out_dir, f"rm_events_part_{zip_index:04d}.zip")
    current_zip = ZipFile(current_zip_path, mode='w', compression=ZIP_DEFLATED)

    track_event_counter = 1000000  # base counter for unique trackEventId construction

    # set default date range if none provided: last year
    if start_date is None or end_date is None:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)

    for parcel_index in range(1, parcels+1):
        # parcel-level constants
        is_acc = random.random() < account_ratio
        unique_item_id = make_unique_item_id(is_acc)
        upu_tracking = make_upu_tracking()
        mail_piece_weight = random.choice([1,2,3])   # sample weight choices
        barcode_creation = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        barcode_creation_date = iso_date(barcode_creation)
        product_id = random.choice([100,101,102])

        # choose postcode weighted uniformly for now, optionally you can add geographic weighting
        postcode = random.choice(postcode_list)

        # choose a consistent userId per parcel? We'll vary per scan
        base_user = "test"

        # Build mailPiece once for this parcel
        mailpiece_xml = build_mailpiece_xml(unique_item_id,
                                            mail_piece_weight,
                                            barcode_creation_date,
                                            product_id,
                                            upu_tracking,
                                            postcode)

        # simulate scan timestamps for 4 events: start from barcode_creation + small offset
        scan_time = barcode_creation + timedelta(minutes=random.randint(1, 180))
        # For each event in sequence produce scan xml
        for scan_index, seq in enumerate(EVENT_SEQUENCE, start=1):
            trackedEventCode, workProcessCode, eventReason, desc, min_gap_h, max_gap_h = seq

            # site/device selection (different per event to mimic different offices)
            site_choice = random.choice(list(zip(functional_ids, site_ids)))
            functional_location_id, site_id = site_choice

            # device id: mimic sample lengths (some digits); create consistent-looking device id
            device_id = "".join(str(random.randint(0,9)) for _ in range(15))

            # message & track ids
            message_id = make_message_id()
            track_event_id = make_track_event_id(track_event_counter)
            track_event_counter += 1

            # timestamps
            scan_ts_dt = scan_time
            # transmission timestamp a random 5-120 seconds after event
            transmission_dt = scan_ts_dt + timedelta(seconds=random.randint(5, 120))
            # transmissionComplete and eventReceived will be transmission_dt + 14s (we'll reuse transmission string)
            scan_ts = iso_datetime_with_tz(scan_ts_dt)
            transmission_ts = iso_datetime_with_tz(transmission_dt)

            # optional fields
            include_route = (scan_index in (3,4))  # sample had routeOrWalkNumber in scan 3 & 4
            route_or_walk_number = str(random.randint(100000, 9999999)) if include_route else None
            include_neighbour = (scan_index == 4 and random.random() < 0.15)  # 15% delivered to neighbour
            
            # ----- Notification variation rules -----
            include_auxiliary = (scan_index == 1)
            email = phone = None

            if include_auxiliary:
                # Determine notification configuration scenario (percentages of parcels)
                notif_rand = random.random()
                if notif_rand < 0.10:
                    # 10% → no notification details at all
                    email = None
                    phone = None
                elif notif_rand < 0.20:
                    # next 10% → only email
                    email = random_email(unique_item_id)
                    phone = None
                elif notif_rand < 0.30:
                    # next 10% → only mobile
                    email = None
                    phone = random_phone()
                else:
                    # remaining 70% → both
                    email = random_email(unique_item_id)
                    phone = random_phone()

                # --- Special “notification rules vs input data” scenarios ---
                # randomly inject some inconsistencies between notification rules & input data
                # (just to simulate the described edge cases)
                rule_rand = random.random()
                if rule_rand < 0.05:
                    # notification rule expects mobile, but input has only email
                    email = random_email(unique_item_id)
                    phone = None
                elif 0.05 <= rule_rand < 0.10:
                    # notification rule configured but input has no contact details
                    email = None
                    phone = None
                elif 0.10 <= rule_rand < 0.15:
                    # notification configured (mobile rule) but input data has both — valid case
                    email = random_email(unique_item_id)
                    phone = random_phone()

            manualscan_xml = build_manualscan_xml(
                scan_index=scan_index,
                message_id=message_id,
                track_event_id=track_event_id,
                device_id=device_id,
                user_id=base_user,
                functional_location_id=functional_location_id,
                site_id=site_id,
                tracked_event_code=trackedEventCode,
                scan_ts=scan_ts,
                transmission_ts=transmission_ts,
                event_reason=eventReason,
                work_process_code=workProcessCode,
                include_auxiliary=include_auxiliary,
                email=email,
                phone=phone,
                include_route=include_route,
                route_or_walk_number=route_or_walk_number,
                include_neighbour=include_neighbour
            )

            xml_str = build_full_xml_string(mailpiece_xml, manualscan_xml)

            # filename convention
            filename = f"tracking_{unique_item_id}_{scan_index}.xml"

            # write into current zip
            current_zip.writestr(filename, xml_str)
            file_counter += 1

            # advance scan_time by a realistic gap (random between min_gap_h and max_gap_h)
            gap_hours = random.uniform(min_gap_h, max_gap_h)
            scan_time = scan_time + timedelta(hours=gap_hours)

            # rotate zip if batch_size reached
            if file_counter % batch_size == 0:
                current_zip.close()
                print(f"Wrote {file_counter} files -> {current_zip_path}")
                zip_index += 1
                current_zip_path = os.path.join(out_dir, f"rm_events_part_{zip_index:04d}.zip")
                current_zip = ZipFile(current_zip_path, mode='w', compression=ZIP_DEFLATED)

        # end of scans for this parcel

    # close last zip if not closed
    if current_zip.fp is not None:
        current_zip.close()
        print(f"Finalized {current_zip_path}")
    print(f"Generation complete. Total files written: {file_counter}")

# ---------------------------
# CLI
# ---------------------------
# def parse_args():
#     p = argparse.ArgumentParser(description="Generate dummy Royal Mail manualScan XML events.")
#     p.add_argument("--parcels", type=int, default=100, help="Number of parcels to generate (each produces 4 events).")
#     p.add_argument("--batch-size", type=int, default=100000, help="How many XML files per zip file.")
#     p.add_argument("--output-dir", type=str, default="out", help="Output directory for zip batches.")
#     p.add_argument("--postcode-file", type=str, default="", help="Path to newline/CSV file containing real UK postcodes (no header required).")
#     p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
#     p.add_argument("--account-ratio", type=float, default=0.30, help="Fraction of parcels that use 21-digit account IDs (0..1).")
#     p.add_argument("--start-date", type=str, default="", help="Start date (YYYY-MM-DD). Default: 365 days before now.")
#     p.add_argument("--end-date", type=str, default="", help="End date (YYYY-MM-DD). Default: now.")
#     p.add_argument("--sample", dest="sample", action="store_true", help="Run in sample mode (smaller site pool, faster).")
#     return p.parse_args()

# def parse_date_or_none(s):
#     if not s:
#         return None
#     return datetime.strptime(s, "%Y-%m-%d")

if __name__ == "__main__":
    # args = parse_args()
    # postcode_list = load_postcodes(args.postcode_file)
    # start_dt = parse_date_or_none(args.start_date)
    # end_dt = parse_date_or_none(args.end_date)
    # print("Configuration:")
    # print("  parcels:", args.parcels)
    # print("  batch_size:", args.batch_size)
    # print("  output_dir:", args.output_dir)
    # print("  postcode_file:", args.postcode_file or "(builtin sample used)")
    # print("  seed:", args.seed)
    # print("  account_ratio:", args.account_ratio)
    # print("  sample_mode:", args.sample)
    # generate(
    #     parcels=args.parcels,
    #     postcode_list=postcode_list,
    #     out_dir=args.output_dir,
    #     batch_size=args.batch_size,
    #     account_ratio=args.account_ratio,
    #     seed=args.seed,
    #     start_date=start_dt,
    #     end_date=end_dt,
    #     sample_mode=args.sample
    # )
