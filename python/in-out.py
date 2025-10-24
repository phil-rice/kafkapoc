#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_rm_pairs.py

Generates paired input/output XML data:

INPUT (MPEr scan XMLs)
- 4 scan events per parcel: EVDAV, EVIMC, EVGPD, ENKDN
- uniqueItemId: 30% are 21-digit (embed 10-digit AccountID as 11[Acct10]001091111), 70% are 11-digit
- UK postcodes from provided file (fallback to a small baked-in list for testing)
- Contact mix per parcel:
  - 10%: none
  - 10%: only email
  - 10%: only mobile
  - 70%: both
- ProductCategory distribution:
  - Tracked24 (40%), Tracked48 (40%), SpecialDelivery09 (10%), SpecialDelivery13 (10%)

OUTPUT (Notification XMLs)
- For each rule-eligible event (EVDAV, EVGPD, ENKDN), create ONE output XML per parcel+event
- Inside it, include one <notificationSegment> per channel that qualifies (email and/or sms)
- trackedEventCode = EventPrefix + "RS"
- notificationDestinationType: 1=email, 2=sms
- Event timestamp = originating event timestamp + 60s
- Primary uniqueIDType=4; Linked uniqueIDType=1 (UPUTrackingNumber)

Also writes a summary CSV: notifications_summary.csv (counts & flags).

Author: Generated for user
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from zipfile import ZipFile, ZIP_DEFLATED

# ---------------------------
# Constants & Rules
# ---------------------------

# Event flow per parcel (4 scans)
EVENT_FLOW = ["EVDAV", "EVIMC", "EVGPD", "ENKDN"]

# Time gaps (hours) between successive scans: (min, max)
EVENT_GAPS_HOURS = {
    "EVDAV->EVIMC": (1.0, 24.0),
    "EVIMC->EVGPD": (4.0, 36.0),
    "EVGPD->ENKDN": (0.5, 24.0),
}

# ProductCategory distribution
PRODUCT_CATEGORIES = [
    ("Tracked24",        0.40),
    ("Tracked48",        0.40),
    ("SpecialDelivery09",0.10),
    ("SpecialDelivery13",0.10),
]

# Contact mix per parcel
# (has_email, has_mobile, share)
CONTACT_MIX = [
    (False, False, 0.10),  # 10% none
    (True,  False, 0.10),  # 10% only email
    (False, True,  0.10),  # 10% only mobile
    (True,  True,  0.70),  # 70% both
]

# Account vs non-account uniqueItemId mix
ACCOUNT_RATIO = 0.30  # 30% 21-digit with embedded AccountID

# Rules table: (EventCode, ProductCategory) -> (EventPrefix, email_template_exists, sms_template_exists)
RULES = {
    ("EVDAV", "Tracked24"):         ("NRA", True,  False),
    ("EVGPD", "Tracked24"):         ("NRB", True,  True),
    ("ENKDN", "Tracked24"):         ("NRC", True,  False),

    ("EVGPD", "Tracked48"):         ("NRE", True,  True),
    ("ENKDN", "Tracked48"):         ("NRF", False, True),

    ("EVDAV", "SpecialDelivery09"): ("NRG", True,  True),
    ("EVGPD", "SpecialDelivery09"): ("NRH", True,  True),
    ("ENKDN", "SpecialDelivery09"): ("NRI", False, True),

    ("EVDAV", "SpecialDelivery13"): ("NRJ", False, True),
    ("EVGPD", "SpecialDelivery13"): ("NRK", True,  True),
    ("ENKDN", "SpecialDelivery13"): ("NRL", True,  True),
}

# Namespaces for INPUT and OUTPUT XML
INPUT_NS_HEADER = (
    '<ptp:MPE xmlns:dt="http://www.royalmailgroup.com/cm/rmDatatypes/V1" '
    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
    'xmlns:ptp="http://www.royalmailgroup.com/cm/ptpMailPiece/V1">'
)

OUTPUT_NS_HEADER = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<ptp:MPE xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
    'xmlns:ptp="http://www.royalmailgroup.com/cm/ptpMailPiece/V1.3" '
    'xsi:schemaLocation="http://www.royalmailgroup.com/cm/ptpMailPiece/V1.3 ptpMailPiece.xsd">'
)

DEFAULT_POSTCODES = [
    "EC1A1BB","W1A0AX","M11AE","B338TH","UB70BH","SW1A1AA","E14BH","G11AA","BT71NN","L16XX",
    "SE11ZZ","N16XY","NE10AA","TN12AB","GU11AA","CF101BH","EH12AB","AB101NN","KA11BB","DD11AA"
]

# ---------------------------
# Helpers
# ---------------------------

def choose_weighted(options_with_weights):
    r = random.random()
    cumulative = 0.0
    for value, weight in options_with_weights:
        cumulative += weight
        if r <= cumulative:
            return value
    return options_with_weights[-1][0]

def load_postcodes(path):
    pcs = []
    if path and os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip().replace(" ", "").upper()
                if s:
                    pcs.append(s)
    return pcs if pcs else DEFAULT_POSTCODES[:]

def make_unique_item_id():
    """Return (uniqueItemId, embedded_account_id or None)."""
    if random.random() < ACCOUNT_RATIO:
        # 21-digit with embedded 10-digit account id
        acct = "".join(str(random.randint(0, 9)) for _ in range(10))
        uid = f"11{acct}001091111"
        return uid, acct
    else:
        # 11-digit non-account
        uid = "".join(str(random.randint(0, 9)) for _ in range(11))
        return uid, None

def make_upu_tracking():
    digits = "".join(str(random.randint(0, 9)) for _ in range(9))
    return f"YA{digits}GB"

def iso_date(dt):
    return dt.strftime("%Y-%m-%d")

def iso_datetime_with_tz(dt):
    # Keep +01:00 to mirror samples
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + "+01:00"

def random_email(unique_item_id):
    return f"user{unique_item_id[:6]}@example.com"

def random_mobile():
    # simple UK-like 11-digit mobile starting with 07
    return "07" + "".join(str(random.randint(0, 9)) for _ in range(9))

def pick_contact_mix():
    r = random.random()
    cumulative = 0.0
    for has_email, has_mobile, share in CONTACT_MIX:
        cumulative += share
        if r <= cumulative:
            return has_email, has_mobile
    return CONTACT_MIX[-1][0], CONTACT_MIX[-1][1]

def build_mailpiece_input_xml(unique_item_id, upu_tracking, postcode, barcode_creation_date, product_id):
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
    event_code, scan_ts, transmission_ts, functional_location_id, site_id,
    device_id, user_id, include_route=False, route=None,
    include_aux=False, email=None, mobile=None
):
    parts = ["<manualScan>"]
    if include_route and route:
        parts.append(f"<routeOrWalkNumber>{route}</routeOrWalkNumber>")
    parts.append(f"<messageId>{uuid.uuid4()}</messageId>")
    parts.append(f"<trackEventId>{random.randint(10**15, 10**16-1)}</trackEventId>")
    parts.append(f"<deviceId>{device_id}</deviceId>")
    parts.append(f"<userId>{user_id}</userId>")
    parts.append("<RMGLocation>")
    parts.append(f"<functionalLocationId>{functional_location_id}</functionalLocationId>")
    parts.append(f"<siteId>{site_id}</siteId>")
    parts.append("</RMGLocation>")
    parts.append("<scanLocation><altitude>0.0</altitude><longitude>0.0</longitude><latitude>0.0</latitude></scanLocation>")
    parts.append(f"<trackedEventCode>{event_code}</trackedEventCode>")
    parts.append(f"<scanTimestamp>{scan_ts}</scanTimestamp>")
    parts.append(f"<eventTimestamp>{scan_ts}</eventTimestamp>")
    parts.append(f"<transmissionTimestamp>{transmission_ts}</transmissionTimestamp>")
    parts.append(f"<transmissionCompleteTimestamp>{transmission_ts}</transmissionCompleteTimestamp>")
    parts.append(f"<eventReceivedTimestamp>{transmission_ts}</eventReceivedTimestamp>")
    parts.append(f"<eventReason>{random.choice([11,22,33,44])}</eventReason>")
    parts.append("<manualScanIndicator>false</manualScanIndicator>")
    parts.append(f"<workProcessCode>{ {'EVDAV':100,'EVIMC':200,'EVGPD':300,'ENKDN':400}[event_code] }</workProcessCode>")
    if include_aux:
        parts.append("<auxiliaryData>")
        if email:
            parts.append("<data><name>RECIPIENT_EMAILID</name><value>")
            parts.append(email)
            parts.append("</value></data>")
        if mobile:
            parts.append("<data><name>RECIPIENT_MOBILENO</name><value>")
            parts.append(mobile)
            parts.append("</value></data>")
        parts.append("</auxiliaryData>")
    parts.append("</manualScan>")
    return "".join(parts)

def build_output_notification_header(unique_item_id, upu_tracking):
    return (
        "<mailPiece>"
        "<mailPieceIdentifier>"
        "<primaryIdentifier>"
        f"<uniqueIDString>{unique_item_id}</uniqueIDString>"
        "<uniqueIDType>4</uniqueIDType>"
        "</primaryIdentifier>"
        "<linkedIdentifier>"
        f"<uniqueIDString>{upu_tracking}</uniqueIDString>"
        "<uniqueIDType>1</uniqueIDType>"
        "</linkedIdentifier>"
        "</mailPieceIdentifier>"
        "</mailPiece>"
    )

def build_notification_segment(event_prefix, event_code, destination_value, dest_type, base_event_ts):
    # dest_type: 1=email, 2=sms
    # eventTimestamp = base event + 60 seconds
    ts = iso_datetime_with_tz(base_event_ts + timedelta(seconds=60))
    tracked = f"{event_prefix}RS"
    msg_id = str(random.randint(10**12, 10**13 - 1))
    return (
        "<notificationSegment>"
        f"<trackedEventCode>{tracked}</trackedEventCode>"
        f"<eventTimestamp>{ts}</eventTimestamp>"
        f"<notificationDestination>{destination_value}</notificationDestination>"
        f"<notificationDestinationType>{dest_type}</notificationDestinationType>"
        f"<notificationMessageID>{msg_id}</notificationMessageID>"
        f"<originatingTrackedEventCode>{event_code}</originatingTrackedEventCode>"
        "<notificationRecipientType>R</notificationRecipientType>"
        "</notificationSegment>"
    )

# ---------------------------
# Main generation
# ---------------------------

def generate_pairs(
    parcels: int,
    output_dir: str,
    batch_size: int = 100000,
    postcode_file: str = "",
    seed: int = 42,
    start_date: str = "",
    end_date: str = ""
):
    random.seed(seed)

    os.makedirs(output_dir, exist_ok=True)
    in_dir = os.path.join(output_dir, "inputs")
    out_dir = os.path.join(output_dir, "outputs")
    os.makedirs(in_dir, exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)

    postcodes = load_postcodes(postcode_file)

    # Time range
    now = datetime.now()
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else (now - timedelta(days=365))
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d") if end_date   else now

    # Site pools
    site_pool_size = 500
    site_ids = [str(i+1).zfill(6) for i in range(site_pool_size)]
    func_ids = [str(i+1) for i in range(site_pool_size)]

    # Zips
    in_zip_index, out_zip_index = 1, 1
    in_zip = ZipFile(os.path.join(in_dir, f"mp_inputs_{in_zip_index:04d}.zip"), "w", ZIP_DEFLATED)
    out_zip = ZipFile(os.path.join(out_dir, f"mp_notifications_{out_zip_index:04d}.zip"), "w", ZIP_DEFLATED)

    written_in, written_out = 0, 0

    # Summary
    summary_path = os.path.join(output_dir, "notifications_summary.csv")
    with open(summary_path, "w", newline="", encoding="utf-8") as csvf:
        writer = csv.writer(csvf)
        writer.writerow([
            "uniqueItemId","category","hasEmail","hasMobile",
            "event","emailNotif","smsNotif"
        ])

        for p in range(parcels):
            # IDs + category + contacts
            unique_item_id, acct = make_unique_item_id()
            upu = make_upu_tracking()
            category = choose_weighted(PRODUCT_CATEGORIES)
            has_email, has_mobile = pick_contact_mix()
            email = random_email(unique_item_id) if has_email else None
            mobile = random_mobile() if has_mobile else None

            postcode = random.choice(postcodes)
            product_id = {
                "Tracked24": "100",
                "Tracked48": "101",
                "SpecialDelivery09": "109",
                "SpecialDelivery13": "113",
            }[category]

            # Base time per parcel
            barcode_creation = start_dt + timedelta(seconds=random.randint(0, int((end_dt - start_dt).total_seconds())))
            # First scan a bit after barcode creation
            scan_times = {}
            t1 = barcode_creation + timedelta(minutes=random.randint(1, 180))
            scan_times["EVDAV"] = t1
            # gaps
            g1 = random.uniform(*EVENT_GAPS_HOURS["EVDAV->EVIMC"])
            scan_times["EVIMC"] = scan_times["EVDAV"] + timedelta(hours=g1)
            g2 = random.uniform(*EVENT_GAPS_HOURS["EVIMC->EVGPD"])
            scan_times["EVGPD"] = scan_times["EVIMC"] + timedelta(hours=g2)
            g3 = random.uniform(*EVENT_GAPS_HOURS["EVGPD->ENKDN"])
            scan_times["ENKDN"] = scan_times["EVGPD"] + timedelta(hours=g3)

            # build one mailPiece for input reuse
            mp_input_xml = build_mailpiece_input_xml(
                unique_item_id, upu, postcode, iso_date(barcode_creation), product_id
            )

            # For each event in flow -> write INPUT scan xml
            for idx, ev in enumerate(EVENT_FLOW, start=1):
                scan_ts = iso_datetime_with_tz(scan_times[ev])
                tx_ts = iso_datetime_with_tz(scan_times[ev] + timedelta(seconds=random.randint(5,120)))
                func = random.choice(func_ids)
                site = random.choice(site_ids)
                dev = "".join(str(random.randint(0,9)) for _ in range(15))
                include_route = (ev in ("EVGPD","ENKDN"))
                route_no = str(random.randint(100000, 9999999)) if include_route else None
                include_aux = (idx == 1)  # like sample: notification details only in scan 1

                manual = build_manualscan_input_xml(
                    event_code=ev,
                    scan_ts=scan_ts,
                    transmission_ts=tx_ts,
                    functional_location_id=func,
                    site_id=site,
                    device_id=dev,
                    user_id="test",
                    include_route=include_route,
                    route=route_no,
                    include_aux=include_aux,
                    email=email if include_aux else None,
                    mobile=mobile if include_aux else None
                )

                xml_input = INPUT_NS_HEADER + mp_input_xml + manual + "</ptp:MPE>"
                in_name = f"tracking_{unique_item_id}_{idx}.xml"
                in_zip.writestr(in_name, xml_input)
                written_in += 1

                if written_in % batch_size == 0:
                    in_zip.close()
                    in_zip_index += 1
                    in_zip = ZipFile(os.path.join(in_dir, f"mp_inputs_{in_zip_index:04d}.zip"), "w", ZIP_DEFLATED)

            # For each RULE-eligible event -> produce one OUTPUT xml with 0..2 notificationSegment(s)
            for ev in ("EVDAV", "EVGPD", "ENKDN"):
                email_notif = False
                sms_notif = False
                segments = []

                rule_key = (ev, category)
                if rule_key in RULES:
                    prefix, has_email_tpl, has_sms_tpl = RULES[rule_key]
                    base_ts = scan_times[ev]

                    # email?
                    if has_email_tpl and has_email:
                        seg = build_notification_segment(
                            event_prefix=prefix,
                            event_code=ev,
                            destination_value=email,
                            dest_type=1,
                            base_event_ts=base_ts
                        )
                        segments.append(seg)
                        email_notif = True

                    # sms?
                    if has_sms_tpl and has_mobile:
                        seg = build_notification_segment(
                            event_prefix=prefix,
                            event_code=ev,
                            destination_value=mobile,
                            dest_type=2,
                            base_event_ts=base_ts
                        )
                        segments.append(seg)
                        sms_notif = True

                # record summary row per event
                writer.writerow([
                    unique_item_id, category, int(has_email), int(has_mobile),
                    ev, int(email_notif), int(sms_notif)
                ])

                # If any segments were generated, write one output XML containing them
                if segments:
                    header = build_output_notification_header(unique_item_id, upu)
                    xml_out = OUTPUT_NS_HEADER + header + "".join(segments) + "</ptp:MPE>"
                    out_name = f"notify_{unique_item_id}_{ev}.xml"
                    out_zip.writestr(out_name, xml_out)
                    written_out += 1

                    if written_out % batch_size == 0:
                        out_zip.close()
                        out_zip_index += 1
                        out_zip = ZipFile(os.path.join(out_dir, f"mp_notifications_{out_zip_index:04d}.zip"), "w", ZIP_DEFLATED)

    # Close zips
    in_zip.close()
    out_zip.close()

    print(f"Done. Input scans written: {written_in}. Notification files written: {written_out}.")
    print(f"Inputs:   {in_dir}")
    print(f"Outputs:  {out_dir}")
    print(f"Summary:  {summary_path}")

# ---------------------------
# CLI
# ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Generate MPEr scan XML inputs and notification XML outputs (paired).")
    ap.add_argument("--parcels", type=int, default=100, help="Number of parcels (each has 4 input scans).")
    ap.add_argument("--output-dir", type=str, default="out_pairs", help="Base output directory.")
    ap.add_argument("--batch-size", type=int, default=100000, help="How many XML files per zip.")
    ap.add_argument("--postcode-file", type=str, default="", help="Path to file of real UK postcodes (one per line).")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    ap.add_argument("--start-date", type=str, default="", help="Start date YYYY-MM-DD (default: now-365d).")
    ap.add_argument("--end-date", type=str, default="", help="End date YYYY-MM-DD (default: now).")
    args = ap.parse_args()

    generate_pairs(
        parcels=args.parcels,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        postcode_file=args.postcode_file,
        seed=args.seed,
        start_date=args.start_date,
        end_date=args.end_date
    )

if __name__ == "__main__":
    main()
