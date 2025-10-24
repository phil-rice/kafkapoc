"""
generate_rm_full_dataset.py

Generates:
 - Input MPEr scan XML files (4 scans per parcel)
 - Output notification XML files (1 per parcel-event containing 0..2 notificationSegment blocks)
 - notifications_summary.csv (one row per parcel-event summarising which notifications were emitted)
 - postcode.csv (add_line1,add_line2,city,county,country,pincode)  -> one row per postcode used
 - postcode_suffix.csv (add_line1,postcode,po_suffix)             -> 1-5 suffix rows per postcode

Design follows user requirements:
 - 10% parcels: no notification contact
 - 10% parcels: only email
 - 10% parcels: only mobile
 - 70% parcels: both
 - 30% parcels: 21-digit uniqueItemId embedding a 10-digit AccountID in pattern 11[Acct10]001091111
 - 70% parcels: 11-digit uniqueItemId (online orders)
 - ProductCategory distribution: Tracked24 40%, Tracked48 40%, SD09 10%, SD13 10%
 - Events per parcel: EVDAV, EVIMC, EVGPD, ENKDN
 - Notification rules and trackedEventCode prefix -> trackedEventCode = prefix + "RS"
 - notificationDestinationType: 1=email, 2=sms
 - auxiliary contact data only placed in the first scan's auxiliaryData block (like sample)
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from zipfile import ZipFile, ZIP_DEFLATED

# ---------------------------
# Configuration & rules
# ---------------------------
EVENT_FLOW = ["EVDAV", "EVIMC", "EVGPD", "ENKDN"]

EVENT_GAPS_HOURS = {
    "EVDAV->EVIMC": (1.0, 24.0),
    "EVIMC->EVGPD": (4.0, 36.0),
    "EVGPD->ENKDN": (0.5, 24.0),
}

PRODUCT_CATEGORIES = [
    ("Tracked24", 0.40),
    ("Tracked48", 0.40),
    ("SpecialDelivery09", 0.10),
    ("SpecialDelivery13", 0.10),
]

# contact mix: (has_email, has_mobile, share)
CONTACT_MIX = [
    (False, False, 0.10),
    (True, False, 0.10),
    (False, True, 0.10),
    (True, True, 0.70),
]

ACCOUNT_RATIO = 0.30  # 30% account (21-digit with embedded 10-digit account ID)

# Notification rules: (EventCode, ProductCategory) -> (EventPrefix, email_template_exists, sms_template_exists)
RULES = {
    ("EVDAV", "Tracked24"): ("NRA", True, False),
    ("EVGPD", "Tracked24"): ("NRB", True, True),
    ("ENKDN", "Tracked24"): ("NRC", True, False),
    ("EVGPD", "Tracked48"): ("NRE", True, True),
    ("ENKDN", "Tracked48"): ("NRF", False, True),
    ("EVDAV", "SpecialDelivery09"): ("NRG", True, True),
    ("EVGPD", "SpecialDelivery09"): ("NRH", True, True),
    ("ENKDN", "SpecialDelivery09"): ("NRI", False, True),
    ("EVDAV", "SpecialDelivery13"): ("NRJ", False, True),
    ("EVGPD", "SpecialDelivery13"): ("NRK", True, True),
    ("ENKDN", "SpecialDelivery13"): ("NRL", True, True),
}

# Namespaces / headers
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

# Small default postcode list in case user doesn't provide one (no spaces)
DEFAULT_POSTCODES = [
    "EC1A1BB",
    "W1A0AX",
    "M11AE",
    "B338TH",
    "UB70BH",
    "SW1A1AA",
    "E14BH",
    "G11AA",
    "BT71NN",
    "L16XX",
    "SE11ZZ",
    "N16XY",
    "NE10AA",
    "TN12AB",
    "GU11AA",
    "CF101BH",
    "EH12AB",
    "AB101NN",
    "KA11BB",
    "DD11AA",
]

# A realistic list of UK cities and counties (sample set covering UK)
UK_LOCATIONS = [
    ("London", "Greater London"),
    ("Manchester", "Greater Manchester"),
    ("Birmingham", "West Midlands"),
    ("Leeds", "West Yorkshire"),
    ("Glasgow", "Glasgow City"),
    ("Liverpool", "Merseyside"),
    ("Bristol", "City of Bristol"),
    ("Sheffield", "South Yorkshire"),
    ("Newcastle upon Tyne", "Tyne and Wear"),
    ("Nottingham", "Nottinghamshire"),
    ("Leicester", "Leicestershire"),
    ("Edinburgh", "Midlothian"),
    ("Cardiff", "South Glamorgan"),
    ("Belfast", "County Antrim"),
    ("Swansea", "West Glamorgan"),
    ("Plymouth", "Devon"),
    ("Southampton", "Hampshire"),
    ("Norwich", "Norfolk"),
    ("Exeter", "Devon"),
    ("Brighton", "East Sussex"),
    ("Oxford", "Oxfordshire"),
    ("Cambridge", "Cambridgeshire"),
    ("York", "North Yorkshire"),
    ("Milton Keynes", "Buckinghamshire"),
    ("Coventry", "West Midlands"),
    ("Stoke-on-Trent", "Staffordshire"),
    ("Reading", "Berkshire"),
    ("Preston", "Lancashire"),
    ("Bath", "Somerset"),
    ("Durham", "County Durham"),
]

# ---------------------------
# Utilities
# ---------------------------


def choose_weighted(options):
    r = random.random()
    cumulative = 0.0
    for value, weight in options:
        cumulative += weight
        if r <= cumulative:
            return value
    return options[-1][0]


def load_postcodes(path):
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


def make_unique_item_id():
    if random.random() < ACCOUNT_RATIO:
        acct = "".join(str(random.randint(0, 9)) for _ in range(10))
        uid = f"11{acct}001091111"
        return uid, acct
    else:
        uid = "".join(str(random.randint(0, 9)) for _ in range(11))
        return uid, None


def make_upu_tracking():
    digits = "".join(str(random.randint(0, 9)) for _ in range(9))
    return f"YA{digits}GB"


def iso_date(dt):
    return dt.strftime("%Y-%m-%d")


def iso_datetime_with_tz(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + "+01:00"


def random_email(unique_item_id):
    return f"user{unique_item_id[:6]}@example.com"


def random_mobile():
    return "07" + "".join(str(random.randint(0, 9)) for _ in range(9))


def pick_contact_mix():
    r = random.random()
    cumulative = 0.0
    for has_email, has_mobile, share in CONTACT_MIX:
        cumulative += share
        if r <= cumulative:
            return has_email, has_mobile
    return CONTACT_MIX[-1][0], CONTACT_MIX[-1][1]


def random_address_line(street_words=3):
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


def make_po_suffix():
    # two uppercase letters
    return "".join(chr(random.randint(65, 90)) for _ in range(2))


# ---------------------------
# XML builders
# ---------------------------


def build_mailpiece_input_xml(
    unique_item_id, upu_tracking, postcode, barcode_creation_date, product_id
):
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
    event_code,
    scan_ts,
    transmission_ts,
    functional_location_id,
    site_id,
    device_id,
    user_id,
    include_route=False,
    route=None,
    include_aux=False,
    email=None,
    mobile=None,
):
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
        "<scanLocation><altitude>0.0</altitude><longitude>0.0</longitude><latitude>0.0</latitude></scanLocation>"
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
    parts.append(
        f"<workProcessCode>{ {'EVDAV':100,'EVIMC':200,'EVGPD':300,'ENKDN':400}[event_code] }</workProcessCode>"
    )
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


def build_notification_segment(
    event_prefix, event_code, destination_value, dest_type, base_event_ts
):
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
# Main generator
# ---------------------------


def generate_dataset(
    parcels,
    output_dir,
    batch_size=100000,
    postcode_file="",
    seed=42,
    start_date=None,
    end_date=None,
):
    random.seed(seed)

    os.makedirs(output_dir, exist_ok=True)
    inputs_dir = os.path.join(output_dir, "inputs")
    outputs_dir = os.path.join(output_dir, "outputs")
    os.makedirs(inputs_dir, exist_ok=True)
    os.makedirs(outputs_dir, exist_ok=True)

    postcodes_master = load_postcodes(postcode_file)

    now = datetime.now()
    start_dt = (
        datetime.strptime(start_date, "%Y-%m-%d")
        if start_date
        else (now - timedelta(days=365))
    )
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else now

    site_pool_size = 500
    site_ids = [str(i + 1).zfill(6) for i in range(site_pool_size)]
    func_ids = [str(i + 1) for i in range(site_pool_size)]

    # zip files for inputs & outputs (rotate by batch_size)
    in_zip_idx = 1
    out_zip_idx = 1
    in_zip = ZipFile(
        os.path.join(inputs_dir, f"mp_inputs_{in_zip_idx:04d}.zip"), "w", ZIP_DEFLATED
    )
    out_zip = ZipFile(
        os.path.join(outputs_dir, f"mp_notifications_{out_zip_idx:04d}.zip"),
        "w",
        ZIP_DEFLATED,
    )
    written_in = 0
    written_out = 0
    combined_dir = os.path.join(output_dir, "combined")
    os.makedirs(combined_dir, exist_ok=True)
    combined_zip_idx = 1
    combined_zip = ZipFile(os.path.join(combined_dir, f"test_combined_{combined_zip_idx:04d}.zip"), "w", ZIP_DEFLATED)
    written_combined = 0


    # Collections for postcode CSVs
    used_postcodes = set()
    postcode_to_location = (
        {}
    )  # postcode -> (add_line1, add_line2, city, county, country)
    postcode_suffix_rows = []  # list of (add_line1, postcode, po_suffix)

    # Summary CSV
    summary_csv_path = os.path.join(output_dir, "notifications_summary.csv")
    with open(summary_csv_path, "w", newline="", encoding="utf-8") as summary_f:
        writer = csv.writer(summary_f)
        writer.writerow(
            [
                "uniqueItemId",
                "productCategory",
                "hasEmail",
                "hasMobile",
                "event",
                "emailNotif",
                "smsNotif",
            ]
        )

        for i in range(parcels):
            unique_item_id, acct = make_unique_item_id()
            upu = make_upu_tracking()
            category = choose_weighted(PRODUCT_CATEGORIES)
            has_email, has_mobile = pick_contact_mix()
            email = random_email(unique_item_id) if has_email else None
            mobile = random_mobile() if has_mobile else None

            # postcode selection: pick from provided master list for realism
            postcode = random.choice(postcodes_master)
            used_postcodes.add(postcode)

            # map postcode -> location if not already created (create realistic city/county)
            if postcode not in postcode_to_location:
                city, county = random.choice(UK_LOCATIONS)
                add_line1 = random_address_line()
                add_line2 = (
                    ""
                    if random.random() < 0.5
                    else ("Flat " + str(random.randint(1, 20)))
                )
                country = "GB"
                postcode_to_location[postcode] = (
                    add_line1,
                    add_line2,
                    city,
                    county,
                    country,
                )

                # create 1-5 suffix rows for this postcode
                suffix_count = random.randint(1, 5)
                for _ in range(suffix_count):
                    add_line_suffix = random_address_line(street_words=2)
                    po_suffix = make_po_suffix()
                    postcode_suffix_rows.append((add_line_suffix, postcode, po_suffix))

            # product id mapping (for mailPiece)
            product_id = {
                "Tracked24": "100",
                "Tracked48": "101",
                "SpecialDelivery09": "109",
                "SpecialDelivery13": "113",
            }[category]

            # barcode creation date/time
            barcode_creation = start_dt + timedelta(
                seconds=random.randint(0, int((end_dt - start_dt).total_seconds()))
            )
            barcode_creation_date = iso_date(barcode_creation)

            # compute scan times for the 4 events
            t1 = barcode_creation + timedelta(minutes=random.randint(1, 180))
            t2 = t1 + timedelta(hours=random.uniform(*EVENT_GAPS_HOURS["EVDAV->EVIMC"]))
            t3 = t2 + timedelta(hours=random.uniform(*EVENT_GAPS_HOURS["EVIMC->EVGPD"]))
            t4 = t3 + timedelta(hours=random.uniform(*EVENT_GAPS_HOURS["EVGPD->ENKDN"]))
            scan_times = {"EVDAV": t1, "EVIMC": t2, "EVGPD": t3, "ENKDN": t4}

            # Build shared mailPiece XML block
            mp_input_xml = build_mailpiece_input_xml(
                unique_item_id, upu, postcode, barcode_creation_date, product_id
            )
            parcel_inputs = []   # collect stripped input MPE blocks
            parcel_outputs = []  # collect stripped output MPE blocks


            # Write 4 input scan XMLs (aux contact only in first scan)
            for idx, ev in enumerate(EVENT_FLOW, start=1):
                scan_dt = scan_times[ev]
                scan_ts = iso_datetime_with_tz(scan_dt)
                tx_ts = iso_datetime_with_tz(
                    scan_dt + timedelta(seconds=random.randint(5, 120))
                )
                func = random.choice(func_ids)
                site = random.choice(site_ids)
                device_id = "".join(str(random.randint(0, 9)) for _ in range(15))
                include_route = ev in ("EVGPD", "ENKDN")
                route_no = (
                    str(random.randint(100000, 9999999)) if include_route else None
                )
                include_aux = idx == 1  # only in first scan like sample

                manual_xml = build_manualscan_input_xml(
                    ev,
                    scan_ts,
                    tx_ts,
                    func,
                    site,
                    device_id,
                    "test",
                    include_route=include_route,
                    route=route_no,
                    include_aux=include_aux,
                    email=(email if include_aux else None),
                    mobile=(mobile if include_aux else None),
                )

                xml_input = INPUT_NS_HEADER + mp_input_xml + manual_xml + "</ptp:MPE>"
                parcel_inputs.append("<MPE>" + mp_input_xml + manual_xml + "</MPE>")
                in_filename = f"tracking_{unique_item_id}_{idx}.xml"
                in_zip.writestr(in_filename, xml_input)
                written_in += 1

                if written_in % batch_size == 0:
                    in_zip.close()
                    in_zip_idx += 1
                    in_zip = ZipFile(
                        os.path.join(inputs_dir, f"mp_inputs_{in_zip_idx:04d}.zip"),
                        "w",
                        ZIP_DEFLATED,
                    )

            # For each rule-eligible event, create at most one output XML bundling segments
            for ev in ("EVDAV", "EVGPD", "ENKDN"):
                segments = []
                email_notif = False
                sms_notif = False
                key = (ev, category)
                if key in RULES:
                    prefix, tpl_email, tpl_sms = RULES[key]
                    base_dt = scan_times[ev]

                    if tpl_email and has_email:
                        seg = build_notification_segment(prefix, ev, email, 1, base_dt)
                        segments.append(seg)
                        email_notif = True
                    if tpl_sms and has_mobile:
                        seg = build_notification_segment(prefix, ev, mobile, 2, base_dt)
                        segments.append(seg)
                        sms_notif = True

                writer.writerow(
                    [
                        unique_item_id,
                        category,
                        int(has_email),
                        int(has_mobile),
                        ev,
                        int(email_notif),
                        int(sms_notif),
                    ]
                )

                if segments:
                    header = build_output_notification_header(unique_item_id, upu)
                    xml_out = (
                        OUTPUT_NS_HEADER + header + "".join(segments) + "</ptp:MPE>"
                    )
                    parcel_outputs.append("<MPE>" + header + "".join(segments) + "</MPE>")
                    out_name = f"notify_{unique_item_id}_{ev}.xml"
                    out_zip.writestr(out_name, xml_out)
                    written_out += 1

                    if written_out % batch_size == 0:
                        out_zip.close()
                        out_zip_idx += 1
                        out_zip = ZipFile(
                            os.path.join(
                                outputs_dir, f"mp_notifications_{out_zip_idx:04d}.zip"
                            ),
                            "w",
                            ZIP_DEFLATED,
                        )
                # --- Build combined input-output test XML ---
            combined_content = ["<test><input>"]
            combined_content.extend(parcel_inputs)
            combined_content.append("</input><output>")
            combined_content.extend(parcel_outputs)
            combined_content.append("</output></test>")
            combined_xml = "".join(combined_content)
            combined_name = f"test_{unique_item_id}.xml"
            combined_zip.writestr(combined_name, combined_xml)
            written_combined += 1

            if written_combined % batch_size == 0:
                combined_zip.close()
                combined_zip_idx += 1
                combined_zip = ZipFile(os.path.join(combined_dir, f"test_combined_{combined_zip_idx:04d}.zip"), "w", ZIP_DEFLATED)


    # finalize zips
    in_zip.close()
    out_zip.close()
    combined_zip.close()

    # Write postcode.csv and postcode_suffix.csv
    postcode_csv_path = os.path.join(output_dir, "postcode.csv")
    postcode_suffix_csv_path = os.path.join(output_dir, "postcode_suffix.csv")

    with open(postcode_csv_path, "w", newline="", encoding="utf-8") as pc_f:
        w = csv.writer(pc_f)
        w.writerow(["add_line1", "add_line2", "city", "county", "country", "pincode"])
        for pc in sorted(used_postcodes):
            add1, add2, city, county, country = postcode_to_location[pc]
            w.writerow([add1, add2, city, county, country, pc])

    with open(postcode_suffix_csv_path, "w", newline="", encoding="utf-8") as ps_f:
        w = csv.writer(ps_f)
        w.writerow(["add_line1", "postcode", "po_suffix"])
        for add1, pc, suf in postcode_suffix_rows:
            w.writerow([add1, pc, suf])

    print(f"Completed generation.")
    print(f"Input scans written: {written_in} (zips in {inputs_dir})")
    print(f"Notifications written: {written_out} (zips in {outputs_dir})")
    print(f"Summary CSV: {summary_csv_path}")
    print(f"Postcode CSV: {postcode_csv_path}")
    print(f"Postcode suffix CSV: {postcode_suffix_csv_path}")


# ---------------------------
# CLI
# ---------------------------


def parse_args():
    p = argparse.ArgumentParser(
        description="Generate MPEr inputs, notification outputs, and postcode CSVs."
    )
    p.add_argument(
        "--parcels",
        type=int,
        default=100,
        help="Number of parcels to generate (each has 4 input scans).",
    )
    p.add_argument(
        "--output-dir", type=str, default="out_full", help="Output directory."
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=100000,
        help="How many XML files per zip before rotating.",
    )
    p.add_argument(
        "--postcode-file",
        type=str,
        default="",
        help="Optional file of real UK postcodes (one per line).",
    )
    p.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility."
    )
    p.add_argument(
        "--start-date",
        type=str,
        default="",
        help="Start date YYYY-MM-DD (default now-365d).",
    )
    p.add_argument(
        "--end-date", type=str, default="", help="End date YYYY-MM-DD (default now)."
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate_dataset(
        parcels=args.parcels,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        postcode_file=args.postcode_file,
        seed=args.seed,
        start_date=args.start_date,
        end_date=args.end_date,
    )
