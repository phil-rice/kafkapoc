"""
"""
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
from multiprocessing import Pool, cpu_count

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
    "SW1A0AA",
    "SW1A2AA",
    "WC2N5DU",
    "E16AN",
    "NW16XE",
    "W1J9HP",
    "SE17PB",
    "WC1E7HX",
    "E202ST",
    "TW62GA",
    "M25PD",
    "M160RA",
    "M113FF",
    "L39AG",
    "L40TH",
    "B11BB",
    "B66HE",
    "LS14AG",
    "LS110ES",
    "S12HH",
    "NE14ST",
    "NE18QP",
    "BS14UL",
    "BS81TH",
    "SO147FY",
    "PO13AX",
    "BN11EE",
    "PL12AA",
    "TR12EH",
    "EX11HS",
    "CB23QG",
    "OX12JD",
    "NR21RL",
    "IP11AE",
    "MK93EP",
    "RG12AG",
    "SL41NJ",
    "CT12EH",
    "YO17HH",
    "HU12AA",
    "G21DY",
    "G128QQ",
    "EH11RE",
    "EH89YL",
    "AB243FX",
    "G403RE",
    "G512XF",
    "DD14HN",
    "FK94LA",
    "IV35SS",
    "CF103NP",
    "SA13SN",
    "LL572DG",
    "SY233BY",
    "NP201GD",
    "LD16AA",
    "BT11AA",
    "BT95DL",
    "BT486SB",
    "BT601AA",
    "IM11AE",
    "GY11WG",
    "JE24WE",
    "TR210HE",
    "HS12AA",
    "KW14YT",
    "ZE10AX",
    "PA209NH",
    "KY169AJ",
    "PO301UD",
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
    ("Edinburgh", "City of Edinburgh"),
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
    ("Portsmouth", "Hampshire"),
    ("Bournemouth", "Dorset"),
    ("Guildford", "Surrey"),
    ("Chelmsford", "Essex"),
    ("Maidstone", "Kent"),
    ("Luton", "Bedfordshire"),
    ("Swindon", "Wiltshire"),
    ("Cheltenham", "Gloucestershire"),
    ("Salisbury", "Wiltshire"),
    ("Canterbury", "Kent"),
    ("Truro", "Cornwall"),
    ("Poole", "Dorset"),
    ("Hastings", "East Sussex"),
    ("Ashford", "Kent"),
    ("Derby", "Derbyshire"),
    ("Wolverhampton", "West Midlands"),
    ("Telford", "Shropshire"),
    ("Northampton", "Northamptonshire"),
    ("Peterborough", "Cambridgeshire"),
    ("Ipswich", "Suffolk"),
    ("Lincoln", "Lincolnshire"),
    ("Worcester", "Worcestershire"),
    ("Shrewsbury", "Shropshire"),
    ("Grimsby", "Lincolnshire"),
    ("Hereford", "Herefordshire"),
    ("Bradford", "West Yorkshire"),
    ("Hull", "East Riding of Yorkshire"),
    ("Sunderland", "Tyne and Wear"),
    ("Middlesbrough", "North Yorkshire"),
    ("Wakefield", "West Yorkshire"),
    ("Blackpool", "Lancashire"),
    ("Bolton", "Greater Manchester"),
    ("Doncaster", "South Yorkshire"),
    ("Chester", "Cheshire"),
    ("Carlisle", "Cumbria"),
    ("Barrow-in-Furness", "Cumbria"),
    ("Rochdale", "Greater Manchester"),
    ("Stockport", "Greater Manchester"),
    ("Aberdeen", "Aberdeenshire"),
    ("Dundee", "Angus"),
    ("Inverness", "Highland"),
    ("Stirling", "Stirling"),
    ("Perth", "Perth and Kinross"),
    ("Falkirk", "Falkirk"),
    ("Oban", "Argyll and Bute"),
    ("St Andrews", "Fife"),
    ("Newport", "Gwent"),
    ("Wrexham", "Wrexham County Borough"),
    ("Bangor", "Gwynedd"),
    ("Carmarthen", "Carmarthenshire"),
    ("Aberystwyth", "Ceredigion"),
    ("Llandudno", "Conwy County Borough"),
    ("Haverfordwest", "Pembrokeshire"),
    ("Merthyr Tydfil", "Merthyr Tydfil County Borough"),
    ("Derry/Londonderry", "County Londonderry"),
    ("Lisburn", "County Antrim"),
    ("Newry", "County Down"),
    ("Armagh", "County Armagh"),
    ("Coleraine", "County Londonderry"),
    ("Enniskillen", "County Fermanagh"),
    ("Ballymena", "County Antrim"),
    ("St Helier", "Jersey"),
    ("Douglas", "Isle of Man"),
    ("St Peter Port", "Guernsey"),
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


def random_unique_item_id():
    if random.random() < ACCOUNT_RATIO:
        acct = "".join(str(random.randint(0, 9)) for _ in range(10))
        uid = f"11{acct}001091111"
        return uid, acct
    else:
        uid = "".join(str(random.randint(0, 9)) for _ in range(11))
        return uid, None


def random_upu_tracking():
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


def random_po_suffix():
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


def generate_parcels_worker(args):
    """Worker function that generates a chunk of parcels."""
    (
        worker_id,
        start_parcel,
        end_parcel,
        output_dir,
        batch_size,
        postcode_list,
        postcode_to_location,
        func_ids,
        site_ids,
        seed,
        start_dt,
        end_dt,
    ) = args

    # Set unique seed per worker
    random.seed(seed + worker_id)

    # Create worker-specific directories
    inputs_dir = os.path.join(output_dir, f"worker_{worker_id}", "mp_inputs")
    os.makedirs(inputs_dir, exist_ok=True)

    summary_csv_path = os.path.join(
        output_dir, f"worker_{worker_id}", "notifications_summary.csv"
    )

    used_postcodes = set()
    postcode_suffix_rows = []

    in_zip_idx = 1
    in_zip = ZipFile(
        os.path.join(inputs_dir, f"mp_inputs_{in_zip_idx:04d}.zip"),
        "w",
        ZIP_DEFLATED,
    )
    written_in = 0

    with open(summary_csv_path, "w", newline="", encoding="utf-8") as sumf:
        writer = csv.writer(sumf)
        writer.writerow(
            [
                "uniqueItemId",
                "productCategory",
                "hasEmail",
                "hasMobile",
                "eventCode",
                "emailNotificationSent",
                "smsNotificationSent",
            ]
        )

        for parcel_idx in range(start_parcel, end_parcel):
            # Pick contact mix
            r = random.random()
            cum = 0.0
            has_email = False
            has_mobile = False
            for em, mob, share in CONTACT_MIX:
                cum += share
                if r < cum:
                    has_email, has_mobile = em, mob
                    break

            # Generate unique_item_id first (move this block up before email/mobile generation)
            # Decide if account parcel or online
            is_account = random.random() < ACCOUNT_RATIO
            if is_account:
                account_id = "".join(str(random.randint(0, 9)) for _ in range(10))
                unique_item_id = f"11{account_id}001091111"
            else:
                unique_item_id = "".join(str(random.randint(0, 9)) for _ in range(11))

            email = random_email(unique_item_id) if has_email else None
            mobile = random_mobile() if has_mobile else None

            # Pick product category
            r2 = random.random()
            cum2 = 0.0
            category = "Tracked24"
            for cat, share in PRODUCT_CATEGORIES:
                cum2 += share
                if r2 < cum2:
                    category = cat
                    break

            upu = str(uuid.uuid4()).replace("-", "").upper()[:13]

            postcode = random.choice(postcode_list)
            used_postcodes.add(postcode)

            # Postcode suffix generation
            if random.random() < 0.8:
                num_suffixes = random.randint(1, 5)
                for _ in range(num_suffixes):
                    add_line_suffix = random_address_line()
                    po_suffix = random_po_suffix()
                    postcode_suffix_rows.append((add_line_suffix, postcode, po_suffix))

            product_id = {
                "Tracked24": "100",
                "Tracked48": "101",
                "SpecialDelivery09": "109",
                "SpecialDelivery13": "113",
            }[category]

            barcode_creation = start_dt + timedelta(
                seconds=random.randint(0, int((end_dt - start_dt).total_seconds()))
            )
            barcode_creation_date = iso_date(barcode_creation)

            t1 = barcode_creation + timedelta(minutes=random.randint(1, 180))
            t2 = t1 + timedelta(hours=random.uniform(*EVENT_GAPS_HOURS["EVDAV->EVIMC"]))
            t3 = t2 + timedelta(hours=random.uniform(*EVENT_GAPS_HOURS["EVIMC->EVGPD"]))
            t4 = t3 + timedelta(hours=random.uniform(*EVENT_GAPS_HOURS["EVGPD->ENKDN"]))
            scan_times = {"EVDAV": t1, "EVIMC": t2, "EVGPD": t3, "ENKDN": t4}

            mp_input_xml = build_mailpiece_input_xml(
                unique_item_id, upu, postcode, barcode_creation_date, product_id
            )
            parcel_inputs = []

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
                include_aux = idx == 1

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

    in_zip.close()

    return (worker_id, used_postcodes, postcode_suffix_rows, written_in)


def generate_dataset(
    parcels=100,
    output_dir="out_full",
    batch_size=100000,
    postcode_file="",
    seed=42,
    start_date="",
    end_date="",
    num_workers=None,
):
    """Main function that coordinates multiprocessing."""
    if num_workers is None:
        num_workers = cpu_count()

    print(f"Using {num_workers} worker processes")

    os.makedirs(output_dir, exist_ok=True)

    # Load or use default postcodes
    if postcode_file and os.path.isfile(postcode_file):
        with open(postcode_file, "r", encoding="utf-8") as pf:
            postcode_list = [
                line.strip().replace(" ", "") for line in pf if line.strip()
            ]
        print(f"Loaded {len(postcode_list)} postcodes from {postcode_file}")
    else:
        postcode_list = DEFAULT_POSTCODES
        print(f"Using {len(postcode_list)} default postcodes")

    # Build postcode to location mapping
    postcode_to_location = {}
    for pc in postcode_list:
        add1 = random_address_line()
        add2 = random_address_line()
        city, county = random.choice(UK_LOCATIONS)
        country = "United Kingdom"
        postcode_to_location[pc] = (add1, add2, city, county, country)

    # Generate function and site IDs
    func_ids = [f"F{i:03d}" for i in range(1, 51)]
    site_ids = [f"SITE{i:04d}" for i in range(1, 101)]

    # Parse dates
    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_dt = datetime.now() - timedelta(days=365)

    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = datetime.now()

    print(f"Generating {parcels} parcels from {start_dt.date()} to {end_dt.date()}")

    # Split work among workers
    parcels_per_worker = parcels // num_workers
    worker_args = []

    for worker_id in range(num_workers):
        start_parcel = worker_id * parcels_per_worker
        if worker_id == num_workers - 1:
            end_parcel = parcels  # Last worker gets remainder
        else:
            end_parcel = (worker_id + 1) * parcels_per_worker

        worker_args.append(
            (
                worker_id,
                start_parcel,
                end_parcel,
                output_dir,
                batch_size,
                postcode_list,
                postcode_to_location,
                func_ids,
                site_ids,
                seed,
                start_dt,
                end_dt,
            )
        )

    # Run workers in parallel
    print(f"Starting parallel generation...")
    with Pool(num_workers) as pool:
        results = pool.map(generate_parcels_worker, worker_args)

    # Aggregate results
    all_used_postcodes = set()
    all_postcode_suffix_rows = []
    total_written = 0

    for worker_id, used_postcodes, postcode_suffix_rows, written_in in results:
        all_used_postcodes.update(used_postcodes)
        all_postcode_suffix_rows.extend(postcode_suffix_rows)
        total_written += written_in
        print(f"Worker {worker_id} completed: {written_in} files")

    # Write consolidated postcode CSVs
    postcode_csv_path = os.path.join(output_dir, "postcode.csv")
    postcode_suffix_csv_path = os.path.join(output_dir, "postcode_suffix.csv")

    with open(postcode_csv_path, "w", newline="", encoding="utf-8") as pc_f:
        w = csv.writer(pc_f)
        w.writerow(["add_line1", "add_line2", "city", "county", "country", "pincode"])
        for pc in sorted(all_used_postcodes):
            add1, add2, city, county, country = postcode_to_location[pc]
            w.writerow([add1, add2, city, county, country, pc])

    with open(postcode_suffix_csv_path, "w", newline="", encoding="utf-8") as ps_f:
        w = csv.writer(ps_f)
        w.writerow(["add_line1", "postcode", "po_suffix"])
        for add1, pc, suf in all_postcode_suffix_rows:
            w.writerow([add1, pc, suf])

    print(f"Completed generation.")
    print(f"Total input scans written: {total_written}")
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
    p.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker processes (default: CPU count).",
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
        num_workers=args.workers,
    )
