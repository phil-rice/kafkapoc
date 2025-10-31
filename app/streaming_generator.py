"""
Streaming event generator - generates events on-demand without storage
"""

import csv
import os
import random
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Iterator, Tuple, List

from .logger import logger
from .models import EVENT_FLOW, PRODUCT_CATEGORIES, UK_LOCATIONS
from .config_store import GenerationConfig
from .config import (
    AZURE_CONNECTION_STRING,
    AZURE_CONTAINER_NAME,
    AZURE_BLOB_PREFIX,
    AZURE_AVAILABLE,
)
from .utils import (
    build_mailpiece_input_xml,
    build_manualscan_input_xml,
    choose_weighted,
    extract_metadata_for_kafka,
    iso_date,
    iso_datetime_with_tz,
    make_unique_item_id,
    make_upu_tracking,
    make_po_suffix,
    pack_with_metadata,
    pick_contact_mix,
    random_address_line,
    random_email,
    random_mobile,
)
from .models import INPUT_NS_HEADER, EVENT_GAPS_HOURS

if AZURE_AVAILABLE:
    from azure.storage.blob import BlobServiceClient


def gen_scan_times(
    creation: datetime, rnd: random.Random
) -> Tuple[datetime, datetime, datetime, datetime]:
    """Generate scan times using exact source logic"""
    t1 = creation + timedelta(minutes=rnd.randint(1, 180))
    t2 = t1 + timedelta(hours=rnd.uniform(*EVENT_GAPS_HOURS["EVDAV->EVIMC"]))
    t3 = t2 + timedelta(hours=rnd.uniform(*EVENT_GAPS_HOURS["EVIMC->EVGPD"]))
    t4 = t3 + timedelta(hours=rnd.uniform(*EVENT_GAPS_HOURS["EVGPD->ENKDN"]))
    return t1, t2, t3, t4


def build_full_xml(
    unique_item_id: str,
    upu: str,
    postcode: str,
    product_id: str,
    event_code: str,
    scan_dt: datetime,
    func_loc: str,
    site_id: str,
    device_id: str,
    user_id: str,
    include_route: bool,
    route: str,
    include_aux: bool,
    email: str,
    mobile: str,
) -> bytes:
    """Build complete XML matching source generation logic"""
    barcode_creation_date = iso_date(scan_dt)
    scan_ts = iso_datetime_with_tz(scan_dt)
    tx_ts = iso_datetime_with_tz(scan_dt + timedelta(seconds=random.randint(5, 120)))

    header = build_mailpiece_input_xml(
        unique_item_id, upu, postcode, barcode_creation_date, product_id
    )
    manual = build_manualscan_input_xml(
        event_code=event_code,
        scan_ts=scan_ts,
        transmission_ts=tx_ts,
        functional_location_id=func_loc,
        site_id=site_id,
        device_id=device_id,
        user_id=user_id,
        include_route=include_route,
        route=route,
        include_aux=include_aux,
        email=email if include_aux else None,
        mobile=mobile if include_aux else None,
    )
    xml = INPUT_NS_HEADER + header + manual + "</ptp:MPE>"
    return xml.encode("utf-8")


def generate_magic_parcel_scan(scan_no: int) -> Tuple[bytes, dict, int]:
    """
    Generate magic parcel scan event

    Args:
        scan_no: Scan number (1, 2, 3, or 4)

    Returns:
        Tuple of (xml_bytes, metadata, timestamp_ms)
    """
    magic_ts = datetime.fromtimestamp(0, tz=timezone.utc)
    event_code = EVENT_FLOW[scan_no - 1]

    xml = build_full_xml(
        "028792170110069554608",
        "YA000000001GB",
        "AA11AA",
        "100",
        event_code,
        magic_ts,
        "000456",
        "000123",
        "DVC-1001",
        "USR-2002",
        False,
        None,
        (scan_no == 1),
        "phil@example.com",
        "07123456789",
    )

    metadata = extract_metadata_for_kafka(xml)
    timestamp_ms = int(magic_ts.timestamp() * 1000)

    return xml, metadata, timestamp_ms


def stream_scan_events(
    config: GenerationConfig,
    scan_no: int,
) -> Iterator[Tuple[bytes, dict, int]]:
    """
    Generate and yield events for a specific scan on-the-fly

    Args:
        config: Generation configuration
        scan_no: Scan number (1, 2, 3, or 4)

    Yields:
        Tuples of (xml_bytes, metadata, timestamp_ms)
    """
    # First, yield magic parcel
    logger.info(f"Generating magic parcel scan {scan_no}")
    yield generate_magic_parcel_scan(scan_no)

    # Initialize random generator with seed
    random.seed(config.seed)

    # Get event code for this scan
    event_code = EVENT_FLOW[scan_no - 1]
    scan_idx = scan_no  # 1-based index

    logger.info(f"Streaming {config.parcels:,} parcels for scan {scan_no}")

    # Generate events for all parcels
    for i in range(config.parcels):
        # Generate parcel data (same logic as original generator)
        unique_item_id, acct = make_unique_item_id()
        upu = make_upu_tracking()
        category = choose_weighted(PRODUCT_CATEGORIES)
        has_email, has_mobile = pick_contact_mix()
        email = random_email(unique_item_id) if has_email else None
        mobile = random_mobile() if has_mobile else None

        postcode = random.choice(config.postcodes)

        # Product ID mapping
        product_id = {
            "Tracked24": "100",
            "Tracked48": "101",
            "SpecialDelivery09": "109",
            "SpecialDelivery13": "113",
        }[category]

        # Barcode creation time
        total_sec = int((config.end_date - config.start_date).total_seconds())
        barcode_creation = config.start_date + timedelta(
            seconds=random.randrange(total_sec)
        )

        # Compute all scan times for this parcel
        t1, t2, t3, t4 = gen_scan_times(barcode_creation, random)
        scan_times = [t1, t2, t3, t4]
        scan_dt = scan_times[scan_no - 1]  # Get this scan's time

        # Build XML for this scan
        func = random.choice(config.func_ids)
        site = random.choice(config.site_ids)
        device_id = "".join(str(random.randint(0, 9)) for _ in range(15))
        include_route = event_code in ("EVGPD", "ENKDN")
        route_no = str(random.randint(100000, 9999999)) if include_route else None
        include_aux = scan_idx == 1  # Only in first scan

        xml = build_full_xml(
            unique_item_id,
            upu,
            postcode,
            product_id,
            event_code,
            scan_dt,
            func,
            site,
            device_id,
            "test",
            include_route,
            route_no,
            include_aux,
            email,
            mobile,
        )

        # Extract metadata
        metadata = extract_metadata_for_kafka(xml)
        timestamp_ms = int(scan_dt.timestamp() * 1000)

        yield xml, metadata, timestamp_ms

        # Progress logging
        if (i + 1) % 10000 == 0:
            logger.info(
                f"Generated {i+1:,}/{config.parcels:,} events for scan {scan_no}"
            )

    logger.success(f"Completed streaming {config.parcels:,} events for scan {scan_no}")


def _upload_to_azure_blob(
    connection_string: str,
    container_name: str,
    local_file_path: str,
    blob_name: str = None,
):
    """Upload a file to Azure Blob Storage"""
    if not AZURE_AVAILABLE:
        logger.warning("Azure Storage Blob SDK not installed. Skipping CSV upload.")
        return

    if blob_name is None:
        blob_name = os.path.basename(local_file_path)

    logger.info(f"Uploading to Azure Blob: {blob_name}")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )

    with open(local_file_path, "rb") as data:
        # Optimized upload with parallel chunks
        blob_client.upload_blob(
            data,
            overwrite=True,
            max_concurrency=8,  # Upload 8 blocks concurrently
            blob_type="BlockBlob",
            timeout=300,  # 5 minute timeout
        )

    logger.success(f"Uploaded to Azure Blob: {blob_name}")


def generate_and_upload_csvs(config: GenerationConfig) -> dict:
    """
    Generate CSV files for parcels and upload to Azure Blob Storage

    Args:
        config: Generation configuration

    Returns:
        Dictionary with CSV generation results
    """
    logger.info(f"Generating CSV files for {config.parcels:,} parcels")

    # Initialize random generator with seed
    random.seed(config.seed)

    # Collections for CSV data
    parcel_address_rows = []
    used_postcodes = set()
    postcode_suffix_rows = []
    postcode_to_location = {}

    # Generate CSV data for all parcels
    for i in range(config.parcels):
        unique_item_id, acct = make_unique_item_id()
        postcode = random.choice(config.postcodes)
        used_postcodes.add(postcode)

        # Create postcode location mapping if not exists
        if postcode not in postcode_to_location:
            city, county = random.choice(UK_LOCATIONS)
            add_line1 = random_address_line()
            add_line2 = "" if random.random() < 0.5 else f"Flat {random.randint(1, 20)}"
            country = "GB"
            postcode_to_location[postcode] = (
                add_line1,
                add_line2,
                city,
                county,
                country,
            )

            # Create 1-5 suffix rows for this postcode
            suffix_count = random.randint(1, 5)
            for _ in range(suffix_count):
                add_line_suffix = random_address_line(street_words=2)
                po_suffix = make_po_suffix()
                postcode_suffix_rows.append((add_line_suffix, postcode, po_suffix))

        # Record parcel address
        add1, add2, city, county, country = postcode_to_location[postcode]
        parcel_address_rows.append(
            (unique_item_id, add1, add2, city, county, country, postcode)
        )

        # Progress logging
        if (i + 1) % 10000 == 0:
            logger.info(f"Generated CSV data for {i+1:,}/{config.parcels:,} parcels")

    # Create temp directory for CSV files
    csv_output = tempfile.mkdtemp(prefix="csv_")

    # Write parcel_address.csv
    parcel_address_csv_path = os.path.join(csv_output, "parcel_address.csv")
    with open(parcel_address_csv_path, "w", newline="", encoding="utf-8") as pa_f:
        w = csv.writer(pa_f)
        w.writerow(
            [
                "uniqueItemId",
                "add_line1",
                "add_line2",
                "city",
                "county",
                "country",
                "postcode",
            ]
        )
        for row in parcel_address_rows:
            w.writerow(row)
    logger.success(f"Created parcel address CSV: {len(parcel_address_rows):,} rows")

    # Write postcode.csv
    postcode_csv_path = os.path.join(csv_output, "postcode.csv")
    with open(postcode_csv_path, "w", newline="", encoding="utf-8") as pc_f:
        w = csv.writer(pc_f)
        w.writerow(["add_line1", "add_line2", "city", "county", "country", "pincode"])
        for pc in sorted(used_postcodes):
            if pc in postcode_to_location:
                add1, add2, city, county, country = postcode_to_location[pc]
                w.writerow([add1, add2, city, county, country, pc])
    logger.success(f"Created postcode CSV: {len(used_postcodes):,} rows")

    # Write postcode_suffix.csv
    postcode_suffix_csv_path = os.path.join(csv_output, "postcode_suffix.csv")
    with open(postcode_suffix_csv_path, "w", newline="", encoding="utf-8") as ps_f:
        w = csv.writer(ps_f)
        w.writerow(["add_line1", "postcode", "po_suffix"])
        for add1, pc, suf in postcode_suffix_rows:
            w.writerow([add1, pc, suf])
    logger.success(f"Created postcode suffix CSV: {len(postcode_suffix_rows):,} rows")

    # Upload to Azure Blob if configured
    azure_conn = AZURE_CONNECTION_STRING
    azure_container = AZURE_CONTAINER_NAME
    azure_prefix = AZURE_BLOB_PREFIX

    csv_files = {
        "parcel_address": parcel_address_csv_path,
        "postcode": postcode_csv_path,
        "postcode_suffix": postcode_suffix_csv_path,
    }

    uploaded_files = []

    if azure_conn and azure_container:
        logger.info(
            f"Uploading CSV files to Azure Blob Storage container: {azure_container} (in parallel)"
        )

        # Upload files in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for name, path in csv_files.items():
                blob_name = (
                    f"{azure_prefix}{name}.csv" if azure_prefix else f"{name}.csv"
                )
                future = executor.submit(
                    _upload_to_azure_blob,
                    azure_conn,
                    azure_container,
                    path,
                    blob_name,
                )
                futures.append((future, blob_name))

            # Wait for all uploads to complete
            for future, blob_name in futures:
                future.result()  # Wait for completion and raise any exceptions
                uploaded_files.append(blob_name)

        logger.success(f"All CSV files uploaded to Azure container: {azure_container}")
    else:
        logger.warning("Azure Storage not configured. CSV files not uploaded.")

    # Cleanup temp directory
    try:
        import shutil

        shutil.rmtree(csv_output)
    except Exception as e:
        logger.warning(f"Failed to cleanup temp directory: {e}")

    return {
        "csv_generated": True,
        "parcel_addresses": len(parcel_address_rows),
        "postcodes": len(used_postcodes),
        "postcode_suffixes": len(postcode_suffix_rows),
        "uploaded_to_azure": bool(azure_conn and azure_container),
        "uploaded_files": uploaded_files if uploaded_files else None,
    }
