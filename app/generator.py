"""
Event generation with multiprocessing support
"""

import csv
import os
import random
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from .logger import logger
from .config import (
    AZURE_AVAILABLE,
    AZURE_CONNECTION_STRING,
    AZURE_CONTAINER_NAME,
    AZURE_BLOB_PREFIX,
)
from .models import (
    Event,
    EVENT_FLOW,
    EVENT_GAPS_HOURS,
    PRODUCT_CATEGORIES,
    UK_LOCATIONS,
    INPUT_NS_HEADER,
)
from .utils import (
    build_mailpiece_input_xml,
    build_manualscan_input_xml,
    choose_weighted,
    extract_metadata_for_kafka,
    iso_date,
    iso_datetime_with_tz,
    make_po_suffix,
    make_unique_item_id,
    make_upu_tracking,
    pack_with_metadata,
    pick_contact_mix,
    random_address_line,
    random_email,
    random_mobile,
)

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


def _worker_make_events(
    seed: int,
    start: datetime,
    end: datetime,
    start_idx: int,
    count: int,
    postcodes_master: List[str],
    site_ids: List[str],
    func_ids: List[str],
) -> Tuple[List[Event], List[Tuple], set, List[Tuple]]:
    """Worker function using exact source generation logic"""
    random.seed(seed + start_idx)
    out: List[Event] = []
    parcel_address_rows = []
    used_postcodes = set()
    postcode_suffix_rows = []
    postcode_to_location = {}

    for i in range(start_idx, start_idx + count):
        unique_item_id, acct = make_unique_item_id()
        upu = make_upu_tracking()
        category = choose_weighted(PRODUCT_CATEGORIES)
        has_email, has_mobile = pick_contact_mix()
        email = random_email(unique_item_id) if has_email else None
        mobile = random_mobile() if has_mobile else None

        # postcode selection
        postcode = random.choice(postcodes_master)
        used_postcodes.add(postcode)

        # Create postcode location mapping
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

            # create 1-5 suffix rows for this postcode
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

        # product id mapping
        product_id = {
            "Tracked24": "100",
            "Tracked48": "101",
            "SpecialDelivery09": "109",
            "SpecialDelivery13": "113",
        }[category]

        # barcode creation within [start, end)
        total_sec = int((end - start).total_seconds())
        barcode_creation = start + timedelta(seconds=random.randrange(total_sec))

        # compute scan times
        t1, t2, t3, t4 = gen_scan_times(barcode_creation, random)
        scan_times = {"EVDAV": t1, "EVIMC": t2, "EVGPD": t3, "ENKDN": t4}

        pid = f"P{(i+1):08d}-{random.getrandbits(32):08x}"

        # Build 4 XMLs
        for idx, ev in enumerate(EVENT_FLOW, start=1):
            scan_dt = scan_times[ev]
            func = random.choice(func_ids)
            site = random.choice(site_ids)
            device_id = "".join(str(random.randint(0, 9)) for _ in range(15))
            include_route = ev in ("EVGPD", "ENKDN")
            route_no = str(random.randint(100000, 9999999)) if include_route else None
            include_aux = idx == 1  # only in first scan

            xml = build_full_xml(
                unique_item_id,
                upu,
                postcode,
                product_id,
                ev,
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

            # Extract metadata once during generation
            metadata = extract_metadata_for_kafka(xml)
            packed_xml = pack_with_metadata(xml, metadata)

            out.append(
                Event(
                    parcel_id=pid,
                    scan_no=idx,
                    event_code=ev,
                    event_time=scan_dt,
                    xml_value=packed_xml,
                    metadata=metadata,
                )
            )

    return out, parcel_address_rows, used_postcodes, postcode_suffix_rows


def _upload_to_azure_blob(
    connection_string: str,
    container_name: str,
    local_file_path: str,
    blob_name: str = None,
):
    """Upload a file to Azure Blob Storage with optimized settings"""
    if not AZURE_AVAILABLE:
        raise RuntimeError(
            "azure-storage-blob not installed. Run: pip install azure-storage-blob"
        )

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


def _write_csv_files(
    output_dir: str,
    parcel_address_rows: List[Tuple],
    used_postcodes: set,
    postcode_suffix_rows: List[Tuple],
    postcode_to_location: dict,
):
    """Write CSV files and upload to Azure if configured"""
    os.makedirs(output_dir, exist_ok=True)

    # Write parcel_address.csv
    parcel_address_csv_path = os.path.join(output_dir, "parcel_address.csv")
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
    postcode_csv_path = os.path.join(output_dir, "postcode.csv")
    with open(postcode_csv_path, "w", newline="", encoding="utf-8") as pc_f:
        w = csv.writer(pc_f)
        w.writerow(["add_line1", "add_line2", "city", "county", "country", "pincode"])
        for pc in sorted(used_postcodes):
            if pc in postcode_to_location:
                add1, add2, city, county, country = postcode_to_location[pc]
                w.writerow([add1, add2, city, county, country, pc])
    logger.success(f"Created postcode CSV: {len(used_postcodes):,} rows")

    # Write postcode_suffix.csv
    postcode_suffix_csv_path = os.path.join(output_dir, "postcode_suffix.csv")
    with open(postcode_suffix_csv_path, "w", newline="", encoding="utf-8") as ps_f:
        w = csv.writer(ps_f)
        w.writerow(["add_line1", "postcode", "po_suffix"])
        for add1, pc, suf in postcode_suffix_rows:
            w.writerow([add1, pc, suf])
    logger.success(f"Created postcode suffix CSV: {len(postcode_suffix_rows):,} rows")

    # Upload to Azure if configured
    azure_conn = AZURE_CONNECTION_STRING
    azure_container = AZURE_CONTAINER_NAME
    azure_prefix = AZURE_BLOB_PREFIX

    if azure_conn and azure_container:
        logger.info(
            f"Uploading CSV files to Azure Blob Storage container: {azure_container} (in parallel)"
        )

        # Define files to upload
        csv_files = {
            "parcel_address": (
                parcel_address_csv_path,
                (
                    f"{azure_prefix}parcel_address.csv"
                    if azure_prefix
                    else "parcel_address.csv"
                ),
            ),
            "postcode": (
                postcode_csv_path,
                f"{azure_prefix}postcode.csv" if azure_prefix else "postcode.csv",
            ),
            "postcode_suffix": (
                postcode_suffix_csv_path,
                (
                    f"{azure_prefix}postcode_suffix.csv"
                    if azure_prefix
                    else "postcode_suffix.csv"
                ),
            ),
        }

        # Upload files in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for name, (local_path, blob_name) in csv_files.items():
                future = executor.submit(
                    _upload_to_azure_blob,
                    azure_conn,
                    azure_container,
                    local_path,
                    blob_name,
                )
                futures.append((future, blob_name))

            # Wait for all uploads to complete
            for future, blob_name in futures:
                future.result()  # Wait for completion and raise any exceptions

        logger.success(f"All CSV files uploaded to Azure container: {azure_container}")


def generate_all_to_lmdb(
    env,  # LmdbBuffer instance
    parcels: int,
    start: datetime,
    end: datetime,
    seed: int,
    workers: int,
    chunk_size: int,
    postcode_file: str = "",
    output_csv_dir: str = "",
):
    """
    Generate all events using exact source logic and store in LMDB

    Args:
        env: LmdbBuffer instance
        parcels: Number of parcels to generate
        start: Start datetime
        end: End datetime
        seed: Random seed
        workers: Number of worker processes (auto-detected if 0)
        chunk_size: Parcels per worker batch
        postcode_file: Optional file with postcodes
        output_csv_dir: If provided, write CSV files and upload to Azure

    Returns:
        Total number of events generated
    """
    from .utils import load_postcodes

    # Auto-detect workers if not specified
    if workers <= 0:
        import multiprocessing

        workers = multiprocessing.cpu_count()
        logger.info(f"Auto-detected {workers} CPU cores for parallel processing")

    seq = 0

    # Load postcodes
    postcodes_master = load_postcodes(postcode_file)

    # Site/func IDs
    site_pool_size = 500
    site_ids = [str(i + 1).zfill(6) for i in range(site_pool_size)]
    func_ids = [str(i + 1) for i in range(site_pool_size)]

    # Collections for CSV generation
    all_parcel_address_rows = []
    all_used_postcodes = set()
    all_postcode_suffix_rows = []
    all_postcode_to_location = {}

    # Magic parcel (goes first): epoch 0 timestamps
    magic_ts = datetime.fromtimestamp(0, tz=timezone.utc)
    for idx, code in enumerate(EVENT_FLOW, start=1):
        xml = build_full_xml(
            "028792170110069554608",
            "YA000000001GB",
            "AA11AA",
            "100",
            code,
            magic_ts,
            "000456",
            "000123",
            "DVC-1001",
            "USR-2002",
            False,
            None,
            (idx == 1),
            "magic@example.com",
            "07123456789",
        )

        metadata = extract_metadata_for_kafka(xml)
        packed_xml = pack_with_metadata(xml, metadata)

        env.put_event(
            Event(
                parcel_id="MAGIC-PARCEL",
                scan_no=idx,
                event_code=code,
                event_time=magic_ts,
                xml_value=packed_xml,
                metadata=metadata,
            ),
            seq,
        )
        seq += 1

    if workers <= 1:
        logger.info(f"Generating {parcels:,} parcels (single-threaded)")
        events, parcel_rows, used_pcs, suffix_rows = _worker_make_events(
            seed, start, end, 0, parcels, postcodes_master, site_ids, func_ids
        )
        all_parcel_address_rows.extend(parcel_rows)
        all_used_postcodes.update(used_pcs)
        all_postcode_suffix_rows.extend(suffix_rows)

        logger.info(f"Writing {len(events):,} events to LMDB")
        for i, e in enumerate(events):
            env.put_event(e, seq)
            seq += 1
            if (i + 1) % max(len(events) // 10, 100000) == 0:
                pct = (i + 1) / len(events) * 100
                logger.info(f"Progress: {i+1:,}/{len(events):,} events ({pct:.0f}%)")

        if output_csv_dir:
            # Build postcode_to_location
            for (
                unique_id,
                add1,
                add2,
                city,
                county,
                country,
                pc,
            ) in all_parcel_address_rows:
                if pc not in all_postcode_to_location:
                    all_postcode_to_location[pc] = (add1, add2, city, county, country)
            _write_csv_files(
                output_csv_dir,
                all_parcel_address_rows,
                all_used_postcodes,
                all_postcode_suffix_rows,
                all_postcode_to_location,
            )

        return seq

    # Multiprocessing path
    logger.info(f"Starting {workers} worker processes for parallel generation")
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = []
        assigned = 0
        while assigned < parcels:
            take = min(chunk_size, parcels - assigned)
            futures.append(
                ex.submit(
                    _worker_make_events,
                    seed,
                    start,
                    end,
                    assigned,
                    take,
                    postcodes_master,
                    site_ids,
                    func_ids,
                )
            )
            assigned += take

        logger.info(f"Submitted {len(futures)} batches to workers")
        logger.info(f"Collecting results and writing to LMDB")

        completed = 0
        for fut in as_completed(futures):
            events, parcel_rows, used_pcs, suffix_rows = fut.result()
            all_parcel_address_rows.extend(parcel_rows)
            all_used_postcodes.update(used_pcs)
            all_postcode_suffix_rows.extend(suffix_rows)

            for e in events:
                env.put_event(e, seq)
                seq += 1
            completed += 1

            pct = completed / len(futures) * 100
            parcels_done = (
                completed * chunk_size if completed < len(futures) else parcels
            )
            logger.info(
                f"Batch {completed}/{len(futures)} complete ({pct:.0f}%) - "
                f"{parcels_done:,}/{parcels:,} parcels, {seq:,} events written"
            )

    # Write CSVs and upload to Azure
    if output_csv_dir:
        logger.info(f"Writing CSV files to {output_csv_dir}")
        # Build postcode_to_location
        for unique_id, add1, add2, city, county, country, pc in all_parcel_address_rows:
            if pc not in all_postcode_to_location:
                all_postcode_to_location[pc] = (add1, add2, city, county, country)

        _write_csv_files(
            output_csv_dir,
            all_parcel_address_rows,
            all_used_postcodes,
            all_postcode_suffix_rows,
            all_postcode_to_location,
        )

    logger.success(f"Generation complete: {seq:,} total events stored in LMDB")
    return seq
