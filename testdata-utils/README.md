# Test Data Generator & Kafka/Event Hub Streamer

High-performance test data generator for Royal Mail parcel tracking events with LMDB storage and Kafka/Azure Event Hub streaming capabilities.
---

## Installation

### Prerequisites

```bash
# Install Python dependencies
pip install -r requirements.txt

# Or install individually:
pip install lmdb confluent-kafka fastapi uvicorn azure-eventhub azure-storage-blob python-dotenv
```

---

## Environment Variables

All scripts now support environment variables for configuration. This is more secure and convenient than passing credentials via command-line arguments.

### Quick Setup

Copy the example configuration file and add your credentials:

```bash
# 1. Copy the example file
cp env.example .env

# 2. Edit .env with your actual credentials (it's already in .gitignore)
nano .env  # or use your preferred editor

# 3. Run the scripts - they automatically load .env
python lmdb_kafka_streamer.py generate --parcels 10000
```

The `.env.example` file includes:
- Your specific Azure Storage Account: **abcparcelcheckpoint**
- Your blob container: **address-lookup**
- Placeholders for keys and connection strings
- Detailed usage instructions

**No need to `source .env`** - the scripts use `python-dotenv` to automatically load variables from `.env` files!

### Alternative: Manual Environment Variables

You can also set environment variables manually if preferred:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=abcparcelcheckpoint;AccountKey=...;EndpointSuffix=core.windows.net"
export AZURE_STORAGE_CONTAINER_NAME="address-lookup"
export AZURE_BLOB_PREFIX=""  # Optional: e.g., "test/" or "prod/"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="mper-input-events"
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
```

---

## Quick Start

### 1. Generate Test Data

```bash
# Generate 10,000 parcels (40,000 events)
python lmdb_kafka_streamer.py generate \
  --parcels 10000 \
  --db ./event_buffer_lmdb \
  --seed 42 \
  --workers 4
```

### 2. Send to Kafka

```bash
python lmdb_kafka_streamer.py emit-chronological \
  --db ./event_buffer_lmdb \
  --bootstrap-servers localhost:9092 \
  --topic mper-input-events \
  --partitions 1
```

### 3. Send to Azure Event Hub

```bash
export EVENTHUB_CONN_STR="Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=mper-input-events"

python lmdb_kafka_streamer.py emit-chronological \
  --db ./event_buffer_lmdb \
  --bootstrap-servers namespace.servicebus.windows.net:9093 \
  --topic mper-input-events \
  --partitions 1 \
  --producer-type eventhub \
  --eventhub-connection-string "$EVENTHUB_CONN_STR"
```

---

## CLI Commands

### `generate` - Generate Test Data

Generate parcels with realistic UK addresses and timestamps, store in LMDB.

```bash
python lmdb_kafka_streamer.py generate [OPTIONS]
```

**Options:**

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--parcels` | int | **Required** | Number of parcels to generate (each has 4 events) |
| `--db` | str | `./event_buffer_lmdb` | LMDB database directory path |
| `--map-size-gb` | int | `32` | LMDB map size in GB |
| `--seed` | int | `42` | Random seed for reproducibility |
| `--start` | str | `2024-10-01T00:00:00` | Start timestamp (ISO format) |
| `--end` | str | `2025-10-01T00:00:00` | End timestamp (ISO format) |
| `--workers` | int | `4` | Number of parallel worker processes |
| `--chunk-size` | int | `5000` | Parcels per worker chunk |
| `--postcode-file` | str | `""` | Optional file with real UK postcodes (one per line) |

**Examples:**

```bash
# Small test dataset
python lmdb_kafka_streamer.py generate --parcels 1000 --db ./test_db

# Large dataset with custom date range
python lmdb_kafka_streamer.py generate \
  --parcels 1000000 \
  --db ./prod_db \
  --start 2024-01-01T00:00:00 \
  --end 2024-12-31T23:59:59 \
  --workers 8 \
  --chunk-size 10000

# With real UK postcodes
python lmdb_kafka_streamer.py generate \
  --parcels 100000 \
  --db ./realistic_db \
  --postcode-file ./uk_postcodes.txt \
  --seed 12345
```

**Output:**

- 4 events per parcel (EVDAV, EVIMC, EVGPD, ENKDN)
- Realistic time gaps between events
- 30% account-based (21-digit IDs), 70% online (11-digit IDs)
- 70% with email+mobile, 10% email only, 10% mobile only, 10% none
- 91 real UK postcodes by default

---

### `emit-chronological` - Send All Events in Order

Emit all events in strict chronological order (includes all 4 event types).

```bash
python lmdb_kafka_streamer.py emit-chronological [OPTIONS]
```

**Options:**

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--db` | str | `./event_buffer_lmdb` | LMDB database directory path |
| `--map-size-gb` | int | `32` | LMDB map size in GB |
| `--bootstrap-servers` | str | **Required** | Kafka/Event Hub broker address |
| `--topic` | str | **Required** | Topic/Event Hub name |
| `--partitions` | int | `1` | Number of partitions (use 1 for strict ordering) |
| `--acks` | str | `all` | Producer acknowledgment level |
| `--limit` | int | `None` | Maximum messages to send (for testing) |
| `--producer-type` | str | `kafka` | Producer type: `kafka` or `eventhub` |
| `--eventhub-connection-string` | str | `None` | Event Hub connection string (required if producer-type=eventhub) |

**Examples:**

```bash
# Send to local Kafka
python lmdb_kafka_streamer.py emit-chronological \
  --db ./event_buffer_lmdb \
  --bootstrap-servers localhost:9092 \
  --topic mper-input-events \
  --partitions 1

# Send to Azure Event Hub
python lmdb_kafka_streamer.py emit-chronological \
  --db ./event_buffer_lmdb \
  --bootstrap-servers namespace.servicebus.windows.net:9093 \
  --topic mper-input-events \
  --partitions 1 \
  --producer-type eventhub \
  --eventhub-connection-string "$EVENTHUB_CONN_STR"

# Test with limit
python lmdb_kafka_streamer.py emit-chronological \
  --db ./event_buffer_lmdb \
  --bootstrap-servers localhost:9092 \
  --topic test-events \
  --limit 1000
```

**Output:**

```
[kafka] Connecting to localhost:9092
[stats] 2025-10-28T15:30:22Z  rate=   32529 msg/s  avg=   32529 msg/s  total=32,615  out=619
[stats] 2025-10-28T15:30:23Z  rate=   31717 msg/s  avg=   32122 msg/s  total=64,511  out=521
[summary] Sent 40,004 messages in 1.2s (33,337 msg/s)
```

---

### `emit-scan` - Send Specific Event Type

Emit only events of a specific scan type (1=EVDAV, 2=EVIMC, 3=EVGPD).

```bash
python lmdb_kafka_streamer.py emit-scan [OPTIONS]
```

**Options:**

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--scan` | int | **Required** | Scan number: 1 (EVDAV), 2 (EVIMC), or 3 (EVGPD) |
| `--db` | str | `./event_buffer_lmdb` | LMDB database directory path |
| `--map-size-gb` | int | `32` | LMDB map size in GB |
| `--bootstrap-servers` | str | **Required** | Kafka/Event Hub broker address |
| `--topic` | str | **Required** | Topic/Event Hub name |
| `--partitions` | int | `1` | Number of partitions |
| `--acks` | str | `all` | Producer acknowledgment level |
| `--limit` | int | `None` | Maximum messages to send |
| `--producer-type` | str | `kafka` | Producer type: `kafka` or `eventhub` |
| `--eventhub-connection-string` | str | `None` | Event Hub connection string |

**Examples:**

```bash
# Send only EVDAV events (Scan 1)
python lmdb_kafka_streamer.py emit-scan \
  --scan 1 \
  --db ./event_buffer_lmdb \
  --bootstrap-servers localhost:9092 \
  --topic mper-input-events

# Send only EVIMC events (Scan 2) to Event Hub
python lmdb_kafka_streamer.py emit-scan \
  --scan 2 \
  --db ./event_buffer_lmdb \
  --bootstrap-servers namespace.servicebus.windows.net:9093 \
  --topic mper-input-events \
  --producer-type eventhub \
  --eventhub-connection-string "$EVENTHUB_CONN_STR"

# Sequential emission (GUI mode)
python lmdb_kafka_streamer.py emit-scan --scan 1 ... # First wave
python lmdb_kafka_streamer.py emit-scan --scan 2 ... # Second wave
python lmdb_kafka_streamer.py emit-scan --scan 3 ... # Third wave
```

**Use Case:** Simulating phased event arrival (e.g., collection → transit → delivery).

---

### `serve` - FastAPI Server

Run a REST API server for programmatic control.

```bash
python lmdb_kafka_streamer.py serve [OPTIONS]
```

**Options:**

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--db` | str | `./event_buffer_lmdb` | LMDB database directory path |
| `--map-size-gb` | int | `32` | LMDB map size in GB |
| `--bootstrap-servers` | str | **Required** | Kafka/Event Hub broker address |
| `--topic` | str | **Required** | Topic/Event Hub name |
| `--partitions` | int | `1` | Number of partitions |
| `--host` | str | `127.0.0.1` | Server bind address (use 0.0.0.0 for external access) |
| `--port` | int | `8080` | Server port |
| `--postcode-file` | str | `""` | Optional postcode file |
| `--producer-type` | str | `kafka` | Producer type: `kafka` or `eventhub` |
| `--eventhub-connection-string` | str | `None` | Event Hub connection string |

**Example:**

```bash
# Start server for Kafka
python lmdb_kafka_streamer.py serve \
  --db ./event_buffer_lmdb \
  --bootstrap-servers localhost:9092 \
  --topic mper-input-events \
  --host 0.0.0.0 \
  --port 8080

# Start server for Event Hub
python lmdb_kafka_streamer.py serve \
  --db ./event_buffer_lmdb \
  --bootstrap-servers namespace.servicebus.windows.net:9093 \
  --topic mper-input-events \
  --host 0.0.0.0 \
  --port 8080 \
  --producer-type eventhub \
  --eventhub-connection-string "$EVENTHUB_CONN_STR"
```

**Server Startup:**

```
INFO:     Started server process [12345]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8080
```

---

## FastAPI Endpoints

Once the server is running, access the API at `http://localhost:8080` (or your configured host/port).

### Interactive Documentation

- **Swagger UI:** `http://localhost:8080/docs`
- **ReDoc:** `http://localhost:8080/redoc`

### `POST /generate` - Generate Data

Generate parcels and store in LMDB.

**Request:**

```bash
curl -X POST "http://localhost:8080/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "parcels": 10000,
    "seed": 42,
    "start": "2024-10-01T00:00:00",
    "end": "2025-10-01T00:00:00",
    "workers": 4,
    "chunk_size": 5000
  }'
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `parcels` | int | 10000 | Number of parcels to generate |
| `seed` | int | 42 | Random seed |
| `start` | str | `2024-10-01T00:00:00` | Start timestamp |
| `end` | str | `2025-10-01T00:00:00` | End timestamp |
| `workers` | int | 4 | Worker processes |
| `chunk_size` | int | 5000 | Parcels per chunk |

**Response:**

```json
{
  "status": "ok",
  "inserted": 40004
}
```

### `POST /emit/chronological` - Emit All Events

Emit all events in chronological order.

**Request:**

```bash
curl -X POST "http://localhost:8080/emit/chronological?limit=1000"
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | None | Maximum messages to send (optional) |

**Response:**

```json
{
  "status": "ok",
  "sent": 40004
}
```

### `POST /emit/scan/{scan_no}` - Emit Specific Scan

Emit events of a specific scan type.

**Request:**

```bash
curl -X POST "http://localhost:8080/emit/scan/1"
curl -X POST "http://localhost:8080/emit/scan/2?limit=5000"
curl -X POST "http://localhost:8080/emit/scan/3"
```

**Path Parameters:**

| Parameter | Type | Values | Description |
|-----------|------|--------|-------------|
| `scan_no` | int | 1, 2, or 3 | Scan number (1=EVDAV, 2=EVIMC, 3=EVGPD) |

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | None | Maximum messages to send (optional) |

**Response:**

```json
{
  "status": "ok",
  "sent": 10001,
  "scan": 1
}
```

**Error Response:**

```json
{
  "status": "error",
  "error": "scan_no must be 1,2,3"
}
```

---

## Message Format

### Kafka Message Structure

Each message sent to Kafka/Event Hub includes:

**Headers:**
```
event_code: EVDAV (or EVIMC, EVGPD, ENKDN)
scan_ts_iso: 2024-10-28T14:23:45+01:00
domainId: 12345678901 (uniqueItemId)
```

**Key:**
```
12345678901 (uniqueItemId as UTF-8 bytes)
```

**Value:**
```xml
<ptp:MPE xmlns:dt="http://www.royalmailgroup.com/cm/rmDatatypes/V1" ...>
  <mailPiece>
    <mailPieceBarcode>
      <royalMailSegment>...</royalMailSegment>
      <channelSegment>
        <uniqueItemId>12345678901</uniqueItemId>
        <mailPieceWeight>1</mailPieceWeight>
        <weightCode>1</weightCode>
        <productId>100</productId>
        <UPUTrackingNumber>YA123456789GB</UPUTrackingNumber>
        <postcode>EC1A1BB</postcode>
        ...
      </channelSegment>
    </mailPieceBarcode>
  </mailPiece>
  <manualScan>
    <trackedEventCode>EVDAV</trackedEventCode>
    <scanTimestamp>2024-10-28T14:23:45+01:00</scanTimestamp>
    ...
    <auxiliaryData>
      <data><name>RECIPIENT_EMAILID</name><value>user123456@example.com</value></data>
      <data><name>RECIPIENT_MOBILENO</name><value>07123456789</value></data>
    </auxiliaryData>
  </manualScan>
</ptp:MPE>
```

**Timestamp:** Event timestamp (from `<scanTimestamp>`)

---

## Data Generation Details

### Event Types

| Event Code | Description | Time from Previous |
|------------|-------------|-------------------|
| **EVDAV** | Despatch - Available | 1-180 min after creation |
| **EVIMC** | In Mail Centre | 1-24 hours after EVDAV |
| **EVGPD** | Out for Delivery | 4-36 hours after EVIMC |
| **ENKDN** | Delivered (final) | 0.5-24 hours after EVGPD |

### ID Formats

- **30% Account-based:** `11{10-digit-account}001091111` (21 digits)
- **70% Online orders:** Random 11-digit number

### Contact Mix

- **70%:** Email + Mobile
- **10%:** Email only
- **10%:** Mobile only
- **10%:** No contact

### Product Categories

- **40%:** Tracked24
- **40%:** Tracked48
- **10%:** SpecialDelivery09
- **10%:** SpecialDelivery13

### Locations

- **91 Real UK Postcodes** (default)
- **95 UK Cities/Counties** (London, Manchester, Edinburgh, etc.)

---

## Azure Event Hub Support

### Quick Start with Event Hub

```bash
# 1. Get connection string from Azure Portal
export EVENTHUB_CONN_STR="Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=SendPolicy;SharedAccessKey=YOUR_KEY;EntityPath=mper-input-events"

# 2. Generate data (same as Kafka)
python lmdb_kafka_streamer.py generate --parcels 10000 --db ./test_db

# 3. Send to Event Hub
python lmdb_kafka_streamer.py emit-chronological \
  --db ./test_db \
  --bootstrap-servers namespace.servicebus.windows.net:9093 \
  --topic mper-input-events \
  --partitions 1 \
  --producer-type eventhub \
  --eventhub-connection-string "$EVENTHUB_CONN_STR"
```

### Key Differences

| Aspect | Kafka | Event Hub |
|--------|-------|-----------|
| **Port** | 9092 | **9093** |
| **Auth** | Various | **SASL/SSL** (connection string) |
| **--producer-type** | `kafka` (default) | `eventhub` |
| **Connection String** | Not needed | **Required** |

### Getting Connection String

1. Go to Azure Portal → Event Hubs namespace
2. Click **"Shared access policies"**
3. Select/create policy with **Send** permission
4. Copy **"Connection string–primary key"**

**Format:**
```
Endpoint=sb://NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=POLICY;SharedAccessKey=KEY;EntityPath=EVENTHUB_NAME
```

### Full Documentation

See `AZURE_EVENTHUB_GUIDE.md` for complete Azure Event Hub documentation.

---

## Legacy Scripts

The following scripts generate ZIP files instead of LMDB. They're retained for backwards compatibility but `lmdb_kafka_streamer.py` is recommended for new projects.

---

### `generate_rm_full_dataset.py`

Generates **both input events AND notification outputs** in ZIP files.

**Small Sample (100 parcels):**

```bash
python generate_rm_full_dataset.py \
  --parcels 100 \
  --output-dir out_sample \
  --batch-size 200 \
  --seed 42
```

**Large Scale (10 million parcels):**

```bash
python generate_rm_full_dataset.py \
  --parcels 10000000 \
  --output-dir out_10M_parcels \
  --batch-size 100000 \
  --seed 42
```

**10 Million Events (2.5M parcels × 4 scans):**

```bash
python generate_rm_full_dataset.py \
  --parcels 2500000 \
  --output-dir out_10M_events \
  --batch-size 100000 \
  --seed 42
```

**Output Structure:**
```
out_sample/
├── inputs/
│   ├── mp_inputs_0001.zip    # Input scan XMLs (4 per parcel)
│   ├── mp_inputs_0002.zip
│   └── ...
├── outputs/
│   ├── mp_notifications_0001.zip  # Notification XMLs
│   ├── mp_notifications_0002.zip
│   └── ...
├── combined/
│   ├── test_combined_0001.zip     # Combined test XMLs
│   └── ...
├── postcode.csv              # Postcode metadata
├── postcode_suffix.csv       # DPS suffixes
└── notifications_summary.csv # Notification summary
```

---

### `generate_rm_inputs_only.py`

Single-threaded generator that creates **only input events** (no notifications) in ZIP files.

**Small Sample:**

```bash
python generate_rm_inputs_only.py \
  --parcels 1000 \
  --output-dir out_inputs_1k \
  --batch-size 100000 \
  --seed 42
```

**Medium Scale:**

```bash
python generate_rm_inputs_only.py \
  --parcels 100000 \
  --output-dir out_inputs_100k \
  --batch-size 100000 \
  --seed 42
```

**With Custom Postcodes:**

```bash
python generate_rm_inputs_only.py \
  --parcels 50000 \
  --output-dir out_custom \
  --postcode-file ./uk_postcodes.txt \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --seed 42
```

**Output Structure:**
```
out_inputs_1k/
├── inputs/
│   ├── mp_inputs_0001.zip    # Input scan XMLs only
│   ├── mp_inputs_0002.zip
│   └── ...
├── postcode.csv
├── postcode_suffix.csv
└── notifications_summary.csv
```

---

### `generate_rm_inputs_only_multi.py`

**Multi-threaded version** of `generate_rm_inputs_only.py` for faster generation.

**With 4 Workers:**

```bash
python generate_rm_inputs_only_multi.py \
  --parcels 100000 \
  --output-dir out_multi_100k \
  --batch-size 100000 \
  --workers 4 \
  --seed 42
```

**With 8 Workers (Large Scale):**

```bash
python generate_rm_inputs_only_multi.py \
  --parcels 1000000 \
  --output-dir out_multi_1M \
  --batch-size 100000 \
  --workers 8 \
  --seed 42
```

**Output Structure:**
```
out_multi_100k/
├── worker_0/
│   ├── mp_inputs/
│   │   └── mp_inputs_0001.zip
│   └── notifications_summary.csv
├── worker_1/
│   └── ...
├── worker_2/
│   └── ...
├── worker_3/
│   └── ...
├── postcode.csv              # Consolidated
└── postcode_suffix.csv       # Consolidated
```

---

### Common Arguments for Legacy Generators

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--parcels` | int | 100 | Number of parcels to generate |
| `--output-dir` | str | `out_full` | Output directory for ZIP files |
| `--batch-size` | int | 100000 | XMLs per ZIP file before rotating |
| `--postcode-file` | str | `""` | Optional custom postcode file (one per line) |
| `--seed` | int | 42 | Random seed for reproducibility |
| `--start-date` | str | Today - 365 days | Earliest event date (YYYY-MM-DD) |
| `--end-date` | str | Today | Latest event date (YYYY-MM-DD) |
| `--workers` | int | CPU count | Worker processes (*_multi.py only) |

---

## Working with ZIP Files

If you've used the legacy generators and have ZIP files, here's how to work with them:

### Extract ZIP Files to Flat Directory

Extract all input XMLs from multiple ZIP files into a single directory:

```bash
# Extract from specific output directory
find ./out_10K/inputs -type f -name "*.zip" -size +0c -print0 | \
  xargs -0 -n 1 -P 8 sh -c 'unzip -q -o "$0" -d ./flat_inputs'

# For multi-worker output
find ./out_multi_100k/worker_*/mp_inputs -type f -name "*.zip" -size +0c -print0 | \
  xargs -0 -n 1 -P 8 sh -c 'unzip -q -o "$0" -d ./flat_inputs'

# Using absolute paths (more reliable)
find ./out_10K/inputs -type f -name "*.zip" -size +0c -print0 | \
  xargs -0 -n 1 -P 8 sh -c 'unzip -q -o "$0" -d "'"$(pwd)"'/flat_inputs"'
```

**Parameters:**
- `-P 8`: Use 8 parallel processes
- `-q`: Quiet mode (no output)
- `-o`: Overwrite existing files

**Result:** All XML files extracted to `flat_inputs/` directory

---

### Publishing ZIP Files to Kafka

#### Option 1: Python Producer (Recommended)

Use `kafka_producer.py` to read from flat directory and send to Kafka:

```bash
python kafka_producer.py \
  --inputs-dir ./flat_inputs/ \
  --bootstrap-servers localhost:9092 \
  --topic mper-input-events \
  --create-topics \
  --partitions 12 \
  --retention-ms 604800000 \
  --linger-ms 500 \
  --compression zstd \
  --max-outstanding 300000 \
  --zip-cache-size 1 \
  --time-compression 0 \
  --min-sleep-ms 0.1
```

**Arguments:**
- `--inputs-dir`: Directory with flat XML files
- `--create-topics`: Create topic if doesn't exist
- `--partitions`: Number of partitions (12 for high throughput)
- `--retention-ms`: Retention time in milliseconds (7 days = 604800000)
- `--linger-ms`: Batching delay (higher = better throughput)
- `--compression`: Compression type (zstd, gzip, snappy, lz4)
- `--max-outstanding`: Max in-flight messages
- `--time-compression`: Time multiplier (0 = as fast as possible)

**Performance:** ~40,000-60,000 msg/s with 8 workers

#### Option 2: Java Producer

Use the Java producer for potentially higher throughput:

```bash
java -jar target/producer-java-jar-with-dependencies.jar \
  --brokers=localhost:9092 \
  --topic=mper-input-events \
  --input=../flat_inputs \
  --reader-threads=4 \
  --sender-threads=8
```

**Arguments:**
- `--brokers`: Kafka bootstrap servers
- `--topic`: Target topic
- `--input`: Directory with XML files
- `--reader-threads`: Threads for reading files
- `--sender-threads`: Threads for sending to Kafka

**Performance:** ~50,000-80,000 msg/s

---

## Migration Guide: ZIP to LMDB

If you're currently using the legacy ZIP-based workflow, here's how to migrate:

### Old Workflow (ZIP-based)

```bash
# 1. Generate ZIP files (slow)
python generate_rm_inputs_only_multi.py --parcels 100000 --output-dir out_100k --workers 4

# 2. Extract ZIPs (takes time + disk space)
find ./out_100k -name "*.zip" -print0 | xargs -0 -n 1 -P 8 sh -c 'unzip -q "$0" -d ./flat_inputs'

# 3. Send to Kafka
python kafka_producer.py --inputs-dir ./flat_inputs/ --bootstrap-servers localhost:9092 --topic events
```

### New Workflow (LMDB-based) ⚡

```bash
# 1. Generate directly to LMDB (faster, less disk)
python lmdb_kafka_streamer.py generate --parcels 100000 --db ./lmdb_db --workers 4

# 2. Send to Kafka (much faster - no XML parsing)
python lmdb_kafka_streamer.py emit-chronological --db ./lmdb_db --bootstrap-servers localhost:9092 --topic events
```

### Benefits of LMDB Approach

| Aspect | ZIP-based | LMDB-based |
|--------|-----------|------------|
| **Generation** | ~8,000 events/s | ~16,000 events/s |
| **Extraction** | Required (slow) | Not needed |
| **Disk Usage** | 2x (ZIP + flat) | 1x (compressed LMDB) |
| **Emission Speed** | ~40,000 msg/s | ~50,000 msg/s |
| **Re-emission** | Re-read ZIPs | Instant from LMDB |
| **Chronological Order** | Not guaranteed | Guaranteed |
| **Memory Usage** | High (file handles) | Low (memory-mapped) |

**Recommendation:** Use `lmdb_kafka_streamer.py` for new projects. Use legacy generators only if you need ZIP files for external tools.

---

## File Structure

```
testdata-utils/
├── lmdb_kafka_streamer.py              # ⭐ Main tool (RECOMMENDED)
│                                       # - LMDB storage + Kafka/Event Hub producer
│                                       # - FastAPI server
│                                       # - 50k+ msg/s throughput
│
├── generate_rm_full_dataset.py        # Legacy: Full dataset generator
│                                       # - Creates inputs + outputs + combined
│                                       # - ZIP files
│
├── generate_rm_inputs_only.py         # Legacy: Single-thread input generator
│                                       # - Input XMLs only
│                                       # - ZIP files
│
├── generate_rm_inputs_only_multi.py   # Legacy: Multi-thread input generator
│                                       # - Input XMLs only
│                                       # - ZIP files with worker directories
│
├── kafka_producer.py                   # Legacy: ZIP-based Kafka producer
│                                       # - Reads flat XML files
│                                       # - ~40k msg/s
│
├── eventhub_producer.py                # Event Hub specific producer
│                                       # - For ZIP file workflow
│
├── AZURE_EVENTHUB_GUIDE.md            # 📖 Azure Event Hub documentation
├── README.md                           # 📖 This file
│
├── event_buffer_lmdb/                  # LMDB database (created by generate)
│   ├── data.mdb                        # Actual data (memory-mapped)
│   └── lock.mdb                        # Lock file
│
├── out_sample/                         # Example output from legacy generators
│   ├── inputs/
│   │   └── mp_inputs_*.zip
│   ├── outputs/                        # (full_dataset only)
│   │   └── mp_notifications_*.zip
│   ├── combined/                       # (full_dataset only)
│   │   └── test_combined_*.zip
│   ├── postcode.csv
│   ├── postcode_suffix.csv
│   └── notifications_summary.csv
│
├── out_multi_100k/                     # Multi-worker output structure
│   ├── worker_0/
│   │   ├── mp_inputs/
│   │   │   └── mp_inputs_*.zip
│   │   └── notifications_summary.csv
│   ├── worker_1/
│   ├── worker_2/
│   ├── worker_3/
│   ├── postcode.csv
│   └── postcode_suffix.csv
│
├── flat_inputs/                        # Extracted XMLs (for legacy workflow)
│   ├── event_001.xml
│   ├── event_002.xml
│   └── ...
│
├── gen-api/                            # FastAPI wrapper (alternative)
│   ├── main.py
│   ├── generator_service.py
│   ├── config.py
│   └── requirements.txt
│
├── producer-java/                      # Java producer (alternative)
│   ├── pom.xml
│   └── src/
│
├── venv/                               # Python virtual environment
└── .gitignore
```

**Quick Reference:**

| File | Purpose | When to Use |
|------|---------|-------------|
| **lmdb_kafka_streamer.py** | All-in-one tool | ✅ **Recommended for all new projects** |
| generate_rm_full_dataset.py | Full dataset with outputs | Need notification XMLs |
| generate_rm_inputs_only.py | Input events only | Small datasets, no parallel needed |
| generate_rm_inputs_only_multi.py | Input events (fast) | Large datasets, need speed |
| kafka_producer.py | Send ZIPs to Kafka | Already have ZIP files |
| eventhub_producer.py | Send ZIPs to Event Hub | Already have ZIP files |

---
