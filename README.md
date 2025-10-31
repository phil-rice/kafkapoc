# Royal Mail EPS POC - Event Generator and Streaming API

A high-performance event generation and streaming service for Royal Mail proof-of-concept testing. This Python FastAPI service generates synthetic parcel tracking events on-demand and streams them to Kafka or Azure Event Hub without persistent storage.

## Architecture

**Stream-on-Demand Architecture**: Events are generated dynamically without persistent storage, enabling memory-efficient, scalable event streaming.

### Key Components

- **FastAPI Service**: REST API for configuration and event emission
- **Event Generator**: Creates realistic Royal Mail parcel tracking events with proper XML format
- **Kafka/Event Hub Producer**: High-throughput streaming with batching and compression
- **CSV Generator**: Produces address lookup data for Azure Blob Storage
- **Configuration Store**: In-memory configuration management

## Data Model

### Event Flow

Each parcel progresses through 4 scan events:

1. **EVDAV** - Acceptance scan (parcel received)
2. **EVIMC** - In-transit scan (processing center)
3. **EVGPD** - Out for delivery scan
4. **ENKDN** - Delivered scan

### Product Categories

- **Tracked24** (40%) - 24-hour tracked delivery
- **Tracked48** (40%) - 48-hour tracked delivery
- **SpecialDelivery09** (10%) - Guaranteed delivery by 9am
- **SpecialDelivery13** (10%) - Guaranteed delivery by 1pm

### Event Data Structure

Each event includes:

- **Unique Item ID**: 11 or 21-digit barcode (30% account-based)
- **UPU Tracking Number**: International format (YA#########GB)
- **Address Details**: Postcode, location, address lines
- **Scan Metadata**: Timestamps, site IDs, device IDs, functional locations
- **Contact Information**: Email and mobile (70% have both)
- **Route Information**: Included in EVGPD and ENKDN events

## Prerequisites

- Python 3.10+
- Docker and Docker Compose (for containerized deployment)
- Kafka or Azure Event Hub (for event streaming)
- Azure Storage Account (for CSV uploads)
- Azure Container Registry (for GitHub Actions deployment)
- Azure Container Apps (for Azure deployment)

## Build, Test, and Deploy

### 1. Build Locally

#### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd rmg-python
```

#### Step 2: Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

#### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

#### Step 4: Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Copy template if available, or create manually
touch .env
```

Required environment variables (see [Environment Variables](#environment-variables) section below).

#### Step 5: Build Docker Image (Optional)

```bash
docker build -t rmg-python:latest .
```

### 2. Test Locally

#### Step 1: Start the Application

**Option A: Run directly with Python**

```bash
python -m app.main
```

**Option B: Run with uvicorn**

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

**Option C: Run with Docker Compose**

```bash
docker-compose up -d
```

The API will be available at `http://localhost:8000`

#### Step 2: Verify Health Check

```bash
curl http://localhost:8000/health
```

Expected response:

```json
{
  "status": "healthy",
  "service": "Event Streaming API",
  "version": "2.0.0",
  "architecture": "stream-on-demand",
  "configuration": {
    "kafka_bootstrap": "localhost:9092",
    "kafka_topic": "mper-input-events",
    "producer_type": "kafka"
  }
}
```

#### Step 3: Test Configuration Endpoint

```bash
curl -X POST http://localhost:8000/generate \
  -H "Content-Type: application/json" \
  -d '{"parcels": 100, "seed": 42}'
```

#### Step 4: Test Event Emission

```bash
# Emit events for scan 1 (EVDAV)
curl -X POST http://localhost:8000/scans/1/emissions \
  -H "Content-Type: application/json" \
  -d '{"limit": 100}'
```

### 3. Deploy Locally

#### Using Docker Compose

1. **Configure environment variables** in `.env` file

2. **Start the service**

```bash
docker-compose up -d
```

3. **View logs**

```bash
docker-compose logs -f event-streaming-api
```

4. **Stop the service**

```bash
docker-compose down
```

#### Using Docker Directly

1. **Build the image**

```bash
docker build -t rmg-python:latest .
```

2. **Run the container**

```bash
docker run -d \
  --name rmg-python \
  -p 8000:8000 \
  --env-file .env \
  -v $(pwd)/logs:/app/logs \
  rmg-python:latest
```

### 4. Build and Deploy with GitHub Actions

#### Prerequisites

1. **Azure Container Registry (ACR)** - Create an ACR instance in Azure
2. **GitHub Secrets** - Configure the following secrets in your GitHub repository:

   - `AZURE_CREDENTIALS` - Azure service principal credentials (JSON format)
   - `REGISTRY` - Azure Container Registry URL (e.g., `yourregistry.azurecr.io`)
   - `ACR_NAME` - Azure Container Registry name

#### Setup GitHub Secrets

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add the following secrets:

   **AZURE_CREDENTIALS:**
   ```bash
   # Create service principal (run in Azure CLI):
   az ad sp create-for-rbac --name "github-actions-sp" \
     --role contributor \
     --scopes /subscriptions/{subscription-id}/resourceGroups/{resource-group} \
     --sdk-auth
   ```
   
   Copy the JSON output and paste it as the secret value.

   **REGISTRY:**
   ```
   yourregistry.azurecr.io
   ```

   **ACR_NAME:**
   ```
   yourregistry
   ```

#### CI Pipeline (Build)

The CI pipeline (`.github/workflows/CI-Python.yaml`) automatically:

1. Builds the Docker image on push to `main` or `develop` branches
2. Scans the image for vulnerabilities using Trivy
3. Logs in to Azure Container Registry
4. Pushes the image to ACR with tag `latest`

**Trigger the workflow:**

- Push to `main` or `develop` branches
- Or manually trigger via GitHub Actions UI (Workflow dispatch)

#### CD Pipeline (Deploy to Azure)

The CD pipeline (`.github/workflows/CD-Python.yaml`) automatically:

1. Triggers after successful CI build completion
2. Logs in to Azure Container Registry
3. Redeploys the Azure Container App with the new image

**Prerequisites for Azure Deployment:**

- Azure Container App named `rmg-python` in resource group `rg-aiforce-rmgpoc-uksouth-001`
- Container App configured to pull from your ACR
- Container App environment variables configured (see [Environment Variables](#environment-variables))

**Update Container App name/resource group:**

Edit `.github/workflows/CD-Python.yaml`:

```yaml
--name rmg-python \  # Change to your container app name
--resource-group rg-aiforce-rmgpoc-uksouth-001 \  # Change to your resource group
--image yourregistry.azurecr.io/rmg-python:latest \  # Update ACR URL
```

### 5. Deploy to Azure Container Apps

#### Manual Deployment

1. **Build and push to ACR**

```bash
# Login to Azure
az login

# Login to ACR
az acr login --name <your-acr-name>

# Build and push
docker build -t <your-acr-name>.azurecr.io/rmg-python:latest .
docker push <your-acr-name>.azurecr.io/rmg-python:latest
```

2. **Deploy to Container App**

```bash
az containerapp update \
  --name rmg-python \
  --resource-group <your-resource-group> \
  --image <your-acr-name>.azurecr.io/rmg-python:latest \
  --set-env-vars KAFKA_BOOTSTRAP_SERVERS=<your-kafka> \
                  KAFKA_TOPIC=mper-input-events \
                  PRODUCER_TYPE=kafka \
                  AZURE_STORAGE_CONNECTION_STRING=<your-storage-connection> \
                  EVENTHUB_CONNECTION_STRING=<your-eventhub-connection>
```

#### Verify Deployment

```bash
# Get Container App URL
az containerapp show \
  --name rmg-python \
  --resource-group <your-resource-group> \
  --query "properties.configuration.ingress.fqdn" \
  --output tsv

# Test health endpoint
curl https://<your-container-app-url>/health
```

## Environment Variables

#### Kafka/Event Hub Configuration

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092    # Kafka broker address
KAFKA_TOPIC=mper-input-events             # Topic name
KAFKA_PARTITIONS=4                        # Number of partitions
PRODUCER_TYPE=kafka                       # 'kafka' or 'eventhub'

# If using Azure Event Hub:
EVENTHUB_CONNECTION_STRING=Endpoint=sb://...
```

#### Azure Blob Storage (for CSV uploads)

```bash
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...
AZURE_STORAGE_CONTAINER_NAME=address-lookup
AZURE_BLOB_PREFIX=                        # Optional prefix for blob names
```

#### API Server Configuration

```bash
API_HOST=0.0.0.0
API_PORT=8000
```

#### Generation Configuration (Optional)

```bash
DEFAULT_SEED=42
DEFAULT_START_DATE=2024-10-01T00:00:00
DEFAULT_END_DATE=2025-10-01T00:00:00
POSTCODE_FILE=                            # Optional custom postcode file
```

## API Endpoints

### Health Check

```http
GET /health
```

Returns service status and configuration details.

**Response:**

```json
{
  "status": "healthy",
  "service": "Event Streaming API",
  "version": "2.0.0",
  "architecture": "stream-on-demand",
  "configuration": {
    "kafka_bootstrap": "localhost:9092",
    "kafka_topic": "mper-input-events",
    "producer_type": "kafka",
    "has_generation_config": true,
    "parcels_configured": 100000
  }
}
```

### Configure Generation

```http
POST /generate
Content-Type: application/json

{
  "parcels": 100000,
  "seed": 42
}
```

Configures the event generation parameters. This endpoint:

- Stores configuration in memory
- Generates and uploads 3 CSV files to Azure Blob Storage
- Does NOT generate events (events are created on-demand when emission endpoints are called)

**Parameters:**

- `parcels` (required): Number of parcels to generate (each has 4 events)
- `seed` (optional): Random seed for reproducibility (default: 42)

**Response:**

```json
{
  "status": "ok",
  "parcels": 100000,
  "seed": 42,
  "events_per_scan": 100001,
  "total_events": 400004,
  "csv": {
    "csv_generated": true,
    "parcel_addresses": 100000,
    "postcodes": 91,
    "postcode_suffixes": 273,
    "uploaded_to_azure": true,
    "uploaded_files": [
      "parcel_address.csv",
      "postcode.csv",
      "postcode_suffix.csv"
    ]
  },
  "message": "Configuration stored for 100,000 parcels..."
}
```

### Emit Events for Specific Scan

```http
POST /scans/{scan_no}/emissions
Content-Type: application/json

{
  "limit": 1000  // optional
}
```

Generates and streams events for a specific scan batch (1, 2, 3, or 4).

**Path Parameters:**

- `scan_no`: Scan number (1=EVDAV, 2=EVIMC, 3=EVGPD, 4=ENKDN)

**Request Body:**

- `limit` (optional): Maximum number of events to emit

**Response:**

```json
{
  "status": "ok",
  "sent": 100001,
  "scan": 1,
  "parcels": 100000,
  "message": "Generated and sent 100,001 events for Scan 1 (magic parcel + 100,000 parcels)"
}
```

## CSV Files Generated

When you call `/generate`, three CSV files are created and uploaded to Azure Blob Storage:

### 1. parcel_address.csv

Parcel address lookup table

**Columns:**

- `uniqueItemId` - Unique 11 or 21-digit barcode
- `add_line1` - Address line 1
- `add_line2` - Address line 2 (flat number, etc.)
- `city` - City name
- `county` - County name
- `country` - Country code (GB)
- `postcode` - UK postcode

### 2. postcode.csv

Postcode location mapping

**Columns:**

- `add_line1` - Address line 1
- `add_line2` - Address line 2
- `city` - City name
- `county` - County name
- `country` - Country code
- `pincode` - Postcode

### 3. postcode_suffix.csv

Postcode suffix variations

**Columns:**

- `add_line1` - Address line variation
- `postcode` - Base postcode
- `po_suffix` - Two-letter suffix

## Configuration Details

### Event Timing Gaps

Events are generated with realistic time gaps between scans:

- **EVDAV → EVIMC**: 1-24 hours
- **EVIMC → EVGPD**: 4-36 hours
- **EVGPD → ENKDN**: 0.5-24 hours

### Contact Information Distribution

- 10% - No contact info
- 10% - Email only
- 10% - Mobile only
- 70% - Both email and mobile

### Account vs. Online Barcodes

- 30% - Account-based (21 digits with embedded 10-digit account ID)
- 70% - Online (11 digits)

### UK Locations

The system includes 95 real UK cities/counties and 91 actual UK postcodes for realistic address generation.

## Project Structure

```
rmg-python/
├── app/
│   ├── main.py                 # Application entry point
│   ├── api.py                  # FastAPI endpoints
│   ├── models.py               # Data models and constants
│   ├── config.py               # Environment configuration
│   ├── config_store.py         # In-memory config management
│   ├── streaming_generator.py  # On-demand event generation
│   ├── streaming_emitter.py    # Event streaming logic
│   ├── kafka_handler.py        # Kafka/Event Hub producer
│   ├── utils.py                # Utility functions
│   ├── emitter.py             # Stats tracking
│   └── logger.py              # Logging configuration
├── logs/                       # Application logs
├── docker-compose.yml         # Docker compose configuration
├── Dockerfile                 # Container definition
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## 🔍 How to Get Azure Credentials

### Event Hub Connection String

1. Go to **Azure Portal**
2. Navigate to your **Event Hubs Namespace**
3. Click **"Shared access policies"**
4. Click **"RootManageSharedAccessKey"** (or create a new policy)
5. Copy **"Connection string–primary key"**

### Blob Storage Connection String

1. Go to **Azure Portal**
2. Navigate to your **Storage Account**
3. Click **"Access keys"** under Security + networking
4. Copy **"Connection string"** from key1 or key2

### Create Storage Container

1. In your Storage Account, go to **"Containers"**
2. Click **"+ Container"**
3. Name it `address-lookup` (or update `.env` with your chosen name)
