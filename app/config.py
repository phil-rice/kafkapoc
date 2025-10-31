"""
Configuration module - loads settings from environment variables
"""

import os
from typing import Optional

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, will use system environment variables

# LMDB Configuration
LMDB_DB_PATH: str = os.getenv("LMDB_DB_PATH", "./event_buffer_lmdb")
LMDB_MAP_SIZE_GB: int = int(os.getenv("LMDB_MAP_SIZE_GB", "64"))

# Kafka/Event Hub Configuration
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "mper-input-events")
KAFKA_PARTITIONS: int = int(os.getenv("KAFKA_PARTITIONS", "1"))
PRODUCER_TYPE: str = os.getenv("PRODUCER_TYPE", "kafka")  # kafka or eventhub

# Event Hub Configuration (if using Event Hubs)
EVENTHUB_CONNECTION_STRING: Optional[str] = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAMESPACE: Optional[str] = os.getenv("EVENTHUB_NAMESPACE")

# Azure Blob Storage Configuration (optional, for CSV uploads)
AZURE_CONNECTION_STRING: Optional[str] = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME: str = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "address-lookup")
AZURE_BLOB_PREFIX: str = os.getenv("AZURE_BLOB_PREFIX", "")

# API Server Configuration
API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
API_PORT: int = int(os.getenv("API_PORT", "8000"))

# Postcode File (optional)
POSTCODE_FILE: str = os.getenv("POSTCODE_FILE", "")

# Generation Configuration
DEFAULT_SEED: int = 42
DEFAULT_START_DATE: str = "2024-10-01T00:00:00"
DEFAULT_END_DATE: str = "2025-10-01T00:00:00"
DEFAULT_CHUNK_SIZE: int = 5000

# Azure Blob Storage availability check
try:
    from azure.storage.blob import BlobServiceClient

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

# Kafka availability check
try:
    from confluent_kafka import Producer

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
