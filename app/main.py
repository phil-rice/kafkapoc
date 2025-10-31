"""
LMDB Kafka Streamer - Main Entry Point

FastAPI service for event generation and streaming.
All configuration via environment variables (.env file).
"""

import sys

from .logger import logger
from .config import (
    API_HOST,
    API_PORT,
    AZURE_CONNECTION_STRING,
    AZURE_CONTAINER_NAME,
    EVENTHUB_CONNECTION_STRING,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_PARTITIONS,
    KAFKA_TOPIC,
    LMDB_DB_PATH,
    LMDB_MAP_SIZE_GB,
    PRODUCER_TYPE,
)
from .api import build_app


# Print startup banner
logger.info("=" * 70)
logger.info("  Royal Mail POC - Event Generator and Streamer")
logger.info("=" * 70)
logger.info("")
logger.info("Configuration:")
logger.info(f"  Kafka Bootstrap:         {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"  Kafka Topic:             {KAFKA_TOPIC}")
logger.info(f"  Kafka Partitions:        {KAFKA_PARTITIONS}")
logger.info(f"  Producer Type:           {PRODUCER_TYPE}")
logger.info("")
logger.info(f"Starting API server on http://{API_HOST}:{API_PORT}")
logger.info("=" * 70)
logger.info("")

# Build the FastAPI app (exported for uvicorn)
app = build_app(
    bootstrap=KAFKA_BOOTSTRAP_SERVERS,
    topic=KAFKA_TOPIC,
    partitions=KAFKA_PARTITIONS,
    producer_type=PRODUCER_TYPE,
    eventhub_conn_str=EVENTHUB_CONNECTION_STRING,
)


if __name__ == "__main__":
    # Run with uvicorn when executed directly
    import uvicorn

    uvicorn.run(app, host=API_HOST, port=API_PORT)
