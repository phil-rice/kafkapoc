"""
FastAPI application with REST endpoints
Stream-on-demand architecture: generate events on-the-fly and stream directly
"""

import multiprocessing
import os
import tempfile
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .logger import logger
from .config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_END_DATE,
    DEFAULT_SEED,
    DEFAULT_START_DATE,
    POSTCODE_FILE,
)
from .config_store import GenerationConfig, get_config_store
from .streaming_emitter import emit_scan_streaming
from .streaming_generator import generate_and_upload_csvs
from .kafka_handler import KafkaOut
from .utils import load_postcodes


# Request models
class GenerateRequest(BaseModel):
    """Request model for /generate endpoint"""

    parcels: int = Field(
        ..., gt=0, description="Number of parcels to generate (each has 4 events)"
    )
    seed: int = Field(DEFAULT_SEED, description="Random seed for reproducibility")


class EmitRequest(BaseModel):
    """Request model for emission endpoints"""

    limit: Optional[int] = Field(
        None, gt=0, description="Optional limit on number of events to emit"
    )


def build_app(
    bootstrap: str,
    topic: str,
    partitions: int,
    postcode_file: str = "",
    producer_type: str = "kafka",
    eventhub_conn_str: str = None,
):
    """
    Build FastAPI application (stream-on-demand architecture)

    Args:
        bootstrap: Kafka bootstrap servers
        topic: Kafka topic name
        partitions: Number of partitions
        postcode_file: Optional postcode file path
        producer_type: 'kafka' or 'eventhub'
        eventhub_conn_str: Event Hub connection string

    Returns:
        FastAPI application instance
    """
    app = FastAPI(
        title="Event Streaming API",
        description="Stream-on-Demand Event Generator for Royal Mail POC",
        version="2.0.0",
    )

    # Enable CORS for React frontend
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Kafka producer (no database needed)
    OUT = KafkaOut(
        bootstrap,
        topic,
        partitions,
        producer_type=producer_type,
        eventhub_conn_str=eventhub_conn_str,
    )

    # Configuration store
    config_store = get_config_store()

    @app.get("/health")
    def health_check():
        """Health check endpoint"""
        config = config_store.get_config()
        return {
            "status": "healthy",
            "service": "Event Streaming API",
            "configuration": {
                "kafka_bootstrap": bootstrap,
                "kafka_topic": topic,
                "producer_type": producer_type,
                "has_generation_config": config_store.has_config(),
            },
        }

    @app.post("/generate")
    def generate(request: GenerateRequest):
        """
        Configure event generation parameters (no storage, events generated on-demand).

        This endpoint stores configuration only. Events are generated when emission endpoints are called.

        Request Body:
        - parcels: Number of parcels to generate (each has 4 events)
        - seed: Random seed for reproducibility (default: 42)
        """
        # Use defaults from config
        start = datetime.fromisoformat(DEFAULT_START_DATE).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(DEFAULT_END_DATE).replace(tzinfo=timezone.utc)

        # Load postcodes
        postcodes = load_postcodes(postcode_file)

        # Create site/func IDs
        site_pool_size = 500
        site_ids = [str(i + 1).zfill(6) for i in range(site_pool_size)]
        func_ids = [str(i + 1) for i in range(site_pool_size)]

        # Store configuration
        config = GenerationConfig(
            parcels=request.parcels,
            seed=request.seed,
            start_date=start,
            end_date=end,
            postcodes=postcodes,
            site_ids=site_ids,
            func_ids=func_ids,
            total_events=request.parcels * 4 + 4,  # +4 for magic parcel
            generated_at=datetime.now(timezone.utc),
        )

        config_store.set_config(config)

        logger.success(
            f"Configuration stored: {request.parcels:,} parcels, seed={request.seed}, "
            f"total events per scan: {request.parcels + 1:,}"
        )

        # Generate and upload CSVs to Azure Blob
        csv_result = generate_and_upload_csvs(config)

        return {
            "status": "ok",
            "parcels": request.parcels,
            "seed": request.seed,
            "events_per_scan": request.parcels + 1,  # +1 for magic parcel
            "total_events": request.parcels * 4 + 4,
            "csv": csv_result,
            "message": (
                f"Configuration stored for {request.parcels:,} parcels. "
                f"CSVs generated and uploaded to Azure Blob. "
                f"Events will be generated on-demand when emission endpoints are called."
            ),
        }

    @app.post("/scans/{scan_no}/emissions")
    def emit_scan_endpoint(scan_no: int, request: EmitRequest = Body(EmitRequest())):
        """
        Generate and emit events for a specific scan (stream-on-demand).
        Magic parcel is emitted first, followed by all configured parcels.

        Path Parameters:
        - scan_no: Scan number (1, 2, 3, or 4)

        Request Body (optional):
        - limit: Optional limit on number of events to emit
        """
        if scan_no not in (1, 2, 3, 4):
            return {"status": "error", "error": "scan_no must be 1, 2, 3, or 4"}

        # Check if configuration exists
        config = config_store.get_config()
        if not config:
            return {
                "status": "error",
                "error": "No generation configuration found. Call /generate first to configure parcels.",
            }

        # Stream events directly to Kafka
        logger.info(
            f"Starting stream for scan {scan_no} with {config.parcels:,} parcels"
        )
        sent = emit_scan_streaming(OUT, config, scan_no, request.limit)

        return {
            "status": "ok",
            "sent": sent,
            "scan": scan_no,
            "parcels": config.parcels,
            "message": f"Generated and sent {sent:,} events for Scan {scan_no} (magic parcel + {config.parcels:,} parcels)",
        }

    return app
