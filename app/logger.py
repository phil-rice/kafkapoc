"""
Logging configuration using loguru
"""

import sys
from loguru import logger

# Remove default handler
logger.remove()

# Add custom handler with formatting
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <level>{message}</level>",
    level="INFO",
    colorize=True,
)

# Add file handler with rotation
logger.add(
    "logs/lmdb_kafka_streamer_{time:YYYY-MM-DD}.log",
    rotation="00:00",  # Rotate daily at midnight
    retention="30 days",  # Keep logs for 30 days
    compression="zip",  # Compress old logs
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
    level="DEBUG",
)

# Export configured logger
__all__ = ["logger"]
