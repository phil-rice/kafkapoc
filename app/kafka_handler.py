"""
Kafka/Event Hub producer handler
"""

from .logger import logger
import threading

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None


class KafkaOut:
    """High-throughput Kafka/Event Hub producer"""

    def __init__(
        self,
        bootstrap: str,
        topic: str,
        partitions: int,
        acks: str = "all",
        producer_type: str = "kafka",
        eventhub_conn_str: str = None,
    ):
        """
        Initialize Kafka producer

        Args:
            bootstrap: Bootstrap servers
            topic: Topic name
            partitions: Number of partitions for the topic
            acks: Acknowledgment level
            producer_type: 'kafka' or 'eventhub'
            eventhub_conn_str: Event Hub connection string (if producer_type='eventhub')
        """
        if Producer is None:
            raise RuntimeError(
                "confluent-kafka not installed. Install it with: pip install confluent-kafka"
            )

        self.topic = topic
        self.producer_type = producer_type
        self.partitions = partitions

        # Base config for high throughput
        conf = {
            "acks": acks,
            "linger.ms": 50,
            "batch.size": 1000000,
            "batch.num.messages": 50000,
            "queue.buffering.max.messages": 4194304,
            "message.max.bytes": 10 * 1024 * 1024,
            "max.in.flight.requests.per.connection": 5,
        }

        if producer_type == "eventhub":
            # Azure Event Hub configuration
            if not eventhub_conn_str:
                raise RuntimeError(
                    "Event Hub connection string required for producer_type='eventhub'"
                )

            # Parse namespace from connection string or bootstrap servers
            if "servicebus.windows.net" in bootstrap:
                namespace = bootstrap.split(".")[0]
                bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"
            else:
                bootstrap_servers = bootstrap

            conf.update(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "PLAIN",
                    "sasl.username": "$ConnectionString",
                    "sasl.password": eventhub_conn_str,
                    "client.id": "lmdb-kafka-streamer",
                }
            )
            logger.info(f"Connecting to Azure Event Hub: {bootstrap_servers}")
        else:
            # Standard Kafka configuration
            conf.update(
                {
                    "bootstrap.servers": bootstrap,
                    "enable.idempotence": True,
                    "compression.type": "zstd",  # Standard Kafka supports zstd
                }
            )
            logger.info(f"Connecting to Kafka: {bootstrap}")

        self.p = Producer(conf)
        self.outstanding = 0
        self.lock = threading.Lock()  # Add lock for thread safety

    def _delivery_callback(self, err, msg):
        """Callback invoked when message is acknowledged by Kafka"""
        with self.lock:
            self.outstanding -= 1
        if err:
            logger.error(f"Message delivery failed: {err}")

    def produce(
        self, value: bytes, event_time_ms: int, key: bytes = None, headers: list = None
    ):
        """
        Produce message with key and headers

        Args:
            value: XML message bytes
            event_time_ms: Event timestamp in milliseconds
            key: Message key (typically uniqueItemId)
            headers: List of (name, value) tuples for Kafka headers
        """
        # Use hash-based partitioning for better distribution
        partition = hash(key) % self.partitions if key else None

        self.p.produce(
            self.topic,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp=event_time_ms,
            callback=self._delivery_callback,  # Add callback
        )
        with self.lock:
            self.outstanding += 1

        # Poll to handle callbacks and prevent buffer overflow
        if self.outstanding % 1000 == 0:
            self.p.poll(0)

    def flush(self):
        """Flush all outstanding messages"""
        while self.outstanding > 0:
            self.p.poll(0.1)
        self.p.flush()
