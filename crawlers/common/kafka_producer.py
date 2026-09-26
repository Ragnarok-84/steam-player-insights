import json
import logging
from kafka import KafkaProducer
from crawlers.config import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

class SteamKafkaProducer:
    def __init__(self, bootstrap_servers=None):
        servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.producer = KafkaProducer(
            bootstrap_servers=servers,
            key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            acks="all",
            compression_type="gzip",
        )
        logger.info(f"Connected to Kafka brokers at: {servers}")

    def send(self, topic: str, key: str, value: dict):
        try:
            future = self.producer.send(topic, key=key, value=value)
            return future
        except Exception as e:
            logger.error(f"Error publishing message to {topic} [key={key}]: {e}")
            raise e

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()
