from aiokafka import AIOKafkaProducer

from config.kafka.config import KAFKA_BOOTSTRAP_SERVERS

producer: AIOKafkaProducer | None = None


async def get_kafka_producer() -> AIOKafkaProducer:
    global producer
    if producer is None:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            acks='all',
            request_timeout_ms=60000,  # 60 seconds
            max_request_size=1048576,  # 1 MB
            linger_ms=10,  # Wait 10ms to batch messages
            max_batch_size=16384,  # 16 KB batches
            value_serializer=lambda v: v.encode("utf-8")
        )
        await producer.start()
    return producer


async def close_kafka_producer():
    global producer
    if producer:
        await producer.stop()
        producer = None
