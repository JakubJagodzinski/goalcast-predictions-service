from aiokafka import AIOKafkaConsumer

from config.kafka.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID

consumer: AIOKafkaConsumer | None = None


async def get_kafka_consumer(topic: str) -> AIOKafkaConsumer:
    global consumer
    if consumer is None:
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: v.decode("utf-8")
        )
        await consumer.start()
    return consumer


async def close_kafka_consumer():
    global consumer
    if consumer:
        await consumer.stop()
        consumer = None
