import asyncio
import logging

from events import prediction_request_event_consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


async def main():
    await prediction_request_event_consumer.consume_and_predict()


if __name__ == '__main__':
    logging.info("Starting the model service")

    asyncio.run(main())

    logging.info("Model service stopped")
