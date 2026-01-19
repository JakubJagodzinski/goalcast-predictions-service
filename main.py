import asyncio
import logging

from events.PredictionRequestEventConsumer import PredictionRequestEventConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


async def main():
    consumer = PredictionRequestEventConsumer()

    try:
        await consumer.consume_and_predict()
    except KeyboardInterrupt:
        logging.info("\nInterrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        logging.info("Cleanup complete")


if __name__ == "__main__":
    asyncio.run(main())
