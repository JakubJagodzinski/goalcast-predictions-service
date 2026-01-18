import json
import logging

from config.kafka.consumer.consumer import get_kafka_consumer
from events.prediction_response_event_producer import send_prediction_response_event
from model.predict import predict_winner

topic: str = "predictions.requests.topic"


async def consume_and_predict():
    logging.info("Starting to consume events")

    consumer = await get_kafka_consumer(topic)

    async for msg in consumer:
        logging.info(f"Received message: {msg.value}")

        data = json.loads(msg.value)

        request_id = data["request_id"]
        home_team = data["home_team"]
        away_team = data["away_team"]

        winner = predict_winner(
            home_team=home_team,
            away_team=away_team
        )

        await send_prediction_response_event(
            request_id=request_id,
            home_team=home_team,
            away_team=away_team,
            winner=winner
        )
