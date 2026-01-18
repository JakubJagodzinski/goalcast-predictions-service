import json
import logging
from datetime import datetime, timezone

from config.kafka.producer.producer import get_kafka_producer

topic: str = "predictions.responses.topic"


async def send_prediction_response_event(request_id: str, home_team: str, away_team: str, winner: str):
    logging.info(f"Sending prediction response event for {home_team} vs {away_team} (request_id: {request_id})")

    event_data = {
        "event_type": "PREDICTION_RESPONSE",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "request_id": request_id,
        "home_team": home_team,
        "away_team": away_team,
        "winner": winner
    }

    logging.info(f"Event data: {event_data}")

    producer = await get_kafka_producer()
    message = json.dumps(event_data)

    await producer.send_and_wait(
        topic,
        value=message,
        headers=[
            ("eventCategory", b"PREDICTION_RESPONSE_EVENT"),
            ("eventType", b"PREDICTION_RESPONSE")
        ]
    )

    logging.info(f"Prediction response event for {home_team} vs {away_team} (request_id: {request_id}) sent successfully")
