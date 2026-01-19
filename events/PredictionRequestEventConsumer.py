import json
import logging

from config.kafka.consumer.consumer import get_kafka_consumer, close_kafka_consumer
from config.kafka.producer.producer import get_kafka_producer, close_kafka_producer
from model.predict import predict_winner


class PredictionRequestEventConsumer:
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.prediction_requests_topic = "predictions.requests.topic"

    async def consume_and_predict(self):
        try:
            self.consumer = await get_kafka_consumer(self.prediction_requests_topic)
            self.producer = await get_kafka_producer()

            logging.info("Started consuming prediction requests...")

            async for msg in self.consumer:
                try:
                    data = json.loads(msg.value)
                    logging.info(f"Processing: {data}")

                    home_team_win, away_team_win, draw = predict_winner(
                        home_team=data.get('home_team'),
                        away_team=data.get('away_team'),
                        model_name=data.get('model_name')
                    )

                    error_occurred = all([
                        home_team_win == 0,
                        away_team_win == 0,
                        draw == 0]
                    )

                    result = json.dumps({
                        'request_id': data.get('request_id'),
                        'model_name': data.get('model_name'),
                        'home_team': data.get('home_team'),
                        'away_team': data.get('away_team'),
                        'home_team_win': home_team_win,
                        'away_team_win': away_team_win,
                        'draw': draw,
                        'error_occurred': error_occurred
                    })

                    await self.producer.send('predictions.responses.topic', value=result)
                    await self.producer.flush()

                    await self.consumer.commit()

                    logging.info("Prediction response event produced successfully.")
                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON: {e}")
                    await self.consumer.commit()

                except Exception as e:
                    logging.error(f"Processing error: {e}")
                    import traceback
                    traceback.print_exc()

        except Exception as e:
            logging.error(f"Consumer error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            logging.info("Shutting down...")
            await close_kafka_consumer()
            await close_kafka_producer()
