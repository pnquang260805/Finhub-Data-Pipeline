import os

from dotenv import load_dotenv

from services.finnhub_service import FinnhubService
from services.kafka_service import KafkaService
from services.redis_lookup import RedisLookup

load_dotenv()

FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN")

if __name__ == "__main__":
    bootstrap = "localhost:9092"
    kafka_service = KafkaService()
    kafka_service.init_producer(bootstrap)

    host = "localhost"
    port = 8003
    redis_lookup = RedisLookup(host, port)

    symbols = ["AAPL", "AMZN", "MSFT", "GOOGL", "META"]
    raw_topic = "raw-trade-topic"

    finnhub_service = FinnhubService(
        FINNHUB_TOKEN, symbols, raw_topic, kafka_service, redis_lookup)
    finnhub_service.run()
