import os

from dotenv import load_dotenv

from services.trade_handler import TradeHandler
from schema.kafka_source_schema import KafkaSchema

load_dotenv()

base_dir = "/opt/flink/lib"
jars = [
    "flink-sql-connector-kafka-3.4.0-1.20",
    "kafka-clients-4.1.0",
    "hudi-flink-bundle_2.12-0.10.1",
    "flink-s3-fs-presto-1.20.2",
    "flink-s3-fs-hadoop-1.20.2",
    "flink-metrics-prometheus-1.20.2"
]

full_path = [os.path.join(base_dir, f"{jar}.jar") for jar in jars]

S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
s3_endpoint = "minio:9000"

settings = {
    "python.execution-mode": "process",
    "s3.access-key": S3_ACCESS_KEY,
    "s3.secret-key": S3_SECRET_KEY,
    "s3.endpoint": s3_endpoint,
    "s3.path.style.access": str(True)
}

kafka_broker = "kafka:29092"
kafka_schema = KafkaSchema()

handler = TradeHandler(jars_path=full_path,
                       settings=settings,
                       kafka_broker=kafka_broker,
                       kafka_schema=kafka_schema)
input_topic = "raw-trade-topic"
preprocess_topic = "preprocess"
preprocess_data = handler.process_trades_with_prometheus(input_topic=input_topic, output_topic=preprocess_topic)
preprocess_data.execute().wait()
