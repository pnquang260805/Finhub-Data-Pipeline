import os

from dotenv import load_dotenv

from services.trade_handler import TradeHandler

load_dotenv()

base_dir = "/opt/flink/usrlib"
jars = [
    "flink-sql-connector-kafka-4.0.1-2.0",
    "kafka-clients-4.1.0",
    "hudi-flink-bundle_2.12-0.10.1",
    "flink-s3-fs-presto-1.20.0",
    "flink-s3-fs-hadoop-1.20.0"
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
    "s3.path.style.access": True
}

kafka_broker = "kafka:29092"

handler = TradeHandler(jars_path=full_path,
                       settings=settings, kafka_broker=kafka_broker)
