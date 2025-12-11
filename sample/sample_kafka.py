import json
from confluent_kafka import Producer
from datetime import datetime
import random
import time

conf = {
    "bootstrap.servers": "localhost:9092",
    "security.protocol": "PLAINTEXT"
}

producer = Producer(conf)

symbols = ["AAPL", "AMZN", "META"]

def gen_msg():
    symbol = random.choice(symbols)
    ts = int(datetime.now().timestamp())
    price = round(random.random() * 1000, 2)
    data = {
        "data": [
            {
                "c": [
                    "1",
                    "8"
                ],
                "p": price,
                "s": symbol,
                "t": ts,
                "v": random.randint(100, 500)
            },
            {
                "c": [
                    "1",
                    "8"
                ],
                "p": price,
                "s": symbol,
                "t": ts,
                "v": random.randint(100, 500)
            }
        ],
        "type": "trade"
    }
    return data

topic = "raw-trade-topic"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


print("📤 Sending message ...")

try:
    n = random.randint(1, 300)
    # n = 1
    for i in range(n):
        print(i)
        serialized = json.dumps(gen_msg())
        producer.produce(topic, value=serialized, callback=delivery_report)
        producer.flush()
        time.sleep(1)
    print(f"Sent successfully.")
except Exception as e:
    print(f"Failed to send message: {e}")
