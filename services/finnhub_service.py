import json
import websocket

from datetime import datetime
from typing import List

from services.kafka_service import KafkaService
from services.redis_lookup import RedisLookup


class FinnhubService:
    def __init__(self, token: str, symbols: List[str], output_topic: str, kafka_service: KafkaService, redis_lookup: RedisLookup):
        self.symbols = symbols
        self.kafka_service = kafka_service
        self.kafka_output_topic = output_topic
        self.token = token
        self.redis_lookup = redis_lookup


    def __on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data["type"] == "ping":
                return

            max_value = max([p["p"] for p in data["data"]])
            min_value = min([p["p"] for p in data["data"]])
            symbol = data["data"][0]["s"]

            today = datetime.now().strftime("%Y%m%d")
            high_key = f"{today}-{symbol}-h"
            low_key = f"{today}-{symbol}-l"
            print(high_key, low_key)
            req_high = self.redis_lookup.get_key(high_key)
            req_low = self.redis_lookup.get_key(low_key)
            if req_high.status_code == 200:
                self.redis_lookup.set_key(key=high_key, value=max(max_value, float(req_high.json()["value"])))
            else:
                self.redis_lookup.set_key(key=high_key, value=max_value)

            if req_low.status_code == 200:
                self.redis_lookup.set_key(key=low_key, value=min(min_value, float(req_low.json()["value"])))
            else:
                self.redis_lookup.set_key(key=low_key, value=min_value)

            serialized = json.dumps(data)
            self.kafka_service.send_message(
                serialized, self.kafka_output_topic)
        except Exception as e:
            print(e)

    def __on_error(self, ws, error):
        print(f"Error: {error}")

    def __on_close(self, ws, close_status_code, close_msg):
        print("Connection closed")

    def __on_open(self, ws):
        for symbol in self.symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

    def run(self):
        socket = f"wss://ws.finnhub.io?token={self.token}"
        ws = websocket.WebSocketApp(
            socket, on_close=self.__on_close, on_error=self.__on_error, on_message=self.__on_message)
        ws.on_open = self.__on_open
        ws.run_forever()
