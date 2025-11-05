import requests

from dataclasses import dataclass

@dataclass
class RedisLookup:
    host: str = "localhost"
    port: int = 8003

    def __post_init__(self):
        self.base_url = f"http://{self.host}:{self.port}/q?"

    def get_key(self, key: str):
        url = f"{self.base_url}" + f"key={key}"
        req = requests.get(url)
        return req

    def set_key(self, key : str, value : str):
        url = f"{self.base_url}" + f"key={key}" + f"&value={value}"
        requests.post(url)