from dataclasses import dataclass

from services.flink_service import FlinkService


@dataclass
class TradeHandler(FlinkService):
    kafka_broker: str

    def __post_init__(self):
        return super().__post_init__()
