from pyflink.table.schema import Schema
from pyflink.table import DataTypes


class KafkaSchema:
    def kafka_source_schema(self) -> Schema:
        return (Schema.new_builder()
            .column("data", DataTypes.ARRAY(
                DataTypes.ROW([
                    DataTypes.FIELD("c", DataTypes.ARRAY(DataTypes.STRING())),
                    DataTypes.FIELD("p", DataTypes.DECIMAL(10, 2)),
                    DataTypes.FIELD("s", DataTypes.STRING()),
                    DataTypes.FIELD("t", DataTypes.BIGINT()),
                    DataTypes.FIELD("v", DataTypes.BIGINT())
                ])
            )).column("type", DataTypes.STRING())
            .build())

    def preprocessed_schema(self) -> Schema:
        return (Schema.new_builder()
                .column("symbol", DataTypes.STRING())
                .column("price", DataTypes.DECIMAL(10, 2))
                .column("volume", DataTypes.BIGINT())
                .column("trade_type", DataTypes.STRING())
                .column("unix_ts", DataTypes.BIGINT())
                .build())