from dataclasses import dataclass
from pyflink.table import Schema, DataTypes, TableDescriptor, FormatDescriptor, Table
from pyflink.table.udf import udf, ScalarFunction, FunctionContext
from pyflink.datastream.functions import RuntimeContext

from services.flink_service import FlinkService


class TrackThroughputFunction(ScalarFunction):
    def __init__(self):
        self.price_gauge = {}
        self.metric_group = None
        self.price_state = {}  # lưu giá trị thực tế để gauge đọc

    def open(self, function_context: FunctionContext):
        self.metric_group = function_context.get_metric_group()

    def eval(self, symbol: str, price: float):
        print(f"Updating gauge for {symbol} = {price}")
        if symbol not in self.price_gauge:
            self.price_state[symbol] = price  # khởi tạo state
            # Lambda đọc từ self.price_state
            self.price_gauge[symbol] = self.metric_group.add_group("symbol", symbol).gauge(
                "last_price", lambda sym=symbol: self.price_state[sym]
            )
        else:
            self.price_state[symbol] = price  # update giá trị

        return symbol

# đăng ký UDF (khi tạo function wrapper)
track_throughput_udf = udf(TrackThroughputFunction(), result_type=DataTypes.STRING())

@dataclass
class TradeHandler(FlinkService):
    kafka_broker: str

    def __post_init__(self):
        return super().__post_init__()

    def __kafka_source_schema(self) -> Schema:
        return (Schema.new_builder()
                .column("data", DataTypes.ARRAY(
            DataTypes.ROW([
                DataTypes.FIELD("c", DataTypes.ARRAY(DataTypes.STRING())),
                DataTypes.FIELD("p", DataTypes.DECIMAL(10, 2)),
                DataTypes.FIELD("s", DataTypes.STRING()),
                DataTypes.FIELD("t", DataTypes.BIGINT()),
                DataTypes.FIELD("v", DataTypes.BIGINT())
            ])
        ))
                .column("type", DataTypes.STRING())
                .build())

    def __kafka_source_table_register(self, input_topic: str, source_table_name: str):
        self.t_env.create_temporary_table(source_table_name,
                                          TableDescriptor.for_connector(connector="kafka")
                                          .schema(self.__kafka_source_schema())
                                          .option("topic", input_topic)
                                          .option('properties.bootstrap.servers', self.kafka_broker)
                                          .option('properties.group.id', 'transaction_group')
                                          .option('scan.startup.mode', 'latest-offset')
                                          .format("json")
                                          .option("json.ignore-parse-errors", "true")
                                          .build()
                                          )

    def __preprocessed_schema(self) -> Schema:
        return (Schema.new_builder()
                .column("symbol", DataTypes.STRING())
                .column("price", DataTypes.DECIMAL(10, 2))
                .column("volume", DataTypes.BIGINT())
                .column("trade_type", DataTypes.STRING())
                .column("unix_ts", DataTypes.BIGINT())
                .build())

    def __preprocessed_schema_register(self, output_topic: str, preprocessed_table_name: str):
        self.t_env.create_temporary_table(preprocessed_table_name,
                                          TableDescriptor.for_connector(connector="kafka").schema(
                                              self.__preprocessed_schema())
                                          .option("properties.bootstrap.servers", self.kafka_broker)
                                          .format(FormatDescriptor.for_format('json')
                                                  .build())
                                          .option("topic", output_topic)
                                          .build()
                                          )

    def __preprocess_data(self, src_table_name: str) -> Table:
        extracted_query = f"""
                    SELECT 
                        track_throughput(CAST(s AS STRING), p) AS symbol,
                        p AS price,
                        v AS volume,
                        `type` AS trade_type,
                        t AS unix_ts
                    FROM `{src_table_name}`
                    CROSS JOIN UNNEST(data) AS t(c, p, s, t, v)
                """
        extracted_table = self.t_env.sql_query(extracted_query)
        return extracted_table

    def process_trades_with_prometheus(self, input_topic: str, output_topic: str):
        """
        Process trades với Prometheus metrics - Sử dụng UDF approach
        """
        source_table_name = "source_trades"
        preprocessed_table_name = "preprocessed_trades"

        # Đăng ký bảng
        self.__kafka_source_table_register(input_topic, source_table_name)
        self.__preprocessed_schema_register(output_topic, preprocessed_table_name)

        # Đăng ký UDF cho metrics tracking
        self.t_env.create_temporary_function("track_throughput", track_throughput_udf)

        processed_table = self.__preprocess_data(source_table_name)

        # Execute
        statement_set = self.t_env.create_statement_set()
        statement_set.add_insert(preprocessed_table_name, processed_table)

        return statement_set.execute().wait()
