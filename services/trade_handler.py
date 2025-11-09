from dataclasses import dataclass
from pyflink.table import Schema, DataTypes, TableDescriptor, FormatDescriptor, Table
from pyflink.table.udf import udf, ScalarFunction, FunctionContext

from defined_functions.price_udf import SymbolPriceUdf
from services.flink_service import FlinkService


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

    def __kafka_source_schema_register(self, input_topic: str, source_table_name: str = "kafka_src_table"):
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

    def __preprocessed_schema_register(self, output_topic: str, preprocessed_table_name: str = "preprocess_table"):
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
                        push_price(CAST(s AS STRING), p) AS symbol,
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
        self.t_env.create_temporary_function("push_price", udf(SymbolPriceUdf(), result_type="STRING"))
        self.__kafka_source_schema_register(input_topic, "kafka_src_table")
        self.__preprocessed_schema_register(output_topic)
        preprocess_table = self.__preprocess_data("kafka_src_table")
        statement_set = self.t_env.create_statement_set()
        # def __preprocessed_schema_register(self, output_topic: str, preprocessed_table_name: str = "preprocess_table"):
        statement_set.add_insert("preprocess_table", preprocess_table)

        return statement_set.execute().wait()