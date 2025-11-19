package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.Shema.KafkaSchema;

public class App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String bootstrapServer = "kafka:29092";
        String inputTopic = "raw-trade-topic";
        String outputTopic = "preprocessed-trade-topic";
        String sourceTableName = "source_table";
        String outputTableName = "preprocessed_table";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServer)
                .setTopics(inputTopic)
                .setGroupId("foo")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSchema kafkaSchema = new KafkaSchema();
        tEnv.createTemporaryTable(sourceTableName, TableDescriptor.forConnector("kafka")
                .schema(kafkaSchema.kafkaSourceSchema())
                .option("topic", inputTopic)
                .option("properties.bootstrap.servers", bootstrapServer)
                .option("scan.startup.mode", "latest-offset")
                .format(FormatDescriptor.forFormat("json")
                        .option("fail-on-missing-field", "false")
                        .option("ignore-parse-errors", "true")
                        .build())
                .build());
        tEnv.createTemporaryTable(outputTableName, TableDescriptor.forConnector("upsert-kafka")
                .schema(kafkaSchema.preprocessedKafkaSourceSchema())
                        .option("topic", outputTopic)
                        .option("properties.bootstrap.servers", bootstrapServer)
                        .option("key.format", "json")
                        .option("value.format", "json")
                .build());

        String flattenQuery = "SELECT \n"
                + "s AS symbol, \n"
                + "p AS price, \n"
                + "v AS volume, \n"
                + "`type` AS trade_type, \n"
                + "t AS unix_ts \n"
                + "FROM `" + sourceTableName + "`\n"
                + "CROSS JOIN UNNEST(data) AS d(c, p, s, t, v)";
        Table flattenTable = tEnv.sqlQuery(flattenQuery);
        // Stream thì ko dùng wait. Phải dùng executeInsert thay vì insertInto
        flattenTable.executeInsert(outputTableName);
    }
}
