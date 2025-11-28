package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.example.Shema.KafkaSchema;

public class App {
    public static void main(String[] args) throws Exception {
        final String ACCESS_KEY = "admin";
        final String SECRET_KEY = "password";

        Configuration conf = new Configuration();
        conf.setString("fs.s3a.access.key", ACCESS_KEY);
        conf.setString("fs.s3a.secret.key", SECRET_KEY);
        conf.setString("fs.s3a.endpoint", "http://minio:9000");
        conf.setString("fs.s3a.path.style.access", "true");
        conf.setString("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"); // !!!
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String bootstrapServer = "kafka:29092";
        String inputTopic = "raw-trade-topic";
        String outputTopic = "preprocessed-trade-topic";
        String sourceTableName = "source_table";
        String outputTableName = "preprocessed_table";

        KafkaSchema kafkaSchema = new KafkaSchema();

        String catalogName = "iceberg";

        String createCatalogQuery = "CREATE CATALOG " + catalogName + " WITH ("
                + "'type'='iceberg',"
                + "'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',"
                + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
                + "'uri'='http://catalog:19120/api/v1',"
                + "'authentication.type'='none',"
                + "'ref'='main',"
                + "'client.assume-role.region'='us-east-1',"
                + "'warehouse' = 's3a://silver/',"
                + "'s3.endpoint'='http://minio:9000',"
                + "'s3.path-style-access'='true'"
                + ")";
        TableResult result = tEnv.executeSql(createCatalogQuery);
        tEnv.useCatalog(catalogName);
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS silver_db");
        tEnv.executeSql("USE silver_db");
        result.print();

        tEnv.createTemporaryTable(sourceTableName, TableDescriptor.forConnector("kafka")
                .schema(kafkaSchema.kafkaSourceSchema())
                .option("topic", inputTopic)
                .option("properties.bootstrap.servers", bootstrapServer)
                .option("scan.startup.mode", "earliest-offset")
                .format(FormatDescriptor.forFormat("json")
                        .option("fail-on-missing-field", "false")
                        .option("ignore-parse-errors", "true")
                        .build())
                .build());

        String createPreprocessedTable =
                "CREATE TABLE IF NOT EXISTS " + outputTableName + " ("
                        + "  symbol STRING NOT NULL,"
                        + "  price DECIMAL(10, 2),"
                        + "  volume BIGINT,"
                        + "  trade_type STRING,"
                        + "  unix_ts BIGINT,"
                        + "  PRIMARY KEY (symbol) NOT ENFORCED"
                        + ") "
                        + "WITH ("
                        + "  'format-version'='2',"
                        + "  'write.format.default'='parquet'"
                        + ")";

        tEnv.executeSql(createPreprocessedTable);

        tEnv.executeSql("SHOW TABLES");

        String flattenQuery = "SELECT \n"
                + "s AS symbol, \n"
                + "p AS price, \n"
                + "v AS volume, \n"
                + "`type` AS trade_type, \n"
                + "t AS unix_ts \n"
                + "FROM `" + sourceTableName + "`\n"
                + "CROSS JOIN UNNEST(data) AS d(c, p, s, t, v)";
        Table flattenTable = tEnv.sqlQuery(flattenQuery);
        String flattenTableName = "flatten_table";
        tEnv.createTemporaryView(flattenTableName, flattenTable);
        String deduplicateQuery = "SELECT symbol, price, volume, trade_type, unix_ts \n"
                + "FROM (\n"
                + "SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol, volume, unix_ts ORDER BY price DESC) AS rn\n"
                + "FROM " + flattenTableName
                + " ) AS temp WHERE rn = 1";

        // Stream thì ko dùng wait. Phải dùng executeInsert thay vì insertInto
//        flattenTable.executeInsert(outputTableName);
        Table deduplicateTable = tEnv.sqlQuery(deduplicateQuery);
        deduplicateTable.executeInsert(outputTableName);
    }
}
