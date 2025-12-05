package org.example;

import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.Interfaces.CatalogService;
import org.example.Service.FlinkService;
import org.example.Service.IcebergCatalogService;
import org.example.Shema.KafkaSchema;

import java.util.HashMap;
import java.util.Map;

public class App {
    public static void main(String[] args) throws Exception {
        FlinkService flinkService = new FlinkService();

        final String ACCESS_KEY = "admin";
        final String SECRET_KEY = "password";
        Map<String, String> configs = new HashMap<>();
        configs.put("fs.s3a.access.key", ACCESS_KEY);
        configs.put("fs.s3a.secret.key", SECRET_KEY);
        configs.put("fs.s3a.endpoint", "http://minio:9000");
        configs.put("fs.s3a.path.style.access", "true");
        configs.put("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

        flinkService.setString(configs);
        flinkService.createTableEnv();
        StreamTableEnvironment tEnv = flinkService.getTEnv();

        CatalogService catalogService = new IcebergCatalogService(flinkService);

        String bootstrapServer = "kafka:29092";
        String inputTopic = "raw-trade-topic";
        String outputTopic = "preprocessed-trade-topic";
        String sourceTableName = "source_table";
        String icebergProcessedTable = "iceberg_processed_table";
        String kafkaProcessedTable = "kafka_processed_table";
        String goldTable = "stock_detail";

        KafkaSchema kafkaSchema = new KafkaSchema();

        // Tạo catalog trước
        String catalogName = "iceberg";
        String warehouseDir = "s3a://silver/";
        String silverDbName = "silver_db";

        catalogService.createCatalog(catalogName, warehouseDir);
        tEnv.executeSql("USE CATALOG " + catalogName);
        catalogService.createDatabase(silverDbName);
        tEnv.executeSql("USE silver_db");

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

        tEnv.createTemporaryTable(kafkaProcessedTable, TableDescriptor.forConnector("upsert-kafka")
                .schema(kafkaSchema.kafkaProcessedSchema())
                .option("topic", outputTopic)
                .option("properties.bootstrap.servers", bootstrapServer)
                .option("key.format", "json")
                .option("value.format", "json")
                .build()
        );

        String createPreprocessedTable =
                "CREATE TABLE IF NOT EXISTS " + icebergProcessedTable + " ("
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
        Table deduplicateTable = tEnv.sqlQuery(deduplicateQuery);

        // Create statement set
        StatementSet statementSet = tEnv.createStatementSet();

        statementSet.addInsert(kafkaProcessedTable, deduplicateTable);
        statementSet.addInsert(icebergProcessedTable, deduplicateTable);

        String createGoldTable =
                "CREATE TABLE IF NOT EXISTS " + goldTable + " ("
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

        statementSet.execute();
    }
}
