package org.example.Service;

import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.Config.IcebergName;
import org.example.Config.KafkaConfig;
import org.example.Interfaces.CatalogService;
import org.example.Shema.KafkaSchema;

public class EnvironmentSetup {
    private final String bootstrapServer;
    private final String ingestTopic;
    private final KafkaSchema kafkaSchema;
    private final StreamTableEnvironment tEnv;
    private final CatalogService catalogService;
    private final String silverKafkaTopic;

    public EnvironmentSetup(FlinkService flinkService, CatalogService catalogService) {
        this.silverKafkaTopic = KafkaConfig.KAFKA_STOCK_SILVER_TOPIC.getValue();
        this.bootstrapServer = KafkaConfig.KAFKA_BOOTSTRAP_SERVER.getValue();
        this.ingestTopic = KafkaConfig.KAFKA_RAW_TOPIC.getValue();
        this.kafkaSchema = new KafkaSchema();
        this.tEnv = flinkService.getTEnv();
        this.catalogService = catalogService;
    }

    private void createIngestTable(String sourceTableName){
        tEnv.createTemporaryTable(sourceTableName, TableDescriptor.forConnector("kafka")
                .schema(this.kafkaSchema.kafkaSourceSchema())
                .option("topic", this.ingestTopic)
                .option("properties.bootstrap.servers", this.bootstrapServer)
                .option("scan.startup.mode", "earliest-offset")
                .format(FormatDescriptor.forFormat("json")
                        .option("fail-on-missing-field", "false")
                        .option("ignore-parse-errors", "true")
                        .build())
                .build());
    }

    private void createSilverKafkaSinkTable(String silverKafkaSinkTable){
        this.tEnv.createTemporaryTable(silverKafkaSinkTable, TableDescriptor.forConnector("upsert-kafka")
                .schema(this.kafkaSchema.kafkaProcessedSchema())
                .option("topic", this.silverKafkaTopic)
                .option("properties.bootstrap.servers", this.bootstrapServer)
                .option("key.format", "json")
                .option("value.format", "json")
                .build()
        );
    }

    private void createStockSilver(String catalog, String schema, String table){
        String stockTable = catalogService.createFullPath(catalog, schema, table);
        String createTableQuery =
                "CREATE TABLE IF NOT EXISTS " + stockTable + " ("
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

        this.tEnv.executeSql(createTableQuery);
    }

    public void execute(){
        // Databases and schema
        String catalogName = IcebergName.CATALOG.getValue();
        String warehouse = IcebergName.WAREHOUSE.getValue();
        String schemaName = IcebergName.SCHEMA.getValue();

        // Table
        String finnhubSourceTable = IcebergName.FINNHUB_STOCK_SOURCE_TABLE.getValue();
        String stockSilverTable = IcebergName.STOCK_SILVER_TABLE.getValue();
        String kafkaStockSilver = KafkaConfig.KAFKA_SILVER_SINK.getValue();

        this.catalogService.createCatalog(catalogName, warehouse);
        this.catalogService.createDatabase(catalogName, schemaName);

        this.createIngestTable(finnhubSourceTable);
        this.createSilverKafkaSinkTable(kafkaStockSilver);
        this.createStockSilver(catalogName, schemaName, stockSilverTable);
    }
}
