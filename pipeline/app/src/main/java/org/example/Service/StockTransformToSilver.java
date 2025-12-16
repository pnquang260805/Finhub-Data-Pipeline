package org.example.Service;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.example.Abstract.Transform;
import org.example.Config.IcebergName;
import org.example.Config.KafkaConfig;

public class StockTransformToSilver extends Transform {
    public StockTransformToSilver(FlinkService flinkService, String catalogName, String warehouse, String schema){
        super(flinkService, catalogName, warehouse, schema);
    }

    private Table flatData(String sourceTableName, String flattenTableName){
        String flattenQuery = "SELECT \n"
                + "s AS symbol, \n"
                + "p AS price, \n"
                + "v AS volume, \n"
                + "`type` AS trade_type, \n"
                + "t AS unix_ts \n"
                + "FROM `" + sourceTableName + "`\n"
                + "CROSS JOIN UNNEST(data) AS d(c, p, s, t, v)";
        Table flattenTable = tEnv.sqlQuery(flattenQuery);
        tEnv.createTemporaryView(flattenTableName, flattenTable);
        return flattenTable;
    }

    private Table deduplicateData(String flattenTableName, String deduplicatedTableName){
        String deduplicateQuery = "SELECT " +
                    "symbol, price, " +
                    "volume, " +
                    "trade_type, " +
                    "unix_ts \n"
                + "FROM (\n"
                    + "SELECT " +
                    "*, " +
                    "ROW_NUMBER() OVER (" +
                        "PARTITION BY symbol, volume, unix_ts ORDER BY price DESC" +
                    ") AS rn\n"
                + "FROM " + flattenTableName
                + " ) AS temp WHERE rn = 1";

        Table deduplicateTable = tEnv.sqlQuery(deduplicateQuery);
        tEnv.createTemporaryView(deduplicatedTableName, deduplicateTable);
        return deduplicateTable;
    }


    @Override
    public void execute() {
        String finnhubSourceTable = IcebergName.FINNHUB_STOCK_SOURCE_TABLE.getValue();
        String flattenTable = "flatten_table";
        String deduplicatedTable = "deduplicated_table";
        String kafkaStockSilver = KafkaConfig.KAFKA_SILVER_SINK.getValue();
        String stockSilverTable = IcebergName.STOCK_SILVER_TABLE.getValue();
        String stockSilverFullPath = String.join(".", this.catalogName, this.schema, stockSilverTable);

        Table flatted = this.flatData(finnhubSourceTable, flattenTable);
        Table deduplicated = this.deduplicateData(flattenTable, deduplicatedTable);
        StatementSet statementSet = this.tEnv.createStatementSet();
        statementSet.addInsert(kafkaStockSilver, deduplicated);
        statementSet.addInsert(stockSilverFullPath, deduplicated);
        statementSet.execute();
    }
}
