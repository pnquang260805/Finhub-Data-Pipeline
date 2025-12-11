package org.example.Service;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.Interfaces.CatalogService;

public class IcebergCatalogService implements CatalogService {
    private final StreamTableEnvironment tEnv;

    public IcebergCatalogService(FlinkService flinkService){
        this.tEnv = flinkService.getTEnv();
    }

    @Override
    public void createCatalog(String catalogName, String warehouseDir) {
        String createCatalogQuery = "CREATE CATALOG " + catalogName + " WITH ("
                + "'type'='iceberg',"
                + "'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',"
                + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
                + "'uri'='http://catalog:19120/api/v1',"
                + "'authentication.type'='none',"
                + "'ref'='main',"
                + "'client.assume-role.region'='us-east-1',"
                + "'warehouse' = '"+warehouseDir+"',"
                + "'s3.endpoint'='http://minio:9000',"
                + "'s3.path-style-access'='true'"
                + ")";
        this.tEnv.executeSql(createCatalogQuery);
    }

    @Override
    public void createDatabase(String catalog, String dbName) {
        String query = "CREATE DATABASE IF NOT EXISTS " + catalog + "." + dbName;
        this.tEnv.executeSql(query);
    }

    @Override
    public String createFullPath(String catalog, String db, String table){
        return String.join(".", catalog, db, table);
    }
}
