package org.example;

import org.apache.flink.table.api.*;
import org.example.Config.IcebergName;
import org.example.Interfaces.CatalogService;
import org.example.Service.EnvironmentSetup;
import org.example.Service.FlinkService;
import org.example.Service.IcebergCatalogService;
import org.example.Service.StockTransformToSilver;

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
        // Pipeline Variables
        String catalogName = IcebergName.CATALOG.getValue();
        String warehouse = IcebergName.WAREHOUSE.getValue();
        String schemaName = IcebergName.SCHEMA.getValue();

        // Setup pipeline
        CatalogService catalogService = new IcebergCatalogService(flinkService);
        EnvironmentSetup setup = new EnvironmentSetup(flinkService, catalogService);
        setup.execute();

        StockTransformToSilver transform = new StockTransformToSilver(flinkService, catalogName, warehouse, schemaName);
        transform.execute();
    }
}
