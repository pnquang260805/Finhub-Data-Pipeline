package org.example.Config;

public enum IcebergName {
    FINNHUB_STOCK_SOURCE_TABLE("source_table"),
    STOCK_SILVER_TABLE("stock_silver"),
    CATALOG("iceberg"),
    WAREHOUSE("s3a://tables/"),
    SCHEMA("dbo"),
    ;
    private final String value;
    IcebergName(String s){
        this.value = s;
    }

    public String getValue(){
        return this.value;
    }
}
