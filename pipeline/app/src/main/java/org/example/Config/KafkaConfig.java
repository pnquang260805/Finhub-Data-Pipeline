package org.example.Config;

public enum KafkaConfig {
    KAFKA_BOOTSTRAP_SERVER("kafka:29092"),
    KAFKA_RAW_TOPIC("finnhub-stock-topic"),
    KAFKA_STOCK_SILVER_TOPIC("stock-silver-topic"),
    KAFKA_SILVER_SINK("stock_silver_table"),
    ;

    private final String value;

    KafkaConfig(String value) {
        this.value = value;
    }

    public String getValue(){
        return this.value;
    }
}
