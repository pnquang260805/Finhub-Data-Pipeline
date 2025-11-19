package org.example.Shema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

public class KafkaSchema {
    public Schema kafkaSourceSchema(){
        return Schema.newBuilder()
                .column(
                        "data",
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        DataTypes.FIELD("c", DataTypes.ARRAY(DataTypes.STRING())),
                                        DataTypes.FIELD("p", DataTypes.DECIMAL(10, 2)),
                                        DataTypes.FIELD("s", DataTypes.STRING()),
                                        DataTypes.FIELD("t", DataTypes.BIGINT()),
                                        DataTypes.FIELD("v", DataTypes.BIGINT())
                                )
                        )
                )
                .column("type", DataTypes.STRING())
                .build();
    }

    public Schema preprocessedKafkaSourceSchema(){
        return Schema.newBuilder()
                .column("symbol", DataTypes.STRING().notNull())
                .column("price", DataTypes.DECIMAL(10, 2))
                .column("volume", DataTypes.BIGINT())
                .column("trade_type", DataTypes.STRING())
                .column("unix_ts", DataTypes.BIGINT())
                .primaryKey("symbol")
                .build();
    }
}
