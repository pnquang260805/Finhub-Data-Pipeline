package org.example.Abstract;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.Service.FlinkService;

public abstract class Transform {
    protected String schema;
    protected FlinkService flinkService;
    protected StreamTableEnvironment tEnv;
    protected String catalogName;
    protected String warehouse;

    public Transform(FlinkService flinkService, String catalogName, String warehouse, String schema){
        this.flinkService = flinkService;
        this.tEnv = flinkService.getTEnv();
        this.catalogName = catalogName;
        this.warehouse = warehouse;
        this.schema = schema;
    }

    public abstract void execute();

}
