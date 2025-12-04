package org.example.Service;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;

public class FlinkService {
    private final Configuration config;
    private StreamTableEnvironment tEnv;

    public FlinkService() {
        this.config = new Configuration();
    }

    public void setString(Map<String, String> configMap){
        for(Map.Entry<String,String> entry : configMap.entrySet()){
            this.config.setString(entry.getKey(), entry.getValue());
        }
    }

    public void createTableEnv(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(this.config);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        this.tEnv = StreamTableEnvironment.create(env, settings);
    }

    public StreamTableEnvironment getTEnv(){
        return this.tEnv;
    }
}
