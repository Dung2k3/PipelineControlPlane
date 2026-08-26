package com.streamflow.controlplane;

import com.streamflow.config.PipelineConfig;
import com.streamflow.controlplane.config.AppConfig;
import com.streamflow.controlplane.configstore.CouchbasePipelineConfigStore;
import com.streamflow.controlplane.runtime.KafkaStreamsRunner;
import com.streamflow.topology.PipelineTopologyBuilder;
import com.streamflow.validation.PipelineValidator;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;


public class Main {

    public static void main(String[] args) {
        String pipelineId = AppConfig.require("PIPELINE_ID");

        CouchbasePipelineConfigStore store = new CouchbasePipelineConfigStore();
        PipelineConfig config = store.load(pipelineId);

        String bootstrapServersOverride = AppConfig.get("BOOTSTRAP_SERVERS");
        if (bootstrapServersOverride != null) {
            config.setBootstrapServers(bootstrapServersOverride);
        }

        new PipelineValidator().validate(config);

        Topology topology = new PipelineTopologyBuilder().build(config);
        System.out.print(topology.describe());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());

        KafkaStreamsRunner.run(topology, props);
    }
}
