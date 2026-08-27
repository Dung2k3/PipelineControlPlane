package com.streamflow.controlplane;

import com.streamflow.config.PipelineConfig;
import com.streamflow.controlplane.config.AppConfig;
import com.streamflow.controlplane.configstore.CouchbasePipelineConfigStore;
import com.streamflow.topology.PipelineTopologyBuilder;
import com.streamflow.validation.PipelineValidator;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
public class MultiPipelineMain {

    public static void main(String[] args) throws InterruptedException {
        List<String> pipelineIds = List.of(AppConfig.require("PIPELINE_IDS").split(","));

        CouchbasePipelineConfigStore store = new CouchbasePipelineConfigStore();
        List<KafkaStreams> instances = new ArrayList<>();

        for (String rawId : pipelineIds) {
            String pipelineId = rawId.trim();
            PipelineConfig config = store.load(pipelineId);
            new PipelineValidator().validate(config);
            Topology topology = new PipelineTopologyBuilder().build(config);

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());

            KafkaStreams streams = new KafkaStreams(topology, props);
            streams.setUncaughtExceptionHandler(throwable -> {
                System.err.println("Pipeline '" + pipelineId + "' gap loi khong bat duoc: " + throwable);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            streams.start();
            instances.add(streams);
            System.out.println("[MultiPipelineMain] Da start pipeline '" + pipelineId
                    + "' (applicationId=" + config.getApplicationId() + ")");
        }

        System.out.println("[MultiPipelineMain] " + instances.size()
                + " pipeline dang chay chung 1 JVM. Heap used="
                + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024) + "MB");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> instances.forEach(KafkaStreams::close)));
        Thread.currentThread().join();
    }
}
