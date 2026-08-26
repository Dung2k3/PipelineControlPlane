package com.streamflow.controlplane.runtime;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 1 pod = 1 pipeline (Model A, xem docs/PipelineControlPlane/plan.md) - SHUTDOWN_APPLICATION o day
 * chi lam chet dung pod nay, k8s tu restart theo Deployment. Khac voi 1 process om nhieu pipeline
 * (khong lam o day) - o do phai tranh SHUTDOWN_APPLICATION vi se keo sap ca cac pipeline khac.
 */
public final class KafkaStreamsRunner {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsRunner.class);

    private KafkaStreamsRunner() {}

    public static void run(Topology topology, Properties props) {
        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            streams.setUncaughtExceptionHandler(
                    throwable -> {
                        log.error(
                                "Unhandled exception trong Kafka Streams",
                                throwable);
                        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse
                                .SHUTDOWN_APPLICATION;
                    });
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
            streams.start();

            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
