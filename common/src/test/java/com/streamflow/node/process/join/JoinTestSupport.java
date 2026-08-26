package com.streamflow.node.process.join;

import com.streamflow.config.NodeConfig;
import com.streamflow.config.PipelineConfig;
import com.streamflow.node.process.join.type.JoinType;
import com.streamflow.node.source.SourceNodeConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class JoinTestSupport {

    static final String LEFT_STREAM_ID = "srcLeft";
    static final String RIGHT_STREAM_ID = "srcRight";

    private JoinTestSupport() {
    }

    static JoinNodeConfig defaultConfig() {
        JoinNodeConfig config = new JoinNodeConfig();
        config.setId("join1");
        config.setLeftStream(LEFT_STREAM_ID);
        config.setRightStream(RIGHT_STREAM_ID);
        config.setLeftStreamName("left");
        config.setRightStreamName("right");
        config.setSelectKey("value.path('id')");
        config.setJoinType(JoinType.INNER);
        config.setWindow(Duration.ofSeconds(5));
        config.setGrace(Duration.ofSeconds(1));
        return config;
    }

    static JoinNodeConfig configWithWindow(Duration window, Duration grace) {
        JoinNodeConfig config = defaultConfig();
        config.setWindow(window);
        config.setGrace(grace);
        return config;
    }

    static TableJoinNodeConfig defaultTableJoinConfig() {
        TableJoinNodeConfig config = new TableJoinNodeConfig();
        config.setId("tableJoin1");
        config.setLeftStream(LEFT_STREAM_ID);
        config.setRightStream(RIGHT_STREAM_ID);
        config.setLeftStreamName("left");
        config.setRightStreamName("right");
        config.setSelectKey("value.path('id')");
        config.setJoinType(JoinType.INNER);
        return config;
    }

    static PipelineConfig defaultPipeline(NodeConfig... extraNodes) {
        PipelineConfig pipeline = new PipelineConfig();
        pipeline.setPipelineId("test-pipeline");
        pipeline.setBootstrapServers("localhost:9092");
        pipeline.setApplicationId("test-app");

        SourceNodeConfig left = new SourceNodeConfig();
        left.setId(LEFT_STREAM_ID);
        SourceNodeConfig right = new SourceNodeConfig();
        right.setId(RIGHT_STREAM_ID);

        List<NodeConfig> nodes = new ArrayList<>(List.of(left, right));
        nodes.addAll(List.of(extraNodes));
        pipeline.setNodes(nodes);
        return pipeline;
    }
}
