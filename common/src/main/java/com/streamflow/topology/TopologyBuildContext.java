package com.streamflow.topology;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.HashMap;
import java.util.Map;

public class TopologyBuildContext {

    private final StreamsBuilder streamsBuilder;
    private final Map<String, KStream<String, JsonNode>> builtNodes = new HashMap<>();

    public TopologyBuildContext(StreamsBuilder streamsBuilder) {
        this.streamsBuilder = streamsBuilder;
    }

    public StreamsBuilder streamsBuilder() {
        return streamsBuilder;
    }

    public void register(String nodeId, KStream<String, JsonNode> stream) {
        builtNodes.put(nodeId, stream);
    }

    public KStream<String, JsonNode> get(String nodeId) {
        KStream<String, JsonNode> stream = builtNodes.get(nodeId);
        if (stream == null) {
            throw new IllegalStateException("Node chua duoc build: " + nodeId);
        }
        return stream;
    }

    public boolean isBuilt(String nodeId) {
        return builtNodes.containsKey(nodeId);
    }
}
