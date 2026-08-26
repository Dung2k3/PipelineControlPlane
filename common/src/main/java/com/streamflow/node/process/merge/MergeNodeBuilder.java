package com.streamflow.node.process.merge;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamflow.node.process.ProcessNodeBuilder;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.streams.kstream.KStream;

public class MergeNodeBuilder extends ProcessNodeBuilder<MergeNodeConfig> {

    @Override
    protected KStream<String, JsonNode> process(MergeNodeConfig node, TopologyBuildContext context) {
        KStream<String, JsonNode> merged = null;
        for (String inputId : node.getInputs()) {
            KStream<String, JsonNode> input = context.get(inputId);
            merged = merged == null ? input : merged.merge(input);
        }
        return merged;
    }
}
