package com.streamflow.node.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamflow.topology.NodeBuilder;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.streams.kstream.KStream;

public abstract class ProcessNodeBuilder<T extends ProcessNodeConfig> implements NodeBuilder<T> {
    @Override
    public final void build(T node, TopologyBuildContext context) {
        KStream<String, JsonNode> output = process(node, context);
        context.register(node.getId(), output);
    }

    protected abstract KStream<String, JsonNode> process(T node, TopologyBuildContext context);
}
