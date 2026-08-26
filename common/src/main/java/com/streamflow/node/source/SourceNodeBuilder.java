package com.streamflow.node.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamflow.serdes.JsonSerdes;
import com.streamflow.topology.NodeBuilder;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public class SourceNodeBuilder implements NodeBuilder<SourceNodeConfig> {

    @Override
    public void build(SourceNodeConfig nodeConfig, TopologyBuildContext context) {
        KStream<String, JsonNode> stream =
                context.streamsBuilder().stream(
                        nodeConfig.getTopic(),
                        Consumed.with(Serdes.String(), JsonSerdes.jsonNode()));
        context.register(nodeConfig.getId(), stream);
    }
}
