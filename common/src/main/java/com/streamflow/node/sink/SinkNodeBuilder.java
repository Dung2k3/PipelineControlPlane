package com.streamflow.node.sink;

import com.streamflow.serdes.JsonSerdes;
import com.streamflow.topology.NodeBuilder;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Produced;

public class SinkNodeBuilder implements NodeBuilder<SinkNodeConfig> {
    @Override
    public void build(SinkNodeConfig nodeConfig, TopologyBuildContext context) {
        context.get(nodeConfig.getInput())
                .to(nodeConfig.getTopic(), Produced.with(Serdes.String(), JsonSerdes.jsonNode()));
    }
}
