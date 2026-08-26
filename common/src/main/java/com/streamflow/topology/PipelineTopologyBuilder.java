package com.streamflow.topology;

import com.streamflow.config.NodeConfig;
import com.streamflow.config.PipelineConfig;
import com.streamflow.node.process.ProcessNodeConfig;
import com.streamflow.node.process.aggregate.AggregateNodeBuilder;
import com.streamflow.node.process.aggregate.AggregateNodeConfig;
import com.streamflow.node.process.cache.CacheSinkNodeBuilder;
import com.streamflow.node.process.cache.CacheSinkNodeConfig;
import com.streamflow.node.process.filter.FilterNodeBuilder;
import com.streamflow.node.process.filter.FilterNodeConfig;
import com.streamflow.node.process.join.JoinNodeBuilder;
import com.streamflow.node.process.join.JoinNodeConfig;
import com.streamflow.node.process.join.TableJoinNodeBuilder;
import com.streamflow.node.process.join.TableJoinNodeConfig;
import com.streamflow.node.process.mapping.EnrichJdbcNodeBuilder;
import com.streamflow.node.process.mapping.EnrichJdbcNodeConfig;
import com.streamflow.node.process.mapping.MappingNodeBuilder;
import com.streamflow.node.process.mapping.MappingNodeConfig;
import com.streamflow.node.process.merge.MergeNodeBuilder;
import com.streamflow.node.process.merge.MergeNodeConfig;
import com.streamflow.node.process.rekey.RekeyNodeBuilder;
import com.streamflow.node.process.rekey.RekeyNodeConfig;
import com.streamflow.node.sink.SinkNodeBuilder;
import com.streamflow.node.sink.SinkNodeConfig;
import com.streamflow.node.source.SourceNodeBuilder;
import com.streamflow.node.source.SourceNodeConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.ArrayList;
import java.util.List;

public class PipelineTopologyBuilder {

    private final NodeBuilder<SourceNodeConfig> sourceBuilder = new SourceNodeBuilder();
    private final NodeBuilder<JoinNodeConfig> joinBuilder = new JoinNodeBuilder();
    private final NodeBuilder<TableJoinNodeConfig> tableJoinBuilder = new TableJoinNodeBuilder();
    private final NodeBuilder<MappingNodeConfig> mappingBuilder = new MappingNodeBuilder();
    private final NodeBuilder<FilterNodeConfig> filterBuilder = new FilterNodeBuilder();
    private final NodeBuilder<MergeNodeConfig> mergeBuilder = new MergeNodeBuilder();
    private final NodeBuilder<EnrichJdbcNodeConfig> enrichJdbcBuilder = new EnrichJdbcNodeBuilder();
    private final NodeBuilder<AggregateNodeConfig> aggregateBuilder = new AggregateNodeBuilder();
    private final NodeBuilder<RekeyNodeConfig> rekeyBuilder = new RekeyNodeBuilder();
    private final NodeBuilder<CacheSinkNodeConfig> cacheSinkBuilder = new CacheSinkNodeBuilder();
    private final NodeBuilder<SinkNodeConfig> sinkBuilder = new SinkNodeBuilder();

    public Topology build(PipelineConfig pipeline) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        TopologyBuildContext context = new TopologyBuildContext(streamsBuilder);

        List<NodeConfig> remaining = new ArrayList<>(pipeline.getNodes());
        while (!remaining.isEmpty()) {
            boolean progressed = false;
            List<NodeConfig> stillRemaining = new ArrayList<>();

            for (NodeConfig node : remaining) {
                if (dependenciesReady(node, context)) {
                    buildOne(node, context);
                    progressed = true;
                } else {
                    stillRemaining.add(node);
                }
            }

            if (!progressed) {
                throw new IllegalStateException(
                        "Khong build duoc phan con lai cua graph: "
                                + stillRemaining.stream().map(NodeConfig::getId).toList());
            }
            remaining = stillRemaining;
        }

        return streamsBuilder.build();
    }

    private boolean dependenciesReady(NodeConfig node, TopologyBuildContext context) {
        if (node instanceof ProcessNodeConfig process) {
            return process.referencedNodeIds().values().stream().allMatch(context::isBuilt);
        }
        if (node instanceof SinkNodeConfig sink) {
            return context.isBuilt(sink.getInput());
        }
        return true;
    }

    private void buildOne(NodeConfig node, TopologyBuildContext context) {
        switch (node.getType()) {
            case SOURCE -> sourceBuilder.build((SourceNodeConfig) node, context);
            case JOIN -> joinBuilder.build((JoinNodeConfig) node, context);
            case TABLE_JOIN -> tableJoinBuilder.build((TableJoinNodeConfig) node, context);
            case MAPPING -> mappingBuilder.build((MappingNodeConfig) node, context);
            case FILTER -> filterBuilder.build((FilterNodeConfig) node, context);
            case MERGE -> mergeBuilder.build((MergeNodeConfig) node, context);
            case JDBC_ENRICH -> enrichJdbcBuilder.build((EnrichJdbcNodeConfig) node, context);
            case AGGREGATE -> aggregateBuilder.build((AggregateNodeConfig) node, context);
            case REKEY -> rekeyBuilder.build((RekeyNodeConfig) node, context);
            case CACHE_SINK -> cacheSinkBuilder.build((CacheSinkNodeConfig) node, context);
            case SINK -> sinkBuilder.build((SinkNodeConfig) node, context);
        }
    }
}
