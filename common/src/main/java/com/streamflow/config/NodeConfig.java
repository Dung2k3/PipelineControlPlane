package com.streamflow.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.streamflow.config.type.NodeType;
import com.streamflow.node.process.aggregate.AggregateNodeConfig;
import com.streamflow.node.process.cache.CacheSinkNodeConfig;
import com.streamflow.node.process.filter.FilterNodeConfig;
import com.streamflow.node.process.join.JoinNodeConfig;
import com.streamflow.node.process.join.TableJoinNodeConfig;
import com.streamflow.node.process.mapping.EnrichJdbcNodeConfig;
import com.streamflow.node.process.mapping.MappingNodeConfig;
import com.streamflow.node.process.merge.MergeNodeConfig;
import com.streamflow.node.process.rekey.RekeyNodeConfig;
import com.streamflow.node.sink.SinkNodeConfig;
import com.streamflow.node.source.SourceNodeConfig;

import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type",
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = SourceNodeConfig.class, name = "SOURCE"),
        @JsonSubTypes.Type(value = JoinNodeConfig.class, name = "JOIN"),
        @JsonSubTypes.Type(value = TableJoinNodeConfig.class, name = "TABLE_JOIN"),
        @JsonSubTypes.Type(value = MappingNodeConfig.class, name = "MAPPING"),
        @JsonSubTypes.Type(value = FilterNodeConfig.class, name = "FILTER"),
        @JsonSubTypes.Type(value = MergeNodeConfig.class, name = "MERGE"),
        @JsonSubTypes.Type(value = EnrichJdbcNodeConfig.class, name = "JDBC_ENRICH"),
        @JsonSubTypes.Type(value = AggregateNodeConfig.class, name = "AGGREGATE"),
        @JsonSubTypes.Type(value = RekeyNodeConfig.class, name = "REKEY"),
        @JsonSubTypes.Type(value = CacheSinkNodeConfig.class, name = "CACHE_SINK"),
        @JsonSubTypes.Type(value = SinkNodeConfig.class, name = "SINK")
})
public abstract class NodeConfig {
    private String id;
    private String name;
    private NodeType type;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public NodeType getType() {
        return type;
    }

    public void setType(NodeType type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> referencedNodeIds() {
        return Map.of();
    }
}
