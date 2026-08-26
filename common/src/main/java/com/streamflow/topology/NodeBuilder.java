package com.streamflow.topology;

public interface NodeBuilder<T> {
    void build(T nodeConfig, TopologyBuildContext context);
}
