package com.streamflow.node.process;

import com.streamflow.config.NodeConfig;

import java.util.Map;

public abstract class ProcessNodeConfig extends NodeConfig {
    @Override
    public abstract Map<String, String> referencedNodeIds();
}
