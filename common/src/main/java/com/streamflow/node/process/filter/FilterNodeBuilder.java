package com.streamflow.node.process.filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamflow.expression.SpelEvaluator;
import com.streamflow.node.process.ProcessNodeBuilder;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.streams.kstream.KStream;

public class FilterNodeBuilder extends ProcessNodeBuilder<FilterNodeConfig> {

    @Override
    protected KStream<String, JsonNode> process(FilterNodeConfig node, TopologyBuildContext context) {
        KStream<String, JsonNode> input = context.get(node.getInput());
        SpelEvaluator predicateEvaluator = new SpelEvaluator(node.getPredicate());
        return input.filter((key, value) -> predicateEvaluator.evaluateBoolean(key, value));
    }
}
