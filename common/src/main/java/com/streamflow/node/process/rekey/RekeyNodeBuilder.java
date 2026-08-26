package com.streamflow.node.process.rekey;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamflow.expression.SpelEvaluator;
import com.streamflow.node.process.ProcessNodeBuilder;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.streams.kstream.KStream;

public class RekeyNodeBuilder extends ProcessNodeBuilder<RekeyNodeConfig> {

    @Override
    protected KStream<String, JsonNode> process(RekeyNodeConfig node, TopologyBuildContext context) {
        KStream<String, JsonNode> input = context.get(node.getInput());
        SpelEvaluator evaluator = new SpelEvaluator(node.getSelectKey());
        return input.selectKey((key, value) -> evaluator.evaluateText(key, value));
    }
}
