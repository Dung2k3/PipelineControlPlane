package com.streamflow.node.process.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamflow.expression.SpelEvaluator;
import com.streamflow.node.process.ProcessNodeBuilder;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.List;

public class MappingNodeBuilder extends ProcessNodeBuilder<MappingNodeConfig> {

    private record CompiledField(String target, boolean fromKey, SpelEvaluator evaluator) {
    }

    @Override
    protected KStream<String, JsonNode> process(MappingNodeConfig node, TopologyBuildContext context) {
        KStream<String, JsonNode> input = context.get(node.getInput());

        List<CompiledField> compiled = new ArrayList<>();
        for (MappingFieldConfig field : node.getFields()) {
            if (field.isFromKey()) {
                compiled.add(new CompiledField(field.getTarget(), true, null));
            } else {
                compiled.add(new CompiledField(field.getTarget(), false, new SpelEvaluator(field.getSource())));
            }
        }

        return input.map((key, value) -> {
            ObjectNode flattened = JsonNodeFactory.instance.objectNode();
            for (CompiledField field : compiled) {
                if (field.fromKey()) {
                    flattened.put(field.target(), key);
                } else {
                    flattened.set(field.target(), field.evaluator().evaluateNode(key, value));
                }
            }
            return KeyValue.pair(key, (JsonNode) flattened);
        });
    }
}
