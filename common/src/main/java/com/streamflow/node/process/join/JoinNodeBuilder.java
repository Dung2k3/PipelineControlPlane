package com.streamflow.node.process.join;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamflow.expression.SpelEvaluator;
import com.streamflow.node.process.ProcessNodeBuilder;
import com.streamflow.serdes.JsonSerdes;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

public class JoinNodeBuilder extends ProcessNodeBuilder<JoinNodeConfig> {

    @Override
    protected KStream<String, JsonNode> process(JoinNodeConfig node, TopologyBuildContext context) {
        KStream<String, JsonNode> leftInput = context.get(node.getLeftStream());
        KStream<String, JsonNode> rightInput = context.get(node.getRightStream());

        SpelEvaluator extractor = new SpelEvaluator(node.getSelectKey());

        KStream<String, JoinRecordWrapper> leftRekeyed =
                leftInput.map(
                        (key, value) ->
                                KeyValue.pair(
                                        extractor.evaluateText(key, value),
                                        new JoinRecordWrapper(key, value)));
        KStream<String, JoinRecordWrapper> rightRekeyed =
                rightInput.map(
                        (key, value) ->
                                KeyValue.pair(
                                        extractor.evaluateText(key, value),
                                        new JoinRecordWrapper(key, value)));

        JoinWindows windows =
                JoinWindows.ofTimeDifferenceAndGrace(node.getWindow(), node.getGrace());

        return leftRekeyed.join(
                rightRekeyed,
                (left, right) ->
                        JoinOutputShaper.shape(
                                node.getLeftStreamName(), node.getRightStreamName(), left, right),
                windows,
                StreamJoined.with(
                        Serdes.String(),
                        JsonSerdes.of(JoinRecordWrapper.class),
                        JsonSerdes.of(JoinRecordWrapper.class)));
    }
}
