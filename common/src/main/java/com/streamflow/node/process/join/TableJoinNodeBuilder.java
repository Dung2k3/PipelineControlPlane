package com.streamflow.node.process.join;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamflow.expression.SpelEvaluator;
import com.streamflow.node.process.ProcessNodeBuilder;
import com.streamflow.serdes.JsonSerdes;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

public class TableJoinNodeBuilder extends ProcessNodeBuilder<TableJoinNodeConfig> {

    @Override
    protected KStream<String, JsonNode> process(TableJoinNodeConfig node, TopologyBuildContext context) {
        KStream<String, JsonNode> leftInput = context.get(node.getLeftStream());
        KStream<String, JsonNode> rightInput = context.get(node.getRightStream());

        SpelEvaluator extractor = new SpelEvaluator(node.getSelectKey());

        KTable<String, JoinRecordWrapper> leftTable =
                rekey(leftInput, extractor)
                        .toTable(Materialized.<String, JoinRecordWrapper, KeyValueStore<Bytes, byte[]>>as(
                                        node.getId() + "-left-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.of(JoinRecordWrapper.class))
                                .withCachingDisabled());

        KTable<String, JoinRecordWrapper> rightTable =
                rekey(rightInput, extractor)
                        .toTable(Materialized.<String, JoinRecordWrapper, KeyValueStore<Bytes, byte[]>>as(
                                        node.getId() + "-right-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.of(JoinRecordWrapper.class))
                                .withCachingDisabled());

        KTable<String, JsonNode> joined =
                leftTable.join(
                        rightTable,
                        (left, right) ->
                                JoinOutputShaper.shape(
                                        node.getLeftStreamName(), node.getRightStreamName(), left, right));

        return joined.toStream();
    }

    private KStream<String, JoinRecordWrapper> rekey(
            KStream<String, JsonNode> input, SpelEvaluator extractor) {
        return input.map(
                (key, value) ->
                        KeyValue.pair(extractor.evaluateText(key, value), new JoinRecordWrapper(key, value)));
    }
}
