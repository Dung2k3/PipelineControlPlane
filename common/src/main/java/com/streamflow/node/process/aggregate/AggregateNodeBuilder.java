package com.streamflow.node.process.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamflow.expression.SpelEvaluator;
import com.streamflow.node.process.ProcessNodeBuilder;
import com.streamflow.node.process.aggregate.type.AggGroupMode;
import com.streamflow.node.process.aggregate.type.AggregationType;
import com.streamflow.serdes.JsonSerdes;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.List;

public class AggregateNodeBuilder extends ProcessNodeBuilder<AggregateNodeConfig> {

    @Override
    protected KStream<String, JsonNode> process(
            AggregateNodeConfig node, TopologyBuildContext context) {
        KStream<String, JsonNode> input = context.get(node.getInput());
        List<AggregationDefinition> aggregations = node.getAggregations();
        AggWindowConfig windowConfig = node.getWindow();

        KGroupedStream<String, JsonNode> groupedStream;
        if (node.getGroupMode() == AggGroupMode.GROUP_BY) {
            SpelEvaluator keyExtractor = new SpelEvaluator(node.getKeyExtractor());
            groupedStream =
                    input.groupBy(
                            (key, value) -> keyExtractor.evaluateText(key, value),
                            Grouped.with(Serdes.String(), JsonSerdes.jsonNode()));
        } else {
            groupedStream = input.groupByKey(Grouped.with(Serdes.String(), JsonSerdes.jsonNode()));
        }

        TimeWindowedKStream<String, JsonNode> windowedStream =
                groupedStream.windowedBy(
                        TimeWindows.ofSizeAndGrace(
                                Duration.ofMillis(windowConfig.getSizeMs()),
                                Duration.ofMillis(windowConfig.getGraceMs())));

        return windowedStream
                .aggregate(
                        () -> (JsonNode) JsonNodeFactory.instance.objectNode(),
                        (key, value, acc) -> accumulate((ObjectNode) acc, value, aggregations),
                        Materialized.<String, JsonNode, WindowStore<Bytes, byte[]>>as(
                                        node.getId() + "-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.jsonNode())
                                .withCachingDisabled())
                .toStream()
                .map(
                        (windowedKey, acc) -> {
                            ObjectNode value = acc.deepCopy();
                            value.put("windowStart", windowedKey.window().start());
                            value.put("windowEnd", windowedKey.window().end());
                            return KeyValue.pair(windowedKey.key(), (JsonNode) value);
                        });
    }

    private ObjectNode accumulate(
            ObjectNode acc, JsonNode value, List<AggregationDefinition> aggregations) {
        for (AggregationDefinition agg : aggregations) {
            String alias = agg.getAlias();
            if (agg.getType() == AggregationType.SUM) {
                double total = acc.path(alias).asDouble(0.0) + value.path(agg.getSourceField()).asDouble(0.0);
                acc.put(alias, total);
            } else {
                long count = acc.path(alias).asLong(0L) + 1;
                acc.put(alias, count);
            }
        }
        return acc;
    }
}
