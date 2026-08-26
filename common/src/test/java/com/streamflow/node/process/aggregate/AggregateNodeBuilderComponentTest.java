package com.streamflow.node.process.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.node.process.aggregate.type.AggGroupMode;
import com.streamflow.node.process.aggregate.type.AggregationType;
import com.streamflow.node.process.aggregate.type.WindowType;
import com.streamflow.serdes.JsonSerdes;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AggregateNodeBuilderComponentTest {

    private static final String INPUT_TOPIC = "aggregate-input";
    private static final String OUTPUT_TOPIC = "aggregate-output";

    private final ObjectMapper mapper = new ObjectMapper();

    private static AggregateNodeConfig configFor(AggregationDefinition aggregation) {
        AggregateNodeConfig config = new AggregateNodeConfig();
        config.setId("agg1");
        config.setInput("src");
        config.setGroupMode(AggGroupMode.GROUP_BY);
        config.setKeyExtractor("value.path('customerId')");
        config.setAggregations(List.of(aggregation));

        AggWindowConfig window = new AggWindowConfig();
        window.setType(WindowType.TUMBLING);
        window.setSizeMs(60_000L);
        window.setGraceMs(0L);
        config.setWindow(window);
        return config;
    }

    private TopologyTestDriver buildDriver(AggregateNodeConfig config) {
        StreamsBuilder builder = new StreamsBuilder();
        TopologyBuildContext context = new TopologyBuildContext(builder);

        KStream<String, JsonNode> input =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.jsonNode()));
        context.register(config.getInput(), input);

        new AggregateNodeBuilder().build(config, context);

        context.get(config.getId()).to(OUTPUT_TOPIC, Produced.with(Serdes.String(), JsonSerdes.jsonNode()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-node-builder-component-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        return new TopologyTestDriver(builder.build(), props);
    }

    @Test
    void sumsSourceFieldPerKeyWithinWindow() throws Exception {
        AggregationDefinition sum = new AggregationDefinition(AggregationType.SUM, "totalAmount");
        sum.setSourceField("amount");
        AggregateNodeConfig config = configFor(sum);

        try (TopologyTestDriver driver = buildDriver(config)) {
            TestInputTopic<String, JsonNode> in = driver.createInputTopic(
                    INPUT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestOutputTopic<String, JsonNode> out = driver.createOutputTopic(
                    OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerdes.jsonNode().deserializer());

            Instant base = Instant.ofEpochMilli(0);
            in.pipeInput("k1", mapper.readTree("{\"customerId\":\"C1\",\"amount\":10}"), base);
            in.pipeInput("k2", mapper.readTree("{\"customerId\":\"C1\",\"amount\":15}"), base);
            in.pipeInput("k3", mapper.readTree("{\"customerId\":\"C2\",\"amount\":100}"), base);

            List<KeyValue<String, JsonNode>> records = out.readKeyValuesToList();
            KeyValue<String, JsonNode> lastForC1 = records.stream()
                    .filter(kv -> kv.key.equals("C1"))
                    .reduce((first, second) -> second)
                    .orElseThrow();
            assertEquals(25.0, lastForC1.value.get("totalAmount").asDouble());
        }
    }

    @Test
    void countsRecordsPerKeyWithinWindow() throws Exception {
        AggregationDefinition count = new AggregationDefinition(AggregationType.COUNT, "eventCount");
        AggregateNodeConfig config = configFor(count);

        try (TopologyTestDriver driver = buildDriver(config)) {
            TestInputTopic<String, JsonNode> in = driver.createInputTopic(
                    INPUT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestOutputTopic<String, JsonNode> out = driver.createOutputTopic(
                    OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerdes.jsonNode().deserializer());

            Instant base = Instant.ofEpochMilli(0);
            in.pipeInput("k1", mapper.readTree("{\"customerId\":\"C1\"}"), base);
            in.pipeInput("k2", mapper.readTree("{\"customerId\":\"C1\"}"), base);

            List<KeyValue<String, JsonNode>> records = out.readKeyValuesToList();
            JsonNode last = records.get(records.size() - 1).value;
            assertEquals(2L, last.get("eventCount").asLong());
        }
    }

    @Test
    void combinesCountAndSumInOneOutputPerKey() throws Exception {
        AggregationDefinition count = new AggregationDefinition(AggregationType.COUNT, "eventCount");
        AggregationDefinition sum = new AggregationDefinition(AggregationType.SUM, "totalAmount");
        sum.setSourceField("amount");
        AggregateNodeConfig config = configFor(count);
        config.setAggregations(List.of(count, sum));

        try (TopologyTestDriver driver = buildDriver(config)) {
            TestInputTopic<String, JsonNode> in = driver.createInputTopic(
                    INPUT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestOutputTopic<String, JsonNode> out = driver.createOutputTopic(
                    OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerdes.jsonNode().deserializer());

            Instant base = Instant.ofEpochMilli(0);
            in.pipeInput("k1", mapper.readTree("{\"customerId\":\"C1\",\"amount\":10}"), base);
            in.pipeInput("k2", mapper.readTree("{\"customerId\":\"C1\",\"amount\":15}"), base);

            List<KeyValue<String, JsonNode>> records = out.readKeyValuesToList();
            JsonNode last = records.get(records.size() - 1).value;
            assertEquals(2L, last.get("eventCount").asLong());
            assertEquals(25.0, last.get("totalAmount").asDouble());
        }
    }
}
