package com.streamflow.node.process.join;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableJoinNodeBuilderComponentTest {

    private static final String LEFT_TOPIC = "table-join-left";
    private static final String RIGHT_TOPIC = "table-join-right";
    private static final String OUTPUT_TOPIC = "table-join-output";

    private final ObjectMapper mapper = new ObjectMapper();

    private TopologyTestDriver buildDriver(TableJoinNodeConfig config) {
        StreamsBuilder builder = new StreamsBuilder();
        TopologyBuildContext context = new TopologyBuildContext(builder);

        KStream<String, JsonNode> left =
                builder.stream(LEFT_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.jsonNode()));
        KStream<String, JsonNode> right =
                builder.stream(RIGHT_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.jsonNode()));
        context.register(config.getLeftStream(), left);
        context.register(config.getRightStream(), right);

        new TableJoinNodeBuilder().build(config, context);

        context.get(config.getId()).to(OUTPUT_TOPIC, Produced.with(Serdes.String(), JsonSerdes.jsonNode()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "table-join-node-builder-component-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        return new TopologyTestDriver(builder.build(), props);
    }

    @Test
    void joinsOnceBothSidesHaveArrivedRegardlessOfTimestamp() throws Exception {
        TableJoinNodeConfig config = JoinTestSupport.defaultTableJoinConfig();
        try (TopologyTestDriver driver = buildDriver(config)) {
            TestInputTopic<String, JsonNode> leftIn = driver.createInputTopic(
                    LEFT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestInputTopic<String, JsonNode> rightIn = driver.createInputTopic(
                    RIGHT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestOutputTopic<String, JsonNode> out = driver.createOutputTopic(
                    OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerdes.jsonNode().deserializer());

            JsonNode leftValue = mapper.readTree("{\"id\":\"K\",\"name\":\"Alice\"}");
            JsonNode rightValue = mapper.readTree("{\"id\":\"K\",\"orderId\":\"O1\"}");

            leftIn.pipeInput("left-key", leftValue, Instant.ofEpochMilli(0));
            // arrives long after left's record, unlike the windowed JOIN this still joins
            rightIn.pipeInput("right-key", rightValue, Instant.ofEpochMilli(60_000));

            List<KeyValue<String, JsonNode>> records = out.readKeyValuesToList();
            assertEquals(1, records.size());
            assertEquals("K", records.get(0).key);

            JsonNode leftWrapped = records.get(0).value.get(config.getLeftStreamName());
            JsonNode rightWrapped = records.get(0).value.get(config.getRightStreamName());
            assertEquals("left-key", leftWrapped.get("key").asText());
            assertEquals("right-key", rightWrapped.get("key").asText());
        }
    }

    @Test
    void updatingEitherSideReJoinsWithLatestValueFromTheOtherSide() throws Exception {
        TableJoinNodeConfig config = JoinTestSupport.defaultTableJoinConfig();
        try (TopologyTestDriver driver = buildDriver(config)) {
            TestInputTopic<String, JsonNode> leftIn = driver.createInputTopic(
                    LEFT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestInputTopic<String, JsonNode> rightIn = driver.createInputTopic(
                    RIGHT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestOutputTopic<String, JsonNode> out = driver.createOutputTopic(
                    OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerdes.jsonNode().deserializer());

            leftIn.pipeInput("left-key", mapper.readTree("{\"id\":\"K\",\"name\":\"Alice\"}"));
            rightIn.pipeInput("right-key", mapper.readTree("{\"id\":\"K\",\"orderId\":\"O1\"}"));
            // second update on the left KTable re-triggers the join using the latest right value
            leftIn.pipeInput("left-key-2", mapper.readTree("{\"id\":\"K\",\"name\":\"Alice2\"}"));

            List<KeyValue<String, JsonNode>> records = out.readKeyValuesToList();
            assertEquals(2, records.size());

            JsonNode secondLeft = records.get(1).value.get(config.getLeftStreamName());
            JsonNode secondRight = records.get(1).value.get(config.getRightStreamName());
            assertEquals("Alice2", secondLeft.get("value").get("name").asText());
            assertEquals("O1", secondRight.get("value").get("orderId").asText());
        }
    }

    // A1,B1,A2,B2 (cung key) -> A1B1, A2B1, A2B2: khong sinh cartesian A1B2 nhu windowed JOIN,
    // vi A1 da bi ghi de boi A2 trong KTable truoc khi B2 toi.
    @Test
    void emitsUpsertPairsNotCartesianCombinations() throws Exception {
        TableJoinNodeConfig config = JoinTestSupport.defaultTableJoinConfig();
        try (TopologyTestDriver driver = buildDriver(config)) {
            TestInputTopic<String, JsonNode> leftIn = driver.createInputTopic(
                    LEFT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestInputTopic<String, JsonNode> rightIn = driver.createInputTopic(
                    RIGHT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestOutputTopic<String, JsonNode> out = driver.createOutputTopic(
                    OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerdes.jsonNode().deserializer());

            leftIn.pipeInput("A1", mapper.readTree("{\"id\":\"K\",\"v\":\"A1\"}"));
            rightIn.pipeInput("B1", mapper.readTree("{\"id\":\"K\",\"v\":\"B1\"}"));
            leftIn.pipeInput("A2", mapper.readTree("{\"id\":\"K\",\"v\":\"A2\"}"));
            rightIn.pipeInput("B2", mapper.readTree("{\"id\":\"K\",\"v\":\"B2\"}"));

            List<KeyValue<String, JsonNode>> records = out.readKeyValuesToList();
            List<String> pairs = records.stream()
                    .map(kv -> kv.value.get(config.getLeftStreamName()).get("value").get("v").asText()
                            + kv.value.get(config.getRightStreamName()).get("value").get("v").asText())
                    .toList();

            assertEquals(List.of("A1B1", "A2B1", "A2B2"), pairs);
        }
    }

    @Test
    void noOutputUntilBothSidesHaveAValueForTheKey() throws Exception {
        TableJoinNodeConfig config = JoinTestSupport.defaultTableJoinConfig();
        try (TopologyTestDriver driver = buildDriver(config)) {
            TestInputTopic<String, JsonNode> leftIn = driver.createInputTopic(
                    LEFT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestOutputTopic<String, JsonNode> out = driver.createOutputTopic(
                    OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerdes.jsonNode().deserializer());

            leftIn.pipeInput("left-key", mapper.readTree("{\"id\":\"K\",\"name\":\"Alice\"}"));

            assertTrue(out.readValuesToList().isEmpty());
        }
    }
}
