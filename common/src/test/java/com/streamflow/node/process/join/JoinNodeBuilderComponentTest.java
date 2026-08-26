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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinNodeBuilderComponentTest {

    private static final String LEFT_TOPIC = "component-left";
    private static final String RIGHT_TOPIC = "component-right";
    private static final String OUTPUT_TOPIC = "component-output";

    private final ObjectMapper mapper = new ObjectMapper();

    private TopologyTestDriver buildDriver(JoinNodeConfig config) {
        StreamsBuilder builder = new StreamsBuilder();
        TopologyBuildContext context = new TopologyBuildContext(builder);

        KStream<String, JsonNode> left =
                builder.stream(LEFT_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.jsonNode()));
        KStream<String, JsonNode> right =
                builder.stream(RIGHT_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.jsonNode()));
        context.register(config.getLeftStream(), left);
        context.register(config.getRightStream(), right);

        new JoinNodeBuilder().build(config, context);

        context.get(config.getId()).to(OUTPUT_TOPIC, Produced.with(Serdes.String(), JsonSerdes.jsonNode()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-node-builder-component-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        return new TopologyTestDriver(builder.build(), props);
    }

    // TC-JOIN-019
    @Test
    void rekeyKeepsOriginalKeyInWrapper() throws Exception {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        try (TopologyTestDriver driver = buildDriver(config)) {
            TestInputTopic<String, JsonNode> leftIn = driver.createInputTopic(
                    LEFT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestInputTopic<String, JsonNode> rightIn = driver.createInputTopic(
                    RIGHT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestOutputTopic<String, JsonNode> out = driver.createOutputTopic(
                    OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerdes.jsonNode().deserializer());

            JsonNode leftValue = mapper.readTree("{\"id\":\"K\",\"name\":\"Alice\"}");
            JsonNode rightValue = mapper.readTree("{\"id\":\"K\",\"orderId\":\"O1\"}");

            leftIn.pipeInput("original-left-key", leftValue, Instant.ofEpochMilli(1000));
            rightIn.pipeInput("original-right-key", rightValue, Instant.ofEpochMilli(1000));

            List<KeyValue<String, JsonNode>> records = out.readKeyValuesToList();
            assertEquals(1, records.size());
            assertEquals("K", records.get(0).key);

            JsonNode leftWrapped = records.get(0).value.get(config.getLeftStreamName());
            assertEquals("original-left-key", leftWrapped.get("key").asText());
            assertEquals(leftValue, leftWrapped.get("value"));
        }
    }

    // TC-JOIN-020
    @Test
    void selectKeyErrorPropagatesWithoutSkip() throws Exception {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        try (TopologyTestDriver driver = buildDriver(config)) {
            TestInputTopic<String, JsonNode> leftIn = driver.createInputTopic(
                    LEFT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
            TestOutputTopic<String, JsonNode> out = driver.createOutputTopic(
                    OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerdes.jsonNode().deserializer());

            JsonNode missingFieldValue = mapper.readTree("{\"name\":\"Alice\"}");

            assertThrows(RuntimeException.class,
                    () -> leftIn.pipeInput("k1", missingFieldValue, Instant.ofEpochMilli(1000)));

            assertTrue(out.readValuesToList().isEmpty());
        }
    }
}
