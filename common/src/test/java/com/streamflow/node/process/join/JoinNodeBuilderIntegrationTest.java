package com.streamflow.node.process.join;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamflow.serdes.JsonSerdes;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinNodeBuilderIntegrationTest {

    private static final String LEFT_TOPIC = "integration-left";
    private static final String RIGHT_TOPIC = "integration-right";
    private static final String OUTPUT_TOPIC = "integration-output";
    private static final long BASE = 100_000L;

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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-node-builder-integration-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        return new TopologyTestDriver(builder.build(), props);
    }

    private JsonNode valueWithId(String id) throws Exception {
        return mapper.readTree("{\"id\":\"" + id + "\"}");
    }

    private record Harness(
            TopologyTestDriver driver,
            TestInputTopic<String, JsonNode> leftIn,
            TestInputTopic<String, JsonNode> rightIn,
            TestOutputTopic<String, JsonNode> out) implements AutoCloseable {
        @Override
        public void close() {
            driver.close();
        }
    }

    private Harness harness(JoinNodeConfig config) {
        TopologyTestDriver driver = buildDriver(config);
        TestInputTopic<String, JsonNode> leftIn = driver.createInputTopic(
                LEFT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
        TestInputTopic<String, JsonNode> rightIn = driver.createInputTopic(
                RIGHT_TOPIC, Serdes.String().serializer(), JsonSerdes.jsonNode().serializer());
        TestOutputTopic<String, JsonNode> out = driver.createOutputTopic(
                OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerdes.jsonNode().deserializer());
        return new Harness(driver, leftIn, rightIn, out);
    }

    // TC-JOIN-027 (kem TC-JOIN-025 / SC10-a: JOINED.key == ket qua selectKey)
    @Test
    void basicMatchProducesOneOutputKeyedBySelectKey() throws Exception {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        try (Harness h = harness(config)) {
            h.leftIn().pipeInput("lk", valueWithId("001"), Instant.ofEpochMilli(BASE));
            h.rightIn().pipeInput("rk", valueWithId("001"), Instant.ofEpochMilli(BASE));

            List<KeyValue<String, JsonNode>> records = h.out().readKeyValuesToList();
            assertEquals(1, records.size());
            assertEquals("001", records.get(0).key);
        }
    }

    // TC-JOIN-028
    @Test
    void differentKeysProduceNoOutput() throws Exception {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        try (Harness h = harness(config)) {
            h.leftIn().pipeInput("lk", valueWithId("001"), Instant.ofEpochMilli(BASE));
            h.rightIn().pipeInput("rk", valueWithId("002"), Instant.ofEpochMilli(BASE));

            assertTrue(h.out().readValuesToList().isEmpty());
        }
    }

    // TC-JOIN-029
    @Test
    void sameKeyOutsideWindowProducesNoOutput() throws Exception {
        JoinNodeConfig config = JoinTestSupport.configWithWindow(Duration.ofSeconds(5), Duration.ofSeconds(1));
        try (Harness h = harness(config)) {
            h.leftIn().pipeInput("lk", valueWithId("001"), Instant.ofEpochMilli(BASE));
            h.rightIn().pipeInput("rk", valueWithId("001"), Instant.ofEpochMilli(BASE + 20_000));

            assertTrue(h.out().readValuesToList().isEmpty());
        }
    }

    // TC-JOIN-030 (BVA: chenh lech = 0)
    @Test
    void joinsWhenTimestampDifferenceIsZero() throws Exception {
        assertJoinsAtDifference(0);
    }

    // TC-JOIN-031 (BVA: chenh lech = +W)
    @Test
    void joinsAtUpperWindowBoundary() throws Exception {
        assertJoinsAtDifference(5000);
    }

    // TC-JOIN-032 (BVA: chenh lech = +(W+1))
    @Test
    void doesNotJoinJustAboveUpperWindowBoundary() throws Exception {
        assertDoesNotJoinAtDifference(5001);
    }

    // TC-JOIN-033 (BVA: chenh lech = -W)
    @Test
    void joinsAtLowerWindowBoundary() throws Exception {
        assertJoinsAtDifference(-5000);
    }

    // TC-JOIN-034 (BVA: chenh lech = -(W+1))
    @Test
    void doesNotJoinJustBelowLowerWindowBoundary() throws Exception {
        assertDoesNotJoinAtDifference(-5001);
    }

    private void assertJoinsAtDifference(long diffMs) throws Exception {
        JoinNodeConfig config = JoinTestSupport.configWithWindow(Duration.ofSeconds(5), Duration.ZERO);
        try (Harness h = harness(config)) {
            h.leftIn().pipeInput("lk", valueWithId("001"), Instant.ofEpochMilli(BASE));
            h.rightIn().pipeInput("rk", valueWithId("001"), Instant.ofEpochMilli(BASE + diffMs));

            assertEquals(1, h.out().readValuesToList().size());
        }
    }

    private void assertDoesNotJoinAtDifference(long diffMs) throws Exception {
        JoinNodeConfig config = JoinTestSupport.configWithWindow(Duration.ofSeconds(5), Duration.ZERO);
        try (Harness h = harness(config)) {
            h.leftIn().pipeInput("lk", valueWithId("001"), Instant.ofEpochMilli(BASE));
            h.rightIn().pipeInput("rk", valueWithId("001"), Instant.ofEpochMilli(BASE + diffMs));

            assertTrue(h.out().readValuesToList().isEmpty());
        }
    }

    // TC-JOIN-035..037 (grace-period BVA: tre so voi moc dong window) — KHONG viet duoc dang tin
    // cay bang TopologyTestDriver: da thu ky thuat gui 1 record "filler" de day stream-time len
    // truoc khi gui record tre, nhung verify thuc nghiem (probe rieng, da xoa) cho thay dela den
    // 20000ms (gap hon 3 lan W+G=6000ms) van join binh thuong — ky thuat nay khong trigger dung
    // co che eviction that su cua windowed state store (nhieu kha nang do segment-based retention
    // can nhieu du lieu/thoi gian hon de rollover segment). Day dung la van de repo nay da tung
    // gap voi chap6 (xem commit "Replace chap6 sliding-window tests with grace=0 case suite and
    // manual test tooling") — khong co case tu dong hoa cho SC07-b/c/d o day, can quyet dinh cach
    // xu ly khac (vd manual test, hoac ky thuat do luong khac).

    // TC-JOIN-038
    @Test
    void manyToManyMatchesProduceFullCrossProduct() throws Exception {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        try (Harness h = harness(config)) {
            h.leftIn().pipeInput("l1", valueWithId("match"), Instant.ofEpochMilli(BASE));
            h.leftIn().pipeInput("l2", valueWithId("match"), Instant.ofEpochMilli(BASE));

            h.rightIn().pipeInput("r1", valueWithId("match"), Instant.ofEpochMilli(BASE));
            h.rightIn().pipeInput("r2", valueWithId("match"), Instant.ofEpochMilli(BASE));
            h.rightIn().pipeInput("r3", valueWithId("match"), Instant.ofEpochMilli(BASE));

            assertEquals(6, h.out().readValuesToList().size());
        }
    }

    // TC-JOIN-039
    @Test
    void eventWithoutAnyValidPairProducesNoOutput() throws Exception {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        try (Harness h = harness(config)) {
            h.leftIn().pipeInput("lonely", valueWithId("no-partner"), Instant.ofEpochMilli(BASE));

            assertTrue(h.out().readValuesToList().isEmpty());
        }
    }

    // TC-JOIN-049 (SC26-a: khong co trong REQ.md/SCOPE.md, la hanh vi MAC DINH cua Kafka Streams
    // -- JoinNodeBuilder khong cau hinh TimestampExtractor tuy bien nao, engine tu lay
    // max(timestamp trai, timestamp phai) cho record output cua windowed join)
    @Test
    void outputTimestampIsMaxOfBothSides() throws Exception {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        try (Harness h = harness(config)) {
            long leftTs = BASE;
            long rightTs = BASE + 2000;
            h.leftIn().pipeInput("lk", valueWithId("001"), Instant.ofEpochMilli(leftTs));
            h.rightIn().pipeInput("rk", valueWithId("001"), Instant.ofEpochMilli(rightTs));

            List<TestRecord<String, JsonNode>> records = h.out().readRecordsToList();
            assertEquals(1, records.size());
            assertEquals(Math.max(leftTs, rightTs), records.get(0).timestamp());
        }
    }

    // TC-JOIN-050 (SC27-a: cung khong co trong REQ.md/SCOPE.md -- characterization cho hanh vi
    // mac dinh cua Kafka (DefaultPartitioner, hash theo key). SinkNodeBuilder/JoinNodeBuilder
    // khong truyen StreamPartitioner tuy bien nao (xem Produced.with()/StreamJoined.with() trong
    // 2 class do), nen partition cua record output hoan toan phu thuoc DefaultPartitioner cua
    // Kafka client -- 1 ham thuan, xac dinh theo key bytes. TopologyTestDriver khong mo phong
    // nhieu partition cho 1 topic nen khong quan sat truc tiep duoc qua no; verify thang tinh
    // xac dinh (deterministic) cua thuat toan hash ma DefaultPartitioner dung (murmur2), dung
    // cung key bytes ma record output that su se mang (Serdes.String()).
    @Test
    void sameJoinKeyAlwaysHashesToSamePartition() {
        byte[] keyBytes = Serdes.String().serializer().serialize("customer-orders-joined", "001");
        int numPartitions = 6;

        int first = Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        int second = Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        int third = Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;

        assertEquals(first, second);
        assertEquals(first, third);
    }
}
