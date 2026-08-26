package com.streamflow.controlplane.configstore;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streamflow.config.PipelineConfig;
import com.streamflow.config.PipelineStatus;
import com.streamflow.config.type.NodeType;
import com.streamflow.node.process.join.JoinNodeConfig;
import com.streamflow.node.process.join.type.JoinType;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test phan parse thuan (JSON -> PipelineConfig) khong dung Couchbase that - doc that toi Couchbase
 * xac nhan thu cong voi cluster dev, giong cach cac lab chapN can broker that khong co unit test
 * (xem CLAUDE.md).
 */
class CouchbasePipelineConfigStoreTest {

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    void parsesJoinPipelineDocumentIncludingVersionAndStatus() {
        String json = """
                {
                  "bootstrapServers": "localhost:9092",
                  "applicationId": "join-config-fixture-app",
                  "version": 3,
                  "status": "ACTIVE",
                  "nodes": [
                    { "id": "srcLeft", "type": "SOURCE", "topic": "customers" },
                    { "id": "srcRight", "type": "SOURCE", "topic": "orders" },
                    {
                      "id": "joinCustomerOrder",
                      "type": "JOIN",
                      "leftStream": "srcLeft",
                      "rightStream": "srcRight",
                      "leftStreamName": "customer",
                      "rightStreamName": "order",
                      "selectKey": "value.path('customerId')",
                      "joinType": "INNER",
                      "window": "PT5S",
                      "grace": "PT1S"
                    },
                    { "id": "sinkJoined", "type": "SINK", "input": "joinCustomerOrder", "topic": "customer-orders-joined" }
                  ]
                }
                """;

        PipelineConfig config = CouchbasePipelineConfigStore.parseConfig(mapper, json, "join-config-fixture");

        assertEquals("join-config-fixture", config.getPipelineId());
        assertEquals(3L, config.getVersion());
        assertEquals(PipelineStatus.ACTIVE, config.getStatus());

        JoinNodeConfig join = config.getNodes().stream()
                .filter(n -> n.getType() == NodeType.JOIN)
                .map(n -> (JoinNodeConfig) n)
                .findFirst()
                .orElseThrow();
        assertEquals(JoinType.INNER, join.getJoinType());
        assertEquals(Duration.ofSeconds(5), join.getWindow());
    }

    @Test
    void defaultsVersionAndStatusWhenDocumentPredatesThoseFields() {
        String json = """
                {
                  "bootstrapServers": "localhost:9092",
                  "applicationId": "legacy-app",
                  "nodes": []
                }
                """;

        PipelineConfig config = CouchbasePipelineConfigStore.parseConfig(mapper, json, "legacy-pipeline");

        assertEquals(0L, config.getVersion());
        assertEquals(PipelineStatus.ACTIVE, config.getStatus());
    }

    @Test
    void ignoresUnknownOperationalFieldsFromCouchbaseDocument() {
        String json = """
                {
                  "bootstrapServers": "localhost:9092",
                  "applicationId": "ops-app",
                  "createdAt": "2026-08-24T10:00:00Z",
                  "updatedBy": "an.nguyen",
                  "nodes": []
                }
                """;

        PipelineConfig config = CouchbasePipelineConfigStore.parseConfig(mapper, json, "ops-pipeline");

        assertEquals("ops-app", config.getApplicationId());
    }

    @Test
    void rejectsMalformedDocumentWithStructuredError() {
        String malformed = "{ \"applicationId\": ";

        PipelineConfigLoadException ex = assertThrows(PipelineConfigLoadException.class,
                () -> CouchbasePipelineConfigStore.parseConfig(mapper, malformed, "broken-pipeline"));

        assertEquals("broken-pipeline", ex.getPipelineId());
        assertTrue(ex.getMessage().contains("broken-pipeline"));
    }
}
