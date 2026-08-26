package com.streamflow.node.process.join;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streamflow.config.type.NodeType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JoinNodeConfigRoundtripTest {

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    // TC-JOIN-040
    @Test
    void roundtripsAllSevenConfiguredFields() throws Exception {
        JoinNodeConfig original = JoinTestSupport.defaultConfig();
        original.setType(NodeType.JOIN);

        String json = mapper.writeValueAsString(original);
        JoinNodeConfig restored = mapper.readValue(json, JoinNodeConfig.class);

        assertEquals(original.getLeftStream(), restored.getLeftStream());
        assertEquals(original.getRightStream(), restored.getRightStream());
        assertEquals(original.getLeftStreamName(), restored.getLeftStreamName());
        assertEquals(original.getRightStreamName(), restored.getRightStreamName());
        assertEquals(original.getSelectKey(), restored.getSelectKey());
        assertEquals(original.getWindow(), restored.getWindow());
        assertEquals(original.getGrace(), restored.getGrace());
        assertEquals(original.getJoinType(), restored.getJoinType());
    }
}
