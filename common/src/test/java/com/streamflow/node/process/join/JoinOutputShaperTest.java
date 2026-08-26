package com.streamflow.node.process.join;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinOutputShaperTest {

    private final ObjectMapper mapper = new ObjectMapper();

    // TC-JOIN-021
    @Test
    void shapesOutputWithBothSidesUnderConfiguredNames() throws Exception {
        JoinRecordWrapper left = new JoinRecordWrapper("leftKey", mapper.readTree("{\"a\":1}"));
        JoinRecordWrapper right = new JoinRecordWrapper("rightKey", mapper.readTree("{\"b\":2}"));

        JsonNode result = JoinOutputShaper.shape("nameA", "nameB", left, right);

        assertEquals("leftKey", result.get("nameA").get("key").asText());
        assertEquals(1, result.get("nameA").get("value").get("a").asInt());
        assertEquals("rightKey", result.get("nameB").get("key").asText());
        assertEquals(2, result.get("nameB").get("value").get("b").asInt());
    }

    // TC-JOIN-022
    @Test
    void preservesNestedStructureInValue() throws Exception {
        JsonNode nested = mapper.readTree("{\"outer\":{\"inner\":[1,2,3]}}");
        JoinRecordWrapper left = new JoinRecordWrapper("k1", nested);
        JoinRecordWrapper right = new JoinRecordWrapper("k2", mapper.readTree("{}"));

        JsonNode result = JoinOutputShaper.shape("left", "right", left, right);

        assertEquals(nested, result.get("left").get("value"));
    }

    // TC-JOIN-023
    @Test
    void wrapsEmptyValueWithoutError() throws Exception {
        JoinRecordWrapper left = new JoinRecordWrapper("k1", mapper.readTree("{}"));
        JoinRecordWrapper right = new JoinRecordWrapper("k2", mapper.readTree("{}"));

        JsonNode result = JoinOutputShaper.shape("left", "right", left, right);

        assertTrue(result.get("left").get("value").isEmpty());
        assertTrue(result.get("right").get("value").isEmpty());
    }

    // TC-JOIN-024
    @Test
    void doesNotLeakMetaAsTopLevelField() throws Exception {
        // _meta (neu co) van con nguyen ben trong <name>.value cua chinh no (day la du lieu
        // that cua event goc, khong phai thu Output Shaper them vao) -- dieu SC11-e/FR-11 cam
        // la he thong TU Y them 1 field "_meta" MOI o cap JOINED.value. Vi Output Shaper luon
        // chi tao dung 2 field cap cao nhat (leftName/rightName), "_meta" khong the nao xuat
        // hien o CAP DO NAY duoc, du input co _meta hay khong.
        JsonNode withMeta = mapper.readTree("{\"id\":\"K\",\"_meta\":{\"source\":\"x\"}}");
        JoinRecordWrapper left = new JoinRecordWrapper("k1", withMeta);
        JoinRecordWrapper right = new JoinRecordWrapper("k2", mapper.readTree("{}"));

        JsonNode result = JoinOutputShaper.shape("left", "right", left, right);

        assertEquals(2, fieldCount(result));
        assertTrue(result.has("left"));
        assertTrue(result.has("right"));
    }

    // TC-JOIN-026
    @Test
    void outputStructureMatchesConfiguredFieldNames() throws Exception {
        JoinRecordWrapper left = new JoinRecordWrapper("k1", mapper.readTree("{\"x\":1}"));
        JoinRecordWrapper right = new JoinRecordWrapper("k2", mapper.readTree("{\"y\":2}"));

        JsonNode result = JoinOutputShaper.shape("nameA", "nameB", left, right);

        assertTrue(result.has("nameA"));
        assertTrue(result.has("nameB"));
        assertTrue(result.get("nameA").has("key"));
        assertTrue(result.get("nameA").has("value"));
        assertTrue(result.get("nameB").has("key"));
        assertTrue(result.get("nameB").has("value"));
    }

    private static int fieldCount(JsonNode node) {
        int count = 0;
        Iterator<String> it = node.fieldNames();
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }
}
