package com.streamflow.expression;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpelEvaluatorTest {

    private final ObjectMapper mapper = new ObjectMapper();

    // TC-JOIN-017
    @Test
    void evaluateTextComputesKeyFromValue() throws Exception {
        JsonNode value = mapper.readTree("{\"id\":\"K\"}");
        SpelEvaluator evaluator = new SpelEvaluator("value.path('id')");

        assertEquals("K", evaluator.evaluateText("someKey", value));
    }

    // TC-JOIN-018
    @Test
    void evaluateTextAppliesConsistentlyToBothSides() throws Exception {
        SpelEvaluator evaluator = new SpelEvaluator("value.path('id')");
        JsonNode left = mapper.readTree("{\"id\":\"K\",\"name\":\"Alice\"}");
        JsonNode right = mapper.readTree("{\"id\":\"K\",\"orderId\":\"O1\"}");

        assertEquals("K", evaluator.evaluateText("leftKey", left));
        assertEquals("K", evaluator.evaluateText("rightKey", right));
    }

    @Test
    void evaluateTextCanReadFromKey() throws Exception {
        JsonNode value = mapper.readTree("{}");
        SpelEvaluator evaluator = new SpelEvaluator("key");

        assertEquals("K1", evaluator.evaluateText("K1", value));
    }

    @Test
    void evaluateBooleanComputesPredicateFromValue() throws Exception {
        JsonNode matching = mapper.readTree("{\"amount\":100,\"expected\":50}");
        JsonNode nonMatching = mapper.readTree("{\"amount\":50,\"expected\":50}");
        SpelEvaluator evaluator =
                new SpelEvaluator("value.path('amount').asDouble() != value.path('expected').asDouble()");

        assertEquals(true, evaluator.evaluateBoolean("k", matching));
        assertEquals(false, evaluator.evaluateBoolean("k", nonMatching));
    }

    @Test
    void evaluateNodeReadsFieldFromPayload() throws Exception {
        JsonNode value = mapper.readTree("{\"id\":\"K\"}");
        SpelEvaluator evaluator = new SpelEvaluator("value.path('id')");

        assertEquals("K", evaluator.evaluateNode("k", value).asText());
    }

    @Test
    void evaluateNodeWrapsLiteralAndComputedValues() throws Exception {
        JsonNode value = mapper.readTree("{}");
        SpelEvaluator literal = new SpelEvaluator("'FIXED_VALUE'");
        SpelEvaluator computed = new SpelEvaluator("T(java.lang.System).currentTimeMillis()");

        assertEquals("FIXED_VALUE", literal.evaluateNode("k", value).asText());
        assertEquals(true, computed.evaluateNode("k", value).isNumber());
    }

    @Test
    void bareExpressionPreservesOriginalType() throws Exception {
        JsonNode value = mapper.readTree("{\"amount\":100}");
        SpelEvaluator evaluator = new SpelEvaluator("value.path('amount')");

        assertTrue(evaluator.evaluateNode("k", value).isNumber());
    }

    @Test
    void templateExpressionMixesLiteralTextWithSpelResult() throws Exception {
        JsonNode value = mapper.readTree("{\"orderId\":\"O1\"}");
        SpelEvaluator evaluator = new SpelEvaluator("order:@{key}:@{value.path('orderId').asText()}");

        assertEquals("order:K1:O1", evaluator.evaluateText("K1", value));
    }
}
