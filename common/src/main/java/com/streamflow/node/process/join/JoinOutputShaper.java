package com.streamflow.node.process.join;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JoinOutputShaper {
    private JoinOutputShaper() {}

    public static JsonNode shape(
            String leftName, String rightName, JoinRecordWrapper left, JoinRecordWrapper right) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.set(leftName, wrap(left));
        result.set(rightName, wrap(right));
        return result;
    }

    private static ObjectNode wrap(JoinRecordWrapper record) {
        ObjectNode wrapped = JsonNodeFactory.instance.objectNode();
        wrapped.put("key", record.key());
        wrapped.set("value", record.value());
        return wrapped;
    }
}
