package com.streamflow.expression;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Root object cho SpelEvaluator: lo ca Kafka record key (String) lan value (JsonNode payload) duoi
 * dang property "key"/"value" - bieu thuc SpEL viet key.xxx / value.xxx thay vi truoc day chi co the
 * thao tac truc tiep tren value (vd path('id')).
 */
public final class SpelRoot {

    private final String key;
    private final JsonNode value;

    public SpelRoot(String key, JsonNode value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public JsonNode getValue() {
        return value;
    }
}
