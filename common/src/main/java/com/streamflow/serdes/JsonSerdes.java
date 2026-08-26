package com.streamflow.serdes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public final class JsonSerdes {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new JavaTimeModule());

    private JsonSerdes() {}

    public static <T> Serde<T> of(Class<T> type) {
        Serializer<T> serializer =
                (topic, data) -> {
                    if (data == null) {
                        return null;
                    }
                    try {
                        return MAPPER.writeValueAsBytes(data);
                    } catch (Exception e) {
                        throw new RuntimeException("Loi serialize JSON cho topic " + topic, e);
                    }
                };

        Deserializer<T> deserializer =
                (topic, bytes) -> {
                    if (bytes == null) {
                        return null;
                    }
                    try {
                        return MAPPER.readValue(bytes, type);
                    } catch (Exception e) {
                        throw new RuntimeException("Loi deserialize JSON cho topic " + topic, e);
                    }
                };

        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<JsonNode> jsonNode() {
        return of(JsonNode.class);
    }
}
