package com.streamflow.controlplane.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class AppConfig {

    private static final Map<String, String> DEFAULTS = loadDefaults();

    private AppConfig() {
    }

    public static String get(String key) {
        String envValue = System.getenv(key);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        return DEFAULTS.get(key);
    }

    public static String require(String key) {
        String value = get(key);
        if (value == null) {
            throw new IllegalStateException("Thieu cau hinh bat buoc '" + key
                    + "' - set qua env var (khong co gia tri mac dinh trong application.yaml)");
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> loadDefaults() {
        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("application.yaml")) {
            if (in == null) {
                return Collections.emptyMap();
            }
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> raw = yamlMapper.readValue(in, Map.class);
            Map<String, String> result = new HashMap<>();
            raw.forEach((key, value) -> {
                if (value != null && !String.valueOf(value).isBlank()) {
                    result.put(key, String.valueOf(value));
                }
            });
            return result;
        } catch (IOException e) {
            throw new IllegalStateException("Khong doc duoc application.yaml", e);
        }
    }
}
