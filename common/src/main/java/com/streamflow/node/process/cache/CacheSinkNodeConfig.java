package com.streamflow.node.process.cache;

import com.streamflow.expression.ValidSpelExpression;
import com.streamflow.node.process.ProcessNodeConfig;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Day 1 KStream ra Redis (thay vi 1 Kafka topic nhu SINK) - dung khi can tra cuu nhanh theo key
 * qua 1 API rieng, khong phai doc lai tu Kafka. keyExpr la SpEL evaluate tren root {key, value} (xem
 * SpelEvaluator) - vd "order:@{key}" hoac "order:@{value.path('orderId').asText()}" (template @{})
 * de tron literal prefix voi bieu thuc, nhat quan voi cach MAPPING/FILTER da lam.
 */
public class CacheSinkNodeConfig extends ProcessNodeConfig {

    @NotBlank(message = "input la bat buoc")
    private String input;

    @NotBlank(message = "keyExpr la bat buoc")
    @ValidSpelExpression
    private String keyExpr;

    @NotNull(message = "mode la bat buoc")
    private CacheMode mode;

    @Override
    public Map<String, String> referencedNodeIds() {
        Map<String, String> refs = new LinkedHashMap<>();
        refs.put("input", input);
        return refs;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getKeyExpr() {
        return keyExpr;
    }

    public void setKeyExpr(String keyExpr) {
        this.keyExpr = keyExpr;
    }

    public CacheMode getMode() {
        return mode;
    }

    public void setMode(CacheMode mode) {
        this.mode = mode;
    }
}
