package com.streamflow.node.process.filter;

import com.streamflow.expression.ValidSpelExpression;
import com.streamflow.node.process.ProcessNodeConfig;
import jakarta.validation.constraints.NotBlank;

import java.util.LinkedHashMap;
import java.util.Map;

public class FilterNodeConfig extends ProcessNodeConfig {

    @NotBlank(message = "input la bat buoc")
    private String input;

    @NotBlank(message = "predicate la bat buoc")
    @ValidSpelExpression
    private String predicate;

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

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }
}
