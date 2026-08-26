package com.streamflow.node.process.mapping;

import com.streamflow.node.process.ProcessNodeConfig;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MappingNodeConfig extends ProcessNodeConfig {

    @NotBlank(message = "input la bat buoc")
    private String input;

    @NotEmpty(message = "fields la bat buoc, phai co it nhat 1 field")
    @Valid
    private List<MappingFieldConfig> fields;

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

    public List<MappingFieldConfig> getFields() {
        return fields;
    }

    public void setFields(List<MappingFieldConfig> fields) {
        this.fields = fields;
    }
}
