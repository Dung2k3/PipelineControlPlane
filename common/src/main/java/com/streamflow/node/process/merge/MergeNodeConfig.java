package com.streamflow.node.process.merge;

import com.streamflow.node.process.ProcessNodeConfig;
import jakarta.validation.constraints.NotEmpty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MergeNodeConfig extends ProcessNodeConfig {

    @NotEmpty(message = "inputs la bat buoc, phai co it nhat 1 phan tu")
    private List<String> inputs;

    @Override
    public Map<String, String> referencedNodeIds() {
        Map<String, String> refs = new LinkedHashMap<>();
        if (inputs != null) {
            for (int i = 0; i < inputs.size(); i++) {
                refs.put("inputs[" + i + "]", inputs.get(i));
            }
        }
        return refs;
    }

    public List<String> getInputs() {
        return inputs;
    }

    public void setInputs(List<String> inputs) {
        this.inputs = inputs;
    }
}
