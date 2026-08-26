package com.streamflow.node.process.aggregate;

import com.streamflow.node.process.aggregate.type.AggregationType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class AggregationDefinition {

    @NotNull(message = "type la bat buoc")
    private AggregationType type;

    private String sourceField;

    @NotBlank(message = "alias la bat buoc")
    private String alias;

    public AggregationDefinition() {
    }

    public AggregationDefinition(AggregationType type, String alias) {
        this.type = type;
        this.alias = alias;
    }

    public AggregationType getType() {
        return type;
    }

    public void setType(AggregationType type) {
        this.type = type;
    }

    public String getSourceField() {
        return sourceField;
    }

    public void setSourceField(String sourceField) {
        this.sourceField = sourceField;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
