package com.streamflow.node.process.mapping;

import jakarta.validation.constraints.NotBlank;

public class EnrichResultFieldConfig {

    @NotBlank(message = "column la bat buoc")
    private String column;

    @NotBlank(message = "target la bat buoc")
    private String target;

    public EnrichResultFieldConfig() {
    }

    public EnrichResultFieldConfig(String column, String target) {
        this.column = column;
        this.target = target;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }
}
