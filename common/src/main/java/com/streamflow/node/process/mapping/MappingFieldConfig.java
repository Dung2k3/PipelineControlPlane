package com.streamflow.node.process.mapping;

import com.streamflow.expression.ValidSpelExpression;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;

public class MappingFieldConfig {

    @NotBlank(message = "target la bat buoc")
    private String target;

    @ValidSpelExpression
    private String source;

    private Boolean fromKey;

    public MappingFieldConfig() {
    }

    public MappingFieldConfig(String target, String source) {
        this.target = target;
        this.source = source;
    }

    public static MappingFieldConfig fromKey(String target) {
        MappingFieldConfig field = new MappingFieldConfig();
        field.target = target;
        field.fromKey = true;
        return field;
    }

    @AssertTrue(message = "moi field phai khai dung 1 trong 2: source hoac fromKey=true")
    private boolean isSourceXorFromKey() {
        boolean hasSource = source != null && !source.isBlank();
        boolean hasFromKey = Boolean.TRUE.equals(fromKey);
        return hasSource ^ hasFromKey;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Boolean getFromKey() {
        return fromKey;
    }

    public void setFromKey(Boolean fromKey) {
        this.fromKey = fromKey;
    }

    public boolean isFromKey() {
        return Boolean.TRUE.equals(fromKey);
    }
}
