package com.streamflow.node.process.aggregate;

import com.streamflow.node.process.aggregate.type.WindowType;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

public class AggWindowConfig {

    @NotNull(message = "type la bat buoc")
    private WindowType type;

    @NotNull(message = "sizeMs la bat buoc")
    @Positive(message = "sizeMs phai > 0")
    private Long sizeMs;

    @NotNull(message = "graceMs la bat buoc")
    @PositiveOrZero(message = "graceMs phai >= 0")
    private Long graceMs;

    private Long inactivityGapMs;

    public WindowType getType() {
        return type;
    }

    public void setType(WindowType type) {
        this.type = type;
    }

    public Long getSizeMs() {
        return sizeMs;
    }

    public void setSizeMs(Long sizeMs) {
        this.sizeMs = sizeMs;
    }

    public Long getGraceMs() {
        return graceMs;
    }

    public void setGraceMs(Long graceMs) {
        this.graceMs = graceMs;
    }

    public Long getInactivityGapMs() {
        return inactivityGapMs;
    }

    public void setInactivityGapMs(Long inactivityGapMs) {
        this.inactivityGapMs = inactivityGapMs;
    }
}
