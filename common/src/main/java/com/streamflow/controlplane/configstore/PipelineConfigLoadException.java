package com.streamflow.controlplane.configstore;

public class PipelineConfigLoadException extends RuntimeException {

    private final String pipelineId;

    public PipelineConfigLoadException(String pipelineId, String message, Throwable cause) {
        super(message, cause);
        this.pipelineId = pipelineId;
    }

    public String getPipelineId() {
        return pipelineId;
    }
}
