package com.streamflow.validation;

public class ValidationException extends RuntimeException {

    private final String pipelineId;
    private final String nodeId;
    private final String field;

    public ValidationException(String pipelineId, String nodeId, String field, String message) {
        super(message);
        this.pipelineId = pipelineId;
        this.nodeId = nodeId;
        this.field = field;
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getField() {
        return field;
    }
}
