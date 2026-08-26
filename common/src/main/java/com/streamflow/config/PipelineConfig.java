package com.streamflow.config;

import jakarta.validation.constraints.NotBlank;

import java.util.List;

public class PipelineConfig {
    private String pipelineId;

    @NotBlank(message = "bootstrapServers is required")
    private String bootstrapServers;

    @NotBlank(message = "applicationId is required")
    private String applicationId;

    private List<NodeConfig>
            nodes;

    // Dung cho PipelineControlPlane (Couchbase-backed store): version tang moi lan config doi,
    // status quyet dinh Activation API co cho phep pickup pipeline nay hay khong. Khong dung boi
    // InMemoryPipelineConfigStore/demo ValidateAndBuildInnerJoinOperation - mac dinh giu nguyen
    // hanh vi cu (version=0, status=ACTIVE) khi cac field nay khong co trong JSON nguon.
    private long version;

    private PipelineStatus status = PipelineStatus.ACTIVE;

    // User chi config "cluster/worker nao chay pipeline nay" qua 2 field nay (Activation API tu
    // deploy Deployment, khong con thao tac kubectl thu cong - xem docs/PipelineControlPlane/
    // plan.md Giai doan 2). Ca 2 null (mac dinh) = khong ghim node nao, de k8s scheduler tu chon.
    private String nodeSelectorKey;

    private String nodeSelectorValue;

    public PipelineConfig(String pipelineId, String bootstrapServers, String applicationId, List<NodeConfig> nodes) {
        this.pipelineId = pipelineId;
        this.bootstrapServers = bootstrapServers;
        this.applicationId = applicationId;
        this.nodes = nodes;
    }

    public PipelineConfig() {
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public void setPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public List<NodeConfig> getNodes() {
        return nodes;
    }

    public void setNodes(List<NodeConfig> nodes) {
        this.nodes = nodes;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public PipelineStatus getStatus() {
        return status;
    }

    public void setStatus(PipelineStatus status) {
        this.status = status;
    }

    public String getNodeSelectorKey() {
        return nodeSelectorKey;
    }

    public void setNodeSelectorKey(String nodeSelectorKey) {
        this.nodeSelectorKey = nodeSelectorKey;
    }

    public String getNodeSelectorValue() {
        return nodeSelectorValue;
    }

    public void setNodeSelectorValue(String nodeSelectorValue) {
        this.nodeSelectorValue = nodeSelectorValue;
    }
}
