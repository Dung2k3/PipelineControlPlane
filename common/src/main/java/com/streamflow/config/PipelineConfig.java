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

    // Resource hint cho container cua pod (k8s quantity format, vd "250m", "512Mi"). Null (mac dinh)
    // = PipelineDeploymentManager tu fallback ve gia tri mac dinh chung, dung cho pipeline nao chua
    // can tune rieng. Cho phep pipeline nang (nhieu Join/Aggregate/JDBC enrich) xin nhieu resource
    // hon pipeline nhe (Filter/Mapping don gian) thay vi ca 2 dung chung 1 muc cung.
    private String cpuRequest;

    private String cpuLimit;

    private String memoryRequest;

    private String memoryLimit;

    // Kafka Streams tuning hint - null (mac dinh) = khong set property tuong ung, de Kafka Streams
    // tu dung default cua no. Pipeline nhieu Join/Aggregate (RocksDB state store nang) can cache lon
    // hon va it commit hon; pipeline chi Filter/Mapping don gian khong can chinh gi. numStreamThreads
    // nen <= so CPU limit da xin trong cpuLimit o tren, khong tu suy ra o day de tranh 2 field ngam
    // rang buoc nhau kho hieu.
    private Integer numStreamThreads;

    private Long statestoreCacheMaxBytes;

    private Long commitIntervalMs;

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

    public String getCpuRequest() {
        return cpuRequest;
    }

    public void setCpuRequest(String cpuRequest) {
        this.cpuRequest = cpuRequest;
    }

    public String getCpuLimit() {
        return cpuLimit;
    }

    public void setCpuLimit(String cpuLimit) {
        this.cpuLimit = cpuLimit;
    }

    public String getMemoryRequest() {
        return memoryRequest;
    }

    public void setMemoryRequest(String memoryRequest) {
        this.memoryRequest = memoryRequest;
    }

    public String getMemoryLimit() {
        return memoryLimit;
    }

    public void setMemoryLimit(String memoryLimit) {
        this.memoryLimit = memoryLimit;
    }

    public Integer getNumStreamThreads() {
        return numStreamThreads;
    }

    public void setNumStreamThreads(Integer numStreamThreads) {
        this.numStreamThreads = numStreamThreads;
    }

    public Long getStatestoreCacheMaxBytes() {
        return statestoreCacheMaxBytes;
    }

    public void setStatestoreCacheMaxBytes(Long statestoreCacheMaxBytes) {
        this.statestoreCacheMaxBytes = statestoreCacheMaxBytes;
    }

    public Long getCommitIntervalMs() {
        return commitIntervalMs;
    }

    public void setCommitIntervalMs(Long commitIntervalMs) {
        this.commitIntervalMs = commitIntervalMs;
    }
}
