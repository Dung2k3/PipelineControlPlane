package com.streamflow.controlplane.k8s;

import com.streamflow.config.PipelineConfig;
import com.streamflow.controlplane.config.AppConfig;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.metrics.v1beta1.ContainerMetrics;
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetrics;
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetricsList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.utils.Serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

public class PipelineDeploymentManager {

    private static final String TEMPLATE_RESOURCE = "/k8s/pipeline-deployment.template.yaml";
    private static final String DEPLOYMENT_PREFIX = "pipeline-";
    private static final String RESTARTED_AT_ANNOTATION = "kubectl.kubernetes.io/restartedAt";
    private static final String PIPELINE_ID_LABEL = "pipelineId";

    private static final Set<String> ERROR_WAITING_REASONS = Set.of(
            "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull",
            "CreateContainerConfigError", "InvalidImageName", "RunContainerError");

    private static final String DEFAULT_CPU_REQUEST = "250m";
    private static final String DEFAULT_CPU_LIMIT = "1";
    private static final String DEFAULT_MEMORY_REQUEST = "512Mi";
    private static final String DEFAULT_MEMORY_LIMIT = "1Gi";

    private final KubernetesClient client;
    private final String namespace;
    private final String templateYaml;

    public PipelineDeploymentManager() {
        this(new KubernetesClientBuilder().build(), AppConfig.get("K8S_NAMESPACE"));
    }

    PipelineDeploymentManager(KubernetesClient client, String namespace) {
        this.client = client;
        this.namespace = namespace;
        this.templateYaml = loadTemplate();
    }

    public DeploymentStatus getStatus(String pipelineId) {
        String deploymentName = DEPLOYMENT_PREFIX + pipelineId;
        Deployment current = client.apps().deployments()
                .inNamespace(namespace)
                .withName(deploymentName)
                .get();

        if (current == null) {
            return new DeploymentStatus(false, null, null, null);
        }

        Integer desiredReplicas = current.getSpec() != null ? current.getSpec().getReplicas() : null;
        Integer readyReplicas = current.getStatus() != null ? current.getStatus().getReadyReplicas() : null;
        return new DeploymentStatus(true, desiredReplicas, readyReplicas == null ? 0 : readyReplicas,
                findPodErrorReason(pipelineId));
    }

    private String findPodErrorReason(String pipelineId) {
        for (Pod pod : client.pods().inNamespace(namespace).withLabel(PIPELINE_ID_LABEL, pipelineId).list().getItems()) {
            if (pod.getStatus() == null || pod.getStatus().getContainerStatuses() == null) {
                continue;
            }
            for (ContainerStatus containerStatus : pod.getStatus().getContainerStatuses()) {
                if (containerStatus.getState() == null || containerStatus.getState().getWaiting() == null) {
                    continue;
                }
                String reason = containerStatus.getState().getWaiting().getReason();
                if (ERROR_WAITING_REASONS.contains(reason)) {
                    return reason;
                }
            }
        }
        return null;
    }

    public ResourceUsage getResourceUsage(String pipelineId) {
        PodMetricsList metricsList;
        try {
            metricsList = client.top().pods()
                    .withLabels(Map.of(PIPELINE_ID_LABEL, pipelineId))
                    .metrics(namespace);
        } catch (KubernetesClientException e) {
            return null;
        }

        if (metricsList.getItems().isEmpty()) {
            return null;
        }

        BigDecimal cpuCores = BigDecimal.ZERO;
        BigDecimal memoryBytes = BigDecimal.ZERO;
        for (PodMetrics podMetrics : metricsList.getItems()) {
            for (ContainerMetrics containerMetrics : podMetrics.getContainers()) {
                Quantity cpu = containerMetrics.getUsage().get("cpu");
                Quantity memory = containerMetrics.getUsage().get("memory");
                if (cpu != null) {
                    cpuCores = cpuCores.add(cpu.getNumericalAmount());
                }
                if (memory != null) {
                    memoryBytes = memoryBytes.add(Quantity.getAmountInBytes(memory));
                }
            }
        }

        long cpuMillis = cpuCores.multiply(BigDecimal.valueOf(1000)).longValue();
        long memoryMi = memoryBytes.divide(BigDecimal.valueOf(1024L * 1024), RoundingMode.HALF_UP).longValue();
        return new ResourceUsage(cpuMillis + "m", memoryMi + "Mi");
    }

    public boolean ensureDeployed(String pipelineId, PipelineConfig config) {
        String deploymentName = DEPLOYMENT_PREFIX + pipelineId;
        Deployment current = client.apps().deployments()
                .inNamespace(namespace)
                .withName(deploymentName)
                .get();

        if (current == null) {
            create(pipelineId, config);
            return true;
        }
        restart(pipelineId, deploymentName);
        return false;
    }

    public boolean undeploy(String pipelineId) {
        String deploymentName = DEPLOYMENT_PREFIX + pipelineId;
        Deployment current = client.apps().deployments()
                .inNamespace(namespace)
                .withName(deploymentName)
                .get();

        if (current == null) {
            return false;
        }

        try {
            client.apps().deployments()
                    .inNamespace(namespace)
                    .withName(deploymentName)
                    .delete();
        } catch (KubernetesClientException e) {
            throw new DeploymentOperationException(pipelineId, namespace, deploymentName, e);
        }
        return true;
    }

    private void create(String pipelineId, PipelineConfig config) {
        String yaml = templateYaml
                .replace("${PIPELINE_ID}", pipelineId)
                .replace("${IMAGE}", AppConfig.get("PIPELINE_IMAGE"))
                .replace("${COUCHBASE_CONNECTION_STRING}", AppConfig.get("PIPELINE_COUCHBASE_CONNECTION_STRING"))
                .replace("${COUCHBASE_USERNAME}", AppConfig.get("COUCHBASE_USERNAME"))
                .replace("${COUCHBASE_PASSWORD}", AppConfig.get("COUCHBASE_PASSWORD"))
                .replace("${COUCHBASE_BUCKET}", AppConfig.get("COUCHBASE_BUCKET"))
                .replace("${CPU_REQUEST}", orDefault(config.getCpuRequest(), DEFAULT_CPU_REQUEST))
                .replace("${CPU_LIMIT}", orDefault(config.getCpuLimit(), DEFAULT_CPU_LIMIT))
                .replace("${MEMORY_REQUEST}", orDefault(config.getMemoryRequest(), DEFAULT_MEMORY_REQUEST))
                .replace("${MEMORY_LIMIT}", orDefault(config.getMemoryLimit(), DEFAULT_MEMORY_LIMIT));

        Deployment deployment = Serialization.unmarshal(yaml, Deployment.class);
        deployment.getMetadata().setNamespace(namespace);
        if (config.getNodeSelectorKey() != null && config.getNodeSelectorValue() != null) {
            deployment.getSpec().getTemplate().getSpec()
                    .setNodeSelector(Map.of(config.getNodeSelectorKey(), config.getNodeSelectorValue()));
        }

        client.apps().deployments().inNamespace(namespace).resource(deployment).create();
    }

    private void restart(String pipelineId, String deploymentName) {
        try {
            client.apps().deployments()
                    .inNamespace(namespace)
                    .withName(deploymentName)
                    .edit(d -> new DeploymentBuilder(d)
                            .editSpec().editTemplate().editMetadata()
                            .addToAnnotations(RESTARTED_AT_ANNOTATION, Instant.now().toString())
                            .endMetadata().endTemplate().endSpec()
                            .build());
        } catch (KubernetesClientException e) {
            throw new DeploymentOperationException(pipelineId, namespace, deploymentName, e);
        }
    }

    private static String orDefault(String value, String fallback) {
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private static String loadTemplate() {
        try (InputStream in = PipelineDeploymentManager.class.getResourceAsStream(TEMPLATE_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Khong tim thay resource " + TEMPLATE_RESOURCE + " trong classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Khong doc duoc " + TEMPLATE_RESOURCE, e);
        }
    }

    public static class DeploymentOperationException extends RuntimeException {
        public DeploymentOperationException(String pipelineId, String namespace, String deploymentName, Throwable cause) {
            super("Loi thao tac Deployment '" + deploymentName + "' trong namespace '" + namespace
                    + "' cho pipelineId=" + pipelineId, cause);
        }
    }

    public record DeploymentStatus(boolean deployed, Integer desiredReplicas, Integer readyReplicas, String errorReason) {
    }

    public record ResourceUsage(String cpu, String memory) {
    }
}
