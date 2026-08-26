package com.streamflow.controlplane.k8s;

import com.streamflow.config.PipelineConfig;
import com.streamflow.controlplane.config.AppConfig;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.utils.Serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

public class PipelineDeploymentManager {

    private static final String TEMPLATE_RESOURCE = "/k8s/pipeline-deployment.template.yaml";
    private static final String DEPLOYMENT_PREFIX = "pipeline-";
    private static final String RESTARTED_AT_ANNOTATION = "kubectl.kubernetes.io/restartedAt";

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
                .replace("${COUCHBASE_BUCKET}", AppConfig.get("COUCHBASE_BUCKET"));

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
}
