package com.streamflow.controlplane.k8s;

import com.streamflow.config.PipelineConfig;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Dung mock server gia lap k8s API qua HTTP (crud = true: hanh xu nhu 1 CRUD store trong bo nho) -
 * khong can minikube/cluster song luc chay test.
 */
@EnableKubernetesMockClient(crud = true)
class PipelineDeploymentManagerTest {

    KubernetesClient client;

    @Test
    void createsNewDeploymentWhenNoneExists() {
        PipelineDeploymentManager manager = new PipelineDeploymentManager(client, "default");
        PipelineConfig config = new PipelineConfig("orders", "broker:29092", "orders-app", java.util.List.of());
        config.setNodeSelectorKey("worker");
        config.setNodeSelectorValue("worker-a");

        boolean created = manager.ensureDeployed("orders", config);

        assertTrue(created);
        Deployment deployment = client.apps().deployments().inNamespace("default").withName("pipeline-orders").get();
        assertNotNull(deployment);
        assertEquals("worker-a", deployment.getSpec().getTemplate().getSpec().getNodeSelector().get("worker"));
        assertEquals("orders",
                envValue(deployment, "PIPELINE_ID"));
    }

    @Test
    void createsWithoutNodeSelectorWhenNotConfigured() {
        PipelineDeploymentManager manager = new PipelineDeploymentManager(client, "default");
        PipelineConfig config = new PipelineConfig("payments", "broker:29092", "payments-app", java.util.List.of());

        manager.ensureDeployed("payments", config);

        Deployment deployment = client.apps().deployments().inNamespace("default").withName("pipeline-payments").get();
        assertTrue(deployment.getSpec().getTemplate().getSpec().getNodeSelector() == null
                || deployment.getSpec().getTemplate().getSpec().getNodeSelector().isEmpty());
    }

    @Test
    void restartsExistingDeploymentInsteadOfRecreating() {
        Deployment existing = new DeploymentBuilder()
                .withNewMetadata().withName("pipeline-fraud").withNamespace("default").endMetadata()
                .withNewSpec()
                .withNewTemplate()
                .withNewMetadata().endMetadata()
                .withNewSpec().endSpec()
                .endTemplate()
                .withNewSelector().addToMatchLabels("app", "fraud").endSelector()
                .endSpec()
                .build();
        client.apps().deployments().inNamespace("default").resource(existing).create();

        PipelineDeploymentManager manager = new PipelineDeploymentManager(client, "default");
        PipelineConfig config = new PipelineConfig("fraud", "broker:29092", "fraud-app", java.util.List.of());

        boolean created = manager.ensureDeployed("fraud", config);

        assertFalse(created);
        Deployment updated = client.apps().deployments().inNamespace("default").withName("pipeline-fraud").get();
        assertTrue(updated.getSpec().getTemplate().getMetadata().getAnnotations().containsKey("kubectl.kubernetes.io/restartedAt"));
    }

    @Test
    void undeployDeletesExistingDeployment() {
        Deployment existing = new DeploymentBuilder()
                .withNewMetadata().withName("pipeline-orders").withNamespace("default").endMetadata()
                .withNewSpec()
                .withNewTemplate()
                .withNewMetadata().endMetadata()
                .withNewSpec().endSpec()
                .endTemplate()
                .withNewSelector().addToMatchLabels("app", "orders").endSelector()
                .endSpec()
                .build();
        client.apps().deployments().inNamespace("default").resource(existing).create();

        PipelineDeploymentManager manager = new PipelineDeploymentManager(client, "default");

        boolean deleted = manager.undeploy("orders");

        assertTrue(deleted);
        assertNull(client.apps().deployments().inNamespace("default").withName("pipeline-orders").get());
    }

    @Test
    void undeployReturnsFalseWhenDeploymentDoesNotExist() {
        PipelineDeploymentManager manager = new PipelineDeploymentManager(client, "default");

        boolean deleted = manager.undeploy("ghost");

        assertFalse(deleted);
    }

    private static String envValue(Deployment deployment, String name) {
        return deployment.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream()
                .filter(e -> e.getName().equals(name))
                .findFirst()
                .orElseThrow()
                .getValue();
    }
}
