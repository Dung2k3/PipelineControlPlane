package com.streamflow.controlplane.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamflow.config.PipelineConfig;
import com.streamflow.controlplane.config.AppConfig;
import com.streamflow.controlplane.configstore.CouchbasePipelineConfigStore;
import com.streamflow.controlplane.configstore.PipelineConfigLoadException;
import com.streamflow.controlplane.k8s.PipelineDeploymentManager;
import com.streamflow.topology.PipelineTopologyBuilder;
import com.streamflow.validation.PipelineValidator;
import com.streamflow.validation.ValidationException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ActivationApiApp {

    private static final Pattern ACTIVATE_PATH = Pattern.compile("^/pipelines/([^/]+)/activate$");
    private static final Pattern PIPELINE_PATH = Pattern.compile("^/pipelines/([^/]+)$");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static CouchbasePipelineConfigStore configStore;
    private static PipelineDeploymentManager deploymentManager;

    public static void main(String[] args) throws IOException {
        configStore = new CouchbasePipelineConfigStore();
        deploymentManager = buildDeploymentManagerOrNull();

        int port = Integer.parseInt(AppConfig.get("API_PORT"));
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/pipelines", ActivationApiApp::handlePipelines);
        server.setExecutor(null);
        server.start();

        System.out.println("ActivationApiApp dang lang nghe tren port " + port
                + (deploymentManager == null ? " (K8S DEPLOY TAT - xem log khoi dong o tren)" : ""));
    }

    private static void handlePipelines(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();

        Matcher activateMatcher = ACTIVATE_PATH.matcher(path);
        if (activateMatcher.matches()) {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, errorNode("chi ho tro POST cho route nay"));
                return;
            }
            activate(exchange, activateMatcher.group(1));
            return;
        }

        Matcher pipelineMatcher = PIPELINE_PATH.matcher(path);
        if (pipelineMatcher.matches()) {
            if (!"DELETE".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, errorNode("chi ho tro DELETE cho route nay"));
                return;
            }
            deactivate(exchange, pipelineMatcher.group(1));
            return;
        }

        sendJson(exchange, 404, errorNode("khong tim thay route: " + path));
    }

    private static void activate(HttpExchange exchange, String pipelineId) throws IOException {
        PipelineConfig config;
        try {
            config = configStore.load(pipelineId);
        } catch (PipelineConfigLoadException e) {
            sendJson(exchange, 404, errorNode(e.getMessage()));
            return;
        }

        try {
            new PipelineValidator().validate(config);
        } catch (ValidationException e) {
            ObjectNode body = errorNode(e.getMessage());
            body.put("pipelineId", e.getPipelineId());
            if (e.getNodeId() != null) {
                body.put("nodeId", e.getNodeId());
            }
            if (e.getField() != null) {
                body.put("field", e.getField());
            }
            sendJson(exchange, 400, body);
            return;
        }

        try {
            new PipelineTopologyBuilder().build(config);
        } catch (RuntimeException e) {
            sendJson(exchange, 400, errorNode("Build topology that bai: " + e.getMessage()));
            return;
        }

        long newVersion;
        try {
            newVersion = configStore.bumpVersion(pipelineId);
        } catch (PipelineConfigLoadException e) {
            sendJson(exchange, 404, errorNode(e.getMessage()));
            return;
        }

        ObjectNode body = MAPPER.createObjectNode();
        body.put("pipelineId", pipelineId);
        body.put("version", newVersion);

        if (deploymentManager == null) {
            body.put("k8sDeployed", false);
            body.put("k8sError", "K8s client khong khoi tao duoc luc start ActivationApiApp - xem log");
        } else {
            try {
                boolean created = deploymentManager.ensureDeployed(pipelineId, config);
                body.put("k8sDeployed", true);
                body.put("k8sAction", created ? "created" : "restarted");
            } catch (RuntimeException e) {
                body.put("k8sDeployed", false);
                body.put("k8sError", e.getMessage());
            }
        }

        sendJson(exchange, 200, body);
    }

    /**
     * Giai doan 4 - CHI xoa Deployment k8s. Khong dong toi internal topic/consumer group/state store
     * Kafka Streams cua pipeline (quyet dinh co chu dich - xem docs/PipelineControlPlane/plan.md muc
     * Giai doan 4) - viec don Kafka van chay thu cong qua PipelineReset khi can.
     */
    private static void deactivate(HttpExchange exchange, String pipelineId) throws IOException {
        if (deploymentManager == null) {
            sendJson(exchange, 503, errorNode(
                    "K8s client khong khoi tao duoc luc start ActivationApiApp - khong the xoa Deployment"));
            return;
        }

        boolean deleted;
        try {
            deleted = deploymentManager.undeploy(pipelineId);
        } catch (RuntimeException e) {
            sendJson(exchange, 500, errorNode("Xoa Deployment that bai: " + e.getMessage()));
            return;
        }

        if (!deleted) {
            sendJson(exchange, 404, errorNode("Khong tim thay Deployment dang chay cho pipeline '" + pipelineId + "'"));
            return;
        }

        ObjectNode body = MAPPER.createObjectNode();
        body.put("pipelineId", pipelineId);
        body.put("k8sDeleted", true);
        body.put("note", "Chi xoa Deployment k8s. Internal topic/consumer group/state store KHONG bi dong "
                + "toi - chay PipelineReset (ValidateAndBuildInnerJoinOperation) thu cong neu can don Kafka.");
        sendJson(exchange, 200, body);
    }

    private static PipelineDeploymentManager buildDeploymentManagerOrNull() {
        try {
            return new PipelineDeploymentManager();
        } catch (RuntimeException e) {
            System.err.println("Khong khoi tao duoc Kubernetes client - activate() se van validate/bump "
                    + "version nhung KHONG tao/reload Deployment duoc: " + e.getMessage());
            return null;
        }
    }

    private static ObjectNode errorNode(String message) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("error", message);
        return node;
    }

    private static void sendJson(HttpExchange exchange, int status, ObjectNode body) throws IOException {
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
