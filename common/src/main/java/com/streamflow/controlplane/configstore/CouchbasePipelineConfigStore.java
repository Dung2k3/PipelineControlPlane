package com.streamflow.controlplane.configstore;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streamflow.config.PipelineConfig;
import com.streamflow.controlplane.couchbase.CouchbaseConnection;

import java.util.ArrayList;
import java.util.List;

public class CouchbasePipelineConfigStore {

    private static final String DOC_PREFIX = "pipeline::";

    private final Collection collection;
    private final Cluster cluster;
    private final String bucketName;
    private final ObjectMapper mapper = buildMapper();

    public CouchbasePipelineConfigStore() {
        this(CouchbaseConnection.pipelineCollection(), CouchbaseConnection.cluster(), CouchbaseConnection.bucketName());
    }

    CouchbasePipelineConfigStore(Collection collection, Cluster cluster, String bucketName) {
        this.collection = collection;
        this.cluster = cluster;
        this.bucketName = bucketName;
    }

    public PipelineConfig load(String pipelineId) {
        String docId = DOC_PREFIX + pipelineId;
        String json;
        try {
            json = collection.get(docId).contentAsObject().toString();
        } catch (DocumentNotFoundException e) {
            throw new PipelineConfigLoadException(
                    pipelineId,
                    "Khong tim thay pipeline config trong Couchbase cho pipelineId=" + pipelineId, e);
        } catch (RuntimeException e) {
            throw new PipelineConfigLoadException(
                    pipelineId, "Loi doc pipeline config tu Couchbase cho pipelineId=" + pipelineId, e);
        }
        return parseConfig(mapper, json, pipelineId);
    }

    public List<PipelineConfig> listAll() {
        String statement = "SELECT META(p).id AS docId, p.* FROM `" + bucketName + "` AS p "
                + "WHERE META(p).id LIKE $prefix";
        QueryResult result;
        try {
            result = cluster.query(statement, QueryOptions.queryOptions()
                    .parameters(JsonObject.create().put("prefix", DOC_PREFIX + "%")));
        } catch (RuntimeException e) {
            throw new PipelineConfigLoadException(null, "Loi liet ke pipeline config tu Couchbase", e);
        }

        List<PipelineConfig> configs = new ArrayList<>();
        for (JsonObject row : result.rowsAsObject()) {
            String docId = row.getString("docId");
            row.removeKey("docId");
            String pipelineId = docId.substring(DOC_PREFIX.length());
            configs.add(parseConfig(mapper, row.toString(), pipelineId));
        }
        return configs;
    }

    static PipelineConfig parseConfig(ObjectMapper mapper, String json, String pipelineId) {
        try {
            PipelineConfig config = mapper.readValue(json, PipelineConfig.class);
            config.setPipelineId(pipelineId);
            return config;
        } catch (JsonProcessingException e) {
            throw new PipelineConfigLoadException(
                    pipelineId,
                    "Pipeline config khong hop le cho pipelineId=" + pipelineId + ": " + e.getOriginalMessage(),
                    e);
        }
    }

    private static ObjectMapper buildMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
}
