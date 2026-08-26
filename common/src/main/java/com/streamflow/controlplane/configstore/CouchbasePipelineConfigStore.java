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

    public long bumpVersion(String pipelineId) {
        String docId = DOC_PREFIX + pipelineId;
        String statement = "UPDATE `" + bucketName + "` USE KEYS $docId "
                + "SET version = IFMISSINGORNULL(version, 0) + 1 "
                + "RETURNING version";
        QueryResult result;
        try {
            result = cluster.query(statement,
                    QueryOptions.queryOptions().parameters(JsonObject.create().put("docId", docId)));
        } catch (RuntimeException e) {
            throw new PipelineConfigLoadException(
                    pipelineId, "Loi tang version pipeline config tren Couchbase cho pipelineId=" + pipelineId, e);
        }
        List<JsonObject> rows = result.rowsAsObject();
        if (rows.isEmpty()) {
            throw new PipelineConfigLoadException(
                    pipelineId,
                    "Khong tim thay pipeline config trong Couchbase cho pipelineId=" + pipelineId, null);
        }
        return rows.get(0).getLong("version");
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
                // Doc Couchbase co the tich luy field van hanh (createdAt, updatedBy...) khong co
                // trong PipelineConfig - khong de nhung field do lam load that bai.
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
}
