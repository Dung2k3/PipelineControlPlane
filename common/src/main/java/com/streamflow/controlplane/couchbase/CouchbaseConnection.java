package com.streamflow.controlplane.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.streamflow.controlplane.config.AppConfig;

public final class CouchbaseConnection {

    private static final String BUCKET_NAME = AppConfig.get("COUCHBASE_BUCKET");
    private static final Cluster CLUSTER = buildCluster();
    private static final Bucket BUCKET = CLUSTER.bucket(BUCKET_NAME);

    private CouchbaseConnection() {
    }

    public static Collection pipelineCollection() {
        String scope = AppConfig.get("COUCHBASE_SCOPE");
        String collection = AppConfig.get("COUCHBASE_COLLECTION");
        return BUCKET.scope(scope).collection(collection);
    }

    public static Cluster cluster() {
        return CLUSTER;
    }

    public static String bucketName() {
        return BUCKET_NAME;
    }

    private static Cluster buildCluster() {
        String connectionString = AppConfig.get("COUCHBASE_CONNECTION_STRING");
        String username = AppConfig.get("COUCHBASE_USERNAME");
        String password = AppConfig.get("COUCHBASE_PASSWORD");
        return Cluster.connect(connectionString, username, password);
    }
}
