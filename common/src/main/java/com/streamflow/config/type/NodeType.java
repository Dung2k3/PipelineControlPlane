package com.streamflow.config.type;

public enum NodeType {
    SOURCE,
    JOIN,
    TABLE_JOIN,
    MAPPING,
    FILTER,
    MERGE,
    JDBC_ENRICH,
    AGGREGATE,
    REKEY,
    CACHE_SINK,
    SINK
}
