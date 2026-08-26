package com.streamflow.node.process.join;

import com.fasterxml.jackson.databind.JsonNode;

public record JoinRecordWrapper(String key, JsonNode value) {}
