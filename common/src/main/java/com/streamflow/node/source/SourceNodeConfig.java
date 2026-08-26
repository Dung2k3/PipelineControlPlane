package com.streamflow.node.source;

import com.streamflow.config.NodeConfig;
import jakarta.validation.constraints.NotBlank;

public class SourceNodeConfig extends NodeConfig {
    @NotBlank(message = "topic la bat buoc")
    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
