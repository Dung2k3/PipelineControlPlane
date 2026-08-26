package com.streamflow.node.sink;

import com.streamflow.config.NodeConfig;
import jakarta.validation.constraints.NotBlank;

public class SinkNodeConfig extends NodeConfig {
    private String input;

    @NotBlank(message = "topic la bat buoc")
    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }
}
