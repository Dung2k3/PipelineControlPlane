package com.streamflow.node.process.rekey;

import com.streamflow.config.NodeConfig;
import com.streamflow.config.PipelineConfig;
import com.streamflow.node.source.SourceNodeConfig;
import com.streamflow.validation.NodeValidator;
import com.streamflow.validation.ValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RekeyNodeValidatorTest {

    private static final String INPUT_ID = "srcInput";

    private final NodeValidator validator = new NodeValidator();

    private static RekeyNodeConfig defaultConfig() {
        RekeyNodeConfig config = new RekeyNodeConfig();
        config.setId("rekey1");
        config.setInput(INPUT_ID);
        config.setSelectKey("value.path('orderId')");
        return config;
    }

    private static PipelineConfig defaultPipeline() {
        PipelineConfig pipeline = new PipelineConfig();
        pipeline.setPipelineId("test-pipeline");
        pipeline.setBootstrapServers("localhost:9092");
        pipeline.setApplicationId("test-app");

        SourceNodeConfig input = new SourceNodeConfig();
        input.setId(INPUT_ID);

        pipeline.setNodes(List.<NodeConfig>of(input));
        return pipeline;
    }

    @Test
    void acceptsValidSelectKeySyntax() {
        RekeyNodeConfig config = defaultConfig();
        PipelineConfig pipeline = defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    @Test
    void rejectsMissingSelectKey() {
        RekeyNodeConfig config = defaultConfig();
        config.setSelectKey(null);
        PipelineConfig pipeline = defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("selectKey", ex.getField());
    }

    @Test
    void rejectsInvalidSelectKeySyntax() {
        RekeyNodeConfig config = defaultConfig();
        config.setSelectKey("path('orderId'"); // thieu dau ngoac dong
        PipelineConfig pipeline = defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("selectKey", ex.getField());
    }
}
