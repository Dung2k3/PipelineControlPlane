package com.streamflow.node.process.filter;

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

class FilterNodeValidatorTest {

    private static final String INPUT_ID = "srcInput";

    private final NodeValidator validator = new NodeValidator();

    private static FilterNodeConfig defaultConfig() {
        FilterNodeConfig config = new FilterNodeConfig();
        config.setId("filter1");
        config.setInput(INPUT_ID);
        config.setPredicate("value.path('amount').asDouble() > 0");
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
    void acceptsValidPredicateSyntax() {
        FilterNodeConfig config = defaultConfig();
        PipelineConfig pipeline = defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    @Test
    void rejectsInvalidPredicateSyntax() {
        FilterNodeConfig config = defaultConfig();
        config.setPredicate("path('amount'"); // thieu dau ngoac dong
        PipelineConfig pipeline = defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("predicate", ex.getField());
    }
}
