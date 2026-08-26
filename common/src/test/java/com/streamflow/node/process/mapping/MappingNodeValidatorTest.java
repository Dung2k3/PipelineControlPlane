package com.streamflow.node.process.mapping;

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

class MappingNodeValidatorTest {

    private static final String INPUT_ID = "srcInput";

    private final NodeValidator validator = new NodeValidator();

    private static MappingNodeConfig defaultConfig() {
        MappingNodeConfig config = new MappingNodeConfig();
        config.setId("mapping1");
        config.setInput(INPUT_ID);
        config.setFields(List.of(new MappingFieldConfig("orderId", "value.path('orderId')")));
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
    void acceptsValidFieldSourceSyntax() {
        MappingNodeConfig config = defaultConfig();
        PipelineConfig pipeline = defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    @Test
    void acceptsLiteralFieldSourceExpression() {
        MappingNodeConfig config = defaultConfig();
        config.setFields(List.of(new MappingFieldConfig("type", "'AMOUNT_MISMATCH'")));
        PipelineConfig pipeline = defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    @Test
    void rejectsInvalidFieldSourceSyntax() {
        MappingNodeConfig config = defaultConfig();
        config.setFields(List.of(new MappingFieldConfig("orderId", "path('orderId'"))); // thieu dau ngoac dong
        PipelineConfig pipeline = defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("fields[0].source", ex.getField());
    }

    @Test
    void acceptsFieldWithFromKeyInsteadOfSource() {
        MappingNodeConfig config = defaultConfig();
        config.setFields(List.of(MappingFieldConfig.fromKey("orderId")));
        PipelineConfig pipeline = defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    @Test
    void rejectsFieldWithNeitherSourceNorFromKey() {
        MappingNodeConfig config = defaultConfig();
        config.setFields(List.of(new MappingFieldConfig("orderId", null)));
        PipelineConfig pipeline = defaultPipeline();

        assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
    }

    @Test
    void rejectsFieldWithBothSourceAndFromKey() {
        MappingNodeConfig config = defaultConfig();
        MappingFieldConfig field = new MappingFieldConfig("orderId", "value.path('orderId')");
        field.setFromKey(true);
        config.setFields(List.of(field));
        PipelineConfig pipeline = defaultPipeline();

        assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
    }
}
