package com.streamflow.node.process.aggregate;

import com.streamflow.config.NodeConfig;
import com.streamflow.config.PipelineConfig;
import com.streamflow.node.process.aggregate.type.AggGroupMode;
import com.streamflow.node.process.aggregate.type.AggregationType;
import com.streamflow.node.process.aggregate.type.WindowType;
import com.streamflow.node.source.SourceNodeConfig;
import com.streamflow.validation.NodeValidator;
import com.streamflow.validation.ValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AggregateNodeValidatorTest {

    private static final String INPUT_ID = "srcInput";

    private final NodeValidator validator = new NodeValidator();

    private static AggregateNodeConfig defaultConfig() {
        AggregateNodeConfig config = new AggregateNodeConfig();
        config.setId("agg1");
        config.setInput(INPUT_ID);
        config.setGroupMode(AggGroupMode.GROUP_BY_KEY);
        config.setAggregations(List.of(new AggregationDefinition(AggregationType.COUNT, "count_1")));
        AggWindowConfig window = new AggWindowConfig();
        window.setType(WindowType.TUMBLING);
        window.setSizeMs(300000L);
        window.setGraceMs(30000L);
        config.setWindow(window);
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
    void acceptsGroupByKeyWithoutKeyExtractor() {
        AggregateNodeConfig config = defaultConfig();
        PipelineConfig pipeline = defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    @Test
    void acceptsGroupByWithKeyExtractor() {
        AggregateNodeConfig config = defaultConfig();
        config.setGroupMode(AggGroupMode.GROUP_BY);
        config.setKeyExtractor("value.path('customerId')");
        PipelineConfig pipeline = defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    @Test
    void rejectsGroupByWithoutKeyExtractor() {
        AggregateNodeConfig config = defaultConfig();
        config.setGroupMode(AggGroupMode.GROUP_BY);
        PipelineConfig pipeline = defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("keyExtractorPresentWhenGroupBy", ex.getField());
    }

    @Test
    void acceptsMultipleAggregationsCombiningCountAndSum() {
        AggregateNodeConfig config = defaultConfig();
        AggregationDefinition sum = new AggregationDefinition(AggregationType.SUM, "sum_1");
        sum.setSourceField("amount");
        config.setAggregations(List.of(
                new AggregationDefinition(AggregationType.COUNT, "count_1"), sum));
        PipelineConfig pipeline = defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    @Test
    void rejectsDuplicateAggregationAliases() {
        AggregateNodeConfig config = defaultConfig();
        config.setAggregations(List.of(
                new AggregationDefinition(AggregationType.COUNT, "count_1"),
                new AggregationDefinition(AggregationType.COUNT, "count_1")));
        PipelineConfig pipeline = defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("aggregationAliasesUnique", ex.getField());
    }

    @Test
    void acceptsSumAggregationWithSourceField() {
        AggregateNodeConfig config = defaultConfig();
        AggregationDefinition sum = new AggregationDefinition(AggregationType.SUM, "sum_1");
        sum.setSourceField("amount");
        config.setAggregations(List.of(sum));
        PipelineConfig pipeline = defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    @Test
    void rejectsSumAggregationWithoutSourceField() {
        AggregateNodeConfig config = defaultConfig();
        config.setAggregations(List.of(new AggregationDefinition(AggregationType.SUM, "sum_1")));
        PipelineConfig pipeline = defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("sourceFieldPresentForAllSumAggregations", ex.getField());
    }

    @Test
    void rejectsNonTumblingWindow() {
        AggregateNodeConfig config = defaultConfig();
        config.getWindow().setType(WindowType.SESSION);
        PipelineConfig pipeline = defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("tumblingWindow", ex.getField());
    }
}
