package com.streamflow.node.process.join;

import com.streamflow.config.PipelineConfig;
import com.streamflow.node.process.join.type.JoinType;
import com.streamflow.validation.NodeValidator;
import com.streamflow.validation.ValidationException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JoinNodeValidatorTest {

    private final NodeValidator validator = new NodeValidator();

    // TC-JOIN-001
    @Test
    void rejectsMissingLeftStream() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setLeftStream(null);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("leftStream", ex.getField());
    }

    // TC-JOIN-002
    @Test
    void rejectsMissingRightStream() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setRightStream(null);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("rightStream", ex.getField());
    }

    // TC-JOIN-003
    @Test
    void rejectsUnresolvableLeftStream() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setLeftStream("does-not-exist");
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("leftStream", ex.getField());
    }

    // TC-JOIN-004
    @Test
    void rejectsUnresolvableRightStream() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setRightStream("does-not-exist");
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("rightStream", ex.getField());
    }

    // TC-JOIN-005
    @Test
    void rejectsMissingLeftStreamName() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setLeftStreamName(null);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("leftStreamName", ex.getField());
    }

    // TC-JOIN-006
    @Test
    void rejectsMissingRightStreamName() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setRightStreamName(null);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("rightStreamName", ex.getField());
    }

    // TC-JOIN-007
    @Test
    void rejectsWhenStreamNamesAreEqual() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setLeftStreamName("same");
        config.setRightStreamName("same");
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
    }

    // TC-JOIN-008
    @Test
    void rejectsMissingSelectKey() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setSelectKey(null);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("selectKey", ex.getField());
    }

    // selectKey co cu phap SpEL sai -> reject
    @Test
    void rejectsInvalidSelectKeySyntax() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setSelectKey("path('id'"); // thieu dau ngoac dong
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("selectKey", ex.getField());
    }

    // TC-JOIN-009 (sub-case 1/2: window = 0)
    @Test
    void rejectsZeroWindow() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setWindow(Duration.ZERO);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("window", ex.getField());
    }

    // TC-JOIN-009 (sub-case 2/2: window < 0)
    @Test
    void rejectsNegativeWindow() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setWindow(Duration.ofSeconds(-1));
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("window", ex.getField());
    }

    // window > 10 phut -> reject
    @Test
    void rejectsWindowOverTenMinutes() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setWindow(Duration.ofMinutes(10).plusMillis(1));
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("window", ex.getField());
    }

    // window = dung 10 phut (bien) -> chap nhan
    @Test
    void acceptsWindowAtTenMinutesBoundary() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setWindow(Duration.ofMinutes(10));
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    // TC-JOIN-010
    @Test
    void rejectsMissingWindow() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setWindow(null);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
    }

    // TC-JOIN-011
    @Test
    void rejectsNegativeGrace() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setGrace(Duration.ofSeconds(-1));
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("grace", ex.getField());
    }

    // grace > 30 giay -> reject
    @Test
    void rejectsGraceOverThirtySeconds() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setGrace(Duration.ofSeconds(30).plusMillis(1));
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
        assertEquals("grace", ex.getField());
    }

    // grace = dung 30 giay (bien) -> chap nhan
    @Test
    void acceptsGraceAtThirtySecondsBoundary() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setGrace(Duration.ofSeconds(30));
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    // TC-JOIN-012
    @Test
    void rejectsMissingGrace() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setGrace(null);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
    }

    // TC-JOIN-013 (sub-case 1/2: joinType = null)
    @Test
    void rejectsNullJoinType() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setJoinType(null);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
    }

    // TC-JOIN-013 (sub-case 2/2: joinType = LEFT)
    @Test
    void rejectsNonInnerJoinType() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setJoinType(JoinType.LEFT);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));
    }

    // TC-JOIN-014
    @Test
    void acceptsFullyValidConfig() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    // TC-JOIN-015
    @Test
    void allowsSelfJoin() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setRightStream(JoinTestSupport.LEFT_STREAM_ID);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        assertDoesNotThrow(() -> validator.validate(config, pipeline));
    }

    // TC-JOIN-016
    @Test
    void validationErrorCarriesFullContext() {
        JoinNodeConfig config = JoinTestSupport.defaultConfig();
        config.setSelectKey(null);
        PipelineConfig pipeline = JoinTestSupport.defaultPipeline();

        ValidationException ex =
                assertThrows(ValidationException.class, () -> validator.validate(config, pipeline));

        assertEquals(pipeline.getPipelineId(), ex.getPipelineId());
        assertEquals(config.getId(), ex.getNodeId());
        assertEquals("selectKey", ex.getField());
        assertNotNull(ex.getMessage());
        assertFalse(ex.getMessage().isBlank());
    }
}
