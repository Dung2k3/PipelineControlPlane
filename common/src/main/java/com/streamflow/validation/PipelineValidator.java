package com.streamflow.validation;

import com.streamflow.config.NodeConfig;
import com.streamflow.config.PipelineConfig;
import jakarta.validation.ConstraintViolation;

import java.util.HashSet;
import java.util.Set;

public class PipelineValidator {

    private final NodeValidator nodeValidator = new NodeValidator();

    public void validate(PipelineConfig pipeline) {
        validatePipelineLevelFields(pipeline);

        Set<String> seenIds = new HashSet<>();
        for (NodeConfig node : pipeline.getNodes()) {
            if (!seenIds.add(node.getId())) {
                throw new ValidationException(pipeline.getPipelineId(), node.getId(), "id",
                        "Trung id node trong cung 1 pipeline: " + node.getId());
            }
        }

        for (NodeConfig node : pipeline.getNodes()) {
            nodeValidator.validate(node, pipeline);
        }
    }

    private void validatePipelineLevelFields(PipelineConfig pipeline) {
        Set<ConstraintViolation<PipelineConfig>> violations = BeanValidators.INSTANCE.validate(pipeline);
        if (!violations.isEmpty()) {
            ConstraintViolation<PipelineConfig> first = violations.iterator().next();
            throw new ValidationException(pipeline.getPipelineId(), null,
                    first.getPropertyPath().toString(), first.getMessage());
        }
    }
}
