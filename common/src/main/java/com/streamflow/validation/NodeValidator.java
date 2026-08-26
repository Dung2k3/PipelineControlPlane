package com.streamflow.validation;

import com.streamflow.config.NodeConfig;
import com.streamflow.config.PipelineConfig;
import jakarta.validation.ConstraintViolation;

import java.util.Map;
import java.util.Set;

public final class NodeValidator {

    public void validate(NodeConfig node, PipelineConfig pipeline) {
        for (Map.Entry<String, String> ref : node.referencedNodeIds().entrySet()) {
            String field = ref.getKey();
            String referencedId = ref.getValue();
            if (referencedId == null || referencedId.isBlank()) {
                throw new ValidationException(
                        pipeline.getPipelineId(), node.getId(), field, field + " la bat buoc");
            }
            boolean resolved =
                    pipeline.getNodes().stream().anyMatch(n -> n.getId().equals(referencedId));
            if (!resolved) {
                throw new ValidationException(
                        pipeline.getPipelineId(),
                        node.getId(),
                        field,
                        field + " tham chieu toi node khong ton tai: " + referencedId);
            }
        }
        validateBean(node, pipeline);
    }

    private void validateBean(NodeConfig node, PipelineConfig pipeline) {
        Set<ConstraintViolation<NodeConfig>> violations = BeanValidators.INSTANCE.validate(node);
        if (violations.isEmpty()) {
            return;
        }
        ConstraintViolation<NodeConfig> first = violations.iterator().next();
        throw new ValidationException(
                pipeline.getPipelineId(),
                node.getId(),
                first.getPropertyPath().toString(),
                first.getMessage());
    }
}
