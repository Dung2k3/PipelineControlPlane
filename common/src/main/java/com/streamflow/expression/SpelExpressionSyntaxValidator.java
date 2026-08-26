package com.streamflow.expression;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class SpelExpressionSyntaxValidator implements ConstraintValidator<ValidSpelExpression, String> {

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        if (value == null || value.isBlank()) {
            return true;
        }
        try {
            new SpelEvaluator(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
