package com.streamflow.expression;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Constraint(validatedBy = SpelExpressionSyntaxValidator.class)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidSpelExpression {

    String message() default "bieu thuc SpEL khong hop le cu phap";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
