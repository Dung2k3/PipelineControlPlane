package com.streamflow.validation;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

public final class BeanValidators {

    public static final Validator INSTANCE =
            Validation.buildDefaultValidatorFactory().getValidator();
}
