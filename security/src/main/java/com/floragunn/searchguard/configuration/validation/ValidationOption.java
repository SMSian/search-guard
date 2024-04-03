package com.floragunn.searchguard.configuration.validation;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.configuration.CType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Predicate;

public record ValidationOption(String configType, ImmutableList<String> omitProperties) {

    public ValidationOption {
        Objects.requireNonNull(configType, "Config type is required");
        Objects.requireNonNull(omitProperties, "List of omitted properties is required");
    }

    public static boolean isPropertyValidationOmitted(ValidationOption[] options, CType<?> type, String propertyName) {
        return Arrays.stream(options) //
            .map(option -> option.isPropertyValidationOmitted(type, propertyName)) //
            .anyMatch(Predicate.isEqual(true));
    }

    public static boolean isPropertyValidationEnabled(ValidationOption[] options, CType<?> type, String propertyName) {
        return ! isPropertyValidationOmitted(options,type, propertyName);
    }

    public boolean isPropertyValidationOmitted(CType<?> type, String propertyName) {
        Objects.requireNonNull(type, "Configuration type is required");
        Objects.requireNonNull(propertyName, "Property name is required");
        return configType.equals(type.getName()) && omitProperties.contains(propertyName);
    }
}
