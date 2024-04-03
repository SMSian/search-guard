package com.floragunn.searchguard.configuration.validation;

public record ValidationSettings(boolean runValidation, ValidationOption[] options) {

    public static final ValidationSettings ENABLED_WITHOUT_OPTIONS = new ValidationSettings(true, new ValidationOption[0]);

    public static final ValidationSettings DISABLED = new ValidationSettings(false, new ValidationOption[0]);

    public static ValidationSettings enabledWithOptions(ValidationOption... options) {
        return new ValidationSettings(true, options);
    }

    public boolean skipValidation() {
        return ! runValidation;
    }
}
