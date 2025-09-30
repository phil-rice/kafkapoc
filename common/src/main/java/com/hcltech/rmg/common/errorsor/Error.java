package com.hcltech.rmg.common.errorsor;

import java.util.List;
import java.util.Optional;

public final class Error<T> implements ErrorsOr<T> {
    private final List<String> errors;

    Error(List<String> errors) {
        this.errors = List.copyOf(errors);
        if (this.errors.isEmpty()) throw new IllegalArgumentException("Errors must not be empty");
    }

    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public boolean isValue() {
        return false;
    }

    @Override
    public Optional<T> getValue() {
        return Optional.empty();
    }

    @Override
    public List<String> getErrors() {
        return errors;
    }

    @Override
    public <T1> ErrorsOr<T1> errorCast() {
        return (ErrorsOr<T1>) this;
    }

    @Override
    public String toString() {
        return "Error(" + errors + ")";
    }
}
