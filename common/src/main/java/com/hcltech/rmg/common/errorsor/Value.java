package com.hcltech.rmg.common.errorsor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class Value<T> implements ErrorsOr<T> {
    private final T value;

    Value(T value) {
        this.value = Objects.requireNonNull(value);
    }

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    public Optional<T> getValue() {
        return Optional.of(value);
    }

    @Override
    public List<String> getErrors() {
        return List.of();
    }

    @Override
    public <T1> ErrorsOr<T1> errorCast() {
        throw new IllegalStateException("Trying to cast Value to Error");
    }

    @Override
    public String toString() {
        return "Value(" + value + ")";
    }
}

