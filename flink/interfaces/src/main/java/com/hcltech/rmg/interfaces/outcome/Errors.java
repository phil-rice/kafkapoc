package com.hcltech.rmg.interfaces.outcome;

import java.util.List;
import java.util.Objects;

public record Errors<T>(List<String> errors) implements Outcome<T> {
    public Errors {
        Objects.requireNonNull(errors, "errors");
        if (errors.isEmpty()) throw new IllegalArgumentException("errors must not be empty");
        errors = List.copyOf(errors);
    }

    @Override
    public String toString() {
        return "Errors" + errors;
    }
}
