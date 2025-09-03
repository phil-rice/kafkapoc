package com.hcltech.rmg.interfaces.outcome;

import java.util.Objects;

public record Value<T>(T value) implements Outcome<T> {
    public Value {
        Objects.requireNonNull(value, "value");
    }

    @Override
    public String toString() {
        return "Value(" + value + ")";
    }
}
