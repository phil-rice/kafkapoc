package com.hcltech.rmg.interfaces.outcome;

import java.util.Objects;

public record Retry<T>(RetrySpec spec) implements Outcome<T> {
    public Retry {
        Objects.requireNonNull(spec, "spec");
    }

    @Override
    public String toString() {
        return "Retry(" + spec + ")";
    }
}
