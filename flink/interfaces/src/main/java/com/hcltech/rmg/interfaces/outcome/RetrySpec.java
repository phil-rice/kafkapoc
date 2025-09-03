package com.hcltech.rmg.interfaces.outcome;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Function;

public record RetrySpec(
        String reason

        ) {
    public RetrySpec {
        Objects.requireNonNull(reason, "reason");
    }


    @Override
    public String toString() {
        return "RetrySpec(reason=" + reason + ")";
    }
}
