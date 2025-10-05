package com.hcltech.rmg.messages;

import java.util.Objects;

/** A raw message received from a broker, before any deserialization or processing. */
public record RawMessage(
        String rawValue,
        long brokerTimestamp,
        long processingTimestamp,
        int partition,
        long offset,
        String traceparent,
        String tracestate,
        String baggage
) {
    public RawMessage {
        Objects.requireNonNull(rawValue, "rawValue must not be null");
    }
}
