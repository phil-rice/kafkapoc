package com.hcltech.rmg.messages;

/**
 * RawMessage is the hexagonal-architecture "port" abstraction of an event
 * as delivered by an external broker (e.g. Kafka).
 * <p>
 * It exposes the raw payload, trace headers for correlation,
 * and both event-time and processing-time clocks so the domain
 * can reason about order and lag without depending on broker APIs.
 */
public interface RawMessage {

    /**
     * @return raw payload as UTF-8 string (never {@code null})
     */
    String rawValue();

    /**
     * The timestamp recorded by the broker for this message.
     * <p>
     * In Kafka this is {@code record.timestamp()}, which can be either
     * producer "create time" or broker "log append time" depending on topic config.
     *
     * @return broker event timestamp in epoch millis
     */
    long brokerTimestamp();

    /**
     * The time this message was consumed from the broker by this pipeline.
     * <p>
     * Used to measure ingestion lag relative to {@link #brokerTimestamp()}.
     *
     * @return local processing time in epoch millis
     */
    long processingTimestamp();

    /**
     * Partition number from which this message was consumed.
     *
     * @return zero-based partition index
     */
    int partition();

    /**
     * W3C Trace Context: {@code traceparent} header value.
     * <p>
     * Use the {@code trace-id} portion as a correlation id if present.
     *
     * @return header value, or {@code null} if not present
     */
    String traceparent();

    /**
     * W3C Trace Context: {@code tracestate} header value (vendor metadata).
     *
     * @return header value, or {@code null} if not present
     */
    String tracestate();

    /**
     * W3C Trace Context: {@code baggage} header value (arbitrary k/v pairs).
     *
     * @return header value, or {@code null} if not present
     */
    String baggage();
}
