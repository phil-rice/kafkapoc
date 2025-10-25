package com.hcltech.rmg.common.metrics;

/**
 * Minimal façade for emitting numeric metrics.
 * <p>Both counters and histograms share the same low-cardinality naming space.</p>
 * Implementations must be thread-safe.
 */
public interface Metrics {

    /** Increment a named counter by 1. */
    void increment(String name);

    /**
     * Record a value in a histogram/timer.
     * <p>Typical use: record durations or sizes in milliseconds/bytes.</p>
     */
    void histogram(String name, long value);

     Metrics nullMetrics= new NullMetrics();
}

class NullMetrics implements Metrics {

    @Override
    public void increment(String name) {
    }

    @Override
    public void histogram(String name, long value) {
    }
}