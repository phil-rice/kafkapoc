package com.hcltech.rmg.metrics;

/**
 * Strategy interface: how to publish metrics snapshots.
 * For rate/delta, the scheduler will compute for a chosen counter (e.g., "processed").
 */
public interface MetricsPrinter<S> {
    void print(MetricsSnapshot<S> snap, long delta, double rate, String primaryCounterName);
}
