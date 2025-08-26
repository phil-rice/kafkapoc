package com.example.metrics;

/**
 * Strategy interface: how to publish metrics snapshots.
 */
public interface MetricsPrinter<S> {
    /**
     * @param snap   current snapshot
     * @param delta  number of records processed since last snapshot
     * @param rate   processing rate (records/sec) since last snapshot
     */
    void print(MetricsSnapshot<S> snap, long delta, double rate);
}
