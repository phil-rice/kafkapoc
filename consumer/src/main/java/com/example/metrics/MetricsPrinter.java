package com.example.metrics;

public interface MetricsPrinter {
    /**
     * @param snapshot   the current counters
     * @param delta      processed since last tick (total)
     * @param ratePerSec approximate processed/sec since last tick
     */
    void print(MetricsSnapshot snapshot, long delta, double ratePerSec);
}
