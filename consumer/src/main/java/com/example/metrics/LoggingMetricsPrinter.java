package com.example.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs metrics snapshots via SLF4J.
 */
public final class LoggingMetricsPrinter<S> implements MetricsPrinter<S> {
    private static final Logger log = LoggerFactory.getLogger(LoggingMetricsPrinter.class);

    @Override
    public void print(MetricsSnapshot<S> snap, long delta, double rate) {
        log.info("METRICS totalProcessed={} (+{}, ~{} msg/s) perShard={}",
                snap.totalProcessed(), delta, String.format("%.2f", rate), snap.processedByShard());
    }
}
