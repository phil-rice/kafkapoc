package com.hcltech.rmg.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class LoggingMetricsPrinter<S> implements MetricsPrinter<S> {
    private static final Logger log = LoggerFactory.getLogger(LoggingMetricsPrinter.class);

    @Override
    public void print(MetricsSnapshot<S> snap, long delta, double rate, String primaryCounterName) {
        long total = snap.counterTotals().getOrDefault(primaryCounterName, 0L);
        Map<S, Long> perShard = snap.countersByShard().getOrDefault(primaryCounterName, Map.of());
        log.info("METRICS [{}] total={} (+{}, ~{} /s) perShard={}",
                primaryCounterName, total, delta, String.format("%.2f", rate), perShard);
    }
}
