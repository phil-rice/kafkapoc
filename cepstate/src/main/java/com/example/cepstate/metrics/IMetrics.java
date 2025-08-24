package com.example.cepstate.metrics;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public interface IMetrics {
    void increment(String metricName);

    static IMetrics nullMetrics() {
        return new IMetrics() {
            @Override
            public void increment(String metricName) {
                // No-op implementation
            }
        };
    }

    static IMetrics memoryMetrics(ConcurrentHashMap<String, LongAdder> counters) {
        return new IMetrics() {
            @Override
            public void increment(String metricName) {
                Objects.requireNonNull(metricName);
                counters.computeIfAbsent(metricName, k -> new LongAdder()).increment();
            }
        };
    }
}
