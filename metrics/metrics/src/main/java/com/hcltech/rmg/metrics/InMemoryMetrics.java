package com.hcltech.rmg.metrics;

import java.util.*;

public final class InMemoryMetrics implements Metrics {
    public final Map<String, Long> counters = new HashMap<>();
    public final Map<String, List<Long>> histograms = new HashMap<>();

    @Override
    public synchronized void increment(String name) {
        counters.merge(name, 1L, Long::sum);
    }

    @Override
    public synchronized void histogram(String name, long value) {
        histograms.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
    }
}
