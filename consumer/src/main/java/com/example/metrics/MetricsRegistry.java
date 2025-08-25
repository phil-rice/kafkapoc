package com.example.metrics;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class MetricsRegistry {
    private final LongAdder totalProcessed = new LongAdder();
    private final ConcurrentHashMap<TopicPartition, LongAdder> perPartition = new ConcurrentHashMap<>();

    /** Call this after a record has been successfully processed. */
    public void recordProcessed(TopicPartition tp) {
        totalProcessed.increment();
        perPartition.computeIfAbsent(tp, k -> new LongAdder()).increment();
    }

    /** Snapshot for printing. Cheap (LongAdder.sum()). */
    public MetricsSnapshot snapshot() {
        Map<TopicPartition, Long> parts = new HashMap<>();
        perPartition.forEach((tp, adder) -> parts.put(tp, adder.sum()));
        return new MetricsSnapshot(System.currentTimeMillis(), totalProcessed.sum(), parts);
    }
}
