package com.example.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Thread-safe registry of processing metrics.
 * 
 * <S> shard type (e.g. Kafka TopicPartition).
 */
public final class MetricsRegistry<S> {
    private final LongAdder totalProcessed = new LongAdder();
    private final ConcurrentHashMap<S, LongAdder> perShard = new ConcurrentHashMap<>();

    /** Record one successfully processed message for the given shard. */
    public void recordProcessed(S shard) {
        totalProcessed.increment();
        perShard.computeIfAbsent(shard, __ -> new LongAdder()).increment();
    }

    /** Snapshot current counts (immutable view). */
    public MetricsSnapshot<S> snapshot() {
        Map<S, Long> counts = perShard.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().sum()
                ));
        return new MetricsSnapshot<>(System.currentTimeMillis(), totalProcessed.sum(), counts);
    }
}
