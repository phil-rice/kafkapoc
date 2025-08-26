package com.example.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Thread-safe metrics registry with:
 *  - Named COUNTERS: increment-only (LongAdder)
 *  - Named GAUGES: last-set value (AtomicLong)
 *
 * <S> shard type (e.g., Kafka TopicPartition)
 */
public final class MetricsRegistry<S> {

    // counters[name][shard] -> LongAdder
    private final ConcurrentHashMap<String, ConcurrentHashMap<S, LongAdder>> counters = new ConcurrentHashMap<>();
    // gauge[name][shard] -> AtomicLong
    private final ConcurrentHashMap<String, ConcurrentHashMap<S, AtomicLong>> gauges = new ConcurrentHashMap<>();

    // ---------- COUNTERS ----------

    /** Increment a counter metric by 1 for the given shard. */
    public void inc(String metricName, S shard) {
        incBy(metricName, shard, 1);
    }

    /** Increment a counter metric by delta (>0) for the given shard. */
    public void incBy(String metricName, S shard, long delta) {
        if (delta <= 0) return;
        counters
                .computeIfAbsent(metricName, __ -> new ConcurrentHashMap<>())
                .computeIfAbsent(shard, __ -> new LongAdder())
                .add(delta);
    }

    // ---------- GAUGES ----------

    /** Set a gauge metric to a value for the given shard. */
    public void set(String metricName, S shard, long value) {
        gauges
                .computeIfAbsent(metricName, __ -> new ConcurrentHashMap<>())
                .computeIfAbsent(shard, __ -> new AtomicLong())
                .set(value);
    }


    // ---------- SNAPSHOT ----------

    /** Immutable snapshot of all counters + gauges at this instant. */
    public MetricsSnapshot<S> snapshot() {
        long now = System.currentTimeMillis();

        // countersByShard: Map<metricName, Map<shard, value>>
        Map<String, Map<S, Long>> countersByShard = counters.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().entrySet().stream()
                                .collect(Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey,
                                        x -> x.getValue().sum()
                                ))
                ));

        // counterTotals: Map<metricName, sum across shards>
        Map<String, Long> counterTotals = countersByShard.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().values().stream().mapToLong(Long::longValue).sum()
                ));

        // gaugesByShard: Map<metricName, Map<shard, value>>
        Map<String, Map<S, Long>> gaugesByShard = gauges.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().entrySet().stream()
                                .collect(Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey,
                                        x -> x.getValue().get()
                                ))
                ));

        return new MetricsSnapshot<>(now, counterTotals, countersByShard, gaugesByShard);
    }
}
