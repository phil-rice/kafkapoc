package com.example.metrics;

import java.util.Map;

/**
 * Immutable snapshot of metrics at a point in time.
 *
 * @param <S> shard type
 */
public record MetricsSnapshot<S>(
        long timestampMs,
        long totalProcessed,
        Map<S, Long> processedByShard
) {}
