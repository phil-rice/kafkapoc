package com.example.metrics;

import java.util.Map;

/** Immutable snapshot of all metrics at a point in time. */
public record MetricsSnapshot<S>(
        long timestampMs,

        // COUNTERS
        Map<String, Long> counterTotals,                 // e.g. "processed" -> 12345
        Map<String, Map<S, Long>> countersByShard,       // e.g. "processed" -> { tp0: 1000, ... }

        // GAUGES
        Map<String, Map<S, Long>> gaugesByShard          // e.g. "lag" -> { tp0: 42, ... }
) {}
