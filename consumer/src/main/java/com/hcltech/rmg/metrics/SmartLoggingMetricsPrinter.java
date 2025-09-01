package com.hcltech.rmg.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Logs a compact summary most of the time, and a full dump every N ticks.
 * Summary = primary counter total/delta/rate + per-gauge min/max across shards.
 * Full    = countersByShard + gaugesByShard.
 */
public final class SmartLoggingMetricsPrinter<S> implements MetricsPrinter<S> {
    private static final Logger log = LoggerFactory.getLogger(SmartLoggingMetricsPrinter.class);

    private final int fullEvery; // e.g., 15 => every 15th tick print full details
    private int tick = 0;

    public SmartLoggingMetricsPrinter(int fullEvery) {
        if (fullEvery < 1) throw new IllegalArgumentException("fullEvery must be >= 1");
        this.fullEvery = fullEvery;
    }

    @Override
    public synchronized void print(MetricsSnapshot<S> snap, long delta, double rate, String primaryCounterName) {
        tick++;
        boolean full = (tick % fullEvery) == 0;

        long total = snap.counterTotals().getOrDefault(primaryCounterName, 0L);
        String rateStr = String.format("%.2f", rate);

        if (!full) {
            // Summary: primary totals + min/max of each gauge
            Map<String, String> gaugesMinMax = summarizeGauges(snap.gaugesByShard());
            log.info("METRICS summary [{}] total={} (+{}, ~{}/s) gauges={}",
                    primaryCounterName, total, delta, rateStr, gaugesMinMax);
            return;
        }

        // Full dump
        log.info("METRICS FULL [{}] total={} (+{}, ~{}/s)\n  countersByShard={}\n  gaugesByShard={}",
                primaryCounterName,
                total, delta, rateStr,
                prettyMapOfMaps(snap.countersByShard()),
                prettyMapOfMaps(snap.gaugesByShard()));
    }

    private Map<String, String> summarizeGauges(Map<String, Map<S, Long>> gaugesByShard) {
        if (gaugesByShard.isEmpty()) return Map.of();
        Map<String, String> out = new LinkedHashMap<>();
        for (Entry<String, Map<S, Long>> e : gaugesByShard.entrySet()) {
            var values = e.getValue().values();
            if (values.isEmpty()) continue;
            long min = values.stream().mapToLong(Long::longValue).min().orElse(0);
            long max = values.stream().mapToLong(Long::longValue).max().orElse(0);
            out.put(e.getKey(), min + ".." + max);
        }
        return out;
    }

    private static <S> String prettyMapOfMaps(Map<String, Map<S, Long>> m) {
        if (m.isEmpty()) return "{}";
        return m.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
    }
}
