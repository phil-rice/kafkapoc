package com.hcltech.rmg.opentelemetry;

import com.hcltech.rmg.metrics.Metrics;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;

import java.util.concurrent.ConcurrentHashMap;

public class OtelMetrics implements Metrics {
    private final Meter meter = GlobalOpenTelemetry.getMeter("rmg.worker");

    private final ConcurrentHashMap<String, LongCounter> counters   = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongHistogram> histos   = new ConcurrentHashMap<>();

    @Override
    public void increment(String name) {
        counters.computeIfAbsent(name, n -> meter.counterBuilder(n).setUnit("1").build())
                .add(1);
    }

    @Override
    public void histogram(String name, long value) {
        histos.computeIfAbsent(name, n -> meter.histogramBuilder(n).ofLongs().setUnit("ms").build())
                .record(value);
    }
}
