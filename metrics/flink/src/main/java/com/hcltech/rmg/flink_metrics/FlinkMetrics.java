package com.hcltech.rmg.flink_metrics;

import com.hcltech.rmg.metrics.Metrics;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

public final class FlinkMetrics implements Metrics {
    private final MetricGroup group;
    private final int maxNames;
    private final boolean createMeters;

    // NEW: factories (default to real Flink classes)
    private final Function<String, Histogram> histogramFactory;
    private final BiFunction<String, Counter, MeterView> meterFactory;

    private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Histogram> histos = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MeterView> meters = new ConcurrentHashMap<>();
    private final Set<String> namesSeen = ConcurrentHashMap.newKeySet();

    // Keep your existing ctor behavior by delegating to the injected one
    public FlinkMetrics(MetricGroup base,
                        String app, String module, String operator, int subtask,
                        int maxNames, boolean createMeters) {
        this(base, app, module, operator, subtask, maxNames, createMeters,
                name -> new DescriptiveStatisticsHistogram(2048),
                (meterName, c) -> new MeterView(c, 60));
    }

    // testable constructor
    public FlinkMetrics(MetricGroup base,
                        String app, String module, String operator, int subtask,
                        int maxNames, boolean createMeters,
                        Function<String, Histogram> histogramFactory,
                        BiFunction<String, Counter, MeterView> meterFactory) {
        this.group = base.addGroup("app", app)
                .addGroup("module", module)
                .addGroup("operator", operator)
                .addGroup("subtask", String.valueOf(subtask));
        this.maxNames = maxNames;
        this.createMeters = createMeters;
        this.histogramFactory = histogramFactory;
        this.meterFactory = meterFactory;
    }

    @Override
    public void increment(String name) {
        final String key = validateAndCount(name);
        Counter c = counters.computeIfAbsent(key, group::counter);
        c.inc();
        if (createMeters) {
            meters.computeIfAbsent(key + ".rate", n -> newMeter(n, c));
        }
    }

    @Override
    public void histogram(String name, long v) {
        final String key = validateAndCount(name);
        histos.computeIfAbsent(key, n -> group.histogram(n, histogramFactory.apply(n)))
                .update(v);
    }

    private String validateAndCount(String name) {
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Bad metric name: " + name);
        }
        if (namesSeen.add(name) && maxNames > 0 && namesSeen.size() > maxNames) {
            namesSeen.remove(name);
            throw new IllegalStateException("Too many distinct metric names (> " + maxNames + ")");
        }
        return name;
    }

    private MeterView newMeter(String meterName, Counter c) {
        MeterView mv = meterFactory.apply(meterName, c);
        group.meter(meterName, mv);
        return mv;
    }

    private static final Pattern NAME_PATTERN = Pattern.compile("[A-Za-z0-9_.-]{1,120}");
}
