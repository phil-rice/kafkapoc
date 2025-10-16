package com.hcltech.rmg.flink_metrics;

import com.hcltech.rmg.metrics.Metrics;
import com.hcltech.rmg.metrics.MetricsFactory;
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

public final class FlinkMetricsFactory implements MetricsFactory<FlinkMetricsParams> {

    private final String app;
    private final String module;
    private final int maxNames;
    private final boolean createMeters;

    public FlinkMetricsFactory(String app, String module, int maxNames, boolean createMeters) {
        this.app = app;
        this.module = module;
        this.maxNames = maxNames;
        this.createMeters = createMeters;
    }

    @Override
    public Metrics create(FlinkMetricsParams params) {
        return new FlinkMetrics(
                params.metricGroup(), app, module, params.callerClass().getSimpleName(), params.subtask(),
                maxNames, createMeters);
    }
}
