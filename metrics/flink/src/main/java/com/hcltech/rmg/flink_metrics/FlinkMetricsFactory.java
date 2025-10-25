package com.hcltech.rmg.flink_metrics;

import com.hcltech.rmg.common.metrics.Metrics;
import com.hcltech.rmg.common.metrics.MetricsFactory;

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
