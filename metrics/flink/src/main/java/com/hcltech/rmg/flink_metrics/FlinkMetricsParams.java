package com.hcltech.rmg.flink_metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;

public record FlinkMetricsParams(
        MetricGroup metricGroup,
        Class<?> callerClass,
        int subtask
) {
    static public FlinkMetricsParams fromRuntime(RuntimeContext context, Class<?> callerClass) {
        return new FlinkMetricsParams(
                context.getMetricGroup(),
                callerClass,
                context.getTaskInfo().getIndexOfThisSubtask()

        );
    }

}
