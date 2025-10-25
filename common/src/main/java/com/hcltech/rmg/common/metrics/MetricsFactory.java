package com.hcltech.rmg.common.metrics;

@FunctionalInterface
public interface MetricsFactory<MetricsParams> {

    Metrics create(MetricsParams params);
}
