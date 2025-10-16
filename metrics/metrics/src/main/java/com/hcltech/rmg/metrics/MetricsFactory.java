package com.hcltech.rmg.metrics;

@FunctionalInterface
public interface MetricsFactory<MetricsParams> {

    Metrics create(MetricsParams params);
}
