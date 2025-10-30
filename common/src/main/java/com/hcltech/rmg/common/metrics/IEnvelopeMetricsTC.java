package com.hcltech.rmg.common.metrics;

public interface IEnvelopeMetricsTC<E> {
    String metricName(E envelope);

    long startProcessingTime(E envelope);
}
