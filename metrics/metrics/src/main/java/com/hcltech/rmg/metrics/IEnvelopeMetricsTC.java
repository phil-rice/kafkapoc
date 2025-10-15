package com.hcltech.rmg.metrics;

public interface IEnvelopeMetricsTC<E> {
    String metricName(E envelope);

    long startProcessingTime(E envelope);
}
