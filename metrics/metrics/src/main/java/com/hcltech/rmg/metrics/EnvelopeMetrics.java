package com.hcltech.rmg.metrics;


import com.hcltech.rmg.common.ITimeService;

/**
 * Single call at the end of the pipeline.
 * Implementation reads outcome + latency from the Envelope and records metrics.
 */
public interface EnvelopeMetrics<E> {
    void addToMetricsAtEnd(E e);

    static <E> EnvelopeMetrics<E> create(ITimeService timeService, Metrics metrics, IEnvelopeMetricsTC<E> metricsTC) {
        return new EnvelopeMetricsImpl<>(timeService, metrics, metricsTC);
    }
}

record EnvelopeMetricsImpl<E>(ITimeService time, Metrics metrics,
                              IEnvelopeMetricsTC<E> tc) implements EnvelopeMetrics<E> {
    @Override
    public void addToMetricsAtEnd(E e) {
        metrics.increment("message.processed.total");
        var name = "message.processed." + tc.metricName(e);
        metrics.increment(name);
        long durationMs = Math.max(0L, time.currentTimeMillis() - tc.startProcessingTime(e));
        metrics.histogram("message.processed.latency", durationMs);
    }
}
