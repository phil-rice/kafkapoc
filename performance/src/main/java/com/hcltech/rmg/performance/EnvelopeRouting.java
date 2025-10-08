// src/main/java/com/hcltech/rmg/performance/EnvelopeRouting.java
package com.hcltech.rmg.performance;

import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Map;
import java.util.Properties;

public final class EnvelopeRouting {
    private EnvelopeRouting() {
    }

    /**
     * Preferred: pass producer config (e.g., SASL, linger.ms, acks).
     */
    public static <CepState,Msg> void routeToKafkaWithMetrics(
            DataStream<ValueEnvelope<CepState,Msg>> values,
            DataStream<ErrorEnvelope<CepState,Msg>> errors,
            DataStream<RetryEnvelope<CepState,Msg>> retries,
            String brokers,
            String processedTopic,
            String errorsTopic,
            String retryTopic,
            Properties producerConfig
    ) {
        final Properties cfg = (producerConfig == null) ? new Properties() : producerConfig;

        values
                .map(new MetricsTap<>("envelopes", MetricsTap.Kind.VALUES))
                .sinkTo(EnvelopeKafkaSinks.valueSink(brokers, processedTopic, cfg))
                .name("values->kafka");

        errors
                .map(new MetricsTap<>("envelopes", MetricsTap.Kind.ERRORS))
                .sinkTo(EnvelopeKafkaSinks.errorSink(brokers, errorsTopic, cfg))
                .name("errors->kafka");

        retries
                .map(new MetricsTap<>("envelopes", MetricsTap.Kind.RETRIES))
                .sinkTo(EnvelopeKafkaSinks.retrySink(brokers, retryTopic, cfg))
                .name("retries->kafka");
    }

    /**
     * Convenience overload: no producer config.
     */
    public static  <CepState,Msg> void routeToKafkaWithMetrics(
            DataStream<ValueEnvelope <CepState,Msg>> values,
            DataStream<ErrorEnvelope <CepState,Msg>> errors,
            DataStream<RetryEnvelope <CepState,Msg>> retries,
            String brokers,
            String processedTopic,
            String errorsTopic,
            String retryTopic
    ) {
        routeToKafkaWithMetrics(values, errors, retries, brokers, processedTopic, errorsTopic, retryTopic, new Properties());
    }
}
