package com.hcltech.rmg.shared_worker;

import com.hcltech.rmg.messages.AiFailureEnvelope;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;
import java.util.Properties;

public final class EnvelopeRouting {
    public static final String processedTopic = "processed";
    public static final String errorsTopic = "errors";
    public static final String retryTopic = "retries";
    public static final List<String> allTopics = List.of(processedTopic, errorsTopic, retryTopic);

    private EnvelopeRouting() {
    }

    /**
     * Preferred: pass producer config (e.g., SASL, linger.ms, acks).
     */
    public static <CepState, Msg> void routeToKafka(
            DataStream<ValueEnvelope<CepState, Msg>> values,
            DataStream<ErrorEnvelope<CepState, Msg>> errors,
            DataStream<RetryEnvelope<CepState, Msg>> retries,
            String brokers,
            String processedTopic,
            String errorsTopic,
            String retryTopic,
            Properties producerConfig) {
        routeToKafkaWithFailures(values, errors, retries, null, brokers, processedTopic, errorsTopic, retryTopic, null, producerConfig);
    }

    public static <CepState, Msg> void routeToKafkaWithFailures(
            DataStream<ValueEnvelope<CepState, Msg>> values,
            DataStream<ErrorEnvelope<CepState, Msg>> errors,
            DataStream<RetryEnvelope<CepState, Msg>> retries,
            DataStream<AiFailureEnvelope<CepState, Msg>> failuresOrNull,
            String brokers,
            String processedTopic,
            String errorsTopic,
            String retryTopic,
            String failureTopic,
            Properties producerConfig
    ) {
        final Properties cfg = (producerConfig == null) ? new Properties() : producerConfig;

        values
                .sinkTo(EnvelopeKafkaSinks.valueSink(brokers, processedTopic, cfg))
                .name("values->kafka");

        errors
                .sinkTo(EnvelopeKafkaSinks.errorSink(brokers, errorsTopic, cfg))
                .name("errors->kafka");

        retries
                .sinkTo(EnvelopeKafkaSinks.retrySink(brokers, retryTopic, cfg))
                .name("retries->kafka");

        if (failuresOrNull != null)
            failuresOrNull
                    .sinkTo(EnvelopeKafkaSinks.failureSink(brokers, failureTopic, cfg))
                    .name("failures->kafka");
    }

    /**
     * Convenience overload: no producer config.
     */
    public static <CepState, Msg> void routeToKafka(
            DataStream<ValueEnvelope<CepState, Msg>> values,
            DataStream<ErrorEnvelope<CepState, Msg>> errors,
            DataStream<RetryEnvelope<CepState, Msg>> retries,
            String brokers,
            String processedTopic,
            String errorsTopic,
            String retryTopic
    ) {
        routeToKafka(values, errors, retries, brokers, processedTopic, errorsTopic, retryTopic, new Properties());
    }
}
