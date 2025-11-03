package com.hcltech.rmg.shared_worker;

import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.messages.AiFailureEnvelope;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import com.hcltech.rmg.messages.ValueEnvelope;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

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
    public static <EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam> void routeToKafka(
            AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam> defn,
            DataStream<ValueEnvelope<CepState, Msg>> values,
            DataStream<ErrorEnvelope<CepState, Msg>> errors,
            DataStream<RetryEnvelope<CepState, Msg>> retries,
            String brokers,
            String processedTopic,
            String errorsTopic,
            String retryTopic,
            Properties producerConfig) {
        routeToKafkaWithFailures(defn, values, errors, retries, null, new AtomicBoolean(false), brokers, processedTopic, errorsTopic, retryTopic, null, producerConfig);
    }

    public static <EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam> void routeToKafkaWithFailures(
            AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam> defn,
            DataStream<ValueEnvelope<CepState, Msg>> values,
            DataStream<ErrorEnvelope<CepState, Msg>> errors,
            DataStream<RetryEnvelope<CepState, Msg>> retries,
            DataStream<AiFailureEnvelope<CepState, Msg>> failuresOrNull,
            AtomicBoolean firstFailureAtomic,
            String brokers,
            String processedTopic,
            String errorsTopic,
            String retryTopic,
            String failureTopic,
            Properties producerConfig
    ) {
        final Properties cfg = (producerConfig == null) ? new Properties() : producerConfig;

        values
                .sinkTo(EnvelopeKafkaSinks.valueSink(defn, brokers, processedTopic, cfg))
                .name("values->kafka");

        errors.map(new ErrorLogger<CepState, Msg>())
                .name("errors->log")
                .sinkTo(EnvelopeKafkaSinks.errorSink(brokers, errorsTopic, cfg))
                .name("errors->kafka");

        retries
                .sinkTo(EnvelopeKafkaSinks.retrySink(brokers, retryTopic, cfg))
                .name("retries->kafka");

        if (failuresOrNull != null)
            failuresOrNull.flatMap(new FirstHitFlatMap<>(firstFailureAtomic))
                    .sinkTo(EnvelopeKafkaSinks.failureSink(brokers, failureTopic, cfg))
                    .name("failures->kafka");
    }

    /**
     * Convenience overload: no producer config.
     */
    public static <EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam>  void routeToKafka(
            AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam> defn,
            DataStream<ValueEnvelope<CepState, Msg>> values,
            DataStream<ErrorEnvelope<CepState, Msg>> errors,
            DataStream<RetryEnvelope<CepState, Msg>> retries,
            String brokers,
            String processedTopic,
            String errorsTopic,
            String retryTopic
    ) {
        routeToKafka(defn,values, errors, retries, brokers, processedTopic, errorsTopic, retryTopic, new Properties());
    }

    static final class ErrorLogger<CepState, Msg> extends RichMapFunction<ErrorEnvelope<CepState, Msg>, ErrorEnvelope<CepState, Msg>> {
        private transient Logger log;

        @Override
        public void open(OpenContext parameters) {
            // Use a stable logger name so logback filters are easy to apply if needed
            this.log = LoggerFactory.getLogger("ErrorsStream");
        }

        @Override
        public ErrorEnvelope<CepState, Msg> map(ErrorEnvelope<CepState, Msg> err) {
            // If ErrorEnvelope exposes structured fields, format them here.
            // Fallback to toString() if not.
            log.warn("ErrorEnvelope: {}", err);
            return err; // pass-through so the downstream sink still gets it
        }
    }
}
