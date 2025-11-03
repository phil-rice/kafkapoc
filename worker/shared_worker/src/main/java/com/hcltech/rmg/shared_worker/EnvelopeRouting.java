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
    public static final String retryTopic  = "retries";
    public static final List<String> allTopics = List.of(processedTopic, errorsTopic, retryTopic);

    private EnvelopeRouting() {}

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
        routeToKafkaWithFailures(defn, values, errors, retries, null, new AtomicBoolean(false),
                brokers, processedTopic, errorsTopic, retryTopic, null, producerConfig);
    }

    /**
     * Route the four streams to Kafka sinks.
     *
     * IMPORTANT (metric name collision fix):
     * - We call .startNewChain() right before each sink. This forces Flink to create a new task
     *   for each sink/committer chain. Without this, multiple Kafka Committer operators may share
     *   the same metric group within a chained task and all try to register the metric
     *   "pendingCommittables", producing:
     *     "Name collision: Group already contains a Metric with the name 'pendingCommittables'."
     *
     * - We also give each sink a unique .name(...) so Flink UI / metrics are clearer.
     */
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

        // values -> kafka
        // Insert a no-op map and startNewChain() so the Kafka Committer runs in its own task
        values
                .map(new IdentityMap<>())                             // no-op to get a SingleOutputStreamOperator
                .name("values-chain-breaker")
                .startNewChain()                                      // ← separates from upstream chain (prevents metric group collision)
                .sinkTo(EnvelopeKafkaSinks.valueSink(defn, brokers, processedTopic, cfg))
                .name("values->kafka");                               // unique operator name (UI/metrics clarity)

        // errors -> (log) -> kafka
        errors
                .map(new ErrorLogger<CepState, Msg>())                // already a map stage
                .name("errors->log")
                .startNewChain()                                      // ← break chain before sink/committer
                .sinkTo(EnvelopeKafkaSinks.errorSink(brokers, errorsTopic, cfg))
                .name("errors->kafka");

        // retries -> kafka
        retries
                .map(new IdentityMap<>())
                .name("retries-chain-breaker")
                .startNewChain()                                      // ← break chain before sink/committer
                .sinkTo(EnvelopeKafkaSinks.retrySink(brokers, retryTopic, cfg))
                .name("retries->kafka");

        // failures (optional) -> kafka
        if (failuresOrNull != null) {
            failuresOrNull
                    .flatMap(new FirstHitFlatMap<>(firstFailureAtomic))
                    .name("failures-first-hit")
                    .startNewChain()                                  // ← break chain before sink/committer
                    .sinkTo(EnvelopeKafkaSinks.failureSink(brokers, failureTopic, cfg))
                    .name("failures->kafka");
        }
    }

    /** No-op identity map used to obtain an operator we can .startNewChain() on. */
    static final class IdentityMap<T> extends RichMapFunction<T, T> {
        @Override public void open(OpenContext parameters) { /* no-op */ }
        @Override public T map(T value) { return value; }
    }

    static final class ErrorLogger<CepState, Msg> extends RichMapFunction<ErrorEnvelope<CepState, Msg>, ErrorEnvelope<CepState, Msg>> {
        private transient Logger log;

        @Override
        public void open(OpenContext parameters) {
            // stable logger name so logback filters are easy to apply if needed
            this.log = LoggerFactory.getLogger("ErrorsStream");
        }

        @Override
        public ErrorEnvelope<CepState, Msg> map(ErrorEnvelope<CepState, Msg> err) {
            log.warn("ErrorEnvelope: {}", err);
            return err; // pass-through so the downstream sink still gets it
        }
    }
}
