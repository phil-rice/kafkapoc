package com.hcltech.rmg.performance;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainer;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.flink_metrics.FlinkMetricsParams;
import com.hcltech.rmg.flinkadapters.MakeEmptyValueEnvelopeFunction;
import com.hcltech.rmg.flinkadapters.NormalPipelineFunction;
import com.hcltech.rmg.kafka.KafkaSourceForFlink;
import com.hcltech.rmg.kafka.WatermarkStrategyProvider;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.Envelope;
import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.RetryEnvelope;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class PerfHarnessMain {

    /**
     * Build the pipeline using the provided environment (no internal getExecutionEnvironment).
     */
    public static <CepState, Msg, Schema> Pipeline<CepState, Msg> buildPipeline(
            StreamExecutionEnvironment env,
            Class<IAppContainerFactory<KafkaConfig, CepState, Msg, Schema, FlinkMetricsParams>> factoryClass,
            String containerId,
            String module,
            int lanes,
            long asyncTimeoutMillis,
            Integer asyncParallelismOverride) {

        AppContainer<KafkaConfig, CepState, Msg, Schema, FlinkMetricsParams> app =
                IAppContainerFactory.resolve(factoryClass, containerId).valueOrThrow();
        final KafkaConfig kafka = app.eventSourceConfig();

        final int totalPartitions = kafka.sourceParallelism();

        env.setParallelism(totalPartitions > 0 ? totalPartitions : env.getParallelism());
        env.getConfig().setAutoWatermarkInterval(0);
        env.enableCheckpointing(30_000);

        var raw = KafkaSourceForFlink.rawKafkaStream(
                containerId,
                env,
                kafka.bootstrapServers(),
                kafka.topic(),
                kafka.groupId(),
                totalPartitions,
                OffsetsInitializer.earliest(),
                Duration.ofSeconds(60),
                WatermarkStrategyProvider.none()
        );

        OutputTag<ErrorEnvelope<CepState, Msg>> errorsTag =
                new OutputTag<>("errors", TypeInformation.of(new TypeHint<ErrorEnvelope<CepState, Msg>>() {
                })) {
                };
        OutputTag<RetryEnvelope<CepState, Msg>> retriesTag =
                new OutputTag<>("retries", TypeInformation.of(new TypeHint<RetryEnvelope<CepState, Msg>>() {
                })) {
                };

        DataStream<RawMessage> keyedStream = raw.keyBy(RawMessage::domainId);

        int asyncParallelism = (asyncParallelismOverride != null) ? asyncParallelismOverride : totalPartitions;
        int partitionsPerSubtask = (int) Math.ceil((double) totalPartitions / Math.max(1, asyncParallelism));
        int capacity = Math.max(1, (int) Math.round(lanes * partitionsPerSubtask * 1.2));

        var withCepState = keyedStream.map(new MakeEmptyValueEnvelopeFunction<>(factoryClass, containerId));

        var processed = AsyncDataStream
                .orderedWait(withCepState,
                        new NormalPipelineFunction<>(factoryClass, containerId, module),
                        asyncTimeoutMillis, TimeUnit.MILLISECONDS, capacity)
                .name("main-async")
                .setParallelism(asyncParallelism);

        var values = processed.process(new SplitEnvelopes<CepState, Msg>(errorsTag, retriesTag)).name("splitter");

        var allErrors = values.getSideOutput(errorsTag);
        var allRetries = values.getSideOutput(retriesTag);

        return new Pipeline<>(env, values, allErrors, allRetries);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

// Web UI fixed off 8081
        conf.setString("rest.address", "localhost");
        conf.setString("rest.port", "8088");
        System.out.println("Flink Web UI at http://localhost:8088");
        conf.setString("metrics.reporter.promjm.factory.class",
                "org.apache.flink.metrics.prometheus.PrometheusReporterFactory");
        conf.setString("metrics.reporter.promjm.port", "9400");

        conf.setString("metrics.reporter.promtm.factory.class",
                "org.apache.flink.metrics.prometheus.PrometheusReporterFactory");
        conf.setString("metrics.reporter.promtm.port", "9401");

        conf.setString("jobmanager.metrics.reporters", "promjm");
        conf.setString("taskmanager.metrics.reporters", "promtm");


        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);


        final String containerId = System.getProperty("app.container", "prod");
        var appContainer = AppContainerFactoryForMapStringObject.resolve(containerId).valueOrThrow();
        final int lanes = 300;

        Pipeline<Map<String, Object>, Map<String, Object>> pipe =
                buildPipeline(env, (Class) AppContainerFactoryForMapStringObject.class,
                        containerId, "notification", lanes, 2_000, null);

        // optional: try to locate the actual /metrics port in background
        new Thread(() -> probeMetricsPort(9400, 9401), "metrics-probe").start();

        // perf printer
        PerfStats.start(2000);

        // sinks
        pipe.values().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.VALUES)).name("values-metrics");
        pipe.errors().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.ERRORS)).name("errors-metrics");
        pipe.retries().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.RETRIES)).name("retries-metrics");

        // route to Kafka
        String brokers = appContainer.eventSourceConfig().bootstrapServers();
        EnvelopeRouting.routeToKafkaWithMetrics(
                pipe.values(), pipe.errors(), pipe.retries(),
                brokers, "processed", "errors", "retry"
        );

        pipe.env().execute("rmg-perf-harness");
    }

    /**
     * Probe http://localhost:[from..to]/metrics and print the first that responds.
     */
    private static void probeMetricsPort(int fromInclusive, int toInclusive) {
        // Give the MiniCluster a moment to boot reporters
        try {
            Thread.sleep(1500);
        } catch (InterruptedException ignored) {
        }
        for (int p = fromInclusive; p <= toInclusive; p++) {
            try {
                URL u = new URL("http://localhost:" + p + "/metrics");
                HttpURLConnection c = (HttpURLConnection) u.openConnection();
                c.setConnectTimeout(400);
                c.setReadTimeout(400);
                c.setRequestMethod("GET");
                int code = c.getResponseCode();
                if (code == 200) {
                    System.out.println("[metrics] Prometheus endpoint: http://localhost:" + p + "/metrics");
                    return;
                }
            } catch (IOException ignored) { /* try next */ }
        }
        System.out.println("[metrics] No /metrics endpoint found in " + fromInclusive + "-" + toInclusive);
    }
}
