package ai_worker.domain;

import com.hcltech.rmg.appcontainer.interfaces.AiDefn;
import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.config.configs.Configs;
import com.hcltech.rmg.flinkadapters.FlinkHelper;
import com.hcltech.rmg.flinkadapters.MetricsCountingSink;
import com.hcltech.rmg.flinkadapters.NormalPipelineFunction;
import com.hcltech.rmg.flinkadapters.PerfStats;
import com.hcltech.rmg.kafka.KafkaTopics;

import com.hcltech.rmg.kafka.ValueErrorRetryStreams;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.shared_worker.BuildPipeline;
import com.hcltech.rmg.shared_worker.EnvelopeRouting;
import com.hcltech.rmg.shared_worker.FirstHitJobKiller;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.codehaus.stax2.validation.XMLValidationSchema;
import org.springframework.stereotype.Component;

import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Binds the AI worker job name to your existing PerfHarness pipeline.
 * This class ONLY builds the graph on the provided env; it does not execute.
 */
@Component
public class AiWorkerJobBuilder implements JobBuilder<StreamExecutionEnvironment> {

    @Override
    public void build(StreamExecutionEnvironment env, RootConfig rootConfig, Configs cfg, String celCondition, AtomicBoolean firstFailureAtomic) throws Exception {
        // Resolve your app container
        var appContainerDefn = AppContainerDefn.withAiDefn(AppContainerFactoryForMapStringObject.class, "ai", new AiDefn(rootConfig, cfg, celCondition));
        var appContainer = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();

        // Make sure Kafka topics exist (best-effort)
        KafkaTopics.ensureTopics(appContainer.eventSourceConfig(), EnvelopeRouting.allTopics, 0, (short) 1);

        // Build pipeline (does NOT execute)
        ValueErrorRetryStreams<Map<String, Object>, Map<String, Object>> pipe = BuildPipeline.buildPipeline(env, appContainerDefn, true);

        // Optional: metrics web port probe + perf printer (same as your sample)
        new Thread(() -> FlinkHelper.probeMetricsPort(9400, 9401), "metrics-probe").start();
        PerfStats.start(2000);

        // Sinks for metrics
        pipe.values().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.VALUES)).name("values-metrics");
        pipe.errors().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.ERRORS)).name("errors-metrics");
        pipe.retries().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.RETRIES)).name("retries-metrics");
        pipe.aiFailures().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.FAILURES)).name("failure-metrics");

        // Route to Kafka
        String brokers = appContainer.eventSourceConfig().bootstrapServer();
        // <EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFR, MetricsParam>
        EnvelopeRouting.routeToKafkaWithFailures(
                appContainerDefn,
                pipe.values(),
                pipe.errors(),
                pipe.retries(),
                pipe.aiFailures(),
                firstFailureAtomic,
                brokers,
                "processed",
                "errors",
                "retry",
                "failures",
                new Properties()
        );
    }
}
