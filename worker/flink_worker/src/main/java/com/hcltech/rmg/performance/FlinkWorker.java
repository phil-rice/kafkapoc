package com.hcltech.rmg.performance;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.flinkadapters.FlinkHelper;
import com.hcltech.rmg.flinkadapters.MetricsCountingSink;
import com.hcltech.rmg.flinkadapters.NormalPipelineFunction;
import com.hcltech.rmg.flinkadapters.PerfStats;
import com.hcltech.rmg.kafka.KafkaTopics;
import com.hcltech.rmg.kafka.ValueErrorRetryStreams;
import com.hcltech.rmg.shared_worker.BuildPipeline;
import com.hcltech.rmg.shared_worker.EnvelopeRouting;
import com.hcltech.rmg.shared_worker.KafkaFlinkHelper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Map;

public final class FlinkWorker {

    public static void main(String[] args) throws Exception {
        var appContainerDefn = AppContainerDefn.of(AppContainerFactoryForMapStringObject.class, "prod");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        var appContainer = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        KafkaFlinkHelper.ensureRocksDbStateBackend(env, appContainer.rocksDBPath());

        if (KafkaTopics.ensureTopics(appContainer.eventSourceConfig(), EnvelopeRouting.allTopics, 12, (short) 1).valueOrThrow()) {
            // just sticking 12/3 in for tests
            System.out.println("Created output topics");
        }
        ValueErrorRetryStreams<Map<String, Object>, Map<String, Object>> pipe = BuildPipeline.buildPipeline(env, appContainerDefn, false);

        // optional: try to locate the actual /metrics port in background
        new Thread(() -> FlinkHelper.probeMetricsPort(9400, 9401), "metrics-probe").start();

        // perf printer
        PerfStats.start(2000);

        // sinks
        pipe.values().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.VALUES)).name("values-metrics");
        pipe.errors().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.ERRORS)).name("errors-metrics");
        pipe.retries().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.RETRIES)).name("retries-metrics");

        // route to Kafka
        String brokers = appContainer.eventSourceConfig().bootstrapServer();
        EnvelopeRouting.routeToKafka(appContainerDefn, pipe.values(), pipe.errors(), pipe.retries(), brokers, "processed", "errors", "retry", appContainer.eventSourceConfig().properties());

        pipe.env().execute("rmg-perf-harness");
    }


}
