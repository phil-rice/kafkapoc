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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public final class FlinkWorker {

    public static void main(String[] args) throws Exception {
        System.setProperty("java.io.tmpdir", "C:\\flink-tmp");
        // print to confirm
        System.out.println("tmpdir = " + System.getProperty("java.io.tmpdir"));
        System.out.println("PATH = " + System.getenv("PATH"));
        System.out.println("java.library.path = " + System.getProperty("java.library.path"));
        // (your existing temp dir setup)
        System.setProperty("java.io.tmpdir", "C:\\flink-tmp");

        // --- Build local paths (portable across Win/*nix) ---
        Path base       = Paths.get(System.getProperty("java.io.tmpdir"));           // e.g. C:\flink-tmp

        // --- Flink Configuration (no YAML) ---
        Configuration conf = FlinkHelper.makeDefaultFlinkConfig();
        // --- Local filesystem targets for checkpoints/savepoints ---
        Path checkpoints = base.resolve("checkpoints");
        Path savepoints  = base.resolve("savepoints");

//        conf.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        // Tell Flink we’re using filesystem checkpoint storage to a local path
        conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpoints.toUri().toString()); // file:///C:/flink-tmp/checkpoints
        conf.set(CheckpointingOptions.SAVEPOINT_DIRECTORY,   savepoints.toUri().toString());  // file:///C:/flink-tmp/savepoints

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        var appContainerDefn = AppContainerDefn.of(AppContainerFactoryForMapStringObject.class, "prod");
        var appContainer = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();
        if (KafkaTopics.ensureTopics(appContainer.eventSourceConfig(), EnvelopeRouting.allTopics, 12, (short) 1).valueOrThrow()) {//just sticking 12/3 in for tests
            System.out.println("Created output topics");
        }
        var func = new NormalPipelineFunction<>(appContainerDefn, "notification");
        ValueErrorRetryStreams<Map<String, Object>, Map<String, Object>> pipe = BuildPipeline.buildPipeline(env, appContainerDefn, func, false);

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
        EnvelopeRouting.routeToKafka(pipe.values(), pipe.errors(), pipe.retries(), brokers, "processed", "errors", "retry");

        pipe.env().execute("rmg-perf-harness");
    }


}
