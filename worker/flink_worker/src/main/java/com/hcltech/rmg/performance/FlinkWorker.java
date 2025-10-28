package com.hcltech.rmg.performance;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.appcontainer.interfaces.IAppContainerFactory;
import com.hcltech.rmg.flinkadapters.FlinkHelper;
import com.hcltech.rmg.flinkadapters.MetricsCountingSink;
import com.hcltech.rmg.flinkadapters.NormalPipelineFunction;
import com.hcltech.rmg.kafka.ValueErrorRetryStreams;
import com.hcltech.rmg.shared_worker.BuildPipeline;
import com.hcltech.rmg.telemetry.AppInsightsTelemetry;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public final class FlinkWorker {

    public static void main(String[] args) throws Exception {

        AppInsightsTelemetry.initGlobal();

        Configuration conf = FlinkHelper.makeDefaultFlinkConfig();

        // Force RocksDB (Flink 2.0 style)
        conf.setString(StateBackendOptions.STATE_BACKEND.key(), "rocksdb");

        String checkpointUrl = getenvOrThrow("AZURE_BLOB_CHECKPOINT_URL");
        String savepointUrl  = getenvOrThrow("AZURE_BLOB_SAVEPOINT_URL");
        conf.setString(CheckpointingOptions.CHECKPOINT_STORAGE.key(), "filesystem");
        conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(), checkpointUrl);
        conf.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),  savepointUrl);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.enableCheckpointing(10_000L);

        AppContainerDefn appContainerDefn =
                AppContainerDefn.of(AppContainerFactoryForMapStringObject.class, "prod");
        var appContainer = IAppContainerFactory.resolve(appContainerDefn).valueOrThrow();

        var func = new NormalPipelineFunction<>(appContainerDefn, "notification");

        ValueErrorRetryStreams<Map<String,Object>, Map<String,Object>> pipe =
                BuildPipeline.buildPipeline(env, appContainerDefn, func, false);

        pipe.values().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.VALUES))
            .name("values-metrics");
        pipe.errors().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.ERRORS))
            .name("errors-metrics");
        pipe.retries().addSink(new MetricsCountingSink<>("envelopes", MetricsCountingSink.Kind.RETRIES))
            .name("retries-metrics");

        env.execute("rmg-aca-cluster");
    }

    private static String getenvOrThrow(String key) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required env var: " + key);
        }
        return v;
    }
}
