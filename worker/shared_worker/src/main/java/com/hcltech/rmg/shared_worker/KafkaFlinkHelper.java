package com.hcltech.rmg.shared_worker;

import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.kafka.KafkaSourceForFlink;
import com.hcltech.rmg.kafka.WatermarkStrategyProvider;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.RawMessage;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

public class KafkaFlinkHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaFlinkHelper.class);

    // ---- Config keys via ConfigOptions (version-friendly) ----
    private static final ConfigOption<Boolean> OPT_INCREMENTAL_A =
            ConfigOptions.key("state.backend.incremental").booleanType().defaultValue(true);
    private static final ConfigOption<Boolean> OPT_INCREMENTAL_B =
            ConfigOptions.key("state.backend.rocksdb.incremental").booleanType().defaultValue(true);

    private static final ConfigOption<Boolean> OPT_MEM_MANAGED =
            ConfigOptions.key("state.backend.rocksdb.memory.managed").booleanType().defaultValue(true);
    private static final ConfigOption<String> OPT_PRESET =
            ConfigOptions.key("state.backend.rocksdb.predefined-options").stringType().defaultValue("FLASH_SSD_OPTIMIZED");
    private static final ConfigOption<Integer> OPT_THREADS =
            ConfigOptions.key("state.backend.rocksdb.thread.num").intType().defaultValue(6);

    private static final ConfigOption<String> OPT_WB_SIZE =
            ConfigOptions.key("state.backend.rocksdb.writebuffer.size").stringType().defaultValue("128mb");
    private static final ConfigOption<Integer> OPT_WB_COUNT =
            ConfigOptions.key("state.backend.rocksdb.writebuffer.count").intType().defaultValue(4);
    private static final ConfigOption<Integer> OPT_WB_NUM_MERGE =
            ConfigOptions.key("state.backend.rocksdb.writebuffer.number-to-merge").intType().defaultValue(2);

    private static final ConfigOption<String> OPT_TARGET_FILE_SIZE =
            ConfigOptions.key("state.backend.rocksdb.target-file-size-base").stringType().defaultValue("128mb");

    private static final ConfigOption<Integer> OPT_L0_TRIGGER =
            ConfigOptions.key("state.backend.rocksdb.level0.file-num-compaction-trigger").intType().defaultValue(6);
    private static final ConfigOption<Integer> OPT_L0_SLOWDOWN =
            ConfigOptions.key("state.backend.rocksdb.level0.slowdown-writes-trigger").intType().defaultValue(12);
    private static final ConfigOption<Integer> OPT_L0_STOP =
            ConfigOptions.key("state.backend.rocksdb.level0.stop-writes-trigger").intType().defaultValue(20);

    // ---- Metrics options (Prometheus + RocksDB metrics) ----
    private static final ConfigOption<String> OPT_REPORTERS =
            ConfigOptions.key("metrics.reporters").stringType().defaultValue("promjm");
    private static final ConfigOption<String> OPT_PROM_CLASS =
            ConfigOptions.key("metrics.reporter.promjm.class").stringType().defaultValue("org.apache.flink.metrics.prometheus.PrometheusReporter");
    private static final ConfigOption<String> OPT_PROM_PORT =
            ConfigOptions.key("metrics.reporter.promjm.port").stringType().defaultValue("9400");
    private static final ConfigOption<Boolean> OPT_SCOPE_REWRITE =
            ConfigOptions.key("metrics.scope.delimiter.rewrite").booleanType().defaultValue(true);
    private static final ConfigOption<Boolean> OPT_RDB_METRICS =
            ConfigOptions.key("state.backend.rocksdb.metrics.enabled").booleanType().defaultValue(true);

    /**
     * Build the RawMessage stream and configure metrics/state/checkpointing.
     */
    public static <ESC, CepState, Msg, Schema, RT, FR, MetricsParam>
    DataStreamSource<RawMessage> createRawMessageStreamFromKafka(
            AppContainerDefn<ESC, CepState, Msg, Schema, RT, FR, MetricsParam> appContainerDefn,
            StreamExecutionEnvironment env,
            KafkaConfig kafka,
            int checkpointingInterval,
            String rocksDBPath,
            boolean useRocksDBStateBackend) {

        final int totalPartitions = kafka.sourceParallelism();
        int parallelism = totalPartitions > 0 ? totalPartitions : env.getParallelism();
        env.setParallelism(parallelism);

        // Metrics: Prometheus endpoint + enable RocksDB metrics
        configureMetrics(env, 9400);

        // Watermarks off for raw stream
        env.getConfig().setAutoWatermarkInterval(0);

        // Checkpointing
        env.enableCheckpointing(checkpointingInterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // If you later confirm barrier backpressure:
        // env.getCheckpointConfig().enableUnalignedCheckpoints(true);

        if (useRocksDBStateBackend) {
            ensureRocksDbStateBackend(env, rocksDBPath);
        }

        // Source
        return KafkaSourceForFlink.rawKafkaStream(
                appContainerDefn,
                env,
                kafka.bootstrapServer(),
                kafka.topic(),
                kafka.groupId(),
                totalPartitions,
                OffsetsInitializer.earliest(),
                Duration.ofSeconds(60),
                WatermarkStrategyProvider.none(),
                kafka.properties());
    }

    /** Public entry that wraps errors with a meaningful message. */
    public static void ensureRocksDbStateBackend(StreamExecutionEnvironment env, String rocksDBPath) {
        try {
            configureRocksDbStateBackend(env, rocksDBPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure RocksDB state backend", e);
        }
    }

    /** Configure RocksDB backend, directories, incremental checkpoints, metrics, and tuning. */
    private static void configureRocksDbStateBackend(StreamExecutionEnvironment env, String rocksDBPath) throws Exception {
        Configuration configuration = new Configuration();

        // --- booleans up front (set breakpoints here if you want) ---
        final boolean rdbMetricsEnabled = true;
        final boolean incrementalA = true;
        final boolean incrementalB = true;

        // Backend selection
        configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");

        // Directories
        DirectorySetup dirs = prepareDirectories(rocksDBPath);
        configuration.set(RocksDBOptions.LOCAL_DIRECTORIES, dirs.localDir().toString());
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, dirs.checkpointsDir().toUri().toString());

        // Ensure RocksDB metrics are ON in the backend config (important to set here)
        configuration.set(OPT_RDB_METRICS, rdbMetricsEnabled);

        // Incremental checkpoints
        configuration.set(OPT_INCREMENTAL_A, incrementalA);
        configuration.set(OPT_INCREMENTAL_B, incrementalB);

        LOG.info("RocksDB backend config — metricsEnabled={}, incrementalA={}, incrementalB={}",
                rdbMetricsEnabled, incrementalA, incrementalB);

        // RocksDB tuning
        applyRocksDbTuning(configuration, /*ssd=*/true);

        // Apply to env
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) cl = KafkaFlinkHelper.class.getClassLoader();
        env.configure(configuration, cl);
    }

    /** Configure metrics (Prometheus reporter + enable RocksDB metrics). */
    private static void configureMetrics(StreamExecutionEnvironment env, int prometheusPort) {
        Configuration cfg = new Configuration();

        // --- booleans up front (set breakpoints here if you want) ---
        final boolean scopeRewrite = true;
        final boolean rdbMetricsEnabled = true;
        final String reporters = "promjm";
        final String promClass = "org.apache.flink.metrics.prometheus.PrometheusReporter";
        final String promPort = String.valueOf(prometheusPort);

        cfg.set(OPT_REPORTERS, reporters);
        cfg.set(OPT_PROM_CLASS, promClass);
        cfg.set(OPT_PROM_PORT, promPort);

        cfg.set(OPT_SCOPE_REWRITE, scopeRewrite);
        cfg.set(OPT_RDB_METRICS, rdbMetricsEnabled);

        LOG.info("Metrics config — reporters={}, promPort={}, scopeRewrite={}, rocksdb.metrics.enabled={}",
                reporters, promPort, scopeRewrite, rdbMetricsEnabled);

        // (Optional) expose REST/Web UI if you want http://host:8081
        // cfg.setInteger(ConfigOptions.key("rest.port").intType().defaultValue(8081), 8081);
        // cfg.setString(ConfigOptions.key("rest.address").stringType().defaultValue("0.0.0.0"), "0.0.0.0");

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) cl = KafkaFlinkHelper.class.getClassLoader();
        env.configure(cfg, cl);
    }

    /** Create/validate RocksDB local + checkpoint dirs. */
    private static DirectorySetup prepareDirectories(String rocksDBPath) throws Exception {
        if (rocksDBPath != null && !rocksDBPath.isBlank()) {
            Path requested = Path.of(rocksDBPath).toAbsolutePath();
            try {
                return initializeDirectories(requested);
            } catch (Exception ex) {
                LOG.warn("Unable to create RocksDB directory at '{}'. Falling back to temp directory.", requested, ex);
            }
        }
        Path fallbackBase = Files.createTempDirectory("flink-rocksdb-");
        LOG.info("Using fallback RocksDB directory at '{}'", fallbackBase);
        return initializeDirectories(fallbackBase);
    }

    private static DirectorySetup initializeDirectories(Path localDir) throws Exception {
        Files.createDirectories(localDir);
        Path checkpointsDir = localDir.resolve("checkpoints");
        Files.createDirectories(checkpointsDir);
        return new DirectorySetup(localDir, checkpointsDir);
    }

    /** Apply safe RocksDB tuning (SSD by default). */
    private static void applyRocksDbTuning(Configuration cfg, boolean ssd) {
        cfg.set(OPT_MEM_MANAGED, true);
        cfg.set(OPT_PRESET, ssd ? "FLASH_SSD_OPTIMIZED" : "SPINNING_DISK_OPTIMIZED");
        cfg.set(OPT_THREADS, 6);

        cfg.set(OPT_WB_SIZE, "128mb");
        cfg.set(OPT_WB_COUNT, 4);
        cfg.set(OPT_WB_NUM_MERGE, 2);

        cfg.set(OPT_TARGET_FILE_SIZE, "128mb");

        cfg.set(OPT_L0_TRIGGER, 6);
        cfg.set(OPT_L0_SLOWDOWN, 12);
        cfg.set(OPT_L0_STOP, 20);
    }

    /** Simple holder for directories. */
    private record DirectorySetup(Path localDir, Path checkpointsDir) {}
}
