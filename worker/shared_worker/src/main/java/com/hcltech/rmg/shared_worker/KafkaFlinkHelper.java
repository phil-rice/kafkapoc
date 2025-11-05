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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaFlinkHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaFlinkHelper.class);

    // ---- Flink state/metrics config keys ----
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

    // ---- Lightweight GC debug logger (runs once per JVM) ----
    private static final AtomicBoolean GC_LOGGER_STARTED = new AtomicBoolean(false);
    private static volatile ScheduledExecutorService GC_EXEC = null;

    /**
     * Start a periodic logger that prints JVM GC totals, per-interval deltas, and overhead% (GC time / uptime).
     * No external metrics systems required. Safe to call multiple times; only starts once.
     */
    public static void startJvmGcDebugLogger(Duration interval) {
        if (interval == null || interval.isNegative() || interval.isZero()) {
            LOG.warn("GC logger not started: invalid interval {}", interval);
            return;
        }
        if (!GC_LOGGER_STARTED.compareAndSet(false, true)) {
            // already running
            return;
        }

        final List<GarbageCollectorMXBean> beans = ManagementFactory.getGarbageCollectorMXBeans();
        final RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();

        GC_EXEC = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gc-debug-logger");
            t.setDaemon(true);
            return t;
        });

        // state for deltas
        final long[] prev = new long[] { 0L /*timeMs*/, 0L /*count*/ };
        GC_EXEC.scheduleAtFixedRate(() -> {
            try {
                long totalTimeMs = 0;
                long totalCount = 0;
                for (GarbageCollectorMXBean b : beans) {
                    long t = b.getCollectionTime();   // ms (may be -1 if not supported)
                    long c = b.getCollectionCount();  // may be -1 if not supported
                    if (t > 0) totalTimeMs += t;
                    if (c > 0) totalCount += c;
                }
                long deltaMs = totalTimeMs - prev[0];
                long deltaCount = totalCount - prev[1];
                prev[0] = totalTimeMs;
                prev[1] = totalCount;

                long upMs = rt.getUptime();
                double overheadPct = (upMs > 0) ? (totalTimeMs * 100.0 / upMs) : 0.0;

                LOG.info("GC: total={} ms ( {} ms), count={} ( {}), uptime={} ms, overhead={}%"
                                + (deltaMs > 0 ? "" : "  (idle)"),
                        totalTimeMs, Math.max(deltaMs, 0),
                        totalCount, Math.max(deltaCount, 0),
                        upMs, String.format("%.2f", overheadPct));
            } catch (Throwable t) {
                LOG.warn("GC logger tick failed", t);
            }
        }, 0L, interval.toMillis(), TimeUnit.MILLISECONDS);

        // best-effort shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ScheduledExecutorService exec = GC_EXEC;
            if (exec != null) {
                exec.shutdownNow();
            }
        }, "gc-debug-logger-shutdown"));

        LOG.info("Started JVM GC debug logger at interval {}", interval);
    }

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

        // (A) Start lightweight GC debug logging every 5s (safe no-op if already started).
        startJvmGcDebugLogger(Duration.ofSeconds(5));

        // 1) Parallelism: match source to partitions if provided.
        final int parallelism = kafka.sourceParallelism() > 0
                ? kafka.sourceParallelism()
                : env.getParallelism();
        env.setParallelism(parallelism);

        // 2) Metrics: Prometheus + (optional) RocksDB internal metrics.
        configureMetrics(env, 9400);

        // 3) Watermarks off for raw stream (pure throughput stage).
        env.getConfig().setAutoWatermarkInterval(0);

        // 4) Checkpointing: exactly-once with minimal safeties.
        env.enableCheckpointing(checkpointingInterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // If you later confirm barrier backpressure:
        // env.getCheckpointConfig().enableUnalignedCheckpoints(true);

        // 5) State backend: RocksDB with large managed memory (only if used).
        if (useRocksDBStateBackend) {
            ensureRocksDbStateBackend(env, rocksDBPath);
        }

        // 6) Kafka consumer props (Event Hubs via Kafka): Profile B  larger, coalesced fetches.
        Properties consumerProps = buildTunedKafkaConsumerProps(kafka);

        // 7) Source: build and set parallelism explicitly.
        DataStreamSource<RawMessage> raw = KafkaSourceForFlink.rawKafkaStream(
                appContainerDefn,
                env,
                kafka.bootstrapServer(),
                kafka.topic(),
                kafka.groupId(),
                parallelism,
                OffsetsInitializer.earliest(),
                Duration.ofSeconds(60),
                WatermarkStrategyProvider.none(),
                consumerProps);

        raw.setParallelism(parallelism);
        return raw;
    }

    /** Public entry that wraps errors with a meaningful message. */
    public static void ensureRocksDbStateBackend(StreamExecutionEnvironment env, String rocksDBPath) {
        try {
            configureRocksDbStateBackend(env, rocksDBPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure RocksDB state backend", e);
        }
    }

    // ===================== Kafka consumer tuning =====================

    /**
     * Build consumer Properties starting from KafkaConfig and layering grouped tweaks.
     */
    private static Properties buildTunedKafkaConsumerProps(KafkaConfig kafka) {
        Properties p = new Properties();
        // Start with user-provided props (auth, EH endpoint, etc.)
        kafka.properties().forEach((k, v) -> p.put(String.valueOf(k), String.valueOf(v)));

        applyBatchingProps(p);
        applyNetworkProps(p);
        applyTimeoutProps(p);
        logEffectiveKafkaProps(p);
        return p;
    }

    /**
     * Profile B: broker coalescing + large per-partition chunk + high total fetch.
     * - fetch.min.bytes = 1MB (~500�2KB messages)
     * - fetch.max.wait.ms = 100ms (allow the broker to fill)
     * - max.partition.fetch.bytes = 8MB (fat chunk per partition)
     * - fetch.max.bytes = 128MB (room for many partitions at once)
     */
    private static void applyBatchingProps(Properties p) {
        p.put("fetch.min.bytes", "1048576");             // 1 MB
        p.put("fetch.max.wait.ms", "100");               // allow coalescing up to 100 ms
        p.put("max.partition.fetch.bytes", "8388608");   // 8 MB per partition
        p.put("fetch.max.bytes", "134217728");           // 128 MB total
        p.put("max.poll.records", "20000");              // big app-side batch (harmless if connector ignores)
    }

    /** Larger socket buffers help sustain high throughput. */
    private static void applyNetworkProps(Properties p) {
        p.put("receive.buffer.bytes", "8388608");          // 8 MB
        p.put("socket.receive.buffer.bytes", "8388608");   // some clients honor this
        p.put("connections.max.idle.ms", "300000");        // we don't want to drop connections
    }

    /** Robust timeouts + gentle backoffs (avoid thrashing on throttles). */
    private static void applyTimeoutProps(Properties p) {
        p.put("session.timeout.ms", "30000");
        p.put("heartbeat.interval.ms", "10000");    // keep ~1:3 with session
        p.put("request.timeout.ms", "180000");

        p.put("retry.backoff.ms", "100");
        p.put("reconnect.backoff.ms", "200");
        p.put("reconnect.backoff.max.ms", "10000");
    }

    private static void logEffectiveKafkaProps(Properties p) {
        try {
            LOG.info("Kafka consumer tuning � fetch.min.bytes={}, fetch.max.wait.ms={}, max.partition.fetch.bytes={}, fetch.max.bytes={}, " +
                            "receive.buffer.bytes={}, socket.receive.buffer.bytes={}, max.poll.records={}, session.timeout.ms={}, heartbeat.interval.ms={}, request.timeout.ms={}",
                    p.getProperty("fetch.min.bytes"),
                    p.getProperty("fetch.max.wait.ms"),
                    p.getProperty("max.partition.fetch.bytes"),
                    p.getProperty("fetch.max.bytes"),
                    p.getProperty("receive.buffer.bytes"),
                    p.getProperty("socket.receive.buffer.bytes"),
                    p.getProperty("max.poll.records"),
                    p.getProperty("session.timeout.ms"),
                    p.getProperty("heartbeat.interval.ms"),
                    p.getProperty("request.timeout.ms"));
        } catch (Throwable ignore) {
            // best-effort logging only
        }
    }

    // ===================== Flink metrics/backends =====================

    /**
     * RocksDB: managed memory mode with a *fixed per-slot* budget and
     * write buffer/file/threads tuned for throughput on large RAM.
     */
    private static void configureRocksDbStateBackend(StreamExecutionEnvironment env, String rocksDBPath) throws Exception {
        Configuration configuration = new Configuration();

        // Keep managed mode & incremental checkpoints
        configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        configuration.set(OPT_RDB_METRICS, true);
        configuration.set(
                ConfigOptions.key("execution.checkpointing.incremental").booleanType().noDefaultValue(),
                true);

        // Directories
        DirectorySetup dirs = prepareDirectories(rocksDBPath);
        configuration.set(RocksDBOptions.LOCAL_DIRECTORIES, dirs.localDir().toString());
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, dirs.checkpointsDir().toUri().toString());

        //  Give RocksDB a real budget per slot (you have 256 GB � 8 GB/slot is safe)
        configuration.setString("state.backend.rocksdb.memory.fixed-per-slot", "8g");
        // Split: 50% write buffers, 10% high-priority (index/filter), remainder block cache
        configuration.setString("state.backend.rocksdb.memory.write-buffer-ratio", "0.5");
        configuration.setString("state.backend.rocksdb.memory.high-prio-pool-ratio", "0.1");

        LOG.info("RocksDB managed memory fixed-per-slot=8g, write-buffer-ratio=0.5, high-prio-pool-ratio=0.1");

        // Apply RocksDB options sized for the above memory
        applyRocksDbTuning(configuration, /*ssd*/ true);

        // Apply to env
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) cl = KafkaFlinkHelper.class.getClassLoader();
        env.configure(configuration, cl);
    }

    /** Prometheus reporter + enable RocksDB metrics. */
    private static void configureMetrics(StreamExecutionEnvironment env, int prometheusPort) {
        Configuration cfg = new Configuration();

        cfg.set(OPT_REPORTERS, "promjm");
        cfg.set(OPT_PROM_CLASS, "org.apache.flink.metrics.prometheus.PrometheusReporter");
        cfg.set(OPT_PROM_PORT, String.valueOf(prometheusPort));
        cfg.set(OPT_SCOPE_REWRITE, true);
        cfg.set(OPT_RDB_METRICS, true);

        LOG.info("Metrics � Prometheus at port {}, RocksDB metrics enabled", prometheusPort);

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

    /**
     * RocksDB throughput tuning (SSD):
     * - 256 MB memtables � 4 � ~1 GB per CF for write buffers
     * - 256 MB target file size (fewer files, less compaction churn)
     * - higher L0 thresholds
     * - more background threads
     */
    private static void applyRocksDbTuning(Configuration cfg, boolean ssd) {
        cfg.set(OPT_MEM_MANAGED, true);
        cfg.set(OPT_PRESET, ssd ? "FLASH_SSD_OPTIMIZED" : "SPINNING_DISK_OPTIMIZED");

        cfg.set(OPT_WB_SIZE, "256mb");
        cfg.set(OPT_WB_COUNT, 4);
        cfg.set(OPT_TARGET_FILE_SIZE, "256mb");

        cfg.set(OPT_L0_TRIGGER, 8);
        cfg.set(OPT_L0_SLOWDOWN, 16);
        cfg.set(OPT_L0_STOP, 24);

        cfg.set(OPT_THREADS, 16);  // you have cores; keep compaction/flush ahead of writers
    }

    /** Simple holder for directories. */
    private record DirectorySetup(Path localDir, Path checkpointsDir) {}
}
