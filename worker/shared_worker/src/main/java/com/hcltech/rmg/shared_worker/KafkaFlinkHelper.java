package com.hcltech.rmg.shared_worker;

import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.kafka.KafkaSourceForFlink;
import com.hcltech.rmg.kafka.WatermarkStrategyProvider;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;

public class KafkaFlinkHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaFlinkHelper.class);
    static public <ESC, CepState, Msg, Schema,RT, FR,MetricsParam> DataStreamSource<RawMessage> createRawMessageStreamFromKafka(AppContainerDefn<ESC, CepState, Msg, Schema,RT, FR,MetricsParam> appContainerDefn, StreamExecutionEnvironment env, KafkaConfig kafka, int checkpointingInterval, String rocksDBPath) {
        final int totalPartitions = kafka.sourceParallelism();
        env.setParallelism(totalPartitions > 0 ? totalPartitions : env.getParallelism());
        env.getConfig().setAutoWatermarkInterval(0);
        env.enableCheckpointing(checkpointingInterval);
        ensureRocksDbStateBackend(env, rocksDBPath);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        var raw = KafkaSourceForFlink.rawKafkaStream(appContainerDefn, env, kafka.bootstrapServer(), kafka.topic(), kafka.groupId(), totalPartitions, OffsetsInitializer.earliest(), Duration.ofSeconds(60), WatermarkStrategyProvider.none(), kafka.extra());
        return raw;
    }

    public static void ensureRocksDbStateBackend(StreamExecutionEnvironment env, String rocksDBPath) {
        try {
            configureRocksDbStateBackend(env, rocksDBPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure RocksDB state backend", e);
        }
    }

    private static void configureRocksDbStateBackend(StreamExecutionEnvironment env, String rocksDBPath) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");

        DirectorySetup directories = prepareDirectories(rocksDBPath);
        configuration.set(RocksDBOptions.LOCAL_DIRECTORIES, directories.localDir().toString());
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, directories.checkpointsDir().toUri().toString());

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = KafkaFlinkHelper.class.getClassLoader();
        }
        env.configure(configuration, cl);
    }

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

    private record DirectorySetup(Path localDir, Path checkpointsDir) {
    }
}
