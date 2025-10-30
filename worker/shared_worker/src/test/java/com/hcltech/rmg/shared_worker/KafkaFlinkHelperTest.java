package com.hcltech.rmg.shared_worker;

import com.hcltech.rmg.appcontainer.impl.AppContainerFactoryForMapStringObject;
import com.hcltech.rmg.appcontainer.interfaces.AppContainerDefn;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.RawMessage;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class KafkaFlinkHelperTest {

    @TempDir
    Path tempDir;

    @Test
    void createRawMessageStreamFromKafka_configuresRocksDbStateBackend() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        var defn = AppContainerDefn.of(AppContainerFactoryForMapStringObject.class, "test");
        KafkaConfig kafka = new KafkaConfig(
                "dummy:9092",
                "topic",
                "group",
                1,
                "earliest",
                Duration.ofSeconds(60),
                new Properties()
        );

        DataStreamSource<RawMessage> stream = KafkaFlinkHelper.createRawMessageStreamFromKafka(
                defn,
                env,
                kafka,
                5000,
                tempDir.toString()
        );

        ReadableConfig configuration = env.getConfiguration();
        assertEquals("rocksdb", configuration.get(StateBackendOptions.STATE_BACKEND));
        assertEquals(tempDir.toAbsolutePath().toString(), configuration.get(RocksDBOptions.LOCAL_DIRECTORIES));

        String expectedCheckpoint = tempDir.toAbsolutePath().resolve("checkpoints").toUri().toString();
        assertEquals(expectedCheckpoint, configuration.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY));

        assertEquals("kafka-topic", stream.getTransformation().getName());
        assertTrue(tempDir.resolve("checkpoints").toFile().exists(), "checkpoint directory should be created");
    }

    @Test
    void ensureRocksDbStateBackendFallsBackWhenTargetPathIsNotWritable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        var defn = AppContainerDefn.of(AppContainerFactoryForMapStringObject.class, "test");
        KafkaConfig kafka = new KafkaConfig(
                "dummy:9092",
                "topic",
                "group",
                1,
                "earliest",
                Duration.ofSeconds(60),
                new Properties()
        );

        Path blockingPath = tempDir.resolve("cannot-create-dir");
        Files.createFile(blockingPath);

        KafkaFlinkHelper.createRawMessageStreamFromKafka(
                defn,
                env,
                kafka,
                5000,
                blockingPath.toString()
        );

        String configuredLocalDir = env.getConfiguration().get(RocksDBOptions.LOCAL_DIRECTORIES);
        assertNotEquals(blockingPath.toAbsolutePath().toString(), configuredLocalDir);

        Path configuredPath = Path.of(configuredLocalDir);
        assertTrue(Files.exists(configuredPath));

        String checkpointUri = env.getConfiguration().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY);
        Path checkpointPath = Path.of(new java.net.URI(checkpointUri));
        assertTrue(Files.exists(checkpointPath));

        Path systemTemp = Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath();
        assertTrue(configuredPath.startsWith(systemTemp));
    }
}
