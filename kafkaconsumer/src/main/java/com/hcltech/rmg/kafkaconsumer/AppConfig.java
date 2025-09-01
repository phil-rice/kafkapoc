package com.hcltech.rmg.kafkaconsumer;

import com.hcltech.rmg.consumer.abstraction.SeekStrategy;
import com.hcltech.rmg.consumer.abstraction.WorkerConfig;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * AppConfig:
 *  - Implements WorkerConfig (topic, pollMs, commitTickMs, buffer, seekStrategy, etc.)
 *  - Exposes Kafka-specific Properties (bootstrap.servers, group.id, client.id)
 *  - Loads from application.properties on the classpath.
 */
public final class AppConfig implements WorkerConfig<TopicPartition, Long> {

    private final Properties props;

    private AppConfig(Properties props) {
        this.props = props;
    }

    /** Load application.properties from classpath. */
    public static AppConfig load() {
        Properties props = new Properties();
        try (InputStream is = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) props.load(is);
            else throw new RuntimeException("application.properties not found on classpath");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
        return new AppConfig(props);
    }

    // ---------------- WorkerConfig (broker-agnostic) ----------------

    @Override
    public String topic() {
        return get("kafka.topic", "test-topic");
    }

    @Override
    public int pollMs() {
        return Integer.parseInt(get("worker.poll.ms", "250"));
    }

    @Override
    public long commitTickMs() {
        return Long.parseLong(get("worker.commit.tick.ms", "2000"));
    }

    @Override
    public int runnerBufferCapacity() {
        return Integer.parseInt(get("worker.buffer.capacity", "500"));
    }

    public int simulatedProcessingDelayMs() {
        return Integer.parseInt(get("simulated.processor.delay.ms", "10"));
    }

    public int metricsPrintMs() {        return Integer.parseInt(get("metrics.print.ms", "2000"));    }

    public int metricsFullEvery() {        return Integer.parseInt(get("metrics.full.every", "10"));    }

    @Override
    public ThreadFactory laneFactory() {
        return Executors.defaultThreadFactory();
    }

    @Override
    public String clientId() {
        return get("kafka.client.id", "worker-1");
    }

    @Override
    public SeekStrategy<TopicPartition, Long> seekStrategy() {
        // worker.seek=beginning|committed (default: committed)
        String mode = get("worker.seek", "committed").toLowerCase();
        return switch (mode) {
            case "beginning" -> SeekStrategy.fromBeginningOnce();
            case "committed" -> SeekStrategy.continueCommitted();
            default -> throw new IllegalArgumentException("Unknown worker.seek: " + mode);
        };
    }

    // ---------------- Kafka-specific ----------------

    /** Group id for Kafka consumer. */
    public String groupId() {
        return get("kafka.group.id", "demo-group");
    }

    /** Bootstrap servers for Kafka cluster. */
    public String bootstrapServers() {
        return get("kafka.bootstrap.servers", "localhost:9092");
    }

    /** Expose a Properties object ready for KafkaConsumer. */
    public Properties kafkaProperties() {
        Properties out = new Properties();
        out.putAll(props);
        out.put("bootstrap.servers", bootstrapServers());
        out.put("group.id", groupId());
        out.put("client.id", clientId());
        out.putIfAbsent("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        out.putIfAbsent("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        out.putIfAbsent("enable.auto.commit", "false");
        return out;
    }

    // ---------------- Helpers ----------------

    private String get(String key, String def) {
        return Objects.toString(props.getProperty(key), def);
    }
}
