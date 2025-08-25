package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class AppConfig {
    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    private final Properties app;

    public AppConfig(Properties app) { this.app = app; }

    public static AppConfig load() { return new AppConfig(loadApplicationProperties()); }

    public static Properties loadApplicationProperties() {
        Properties defaults = new Properties();
        try (InputStream is = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) defaults.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
        return defaults;
    }

    public KafkaConsumer<String, String> newKafkaConsumer() { return newKafkaConsumer(null); }

    public KafkaConsumer<String, String> newKafkaConsumer(String clientIdSuffix) {
        Properties p = new Properties();

        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, get("kafka.bootstrap.servers", "localhost:9092"));
        String baseClientId = get("kafka.client.id", "tools-consumer");
        p.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,
                clientIdSuffix == null ? baseClientId : baseClientId + "-" + clientIdSuffix);
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, get("kafka.group.id", "tools-consumer-group"));

        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, get("kafka.auto.offset.reset", "earliest"));
        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, get("kafka.max.poll.records", "1000"));

        p.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                get("kafka.partition.assignment.strategy",
                        "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"));

        setIfPresent(p, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, getOptional("kafka.max.poll.interval.ms"));
        setIfPresent(p, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,   getOptional("kafka.session.timeout.ms"));
        setIfPresent(p, ConsumerConfig.FETCH_MIN_BYTES_CONFIG,      getOptional("kafka.fetch.min.bytes"));
        setIfPresent(p, ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,    getOptional("kafka.fetch.max.wait.ms"));

        return new KafkaConsumer<>(p);
    }

    /** Topics via CSV, expand prefix+count, or single topic fallback. */
    public List<String> topics() {
        String csv = getOptional("kafka.topics");
        if (csv != null && !csv.isBlank()) {
            return Arrays.stream(csv.split(","))
                    .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
        }
        String prefix = getOptional("kafka.topics.expand.prefix");
        String countStr = getOptional("kafka.topics.expand.count");
        if (prefix != null && countStr != null) {
            int n = Integer.parseInt(countStr);
            List<String> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) out.add(prefix + i);
            return out;
        }
        return List.of(get("kafka.topic", "test-topic"));
    }

    public int pollMs() { return Integer.parseInt(get("consumer.poll.ms", "250")); }
    public String groupId() { return get("kafka.group.id", "tools-consumer-group"); }
    public Path stateDir() { return Paths.get(get("state.dir", "./state").trim()).toAbsolutePath(); }
    public boolean fromBeginning() { return Boolean.parseBoolean(get("consumer.from.beginning", "false")); }
    public long metricsTickMs() { return Long.parseLong(get("metrics.tick.ms", "2000")); }
    public long processorDelayMs() { return Long.parseLong(get("processor.delay.ms", "0")); }

    // --- helpers ---
    private String get(String key, String def) {
        String env = trimToNull(System.getenv(toEnvKey(key)));
        if (env != null) return env;
        String sys = trimToNull(System.getProperty(key));
        if (sys != null) return sys;
        String prop = trimToNull(app.getProperty(key));
        return (prop != null) ? prop : def;
    }

    private String getOptional(String key) {
        String env = trimToNull(System.getenv(toEnvKey(key)));
        if (env != null) return env;
        String sys = trimToNull(System.getProperty(key));
        if (sys != null) return sys;
        return trimToNull(app.getProperty(key)); // may be null
    }

    private static void setIfPresent(Properties p, String name, String val) {
        if (val != null && !val.isBlank()) p.setProperty(name, val);
    }

    private static String toEnvKey(String key) {
        return key.toUpperCase().replace('.', '_').replace('-', '_');
    }

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    /** Debug log to confirm key config at startup. */
    public void logImportantConfig() {
        log.info("CFG bootstrap={} groupId={} clientId={} topic(s)={}",
                get("kafka.bootstrap.servers","localhost:9092"),
                groupId(),
                get("kafka.client.id","tools-consumer"),
                topics());
        log.info("CFG fromBeginning={} (raw='{}') auto.offset.reset={} max.poll.records={} max.poll.interval.ms={} poll.ms={}",
                fromBeginning(),
                app.getProperty("consumer.from.beginning"),
                get("kafka.auto.offset.reset","earliest"),
                get("kafka.max.poll.records","1000"),
                get("kafka.max.poll.interval.ms","<unset>"),
                pollMs());
    }
}
