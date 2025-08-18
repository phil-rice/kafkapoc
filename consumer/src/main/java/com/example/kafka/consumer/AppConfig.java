package com.example.kafka.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public record AppConfig(
        String bootstrapServers,
        String groupId,
        String clientId,
        List<String> topics,
        String autoOffsetReset,
        int maxPollRecords,
        int pollMs,
        String stateDir
) {
    public static AppConfig load() {
        Properties props = new Properties();
        try (InputStream is = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) props.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
        Function<String, String> get = key -> {
            String envKey = toEnvKey(key);
            String fromEnv = System.getenv(envKey);
            if (fromEnv != null && !fromEnv.isBlank()) return fromEnv;
            String fromSys = System.getProperty(key);
            if (fromSys != null && !fromSys.isBlank()) return fromSys;
            return props.getProperty(key);
        };
        String bootstrap = required(get.apply("kafka.bootstrap.servers"), "kafka.bootstrap.servers");
        String groupId = orDefault(get.apply("kafka.group.id"), "demo-consumer");
        String clientId = orDefault(get.apply("kafka.client.id"), "demo-consumer-1");
        String topicCsv = required(get.apply("kafka.topic"), "kafka.topic");
        List<String> topics = Arrays.stream(topicCsv.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
        String autoOffsetReset = orDefault(get.apply("kafka.auto.offset.reset"), "earliest");
        int maxPollRecords = Integer.parseInt(orDefault(get.apply("kafka.max.poll.records"), "500"));
        int pollMs = Integer.parseInt(orDefault(get.apply("kafka.poll.ms"), "500"));
        String stateDir = orDefault(get.apply("app.state.dir"), "state");
        return new AppConfig(bootstrap, groupId, clientId, topics,
                autoOffsetReset, maxPollRecords, pollMs, stateDir);   }

    private static String required(String value, String key) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required config: " + key +
                    " (set in application.properties, -D" + key + ", or env " + toEnvKey(key) + ")");
        }
        return value;
    }

    private static String orDefault(String value, String def) {
        return (value == null || value.isBlank()) ? def : value;
    }

    private static String toEnvKey(String key) {
        return key.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_');
    }
}
