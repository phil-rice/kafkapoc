package com.example.kafka.tools;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ProducerApp {
    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);

    public static void main(String[] args) throws Exception {
        Properties defaults = new Properties();
        try (InputStream is = ProducerApp.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) defaults.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }

        String bootstrap = get("kafka.bootstrap.servers", defaults, "localhost:9092");
        String topic = get("topic", defaults, "test-topic");
        int count = Integer.parseInt(get("count", defaults, "5"));

        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        p.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (int i = 1; i <= count; i++) {
                String key = "key-" + i;
                String value = "{"
                        + "\"id\":\"" + UUID.randomUUID() + "\","
                        + "\"payload\":\"hello-" + i + "\","
                        + "\"timestamp\":\"" + Instant.now() + "\""
                        + "}";

                RecordMetadata meta = producer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
                log.info("Produced to {}-{}@{} key={} value={}", meta.topic(), meta.partition(), meta.offset(), key, value);
            }
        }
        log.info("Done. Sent {} messages to topic {}", count, topic);
    }

    private static String get(String key, Properties defaults, String def) {
        String env = System.getenv(toEnvKey(key));
        if (env != null && !env.isBlank()) return env;
        String sys = System.getProperty(key);
        if (sys != null && !sys.isBlank()) return sys;
        return defaults.getProperty(key, def);
    }

    private static String toEnvKey(String key) {
        return key.toUpperCase().replace('.', '_').replace('-', '_');
    }
}
