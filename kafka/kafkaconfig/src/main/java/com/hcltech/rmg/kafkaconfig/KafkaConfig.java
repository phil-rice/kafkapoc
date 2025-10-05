// com.hcltech.rmg.appcontainer.interfaces.kafka
package com.hcltech.rmg.kafkaconfig;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.time.Duration;
import java.util.Properties;

public record KafkaConfig(
        String bootstrapServers,
        String topic,            // usually 1, but allow many
        String groupId,
        int sourceParallelism,   // desired source operator parallelism
        String startingOffsets,  // "earliest" or "latest"
        Duration partitionDiscovery, // null => disabled
        Properties extra          // any additional Kafka props (client tuning, auth, etc.)
) {

    public static KafkaConfig fromSystemProps() {
        return fromProperties(System.getProperties());
    }

    public static KafkaConfig fromProperties(Properties p) {
        String bootstrap = p.getProperty("kafka.bootstrap", "localhost:9092");
        String topic = p.getProperty("kafka.topic", "test-topic");
        String groupId = p.getProperty("kafka.group.id", "g-" + System.currentTimeMillis());
        int parallelism = getInt(p, "kafka.source.parallelism",
                Integer.getInteger("kafka.partitions", 12));

        String offsets = p.getProperty("kafka.starting.offsets", "earliest").trim().toLowerCase();
        if (!offsets.equals("earliest") && !offsets.equals("latest")) {
            offsets = "earliest"; // fallback
        }

        long discSec = getLong(p, "kafka.partition.discovery.seconds", 60L);
        Duration discovery = (discSec > 0) ? Duration.ofSeconds(discSec) : null;

        Properties extra = new Properties();
        if (discovery != null) {
            extra.put("partition.discovery.interval.ms", String.valueOf(discovery.toMillis()));
        }

        return new KafkaConfig(
                bootstrap,
                topic,
                groupId,
                parallelism,
                offsets,
                discovery,
                extra
        );
    }

    /** Convert the string representation back to a Flink OffsetsInitializer. */
    public OffsetsInitializer toOffsetsInitializer() {
        return switch (startingOffsets) {
            case "latest" -> OffsetsInitializer.latest();
            default -> OffsetsInitializer.earliest();
        };
    }

    private static int getInt(Properties p, String k, Integer def) {
        String v = p.getProperty(k);
        return (v == null) ? (def == null ? 0 : def) : Integer.parseInt(v);
    }

    private static long getLong(Properties p, String k, long def) {
        String v = p.getProperty(k);
        return (v == null) ? def : Long.parseLong(v);
    }
}
