// com.hcltech.rmg.appcontainer.interfaces.kafka
package com.hcltech.rmg.kafkaconfig;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.time.Duration;
import java.util.Properties;

public record KafkaConfig(
        String bootstrapServer,
        boolean eventHub,
        String topic,            // usually 1, but allow many
        String groupId,
        int sourceParallelism,   // source parallelism (usually = #partitions)
        String startingOffsets,  // "earliest" or "latest"
        Duration partitionDiscovery, // null => disabled
        Properties properties          // The kafka properties
) {


    public static KafkaConfig fromSystemProps(boolean eventHub) {
        return fromProperties(System.getProperties(), null, eventHub);
    }

    public static KafkaConfig fromSystemProps(String topicOrNull, boolean eventHub) {
        return fromProperties(System.getProperties(), topicOrNull, eventHub);
    }

    public static KafkaConfig fromProperties(Properties p, String topicOrNull, boolean eventHub) {
        String bootstrap = p.getProperty("kafka.bootstrap", "localhost:9092");
        String topic = topicOrNull == null ? p.getProperty("kafka.topic", "test-topic") : topicOrNull;
        String groupId = p.getProperty("kafka.group.id", "g-" + System.currentTimeMillis());
        int sourceParallelism = getInt(p, "kafka.source.parallelism",
                Integer.getInteger("kafka.partitions", 14));
        int targetParallelism = getInt(p, "kafka.target.partitions", sourceParallelism);

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
                eventHub,
                topic,
                groupId,
                sourceParallelism,
                offsets,
                discovery,
                extra
        );
    }

    /**
     * Convert the string representation back to a Flink OffsetsInitializer.
     */
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
