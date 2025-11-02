package com.hcltech.rmg.worker.eventhubtest;

import com.hcltech.rmg.common.IEnvGetter;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CopyJobConfig {

    public static KafkaConfig source(IEnvGetter env, KafkaDefaults defaults) {
        return build(env, defaults, defaults.inputTopic(), false);
    }

    public static KafkaConfig sink(IEnvGetter env, KafkaDefaults defaults) {
        return build(env, defaults, defaults.outputTopic(), true);
    }

    /** Normal = Event Hubs: derive bootstrap from EVENTHUB_CONNECTION_STRING if not explicitly set. */
    private static KafkaConfig build(IEnvGetter env, KafkaDefaults defaults, String topic, boolean producer) {
        final boolean isEventHub = IEnvGetter.getBooleanOr(env, "KAFKA_IS_EVENT_HUB", true);

        // Bootstrap: env override > derive from EH conn string > code default
        final String bootstrap = deriveBootstrap(env, defaults.bootstrap(), isEventHub);

        // Group strategy:
        // 1) If KAFKA_GROUP_ID is set, always use it.
        // 2) Else, if stableGroup=true, use $Default (or KAFKA_GROUP_ID_PREFIX + "$Default" if you want).
        // 3) Else (ephemeral), generate a fresh group each run.
        String groupId = IEnvGetter.getStringOr(env, "KAFKA_GROUP_ID", null);
        if (groupId == null || groupId.isBlank()) {
            if (defaults.stableGroup()) {
                groupId = "$Default";
            } else {
                String prefix = IEnvGetter.getStringOr(env, "KAFKA_GROUP_ID_PREFIX", "test-");
                groupId = prefix + System.currentTimeMillis() + "-" + UUID.randomUUID();
            }
        }

        // Starting offsets:
        // - If explicitly set via env, honor it.
        // - Else default to "latest" for stable groups, "earliest" for ephemeral runs (nice for tests).
        String startAt = IEnvGetter.getStringOr(env, "KAFKA_STARTING_OFFSETS",
                defaults.stableGroup() ? "latest" : "earliest");
        startAt = (startAt == null) ? "latest" : startAt.trim().toLowerCase();
        if (!startAt.equals("earliest") && !startAt.equals("latest")) startAt = "latest";

        final int parallelism   = IEnvGetter.getIntOr(env, "KAFKA_SOURCE_PARALLELISM", 0);
        final long discoveryMs  = IEnvGetter.getLongOr(env, "KAFKA_PARTITION_DISCOVERY_MS", 0L);
        final Duration discovery= discoveryMs > 0 ? Duration.ofMillis(discoveryMs) : null;

        // Extras AS-WAS: only add discovery on the consumer side
        final Properties extra = new Properties();
        if (!producer && discovery != null) {
            extra.put("partition.discovery.interval.ms", String.valueOf(discovery.toMillis()));
        }

        return new KafkaConfig(
                bootstrap,
                isEventHub,
                topic,
                groupId,
                parallelism,
                startAt,
                discovery,
                extra
        );
    }

    private static String deriveBootstrap(IEnvGetter env, String bootstrapDefault, boolean isEventHub) {
        String explicit = IEnvGetter.getStringOr(env, "KAFKA_BOOTSTRAP", "");
        if (!explicit.isBlank()) return explicit;

        if (isEventHub) {
            String cs = IEnvGetter.getString(env, "EVENTHUB_CONNECTION_STRING"); // required for EH
            String host = parseEventHubHost(cs);
            return host + ":9093";
        }
        return bootstrapDefault;
    }

    private static String parseEventHubHost(String connectionString) {
        // Example: Endpoint=sb://rmgeventhub.servicebus.windows.net/;...
        Pattern p = Pattern.compile("Endpoint=sb://([^/;]+)", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(connectionString);
        if (m.find()) return m.group(1);
        throw new IllegalStateException("Could not parse Endpoint host from EVENTHUB_CONNECTION_STRING");
    }
}
