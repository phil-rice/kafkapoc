package com.hcltech.rmg.kafka;

import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class KafkaTopics {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopics.class);
    private static final String EVENT_HUB_CONNECTION_EXTRA_KEY = "eventhub.connection.string";

    private KafkaTopics() {
    }

    /**
     * Ensure all target topics in KafkaConfig exist.
     * Creates them with targetParallelism partitions if missing.
     * Does nothing for existing topics.
     */
    public static ErrorsOr<Boolean> ensureTopics(KafkaConfig cfg, List<String> targetTopics, int targetParallelism, short replicationFactor) {
        // --- basic validation first (pure) ---
        List<String> errs = new ArrayList<>();
        if (cfg.bootstrapServer() == null || cfg.bootstrapServer().isBlank())
            errs.add("bootstrapServer must be non-empty");
        if (targetTopics == null || targetTopics.isEmpty())
            errs.add("targetTopics must be non-empty");
        if (targetParallelism < 1)
            errs.add("targetParallelism must be >= 1");
        if (replicationFactor < 1)
            errs.add("replicationFactor must be >= 1");

        if (!errs.isEmpty()) return ErrorsOr.errors(errs);

        // Skip topic management for Azure Event Hub deployments, where the SAS user is often
        // scoped to a single entity path and lacks permission to list or create topics.
        if (cfg.eventHub()) {
            LOG.info("Skipping topic existence check/creation because Event Hub connection settings are present");
            return ErrorsOr.lift(false);
        }

        // --- side effects wrapped in trying() ---
        return ErrorsOr.trying(() -> {
            Properties adminProps = new Properties();
            if (cfg.properties() != null) adminProps.putAll(cfg.properties());
            adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServer());

            try (AdminClient admin = AdminClient.create(adminProps)) {
                Set<String> existing = admin.listTopics().names().get();

                List<NewTopic> toCreate = targetTopics.stream()
                        .filter(t -> !existing.contains(t))
                        .map(t -> new NewTopic(t, targetParallelism, replicationFactor))
                        .collect(Collectors.toList());

                if (toCreate.isEmpty()) return false;

                try {
                    admin.createTopics(toCreate).all().get();
                } catch (ExecutionException ee) {
                    if (!(ee.getCause() instanceof TopicExistsException)) throw ee;
                }
                return true;
            }
        }).addPrefixIfError("ensureTopics");
    }

}
