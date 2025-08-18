package com.example.kafka.consumer;

import com.example.kafka.consumer.cepstate.PartitionDbManager;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerRunner {
    private static final Logger log = LoggerFactory.getLogger(ConsumerRunner.class);

    private final AppConfig cfg;
    private final RecordProcessor processor; // your existing interface
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ConsumerRunner(AppConfig cfg, RecordProcessor processor) {
        this.cfg = cfg;
        this.processor = processor;
    }

    public void run() {
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, cfg.groupId());
        p.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, cfg.clientId());
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, cfg.autoOffsetReset());
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(cfg.maxPollRecords()));
        // smoother, partial rebalances:
        p.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                System.getProperty("partition.assignment.strategy",
                        "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"));

        Path stateBase = Paths.get(cfg.stateDir()).toAbsolutePath();
        PartitionDbManager dbs = new PartitionDbManager(stateBase, cfg.groupId());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown requested.");
                running.set(false);
                consumer.wakeup();
            }, "consumer-shutdown"));

            Map<TopicPartition, OffsetAndMetadata> processedOffsets = new HashMap<>();

            ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log.info("Partitions revoked: {}", partitions);
                    // 1) commit what you’ve processed
                    if (!processedOffsets.isEmpty()) {
                        try {
                            consumer.commitSync(processedOffsets);
                            log.info("Committed offsets on revoke: {}", processedOffsets);
                            processedOffsets.clear();
                        } catch (Exception e) {
                            log.warn("Commit on revoke failed", e);
                        }
                    }
                    // 2) close DBs for revoked partitions
                    dbs.onPartitionsRevoked(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.info("Partitions assigned: {}", partitions);
                    // open DBs for assigned partitions
                    dbs.onPartitionsAssigned(partitions);
                }
            };

            consumer.subscribe(cfg.topics(), rebalanceListener);
            log.info("Subscribed to topics: {}", cfg.topics());

            try {
                while (running.get()) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(cfg.pollMs()));
                    if (records.isEmpty()) continue;

                    processedOffsets.clear();
                    for (ConsumerRecord<String, String> rec : records) {
                        TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
                        try {
                            RocksDB db = dbs.dbFor(tp);

                            // If your processor knows about RocksDB, use it:
                            if (processor instanceof PartitionedRecordProcessor prp) {
                                prp.process(rec, db);
                            } else {
                                // Legacy path: call your existing processor,
                                // (you can also do simple persistence here if you want)
                                processor.process(rec);
                            }

                            processedOffsets.put(tp, new OffsetAndMetadata(rec.offset() + 1));
                        } catch (Exception ex) {
                            log.error("Processing failed for {}-{}@{}; leaving offset uncommitted so it can be retried.",
                                    rec.topic(), rec.partition(), rec.offset(), ex);
                            break; // stop this batch; commit only successful so far
                        }
                    }

                    if (!processedOffsets.isEmpty()) {
                        consumer.commitSync(processedOffsets);
                        log.debug("Committed offsets: {}", processedOffsets);
                    }
                }
            } catch (WakeupException we) {
                log.info("WakeupException: shutting down.");
            } catch (Exception e) {
                log.error("Unexpected error in poll loop", e);
            } finally {
                try {
                    consumer.commitSync(); // best-effort final commit
                } catch (Exception e) {
                    log.warn("Final commit failed", e);
                }
                // make sure all DBs are closed
                try {
                    dbs.close();
                } catch (Exception e) {
                    log.warn("Closing DB manager failed", e);
                }
                log.info("Consumer closed.");
            }
        }
    }
}
