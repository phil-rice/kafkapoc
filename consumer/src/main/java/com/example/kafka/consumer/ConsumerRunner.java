package com.example.kafka.consumer;

import com.example.kafka.consumer.cepstate.PartitionDbManager;
import com.example.kafka.consumer.processors.PartitionedRecordProcessor;
import com.example.kafka.consumer.processors.RecordProcessor;
import com.example.metrics.MetricsRegistry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One worker = one KafkaConsumer + poll loop.
 * Subscribe-mode to exercise rebalancing; metrics recorded in a shared MetricsRegistry.
 */
public class ConsumerRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerRunner.class);

    private final AppConfig cfg;
    private final RecordProcessor processor;
    private final List<String> topics;     // workers subscribe to same topics list
    private final String clientIdSuffix;   // unique per worker for observability
    private final MetricsRegistry metrics; // shared, global counters

    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile KafkaConsumer<String, String> consumerRef;

    // Track partitions we've already reset for "fromBeginning" (once per worker lifetime)
    private final Set<TopicPartition> initialized = Collections.synchronizedSet(new HashSet<>());

    public ConsumerRunner(AppConfig cfg,
                          RecordProcessor processor,
                          List<String> topics,
                          String clientIdSuffix,
                          MetricsRegistry metrics) {
        this.cfg = cfg;
        this.processor = processor;
        this.topics = (topics == null || topics.isEmpty()) ? cfg.topics() : topics;
        this.clientIdSuffix = clientIdSuffix;
        this.metrics = metrics;
    }

    /** Orchestrator calls this to stop gracefully. */
    public void requestShutdown() {
        running.set(false);
        KafkaConsumer<String, String> c = consumerRef;
        if (c != null) c.wakeup();
    }

    @Override
    public void run() {
        Path stateBase = cfg.stateDir();
        PartitionDbManager dbs = new PartitionDbManager(stateBase, cfg.groupId());

        try (KafkaConsumer<String, String> consumer = cfg.newKafkaConsumer(clientIdSuffix)) {
            this.consumerRef = consumer;

            final Map<TopicPartition, OffsetAndMetadata> processedOffsets = new HashMap<>();

            ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log.info("Revoked: {} (worker={})", partitions, clientIdSuffix);
                    if (!processedOffsets.isEmpty()) {
                        try {
                            consumer.commitSync(processedOffsets);
                            log.debug("Committed on revoke: {}", processedOffsets);
                        } catch (Exception e) {
                            log.warn("Commit on revoke failed", e);
                        } finally {
                            processedOffsets.clear();
                        }
                    }
                    dbs.onPartitionsRevoked(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.info("Assigned: {} (worker={})", partitions, clientIdSuffix);
                    dbs.onPartitionsAssigned(partitions);
                    if (partitions.isEmpty()) return;

                    try {
                        // Snapshot BEFORE any seek
                        Map<TopicPartition, Long> beg = consumer.beginningOffsets(partitions);
                        Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
                        for (TopicPartition tp : partitions) {
                            OffsetAndMetadata com = consumer.committed(tp);
                            long posBefore;
                            try { posBefore = consumer.position(tp); } catch (Exception e) { posBefore = -1L; }
                            log.info("Assign snapshot {}: begin={} end={} committed={} position(before)={}",
                                    tp, beg.get(tp), end.get(tp),
                                    (com == null ? null : com.offset()), posBefore);
                        }

                        if (cfg.fromBeginning()) {
                            List<TopicPartition> toReset = new ArrayList<>();
                            for (TopicPartition tp : partitions) {
                                if (!initialized.contains(tp)) toReset.add(tp);
                            }
                            if (!toReset.isEmpty()) {
                                consumer.seekToBeginning(toReset);
                                initialized.addAll(toReset);
                                for (TopicPartition tp : toReset) {
                                    long posAfter = consumer.position(tp);
//                                    log.info("After seekToBeginning {}: position(after)={}", tp, posAfter);
                                }
                            } else {
                                log.info("fromBeginning=true but all assigned partitions already initialized; skipping seek");
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Assignment introspection failed", e);
                    }
                }
            };

            consumer.subscribe(this.topics, rebalanceListener);
            log.info("Subscribed to {} (worker={})", this.topics, clientIdSuffix);

            // ---- Infinite poll loop (graceful stop via requestShutdown) ----
            for (;;) {
                if (!running.get()) break;

                try {
                    ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(cfg.pollMs()));
                    if (batch.isEmpty()) continue;

//                    log.info("Polled {} records (worker={})", batch.count(), clientIdSuffix);
                    processedOffsets.clear();

                    for (ConsumerRecord<String, String> rec : batch) {
                        TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
                        try {
                            RocksDB db = dbs.dbFor(tp);

                            if (processor instanceof PartitionedRecordProcessor prp) {
                                prp.process(rec, db);
                            } else {
                                processor.process(rec);
                            }

                            // Global metrics: count one successfully processed record
                            metrics.recordProcessed(tp);

                            // Mark offset as processed
                            processedOffsets.put(tp, new OffsetAndMetadata(rec.offset() + 1));
                        } catch (Exception ex) {
                            log.error("Processing failed for {}-{}@{}; leaving offset uncommitted.",
                                    rec.topic(), rec.partition(), rec.offset(), ex);
                            // stop this batch; commit only successes so far
                            break;
                        }
                    }

                    if (!processedOffsets.isEmpty()) {
                        try {
                            consumer.commitSync(processedOffsets);
                            log.debug("Committed offsets: {}", processedOffsets);
                        } catch (Exception e) {
                            log.warn("Commit sync failed", e);
                        }
                    }

                } catch (WakeupException we) {
                    if (!running.get()) break; // shutdown path
                } catch (Exception e) {
                    log.error("Unexpected error in poll loop (worker={})", clientIdSuffix, e);
                }
            }

            // finalization
            try { consumer.commitSync(); } catch (Exception e) { log.warn("Final commit failed", e); }
            try { dbs.close(); } catch (Exception e) { log.warn("DB manager close failed", e); }
            log.info("Worker {} closed.", clientIdSuffix);

        } finally {
            this.consumerRef = null;
        }
    }
}
