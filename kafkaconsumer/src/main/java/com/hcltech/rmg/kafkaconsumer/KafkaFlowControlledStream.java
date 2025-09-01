package com.hcltech.rmg.kafkaconsumer;

import com.hcltech.rmg.consumer.abstraction.AssignmentListener;
import com.hcltech.rmg.consumer.abstraction.Demand;
import com.hcltech.rmg.consumer.abstraction.FlowControlledStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Orchestrator: subscribe, poll with Demand, commit.
 * Delegates assignment, pause/resume, and buffering to helpers.
 *
 * SINGLE-THREADED: call subscribe/poll/commit/close from the same thread.
 */
public final class KafkaFlowControlledStream
        implements FlowControlledStream<ConsumerRecord<String, String>, TopicPartition, Long> {

    private final KafkaConsumer<String, String> consumer;
    private final AssignmentTracker assignment;
    private final PauseResumeApplier pauser;
    private final PartitionBufferStore buffers;

    public KafkaFlowControlledStream(KafkaConsumer<String, String> consumer) {
        this(consumer, 1024);
    }

    public KafkaFlowControlledStream(KafkaConsumer<String, String> consumer, int maxBufferPerPartition) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.buffers = new PartitionBufferStore(maxBufferPerPartition);
        this.assignment = new AssignmentTracker(buffers);
        this.pauser = new PauseResumeApplier();
    }

    @Override
    public void subscribe(String topic, AssignmentListener<TopicPartition> assignmentListener) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(assignmentListener, "assignmentListener");
        consumer.subscribe(List.of(topic), assignment.asRebalanceListener(consumer, assignmentListener));
    }
    @Override
    public Iterable<ConsumerRecord<String, String>> poll(Duration timeout, Demand<TopicPartition> demand) {
        Objects.requireNonNull(demand, "demand");

        // --- 1) Pre-drain existing buffers against the requested allowance ---
        // Collect to a list so we can count how many we drained.
        final List<ConsumerRecord<String, String>> out =
                new ArrayList<>();
        for (ConsumerRecord<String, String> rec : buffers.drain(assignment.assigned(), demand)) {
            out.add(rec);
        }
        final int drained = out.size();

        // Compute remaining global allowance (if any)
        final int globalLeft = demand.globalMax() > 0
                ? Math.max(0, demand.globalMax() - drained)
                : Integer.MAX_VALUE;

        // Build a per-TP "remaining" view for pause/resume (allowance > 0 = resume; 0 = pause)
        final Map<TopicPartition, Integer> remaining = new HashMap<>();
        if (globalLeft > 0) {
            for (TopicPartition tp : assignment.assigned()) {
                final int allow = demand.allowance(tp);
                if (allow > 0) remaining.put(tp, allow);
            }
        }

        // --- 2) Apply pause/resume based on remaining allowance BEFORE polling ---
        pauser.apply(consumer, assignment.assigned(), Demand.of(remaining, globalLeft));

        // --- 3) Poll: if nothing left to fetch, heartbeat only (Duration.ZERO) ---
        final boolean wantMore = globalLeft > 0;
        final ConsumerRecords<String, String> polled =
                consumer.poll(wantMore ? timeout : Duration.ZERO);

        // Stash any newly polled records into per-partition buffers
        if (!polled.isEmpty()) {
            buffers.add(polled, assignment.tpCache());
        }

        // --- 4) Final drain to satisfy any leftover allowance ---
        if (wantMore) {
            for (ConsumerRecord<String, String> rec :
                    buffers.drain(assignment.assigned(), Demand.of(remaining, globalLeft))) {
                out.add(rec);
            }
        }

        return out;
    }


    @Override
    public void commit(Map<TopicPartition, Long> positions) {
        if (positions == null || positions.isEmpty()) return;
        Map<TopicPartition, OffsetAndMetadata> map = positions.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
        consumer.commitSync(map);
    }

    @Override
    public boolean supportsPreciseShardFlowControl() {
        return true; // Kafka supports per-partition pause/resume
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } finally {
            assignment.clear();
            pauser.clear();
            buffers.clear();
        }
    }
}
