package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.AssignmentListener;
import com.example.kafka.consumer.abstraction.Demand;
import com.example.kafka.consumer.abstraction.FlowControlledStream;
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
        // A) Pre-drain existing buffers against the requested allowance
        var out = buffers.drain(assignment.assigned(), demand);
        int drained = 0;
        for (var __ : out) drained++; // count quickly

        // If we already filled the global allowance, just heartbeat and return what we have
        int globalLeft = demand.globalMax() > 0 ? Math.max(0, demand.globalMax() - drained) : Integer.MAX_VALUE;

        // Build a reduced allowance map for what's left
        var remaining = new HashMap<TopicPartition, Integer>();
        if (globalLeft > 0) {
            for (var tp : assignment.assigned()) {
                int allow = demand.allowance(tp);
                if (allow > 0) remaining.put(tp, allow); // (we purposely don't decrement per-TP; buffers.drain already respected per-TP)
            }
        }

        // B) Apply pause/resume based on remaining allowance
        pauser.apply(consumer, assignment.assigned(), Demand.of(remaining, globalLeft));

        // C) Poll only if we still want more; otherwise heartbeat with zero timeout
        ConsumerRecords<String, String> polled = consumer.poll(globalLeft > 0 ? timeout : Duration.ZERO);
        if (!polled.isEmpty()) buffers.add(polled, assignment.tpCache());

        // D) Final drain to satisfy any leftover allowance
        if (globalLeft > 0) {
            var more = buffers.drain(assignment.assigned(), Demand.of(remaining, globalLeft));
            if (more.iterator().hasNext()) {
                var combined = new ArrayList<ConsumerRecord<String, String>>();
                more.forEach(combined::add);
                out.forEach(combined::add);
                return combined;
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
