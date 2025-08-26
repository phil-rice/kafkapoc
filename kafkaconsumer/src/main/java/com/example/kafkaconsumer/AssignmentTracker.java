package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.AssignmentListener;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/** Tracks assigned partitions, maintains TP cache, and bridges callbacks. */
final class AssignmentTracker {
    private final Set<TopicPartition> assigned = new LinkedHashSet<>(); // deterministic order
    private TopicPartitionCache tpCache = TopicPartitionCache.empty();
    private final PartitionBufferStore buffers;

    AssignmentTracker(PartitionBufferStore buffers) {
        this.buffers = Objects.requireNonNull(buffers, "buffers");
    }

    Set<TopicPartition> assigned() { return assigned; }
    TopicPartitionCache tpCache()  { return tpCache; }

    void clear() {
        assigned.clear();
        tpCache = TopicPartitionCache.empty();
    }

    ConsumerRebalanceListener asRebalanceListener(
            KafkaConsumer<String, String> consumer,
            AssignmentListener<TopicPartition> listener
    ) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(listener, "listener");

        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                listener.onRevoked(new HashSet<>(partitions));
                // cleanup
                assigned.removeAll(partitions);
                buffers.onRevoked(partitions);
                tpCache = TopicPartitionCache.fromAssigned(assigned);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                assigned.addAll(partitions);               // track
                buffers.onAssigned(partitions);            // ensure buffers
                tpCache = TopicPartitionCache.fromAssigned(assigned); // rebuild cache
                listener.onAssigned(new HashSet<>(partitions));       // notify last
            }
        };
    }
}
