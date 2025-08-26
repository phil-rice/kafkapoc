package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.Demand;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/** Per-partition ring buffers. Adds polled records; drains by Demand. */
final class PartitionBufferStore {
    private final Map<TopicPartition, Deque<ConsumerRecord<String, String>>> buffers = new HashMap<>();
    private final int maxBufferPerPartition;

    PartitionBufferStore(int maxBufferPerPartition) {
        this.maxBufferPerPartition = Math.max(1, maxBufferPerPartition);
    }

    void onAssigned(Collection<TopicPartition> parts) {
        for (TopicPartition tp : parts) {
            buffers.computeIfAbsent(tp, __ -> new ArrayDeque<>());
        }
    }

    void onRevoked(Collection<TopicPartition> parts) {
        for (TopicPartition tp : parts) {
            buffers.remove(tp);
        }
    }

    void add(ConsumerRecords<String, String> polled, TopicPartitionCache cache) {
        if (polled == null || polled.isEmpty()) return;
        for (ConsumerRecord<String, String> r : polled) {
            TopicPartition tp = cache.get(r.topic(), r.partition());
            Deque<ConsumerRecord<String, String>> q =
                    buffers.computeIfAbsent(tp, __ -> new ArrayDeque<>());
            if (q.size() < maxBufferPerPartition) {
                q.addLast(r);
            } else {
                // Cap memory: drop oldest then add
                q.pollFirst();
                q.addLast(r);
            }
        }
    }

    Iterable<ConsumerRecord<String, String>> drain(Collection<TopicPartition> assigned, Demand<TopicPartition> demand) {
        if (assigned == null || assigned.isEmpty()) return List.of();

        int remainingGlobal = demand.globalMax() > 0 ? demand.globalMax() : Integer.MAX_VALUE;
        if (remainingGlobal <= 0) return List.of();

        List<ConsumerRecord<String, String>> out = new ArrayList<>();

        for (TopicPartition tp : assigned) {
            if (remainingGlobal <= 0) break;
            int allow = demand.allowance(tp);
            if (allow <= 0) continue;

            Deque<ConsumerRecord<String, String>> q = buffers.get(tp);
            if (q == null || q.isEmpty()) continue;

            int toTake = Math.min(allow, remainingGlobal);
            for (int i = 0; i < toTake && !q.isEmpty(); i++) {
                out.add(q.pollFirst());
            }
            remainingGlobal -= toTake;
        }
        return out;
    }

    void clear() { buffers.clear(); }
}
