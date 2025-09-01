package com.hcltech.rmg.kafkaconsumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Small cache to avoid constructing TopicPartition per record.
 * Built from the current assigned set; if assignments change, we rebuild.
 */
final class TopicPartitionCache {
    private final Map<String, TopicPartition[]> map;

    private TopicPartitionCache(Map<String, TopicPartition[]> map) {
        this.map = map;
    }

    static TopicPartitionCache empty() {
        return new TopicPartitionCache(Map.of());
    }

    static TopicPartitionCache fromAssigned(Collection<TopicPartition> assigned) {
        if (assigned == null || assigned.isEmpty()) return empty();

        // Find max partition index per topic
        Map<String, Integer> max = new HashMap<>();
        for (TopicPartition tp : assigned) {
            max.merge(tp.topic(), tp.partition(), Math::max);
        }
        // Build arrays of TPs per topic
        Map<String, TopicPartition[]> built = new HashMap<>();
        for (var e : max.entrySet()) {
            String topic = e.getKey();
            int size = e.getValue() + 1;
            TopicPartition[] arr = new TopicPartition[size];
            for (int p = 0; p < size; p++) arr[p] = new TopicPartition(topic, p);
            built.put(topic, arr);
        }
        return new TopicPartitionCache(built);
    }

    TopicPartition get(String topic, int partition) {
        TopicPartition[] arr = map.get(topic);
        if (arr != null && partition >= 0 && partition < arr.length) {
            return arr[partition];
        }
        // Fallback (should be rare if cache built from assigned)
        return new TopicPartition(topic, partition);
    }
}
