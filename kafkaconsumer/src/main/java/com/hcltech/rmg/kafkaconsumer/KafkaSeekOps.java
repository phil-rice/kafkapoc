// com.example.kafkaconsumer.KafkaSeekOps
package com.hcltech.rmg.kafkaconsumer;

import com.hcltech.rmg.consumer.abstraction.SeekOps;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class KafkaSeekOps implements SeekOps<TopicPartition, Long>, KafkaOffsetIntrospector {
    private final KafkaConsumer<String, String> consumer;

    public KafkaSeekOps(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void seekToBeginning(Set<TopicPartition> shards) {
        consumer.seekToBeginning(shards);
    }

    // --- KafkaOffsetIntrospector impl ---
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Set<TopicPartition> shards) {
        return consumer.beginningOffsets(shards);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Set<TopicPartition> shards) {
        return consumer.endOffsets(shards);
    }

    @Override
    public <T> Optional<T> capability(Class<T> type) {
        if (type.isAssignableFrom(KafkaOffsetIntrospector.class)) {
            return Optional.of(type.cast(this));
        }
        return Optional.empty();
    }
}
