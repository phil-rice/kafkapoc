package com.example.kafka.consumer.abstraction;

import java.util.Optional;
import java.util.Set;

/**
 * Abstraction over broker seek operations.
 *
 * @param <S> shard type (e.g., TopicPartition for Kafka)
 * @param <P> position type (e.g., offset for Kafka)
 */
public interface SeekOps<S,P> {
    /** Reset position to the beginning for given shards. */
    void seekToBeginning(Set<S> shards);

    default <T> Optional<T> capability(Class<T> type) {
        return Optional.empty();
    }
}
