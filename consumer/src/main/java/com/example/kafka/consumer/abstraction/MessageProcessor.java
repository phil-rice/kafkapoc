package com.example.kafka.consumer.abstraction;

/** Your business logic. Run outside the consumer loop. */
public interface MessageProcessor<M, S> {
    void process(S shard, M message) throws Exception;
}
