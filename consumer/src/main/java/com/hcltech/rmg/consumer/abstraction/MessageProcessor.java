package com.hcltech.rmg.consumer.abstraction;

/** Your business logic. Run outside the consumer loop. */
public interface MessageProcessor<M, S> {
    void process(S shard, M message) throws Exception;
}
