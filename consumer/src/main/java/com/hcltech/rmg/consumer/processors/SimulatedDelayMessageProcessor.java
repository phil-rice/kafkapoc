package com.hcltech.rmg.consumer.processors;

import com.hcltech.rmg.consumer.abstraction.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * A MessageProcessor that simulates work by sleeping for a fixed duration.
 *
 * @param <M> message type (here we assume Kafka ConsumerRecord)
 * @param <S> shard type (Kafka TopicPartition)
 */
public final class SimulatedDelayMessageProcessor
        implements MessageProcessor<ConsumerRecord<String,String>, TopicPartition> {

    private final long delayMs;

    /**
     * @param delayMs how long to sleep per record (ms).
     */
    public SimulatedDelayMessageProcessor(long delayMs) {
        this.delayMs = Math.max(0, delayMs);
    }

    @Override
    public void process(TopicPartition shard, ConsumerRecord<String,String> message) throws Exception {
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted during simulated processing", ie);
            }
        }
        // simulate "work done"; you could extend this with logging or metrics
    }
}
