package com.hcltech.rmg.consumer.processors;

import com.hcltech.rmg.consumer.abstraction.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public final class SimpleMessageProcessor implements MessageProcessor<ConsumerRecord<String, String>, TopicPartition> {


    @Override
    public void process(TopicPartition shard, ConsumerRecord<String, String> message) throws Exception {

    }
}
