package com.example.kafka.consumer.processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface RecordProcessor {
    void process(ConsumerRecord<String, String> record) throws Exception;
}
