package com.example.kafka.consumer.processors;

@FunctionalInterface
public interface RecordProcessorFactory {
    RecordProcessor create();
}
