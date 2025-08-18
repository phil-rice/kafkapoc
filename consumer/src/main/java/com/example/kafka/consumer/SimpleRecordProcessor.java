package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleRecordProcessor implements RecordProcessor {
    private static final Logger log = LoggerFactory.getLogger(SimpleRecordProcessor.class);

    @Override
    public void process(ConsumerRecord<String, String> record) {
        log.info("Consumed record topic={} partition={} offset={} key={} value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        // Put your real business logic here.
    }
}
