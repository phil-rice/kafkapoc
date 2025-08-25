package com.example.kafka.consumer.processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class SimpleRecordProcessor implements RecordProcessor {
    private static final Logger log = LoggerFactory.getLogger(SimpleRecordProcessor.class);
    private static AtomicLong counter = new AtomicLong(0);

    @Override
    public void process(ConsumerRecord<String, String> record) {
        long countValue = counter.getAndIncrement();
//        if (countValue % 10000 == 0) {
//            log.info("Consumed record Count={} topic={} partition={} offset={} key={} value={}. ",
//                    countValue,
//                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
//        }
        // Put your real business logic here.
    }
}
