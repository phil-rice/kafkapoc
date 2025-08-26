package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.RecordAccess;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class KafkaRecordAccess  implements RecordAccess<ConsumerRecord<String, String>, TopicPartition, Long, String, String> {
    @Override
    public TopicPartition shardOf(ConsumerRecord<String, String> r) {
        return new TopicPartition(r.topic(), r.partition());
    }

    @Override
    public Long positionOf(ConsumerRecord<String, String> r) {
        return r.offset();
    }

    @Override
    public String keyOf(ConsumerRecord<String, String> r) {
        return r.key();
    }

    @Override
    public String valueOf(ConsumerRecord<String, String> r) {
        return r.value();
    }

    @Override
    public long timestampMsOf(ConsumerRecord<String, String> r) {
        return r.timestamp();
    }
}
