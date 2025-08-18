package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.rocksdb.RocksDB;

@FunctionalInterface
public interface PartitionedRecordProcessor {
    void process(ConsumerRecord<String, String> record, RocksDB db) throws Exception;
}
