package com.example.kafka.consumer.abstraction;

public interface RecordAccess<M,S,P,K,V> {
    S shardOf(M msg);       // native shard/partition id
    P positionOf(M msg);    // native position/offset/message-id
    K keyOf(M msg);
    V valueOf(M msg);
    long timestampMsOf(M msg);
}