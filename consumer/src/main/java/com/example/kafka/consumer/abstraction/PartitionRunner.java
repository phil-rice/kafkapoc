package com.example.kafka.consumer.abstraction;

public interface PartitionRunner<M, S, P> {
    boolean isIdle();                    // true => we can accept 1 more now
    boolean tryStart(M message, P next); // starts processing immediately; false if busy
    P commitReadyNext();                 // next position ready to commit; null if none
    void stopAndDrain(long timeoutMs);   // stop runner and wait for in-flight to finish
    int remainingCapacity();
}
