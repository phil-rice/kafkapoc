package com.example.kafka.consumer.abstraction;

public interface PartitionRunner<M,S,P> {
    boolean isIdle();                 // keep for convenience
    int outstanding();                // NEW: queued + in-flight
    boolean tryStart(M message, P next);
    P commitReadyNext();
    void stopAndDrain(long timeoutMs);
    int remainingCapacity() ;
}
