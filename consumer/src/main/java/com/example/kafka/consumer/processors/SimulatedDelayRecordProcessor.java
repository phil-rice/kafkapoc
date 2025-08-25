package com.example.kafka.consumer.processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimulatedDelayRecordProcessor implements RecordProcessor {
    private final long delayMs;

    public SimulatedDelayRecordProcessor(long delayMs) {
        this.delayMs = Math.max(0, delayMs);
    }

    @Override
    public void process(ConsumerRecord<String, String> record) {
        if (delayMs <= 0) return;
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during simulated processing", ie);
        }
        // put real work here later if you like
    }
}
