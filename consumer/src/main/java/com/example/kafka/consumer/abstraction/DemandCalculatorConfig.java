package com.example.kafka.consumer.abstraction;

/** Immutable knobs for demand calculation. */
public record DemandCalculatorConfig(
        int perPollPerPartition,
        int maxOutstandingPerPartition,
        int globalMax // <=0 means "no global cap"
) {
    public DemandCalculatorConfig {
        if (perPollPerPartition < 1) throw new IllegalArgumentException("perPollPerPartition >= 1");
        if (maxOutstandingPerPartition < 1) throw new IllegalArgumentException("maxOutstandingPerPartition >= 1");
    }
}
