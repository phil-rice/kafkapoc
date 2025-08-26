package com.example.kafka.consumer.abstraction;

/**
 * Optional processor capability that hints an upper bound for per-shard outstanding
 * (queued + in-flight). The worker/demand calculator will clamp configured limits
 * with this hint if present.
 */
public interface ProcessorProfile<S> {

    /**
     * @param configuredMax the configured maxOutstandingPerPartition
     * @return a value in [1 .. configuredMax] expressing a soft ceiling for this processor.
     *         Default implementations should return configuredMax (no extra limit).
     */
    default int maxOutstandingHint(S shard, int configuredMax) {
        return configuredMax;
    }
}
