package com.hcltech.rmg.consumer.abstraction;

/**
 * Immutable snapshot of one shard's runner intake state.
 * These values are sampled just before demand is computed.
 */
public interface RunnerState {
    /**
     * How many additional messages this runner can accept RIGHT NOW
     * into its internal queue without blocking.
     * - Non-negative; excludes in-flight work.
     */
    int remainingCapacity();

    /**
     * Messages accepted but not fully completed (queued + in-flight).
     * - Non-negative; 0 means the shard is idle.
     */
    int outstanding();
}
