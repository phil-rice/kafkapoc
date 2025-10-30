package com.hcltech.rmg.common.async;

/**
 * Subtask-wide throttle (M). Non-blocking acquire before launching async work,
 * and release after the ordered commit/drain of the corresponding result.
 */
public interface PermitManager {
    /** Try to acquire one permit without blocking. Returns true on success. */
    boolean tryAcquire();

    /** Release one previously acquired permit. */
    void release();

    /** Current number of available permits (for metrics/observability). */
    int availablePermits();

    /** Maximum number of permits configured (for metrics/observability). */
    int maxPermits();
}
