package com.hcltech.rmg.common.async;

import java.util.concurrent.Semaphore;

/**
 * Semaphore-backed implementation of {@link PermitManager}.
 * - Non-blocking acquisition (tryAcquire) to keep the operator cooperative.
 * - Fair semaphore optional (FIFO) to reduce starvation under heavy contention.
 */
public final class SemaphorePermitManager implements PermitManager {
    private final Semaphore sem;
    private final int max;

    /**
     * @param maxInFlight maximum concurrent async launches allowed for this subtask (M). Must be > 0.
     * @param fair if true, uses a fair semaphore (slightly more overhead, better fairness under contention).
     */
    public SemaphorePermitManager(int maxInFlight, boolean fair) {
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("maxInFlight must be > 0");
        }
        this.sem = new Semaphore(maxInFlight, fair);
        this.max = maxInFlight;
    }

    /** Convenience: non-fair (higher throughput). */
    public SemaphorePermitManager(int maxInFlight) {
        this(maxInFlight, false);
    }

    @Override
    public boolean tryAcquire() {
        return sem.tryAcquire();
    }

    @Override
    public void release() {
        sem.release();
    }

    @Override
    public int availablePermits() {
        return sem.availablePermits();
    }

    @Override
    public int maxPermits() {
        return max;
    }
}
