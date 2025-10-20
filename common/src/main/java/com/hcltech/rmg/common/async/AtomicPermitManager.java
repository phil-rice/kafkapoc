package com.hcltech.rmg.common.async;

public final class AtomicPermitManager implements PermitManager {
    private final int max;
    private final java.util.concurrent.atomic.AtomicInteger avail;

    public AtomicPermitManager(int maxInFlight) {
        if (maxInFlight <= 0) throw new IllegalArgumentException("maxInFlight must be > 0");
        this.max = maxInFlight;
        this.avail = new java.util.concurrent.atomic.AtomicInteger(maxInFlight);
    }

    @Override
    public boolean tryAcquire() {
        int cur;
        do {
            cur = avail.get();
            if (cur == 0) return false;
        } while (!avail.compareAndSet(cur, cur - 1));
        return true;
    }

    @Override
    public void release() {
        int cur;
        do {
            cur = avail.get();
            // Hard guard: never exceed max (catches double-release bugs)
            if (cur == max) {
                // In prod you might log+return; during bring-up, throw to surface wiring mistakes fast.
                throw new IllegalStateException("PermitManager.release over max (" + max + ")");
            }
        } while (!avail.compareAndSet(cur, cur + 1));
    }

    @Override
    public int availablePermits() { return avail.get(); }

    @Override
    public int maxPermits() { return max; }
}
