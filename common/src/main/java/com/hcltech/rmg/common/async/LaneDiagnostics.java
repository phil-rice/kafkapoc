package com.hcltech.rmg.common.async;

/** Immutable snapshot of lane health for diagnostics. */
public final class LaneDiagnostics {
    public final int laneCount;
    public final int empty;
    public final int full;
    public final int inUse;              // neither empty nor full
    public final int headsPastTimeout;   // head started >= timeoutNanos ago
    public final long nowNanos;
    public final long timeoutNanos;

    public LaneDiagnostics(int laneCount, int empty, int full, int inUse,
                           int headsPastTimeout, long nowNanos, long timeoutNanos) {
        this.laneCount = laneCount;
        this.empty = empty;
        this.full = full;
        this.inUse = inUse;
        this.headsPastTimeout = headsPastTimeout;
        this.nowNanos = nowNanos;
        this.timeoutNanos = timeoutNanos;
    }

    @Override public String toString() {
        return "LaneDiagnostics{" +
                "laneCount=" + laneCount +
                ", empty=" + empty +
                ", full=" + full +
                ", inUse=" + inUse +
                ", headsPastTimeout=" + headsPastTimeout +
                ", nowNanos=" + nowNanos +
                ", timeoutNanos=" + timeoutNanos +
                '}';
    }
}
