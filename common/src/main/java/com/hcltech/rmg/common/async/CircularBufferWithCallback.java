package com.hcltech.rmg.common.async;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A lightweight, array-backed circular buffer that stores out-of-order async results for a single logical key.
 * When consecutive results (by sequence) become available, the buffer drains them in order through a synchronous callback.
 * <p>
 * Single-threaded API: all calls (put, drain, reset) must be made from the same thread.
 *
 * <p><b>Flink-oriented variant:</b> This version removes epochs. Instead, a simple lifecycle flag
 * <code>finished</code> ignores late completions after the operator/subtask has been closed/cancelled.
 *
 * @param <T> value type stored in the buffer. Non-null when present.
 */
public final class CircularBufferWithCallback<T> {

    /** Result of a put attempt. */
    public static enum PutResult {
        /** Added successfully; drain may have been triggered. */
        ACCEPTED,
        /** Slot range exceeded relative to current window; caller should pause/backpressure. */
        REJECTED_WINDOW_FULL,
        /** Sequence is less than baseSeq; already committed and drained earlier. */
        ALREADY_COMMITTED_DROPPED,
        /** Buffer has been finished; late result ignored. */
        DROPPED_AFTER_FINISH
    }

    private final int capacity;
    private final int mask; // capacity - 1 (power-of-two invariant)
    private long baseSeq;   // next expected sequence number (unsigned semantics)
    private int head;       // array index corresponding to baseSeq
    private int size;       // number of occupied slots

    private boolean finished; // ignore late results once true

    private final T[] results; // nullable entries; null means empty

    private final Consumer<? super T> onSuccess;
    private final BiConsumer<? super T, ? super Throwable> onFailure; // may be null

    /**
     * Create a buffer.
     *
     * @param capacity  fixed window size; must be a power of two and > 0 (recommended small, e.g., <= 1024)
     * @param onSuccess synchronous callback invoked for each drained entry
     * @param onFailure error callback if onSuccess throws; may be null. Item is considered consumed either way.
     */
    @SuppressWarnings("unchecked")
    public CircularBufferWithCallback(int capacity,
                                      Consumer<? super T> onSuccess,
                                      BiConsumer<? super T, ? super Throwable> onFailure) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("capacity must be a power of two");
        }
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.onSuccess = Objects.requireNonNull(onSuccess, "onSuccess");
        this.onFailure = onFailure; // nullable allowed

        this.results = (T[]) new Object[capacity];

        this.baseSeq = 0L;
        this.head = 0;
        this.size = 0;
        this.finished = false;
    }

    /**
     * Quick preflight check before launching async work for a given sequence.
     * True iff the unsigned distance from {@code baseSeq} to {@code seq} is within the window
     * and the buffer is not finished.
     */
    public boolean canAdd(long seq) {
        if (finished) return false;
        if (Long.compareUnsigned(seq, baseSeq) < 0) return false;
        // Use 64-bit unsigned distance; only cast after proving it fits within capacity
        long d = seq - baseSeq;
        return Long.compareUnsigned(d, Integer.toUnsignedLong(capacity)) < 0;
    }

    /** Convenience: check if launching the next sequential seq would be allowed. */
    public boolean canLaunchNext(long nextSeq) { return canAdd(nextSeq); }

    public PutResult put(long seq, T value) {
        if (finished) return PutResult.DROPPED_AFTER_FINISH;

        // Unsigned in-window check in 64-bit space (handles wraparound correctly)
        long d = seq - baseSeq; // unsigned distance
        if (Long.compareUnsigned(d, Integer.toUnsignedLong(capacity)) >= 0) {
            // Not in window; decide whether it's already committed or ahead-of-window
            return (Long.compareUnsigned(seq, baseSeq) < 0)
                    ? PutResult.ALREADY_COMMITTED_DROPPED
                    : PutResult.REJECTED_WINDOW_FULL;
        }

        int off = (int) d; // safe now: 0 <= d < capacity
        int idx = (head + off) & mask;

        if (results[idx] != null) {
            throw new IllegalStateException("Duplicate insert for seq=" + Long.toUnsignedString(seq) +
                    " (slot already occupied)");
        }

        results[idx] = Objects.requireNonNull(value, "value");
        size++;

        drain();
        return PutResult.ACCEPTED;
    }

    /** Sequentially apply ready results starting from baseSeq. Stops at first gap. */
    public void drain() {
        while (results[head] != null) {
            T v = results[head];
            // Clear before callback to keep state consistent even if callback throws
            results[head] = null;
            size--;

            try {
                onSuccess.accept(v);
            } catch (Throwable t) {
                if (onFailure != null) {
                    try {
                        onFailure.accept(v, t);
                    } catch (Throwable ignore) {
                        /* swallow to keep draining */
                    }
                }
                // Swallow to continue draining; item is considered consumed
            }

            head = (head + 1) & mask;
            baseSeq = baseSeq + 1; // unsigned increment is same as signed add
        }
    }

    /** Returns true when the buffer has no free slots. */
    public boolean isFull() { return size == capacity; }

    /**
     * Mark the buffer as finished. Subsequent {@link #put(long, Object)} calls are dropped.
     * Use in Flink operator's close/cancel to ignore late completions.
     */
    public void finish() { this.finished = true; }

    /**
     * Clears all data and restarts the buffer (finished=false). Useful for tests or reuse.
     * Not typically needed in Flink since a new operator instance is created on recovery.
     */
    public void reset() {
        Arrays.fill(results, null);
        size = 0;
        head = 0;
        baseSeq = 0L;
        finished = false;
    }

    /** Next expected (base) sequence number (unsigned semantics). Useful for tests/metrics. */
    public long baseSeq() { return baseSeq; }

    /** Current number of occupied slots. */
    public int size() { return size; }

    /** Fixed capacity of the ring. */
    public int capacity() { return capacity; }

    /** Current head index (primarily for tests). */
    public int head() { return head; }

    /** Whether the buffer has been finished. */
    public boolean isFinished() { return finished; }

    // ------------------------------------------------------------
    // Unsigned clock arithmetic utilities (package-private for testing)
    // ------------------------------------------------------------
    static final class Clock64 {
        private Clock64() {}
        /** true if a < b in unsigned 64-bit ordering */
        static boolean isBefore(long a, long b) { return Long.compareUnsigned(a, b) < 0; }
        /** addUnsigned(base, 1) == base + 1 semantics */
        static long inc(long x) { return x + 1; }
    }
}
