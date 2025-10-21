package com.hcltech.rmg.common.async;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Array-backed, power-of-two circular lane.
 * Single-threaded: operator thread only.
 */
public final class Lane<T> implements ILane<T>, ILaneTestHooks<T> {

    private final int depth;
    private final int mask;

    private int headIdx;  // next slot to read
    private int tailIdx;  // next slot to write
    private int count;    // 0..depth

    private final String[] corrId;
    private final long[] startedAtNanos;
    private final T[] values;

    @SuppressWarnings("unchecked")
    public Lane(int depth) {
        if (depth <= 0) throw new IllegalArgumentException("depth must be > 0");
        if (Integer.bitCount(depth) != 1) throw new IllegalArgumentException("depth must be power of two");
        this.depth = depth;
        this.mask = depth - 1;
        this.corrId = new String[depth];
        this.startedAtNanos = new long[depth];
        this.values = (T[]) new Object[depth];
    }

    // ------------------------------------------------------------------

    @Override
    public void enqueue(T t, String correlationId, long startedAtNanos) {
        if (isFull()) throw new IllegalStateException("lane full");
        int i = tailIdx;
        values[i] = Objects.requireNonNull(t);
        corrId[i] = correlationId;
        this.startedAtNanos[i] = startedAtNanos;
        tailIdx = (tailIdx + 1) & mask;
        count++;
    }

    @Override
    public boolean popHead(Consumer<T> consumer) {
        if (isEmpty()) return false;
        int i = headIdx;
        clearSlot(i);
        return true;
    }

    @Override
    public <C> boolean popHead(C ctx, BiConsumer<T, C> consumer) {
        if (isEmpty()) return false;
        int i = headIdx;
        consumer.accept(values[i], ctx);
        clearSlot(i);
        return true;
    }

    private void clearSlot(int i) {
        values[i] = null;
        headIdx = (headIdx + 1) & mask;
        count--;
    }

    // ------------------------------------------------------------------
    @Override
    public T headT() {
        return values[headIdx];
    }

    @Override
    public String headCorrId() {
        return corrId[headIdx];
    }

    @Override
    public long headStartedAtNanos() {
        return startedAtNanos[headIdx];
    }

    @Override
    public boolean isEmpty() {
        return count == 0;
    }

    @Override
    public boolean isFull() {
        return count == depth;
    }

    // ---- test hooks ----
    @Override
    public int _headIdxForTest() {
        return headIdx;
    }

    @Override
    public int _tailIdxForTest() {
        return tailIdx;
    }

    @Override
    public int _countForTest() {
        return count;
    }

    @Override
    public int _maskForTest() {
        return mask;
    }

    @Override
    public int _depthForTest() {
        return depth;
    }

    @Override
    public boolean _containsForTest(T t) {
        for (T v : values) if (v == t) return true;
        return false;
    }

    @Override
    public void _setIdxForTest(int headIdx, int tailIdx, int count) {
        if (count < 0 || count > depth)
            throw new IllegalArgumentException("count out of range 0.." + depth);
        this.headIdx = headIdx & mask;
        this.tailIdx = tailIdx & mask;
        this.count = count;
    }
}
