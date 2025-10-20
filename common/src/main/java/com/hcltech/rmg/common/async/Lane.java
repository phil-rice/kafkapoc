package com.hcltech.rmg.common.async;

import com.hcltech.rmg.common.function.TriConsumer;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Array-backed, power-of-two circular lane.
 * Single-threaded: operator thread only.
 */
public final class Lane<FR,T> implements ILane<FR,T>, ILaneTestHooks<T> {

    private final int depth;
    private final int mask;

    private int headIdx;  // next slot to read
    private int tailIdx;  // next slot to write
    private int count;    // 0..depth

    private final long[] corrId;
    private final long[] startedAtNanos;
    private final T[]    values;
    private final FR[]   futures;

    @SuppressWarnings("unchecked")
    public Lane(int depth) {
        if (depth <= 0) throw new IllegalArgumentException("depth must be > 0");
        if (Integer.bitCount(depth) != 1) throw new IllegalArgumentException("depth must be power of two");
        this.depth = depth;
        this.mask  = depth - 1;
        this.corrId         = new long[depth];
        this.startedAtNanos = new long[depth];
        this.values         = (T[])  new Object[depth];
        this.futures        = (FR[]) new Object[depth];
    }

    // ------------------------------------------------------------------

    @Override
    public void enqueue(T t, FR fr, long correlationId, long startedAtNanos) {
        if (isFull()) throw new IllegalStateException("lane full");
        int i = tailIdx;
        values[i]         = Objects.requireNonNull(t);
        futures[i]        = fr;
        corrId[i]         = correlationId;
        this.startedAtNanos[i] = startedAtNanos;
        tailIdx = (tailIdx + 1) & mask;
        count++;
    }

    @Override
    public boolean popHead(BiConsumer<FR,T> consumer) {
        if (isEmpty()) return false;
        int i = headIdx;
        consumer.accept(futures[i], values[i]);
        clearSlot(i);
        return true;
    }

    @Override
    public <C> boolean popHead(C ctx, TriConsumer<FR,T,C> consumer) {
        if (isEmpty()) return false;
        int i = headIdx;
        consumer.accept(futures[i], values[i], ctx);
        clearSlot(i);
        return true;
    }

    private void clearSlot(int i) {
        values[i] = null;
        futures[i] = null;
        headIdx = (headIdx + 1) & mask;
        count--;
    }

    // ------------------------------------------------------------------
    @Override public T headT()             { return values[headIdx]; }
    @Override public FR headFR()           { return futures[headIdx]; }
    @Override public long headCorrId()     { return corrId[headIdx]; }
    @Override public long headStartedAtNanos() { return startedAtNanos[headIdx]; }
    @Override public boolean isEmpty()     { return count == 0; }
    @Override public boolean isFull()      { return count == depth; }

    // ---- test hooks ----
    @Override public int  _headIdxForTest() { return headIdx; }
    @Override public int  _tailIdxForTest() { return tailIdx; }
    @Override public int  _countForTest()   { return count; }
    @Override public int  _maskForTest()    { return mask; }
    @Override public int  _depthForTest()   { return depth; }

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
        this.count   = count;
    }
}
