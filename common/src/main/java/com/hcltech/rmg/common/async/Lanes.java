package com.hcltech.rmg.common.async;

import java.util.Objects;

/**
 * Array-backed lanes holder; power-of-two laneCount and laneDepth.
 * Uses a Correlator to route items: index = correlator.laneHash(t) & (laneCount - 1).
 * Single-threaded usage only (operator thread).
 */
public final class Lanes<FR, T> implements ILanes<FR, T>, ILanesTestHooks<FR, T> {

    private final Correlator<T> correlator;
    private final ILane<FR, T>[] lanes;
    private final int laneCount;
    private final int laneMask;
    private final int laneDepth;

    @SuppressWarnings("unchecked")
    Lanes(int laneCount, int laneDepth, Correlator<T> correlator) {
        if (laneCount <= 0) throw new IllegalArgumentException("laneCount must be > 0");
        if (laneDepth <= 0) throw new IllegalArgumentException("laneDepth must be > 0");
        if (Integer.bitCount(laneCount) != 1) throw new IllegalArgumentException("laneCount must be a power of two");
        if (Integer.bitCount(laneDepth) != 1) throw new IllegalArgumentException("laneDepth must be a power of two");
        this.correlator = Objects.requireNonNull(correlator, "correlator");

        this.laneCount = laneCount;
        this.laneMask = laneCount - 1;
        this.laneDepth = laneDepth;

        ILane<FR, T>[] arr = (ILane<FR, T>[]) new ILane[laneCount];
        for (int i = 0; i < laneCount; i++) {
            arr[i] = new Lane<>(laneDepth);
        }
        this.lanes = arr;
    }

    // ------------ ILanes (prod) ------------

    @Override
    public ILane<FR, T> lane(T t) {
        int idx = correlator.laneHash(t) & laneMask;
        return lanes[idx];
    }

    // ------------ ILanesTestHooks (tests) ------------

    @Override
    public ILane<FR, T> _laneAt(int index) {
        return lanes[index];
    }

    @Override
    public int _laneMask() {
        return laneMask;
    }

    @Override
    public int _laneDepth() {
        return laneDepth;
    }

    @Override
    public int _laneCount() {
        return laneCount;
    }
}
