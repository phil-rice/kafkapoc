package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.function.SemigroupTc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Consumer;

/**
 * Lane-local, reusable container for a single in-flight generation.
 * Probe support is optional and fully disabled in production.
 */
public class GenerationResult<Acc, Out, Comp> {

    private final int maxSlots;
    private final SemigroupTc<Acc, Out> accTc;

    private final Out[] outOrNullPerSlot;
    private final String[] failedComponentStr;
    private final Throwable[] failedCause;
    private final AtomicIntegerArray slotDone;

    private final AtomicInteger remaining = new AtomicInteger(0);
    private volatile int runEpoch;
    private int genIdx;
    private int size;
    private Acc accIn;
    private Consumer<Acc> onSuccess;
    private Consumer<Throwable> onFailure;

    // ---- probe support -------------------------------------------------------
    public interface SlotProbe<OutT, CompT> {
        default void onRecordSuccess(int epoch, int slot, OutT outOrNull) {
        }

        default void onRecordFailure(int epoch, int slot, CompT comp, Throwable error) {
        }
    }

    private final boolean useProbe;
    private SlotProbe<Out, Comp> probe;

    /**
     * Constructor for production use (probe disabled).
     */
    public GenerationResult(SemigroupTc<Acc, Out> accTc, int maxSlots, int expectedOutputsPerSlot) {
        this(accTc, maxSlots, expectedOutputsPerSlot, false);
    }

    /**
     * Constructor allowing probe usage (for tests).
     */
    public GenerationResult(SemigroupTc<Acc, Out> accTc, int maxSlots, int expectedOutputsPerSlot, boolean useProbe) {
        this.accTc = accTc;
        this.maxSlots = Math.max(1, maxSlots);
        this.useProbe = useProbe;
        this.outOrNullPerSlot = (Out[]) new Object[this.maxSlots];
        this.failedComponentStr = new String[this.maxSlots];
        this.failedCause = new Throwable[this.maxSlots];
        this.slotDone = new AtomicIntegerArray(this.maxSlots);

        int cap = Math.max(1, expectedOutputsPerSlot);
    }

    /**
     * Attach a probe only if constructed with useProbe=true.
     */
    public void setProbe(SlotProbe<Out, Comp> probe) {
        if (!useProbe) {
            throw new IllegalStateException("Probe use disabled for this GenerationResult");
        }
        this.probe = probe;
    }

    // --------------------------------------------------------------------------

    public void beginRun(int genIdx, int size, Acc initialAcc,
                         Consumer<Acc> onSuccess, Consumer<Throwable> onFailure) {
        if (size < 0 || size > maxSlots)
            throw new IllegalArgumentException("size=" + size + " exceeds maxSlots=" + maxSlots);
        this.genIdx = genIdx;
        this.size = size;
        this.accIn = initialAcc;
        this.onSuccess = onSuccess;
        this.onFailure = onFailure;

        for (int i = 0; i < size; i++) {
            slotDone.set(i, 0);
            failedComponentStr[i] = null;
            failedCause[i] = null;
            outOrNullPerSlot[i] = null;
        }
        this.runEpoch++;
        remaining.set(size);
    }

    public int currentEpoch() {
        return runEpoch;
    }

    // ============================ slot completion =============================

    public void recordSuccess(int epoch, int slot, Out outOrNull) {
        if (epoch != runEpoch) return;
        recordSuccessInternal(slot, outOrNull);
    }

    public void recordFailure(int epoch, int slot, Comp comp, Throwable error) {
        if (epoch != runEpoch) return;
        recordFailureInternal(slot, comp, error);
    }

    private void recordSuccessInternal(int slot, Out outOrNull) {
        if (slot >= size) return;
        if (slotDone.compareAndSet(slot, 0, 1)) {
            outOrNullPerSlot[slot] = outOrNull;
            if (useProbe && probe != null) {
                try {
                    probe.onRecordSuccess(runEpoch, slot, outOrNull);
                } catch (Throwable ignored) {
                }
            }
            if (remaining.decrementAndGet() == 0) complete();
        }
    }

    private void recordFailureInternal(int slot, Comp comp, Throwable error) {
        if (slot >= size) return;
        if (slotDone.compareAndSet(slot, 0, 1)) {
            failedComponentStr[slot] = (comp == null ? "null" : String.valueOf(comp));
            failedCause[slot] = error;
            if (useProbe && probe != null) {
                try {
                    probe.onRecordFailure(runEpoch, slot, comp, error);
                } catch (Throwable ignored) {
                }
            }
            if (remaining.decrementAndGet() == 0) complete();
        }
    }

    // ============================ sync helpers ================================

    Out slotBuffer(int slot) {
        if (slot < 0 || slot >= maxSlots)
            throw new IndexOutOfBoundsException("slot=" + slot + ", maxSlots=" + maxSlots);
        return outOrNullPerSlot[slot];
    }

    void successFromBuffer(int slot) {
        if (slot >= size) return;
        if (slotDone.compareAndSet(slot, 0, 1))
            if (remaining.decrementAndGet() == 0) complete();
    }

    // ============================ completion ==================================

    private void complete() {
        int failCount = 0;
        for (int i = 0; i < size; i++)
            if (failedComponentStr[i] != null) failCount++;

        if (failCount > 0) {
            ArrayList<EnrichmentBatchException.ErrorInfo> errs = new ArrayList<>(failCount);
            for (int i = 0; i < size; i++) {
                if (failedComponentStr[i] != null)
                    errs.add(new EnrichmentBatchException.ErrorInfo(i, failedComponentStr[i], failedCause[i]));
            }
            onFailure.accept(new EnrichmentBatchException(genIdx, errs));
            return;
        }

        Acc acc = accIn;
        for (int i = 0; i < size; i++) {
            Out outOrNull = outOrNullPerSlot[i];
            if (outOrNull != null) acc = accTc.add(acc, outOrNull);
        }
        onSuccess.accept(acc);
    }

    int generationIndex() {
        return genIdx;
    }

    int activeSize() {
        return size;
    }

    int maxSlots() {
        return maxSlots;
    }
}
